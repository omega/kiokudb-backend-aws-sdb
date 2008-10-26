package KiokuDB::Backend::AWS::SDB;
# ABSTRACT: KiokuDB backend that persists and searches with Amazon SimpleDB

use Moose;

use Carp qw/croak/;

# TODO: Figure out what the hell sort of roles I should add here :p
with qw(

    KiokuDB::Backend

    KiokuDB::Backend::UnicodeSafe
    
    KiokuDB::Backend::Serialize::JSPON

    KiokuDB::Backend::Query
    KiokuDB::Backend::Query::Simple
);

has 'domain' => (isa => 'Amazon::SimpleDB::Domain', is => 'ro');

has '_last_response' => (isa => 'Amazon::SimpleDB::Response', is => 'rw');

has 'json' => (isa => 'JSON', is => 'ro', lazy_build => 1);

use Encode qw//;
use MIME::Base64 qw//;

use JSON;

sub _build_json {
    my ($self) = @_;
    
    JSON->new()->utf8->pretty;
}

use Data::Dump qw/dump/;

use Amazon::SimpleDB;
use Amazon::SimpleDB::Item;
use Data::Stream::Bulk::Array;

sub BUILD {
    my ($self, $args) = @_;

    croak("missing aws_id and aws_key params in new call for" . __PACKAGE__)
        unless ($args->{aws_id} and $args->{aws_key});
    
    my $sdb = Amazon::SimpleDB->new(
        {
            aws_access_key_id       => $args->{aws_id},
            aws_secret_access_key   => $args->{aws_key},
        }
    );
    
    croak("missing domain argument in new call for " . __PACKAGE__)
        unless ($args->{aws_domain});
    
    # check if we have this domain first. create domain is sloooow
    
    my $r_domains = $sdb->domains;
    my $domain;
    if ($r_domains->is_success) {
        ($domain) = grep { $_->name eq $args->{aws_domain} } $r_domains->results;
    }
    
    unless ($domain) {
        $self->response($sdb->create_domain($args->{aws_domain}));
    }
    
    
    $self->{domain} = $sdb->domain($args->{aws_domain});
    
}

=method response [$response]

Called without an argument, it returns the last response.

Called with an argument, it handles a response from the webservice. 
Each call to the webservice should send the response object here
    $self->response($self->domain->query({ query => 'ass jacket' }));

=cut

sub response {
    my ($self, $res) = @_;
    return $self->_last_response unless $res;
    
    if ($res->is_error) {
        warn " URL: " . $res->http_response->request->uri;
        croak("Error during request: " . $res->code . " (" . $res->message . ")");
    } else {
        $self->_last_response($res);
    }
    return $self->_last_response();
    
}

sub _item {
    my ($self, $id) = @_;
    my $item;
    if (ref($id) eq 'Amazon::SimpleDB::Item') {
        # We just need to fix something..
        $id->{domain} = $id->{domain}->name if (ref($id->{domain}));
        $item = $id;
    } else {
        $item = Amazon::SimpleDB::Item->new({
            account => $self->{domain}->account,
            domain  => $self->{domain}->name,
            name    => $id,
        });
        
    }
    
    return $item;
    
}
sub _entry {
    my ($self, $item) = @_;
    
    # We inflate an ID into a propper object
    $item = $self->_item($item);

    
    # inflate it back into a KiokuDB::Entry-object
    $self->response($item->get_attributes);
    my $doc = $self->response->results;
    # We need to flatten single item arrays (Amazon::SimpleDB returns all attrs
    # as array-refs)
    map { $doc->{$_} = $doc->{$_}->[0] if scalar(@{ $doc->{$_}}) == 1 } keys %$doc;
    if (ref($doc->{_obj})) {
        my $o = $doc->{_obj};
        if (exists($o->{content}) and exists($o->{encoding})) {
            if ($o->{encoding} eq 'base64') {
                $doc->{_obj} = MIME::Base64::decode_base64($o->{content});
            }
        }
    }
    my %doc = %{ $self->json->decode(Encode::decode_utf8($doc->{_obj})) };

    return $self->expand_jspon(\%doc, root => delete $doc->{root});
}
=method get

Implemented as part of the KiokuDB::Backend basic interface.
Fetches one or more objects from the SimpleDB domain

=cut

sub get {
    my $self = shift;
    my @ids = @_;
    
    my @entries = map { $self->_entry($_) } @ids;
    # I think this is the only way we can do this properly?
    return @entries;
}


sub insert {
    my $self = shift;
    my @entries = @_;
    
    foreach my $e (@entries) {
        my $collapsed = $self->collapse_jspon($e);
        my $obj = $self->_item($e->id);
        
        # TODO handle some sort of prev-stuff here?
        my $attr = {
            id => $collapsed->{id},
            _obj => Encode::encode_utf8($self->json->encode($collapsed)),
            exists => 1
        };
        $attr->{root} = $e->root if $e->root;
        foreach my $k (keys %$collapsed) {
            next if ref($collapsed->{$k});
            # Add theese to toplevel
            $attr->{$k} = Encode::encode_utf8($collapsed->{$k});
        }
        $self->response($obj->post_attributes($attr));
    }
}

sub delete {
    my $self = shift;
    my @ids_or_entries = @_;
    
    foreach my $e (@ids_or_entries) {
         my $i = $self->_item(ref($e) ? $e->id : $e);
         
         $i->delete_attributes();
    }
}

sub exists {
    my $self = shift;
    my @ids = @_;
    
    my @exists;
    foreach my $i (@ids) {
        my $r = $self->_item($i);
        
        # inflate it back into a KiokuDB::Entry-object
        $self->response($r->get_attributes('exists'));
        push(@exists, $self->response->results->{exists} ? 1 : 0);
    
    }
    return @exists;
}


sub search {
    my $self = shift;
    my %args = @_;
    warn "PROPPER SEARCH";
    $self->response($self->domain->query(\%args));
    
    my @results = $self->_inflate_results($self->response->results);
    
    return Data::Stream::Bulk::Array->new(
        array => \@results,
    );
    
}

sub simple_search {
    my ( $self, $proto ) = @_;
    warn "SIMPLE SEARCH";
    # convert $proto to a querystring
    my @predicates = ("['root' = '1']");
    foreach my $k (keys %$proto) {
        my $v = $proto->{$k};
        # We don't support complex queries for now
        next if ref($v);
        push(@predicates, "['$k' = '" . Encode::encode_utf8($v) . "']");
    }
    my $q = join(' intersection ', @predicates);
    
#    warn "   QUERY: $q";
    return $self->search(query => $q);
    
}

sub _inflate_results {
    my $self = shift;
    
    
    return map { $_ = $self->_entry($_) } @_;
}

1; # End of KiokuDB::Backend::AWS::SDB
