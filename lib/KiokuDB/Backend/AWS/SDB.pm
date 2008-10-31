package KiokuDB::Backend::AWS::SDB;
# ABSTRACT: KiokuDB backend that persists and searches with Amazon SimpleDB

use Moose;

use Carp qw/croak/;

# TODO: Figure out what the hell sort of roles I should add here :p
with qw(

    KiokuDB::Backend

    KiokuDB::Backend::Serialize::JSPON

    KiokuDB::Backend::Role::UnicodeSafe

    KiokuDB::Backend::Role::Query
    KiokuDB::Backend::Role::Query::Simple

    KiokuDB::Backend::Role::Scan
    KiokuDB::Backend::Role::Clear
);

use Encode qw//;
use MIME::Base64 qw//;

use JSON;

use Data::Dump qw/dump/;

use Amazon::SimpleDB;
use Amazon::SimpleDB::Item;
use Data::Stream::Bulk::Util qw/bulk/;

has 'aws_id' => (isa => 'Str', is => 'ro');
has 'aws_key' => (isa => 'Str', is => 'ro');
has 'aws_domain' => (isa => 'Str', is => 'ro');

has 'create' => (isa => 'Bool', is => 'ro', default => 0);

has 'sdb' => (isa => 'Amazon::SimpleDB', is => 'ro', lazy_build => 1);

has 'domain' => (isa => 'Amazon::SimpleDB::Domain', is => 'ro', lazy_build => 1);

has 'json' => (isa => 'JSON', is => 'ro', lazy_build => 1);

sub _build_json {
    my ($self) = @_;
    
    JSON->new()->utf8; #->pretty;
}
sub _build_sdb {
    my $self = shift;
    Amazon::SimpleDB->new(
        {
            aws_access_key_id       => $self->aws_id,
            aws_secret_access_key   => $self->aws_key,
        }
    );
}
sub _build_domain {
    my ($self) = @_;
    
    
    # check if we have this domain first. create domain is sloooow
    
    my $r_domains = $self->sdb->domains;
    my $domain;
    if ($r_domains->is_success) {
        ($domain) = grep { $_->name eq $self->aws_domain } $r_domains->results;
    }
    
    unless ($domain) {
        if ($self->create) {
            $self->response($self->sdb->create_domain($self->aws_domain));
            # I think we need to delay here for a litt, to make sure the domain exists
            # everywhere
            sleep 5;
        } else {
            croak("Domain '" . $self->aws_domain 
                . "' does not exist, and create not specified" );
        }
    }
    
    
    $self->sdb->domain($self->aws_domain);
    
}


=method response [$response]

Called without an argument, it returns the last response.

Called with an argument, it handles a response from the webservice. 
Each call to the webservice should send the response object here
    $self->response($self->domain->query({ query => 'ass jacket' }));

=cut

sub response {
    my ($self, $res) = @_;

    croak("Calling response without a response-object is deprecated") unless $res;
    if ($res->is_error) {
        warn " URL: " . $res->http_response->request->uri;
        croak("Error during request: " . $res->code . " (" . $res->message . ")");
    }
    return $res;
    
}

sub _item {
    my ($self, $id) = @_;
    my $item;
    if (ref($id) eq 'Amazon::SimpleDB::Item') {
        # We just need to fix something..
        $id->{domain} = $id->domain->name if (ref($id->{domain}));
        $item = $id;
    } else {
        $item = Amazon::SimpleDB::Item->new({
            account => $self->domain->account,
            domain  => $self->domain->name,
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
    my $res = $self->response($item->get_attributes);
    my $doc = $res->results;
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
    # No length means no object
    return undef unless $doc->{_obj};
    my %doc = %{ $self->json->decode(Encode::decode_utf8($doc->{_obj})) };
    # TODO: , backend_data => $item   ?? needed?
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
#        if ($e->prev) {
#            $e->backend_data($e->prev->backend_data);
#        } else {
#            $e->backend_data($obj);
#        }
        my $attr = {
            id => $collapsed->{id},
            _obj => Encode::encode_utf8($self->json->encode($collapsed)),
            exists => 1
        };
        $attr->{root} = (defined($e->root) and $e->root) ? 1 : 0;
        $attr->{'idx_class'} = $collapsed->{'__CLASS__'} if $collapsed->{'__CLASS__'};
        if (ref($collapsed->{data}) eq 'HASH') {
            foreach my $k (keys %{$collapsed->{data}}) {
                next if ref($collapsed->{data}->{$k});
                # Add theese to toplevel, but not topple on the amazon stuff 
                $attr->{"idx_" . $k} = Encode::encode_utf8($collapsed->{data}->{$k});
            }
            
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
        my $res = $self->response($r->get_attributes('exists'));
        push(@exists, $res->results->{exists} ? 1 : 0);
    
    }
    return @exists;
}


sub search {
    my $self = shift;
    my %args = @_;
    # TODO: Fix this so it will do next / limit stuff properly
    my $res = $self->response($self->domain->query(\%args));
    my @results = $self->_inflate_results($res->results);
    return bulk(@results);
    
}

sub simple_search {
    my ( $self, $proto ) = @_;
    # convert $proto to a querystring
    my @predicates; # = ("['root' = '1']");
    foreach my $k (keys %$proto) {
        my $v = $proto->{$k};
        # We don't support complex queries for now
        next if ref($v);
        push(@predicates, "['idx_$k' = '" . Encode::encode_utf8($v) . "']");
    }
    my $q = join(' intersection ', @predicates);
    
#    warn "   QUERY: $q";
    return $self->search(query => $q);
    
}

sub _inflate_results {
    my $self = shift;
    
    
    return map { $_ = $self->_entry($_) } @_;
}


## Methods for Scan

sub all_entries {
    my $self = shift;
    
    return $self->search();
}

sub root_entries {
    my $self = shift;
    return $self->search(query => "['root' = '1']")
    
}

sub child_entries {
    my $self = shift;
    return $self->search(query => "['root' = '0']")
    
    
}


## Methods for Clear

sub clear {
    my $self = shift;
    
    $self->delete($self->all_entries->all);
}
1; # End of KiokuDB::Backend::AWS::SDB
