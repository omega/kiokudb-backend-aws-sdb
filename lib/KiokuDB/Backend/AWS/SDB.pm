package KiokuDB::Backend::AWS::SDB;
# ABSTRACT: KiokuDB backend that persists and searches with Amazon SimpleDB

use Moose;

use Carp qw/croak/;

# TODO: Figure out what the hell sort of roles I should add here :p
with qw(

    KiokuDB::Backend
    
    KiokuDB::Backend::Serialize::JSON

    KiokuDB::Backend::Query
    KiokuDB::Backend::Query::Simple
);

has 'domain' => (isa => 'Amazon::SimpleDB::Domain', is => 'ro');

has '_last_response' => (isa => 'Amazon::SimpleDB::Response', is => 'rw');

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
    my $attrs = $self->response->results;
    
    if ($attrs->{data}) {
        my %args = (
            id => $item->name,
            data => $self->deserialize($attrs->{data}->[0]),
        );
        $args{class} = $attrs->{class}->[0] if $attrs->{class};
        $args{root} = $attrs->{root}->[0] if $attrs->{root};
        return KiokuDB::Entry->new(%args);
    } else {
        # something is missing, we push undef
        return undef;
    }
    
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
        my $obj = $self->_item($e->id);
        my $data = $self->serialize($e->data);
        my $attrs = {
            data => $data,
        };
        $attrs->{class} = $e->class if $e->class;
        $attrs->{root} = $e->root if $e->root;
        # here we should most likely add some index columns?
        foreach my $k (keys %{ $e->data }) {
            next if (ref($e->data->{$k}));
            $attrs->{"idx_$k"} = $e->data->{$k};
        }
        $self->response($obj->post_attributes($attrs));
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
        $self->response($r->get_attributes([qw/class/]));
        push(@exists, $self->response->results->{class} ? 1 : 0);
    
    }
    return @exists;
}


sub search {
    my $self = shift;
    my %args = @_;
    
    $self->response($self->domain->query(\%args));
    
    my @results = $self->response->results;
    
    return $self->_inflate_results(@results);
}

sub simple_search {
    my ( $self, $proto ) = @_;
    
    # convert $proto to a querystring
    my @predicates = ("['root' = '1']");
    foreach my $k (keys %$proto) {
        my $v = $proto->{$k};
        # We don't support complex queries for now
        next if ref($v);
        push(@predicates, "['idx_$k' = '$v']");
    }
    my $q = join(' intersection ', @predicates);
    
    warn "   QUERY: $q";
    my @results = $self->search(query => $q);
    
    return Data::Stream::Bulk::Array->new(
        array => \@results,
    );
}

sub _inflate_results {
    my $self = shift;
    
    
    return map { $_ = $self->_entry($_) } @_;
}

1; # End of KiokuDB::Backend::AWS::SDB
