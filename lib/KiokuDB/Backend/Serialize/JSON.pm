package KiokuDB::Backend::Serialize::JSON;


use Moose::Role;

use JSON;

use namespace::clean -except => 'meta';

with qw(
    KiokuDB::Backend::Serialize
    KiokuDB::Backend::UnicodeSafe
    KiokuDB::Backend::BinarySafe
);

use Data::Dump qw/dump/;

sub serialize {
    my ( $self, $entry ) = @_;
    return to_json($entry);
}

sub deserialize {
    my ( $self, $blob ) = @_;
    return from_json($blob);
}

__PACKAGE__

__END__
