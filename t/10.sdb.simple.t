use Test::More;

use KiokuDB::Test;
use KiokuDB;


BEGIN {
    plan skip_all => 'Testing this module for real costs money.' 
        unless ($ENV{'AMAZON_S3_EXPENSIVE_TESTS'});

    plan skip_all => 'Testing needs AWS_ACCESS_KEY_ID and AWS_ACCESS_KEY_SECRET set'
        unless ($ENV{'AWS_ACCESS_KEY_ID'} && $ENV{'AWS_ACCESS_KEY_SECRET'});
        
    plan 'no_plan';
}


my $aws_id      = $ENV{'AWS_ACCESS_KEY_ID'};
my $aws_key     = $ENV{'AWS_ACCESS_KEY_SECRET'};


use ok 'KiokuDB::Backend::AWS::SDB';

my $backend = KiokuDB::Backend::AWS::SDB->new(
    aws_id => $aws_id,
    aws_key => $aws_key,
    aws_domain => 'kiokudb-test-domain-' . lc($aws_id),
    create => 1
    
);

my $sg = $ENV{KIOKU_SDB_KEEP} || Scope::Guard->new(sub { $backend->domain->delete });

my @entries = ( map { KiokuDB::Entry->new($_) }
    { id => 1, root => 1, data => { name => "foo", age => 3, tags => [qw/a b/] } },
    { id => 2, root => 1, data => { name => "bar", age => 3 } },
    { id => 3, root => 1, data => { name => "gorch", age => 5 } },
    { id => 4, root => 0, data => { name => "zot", age => 3 } },
);

$backend->insert(@entries);

can_ok( $backend, qw(simple_search) );

my $three = $backend->simple_search({ age => 3 });

isa_ok( $three, "Data::Stream::Bulk::Array" );

is_deeply(
    [ sort { $a->id <=> $b->id } $three->all ],
    [ sort { $a->id <=> $b->id } @entries[0 .. 1], $entries[3] ],
    "search",
);

