use Test::More ;

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

run_all_fixtures( KiokuDB->new( backend => $backend ));

