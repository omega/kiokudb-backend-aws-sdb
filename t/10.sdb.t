use Test::More 'no_plan';

use KiokuDB::Test;
use KiokuDB;


unless ($ENV{'AMAZON_S3_EXPENSIVE_TESTS'}) {
    plan skip_all => 'Testing this module for real costs money.';
}

my $aws_id     = $ENV{'AWS_ACCESS_KEY_ID'};
my $aws_key = $ENV{'AWS_ACCESS_KEY_SECRET'};

plan skip_all => 'Testing needs AWS_ACCESS_KEY_ID and AWS_ACCESS_KEY_SECRECT set' 
    unless ($aws_id && $aws_key);


use ok 'KiokuDB::Backend::AWS::SDB';
my $backend = KiokuDB::Backend::AWS::SDB->new(
    aws_id => $aws_id,
    aws_key => $aws_key,
    aws_domain => 'kiokudb-test-domain-' . lc($aws_id),
    
);

run_all_fixtures( KiokuDB->new( backend => $backend ));

END {
#    $backend->domain->delete;
}