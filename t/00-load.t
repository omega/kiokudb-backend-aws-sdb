#!perl -T

use Test::More tests => 1;

use ok 'KiokuDB::Backend::AWS::SDB';
use KiokuDB;
diag( "Testing KiokuDB::Backend::AWS::SDB $KiokuDB::Backend::AWS::SDB::VERSION" .
" ($KiokuDB::VERSION), Perl $], $^X" );
