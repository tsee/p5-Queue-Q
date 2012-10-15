use strict;
use warnings;
use File::Spec;
use Test::More;

use lib (-d 't' ? File::Spec->catdir(qw(t lib)) : 'lib' );
use Queue::Q::Test;
use Queue::Q::TestDistFIFO;

use Queue::Q::NaiveFIFO::Perl;
use Queue::Q::DistFIFO;

my @q = map Queue::Q::NaiveFIFO::Perl->new(), 1..5;

isa_ok($_, "Queue::Q::NaiveFIFO") for @q;
isa_ok($_, "Queue::Q::NaiveFIFO::Perl") for @q;

my $q = Queue::Q::DistFIFO->new(
    shards => \@q,
);
isa_ok($q, "Queue::Q::DistFIFO");

Queue::Q::TestDistFIFO::test_dist_fifo($q);

done_testing();
