use strict;
use warnings;
use File::Spec;
use Test::More;
use Encode;

use lib (-d 't' ? File::Spec->catdir(qw(t lib)) : 'lib' );
use Queue::Q::Test;
use Queue::Q::TestReliableFIFO;

use Queue::Q::ReliableFIFO::Redis;

my ($Host, $Port) = get_redis_connect_info();
skip_no_redis() if not defined $Host;

my $q = Queue::Q::ReliableFIFO::Redis->new(
    server => $Host,
    port => $Port,
    queue_name => "test"
);
isa_ok($q, "Queue::Q::ReliableFIFO");
isa_ok($q, "Queue::Q::ReliableFIFO::Redis");

Queue::Q::TestReliableFIFO::test_claim_fifo($q);

my $a = pack("C*", 28, 29, 30);
$q->enqueue_item($a);
my $item = $q->claim_item();
$q->mark_item_as_done($item);

is($item->data, $a, "verify roundtrip: " . join(',', unpack("C*", $a)));
is(Encode::is_utf8($item->data), Encode::is_utf8($a), "Verifying is_utf8 equivalence for ". join(',', unpack("C*", $a)));

$a = pack("C*", 128, 129, 130);
$q->enqueue_item($a);
$item = $q->claim_item();
$q->mark_item_as_done($item);

is($item->data, $a, "verify roundtrip: " . join(',', unpack("C*", $a)));
is(Encode::is_utf8($item->data), Encode::is_utf8($a), "Verifying is_utf8 equivalence for ". join(',', unpack("C*", $a)));

done_testing();
