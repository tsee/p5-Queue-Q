use strict;
use warnings;
use File::Spec;
use Test::More;

use lib (-d 't' ? File::Spec->catdir(qw(t lib)) : 'lib' );
use Queue::Q::Test;

use Queue::Q::NaiveFIFO::Redis;

my ($Host, $Port) = get_redis_connect_info();
skip_no_redis() if not defined $Host;

my $q = Queue::Q::NaiveFIFO::Redis->new(
    server => $Host,
    port => $Port,
    queue_name => "test"
);
isa_ok($q, "Queue::Q::NaiveFIFO");
isa_ok($q, "Queue::Q::NaiveFIFO::Redis");

# clean up so the tests make sense
$q->flush_queue;
is($q->queue_length, 0, "Flushed queue has no items");

$q->enqueue_item([$_]) for 1..2;
is($q->queue_length, 2, "Queue len check 1");

$q->enqueue_items(151..161);
is($q->queue_length, 13, "Queue len check 2");

my $item = $q->claim_item();
is_deeply($item, [1], "Fetching one item");
is($q->queue_length, 12, "Queue len check 3");

$item = $q->claim_item();
is_deeply($item, [2], "Fetching one item, 2");
is($q->queue_length, 11, "Queue len check 4");

my @items = $q->claim_items();
is_deeply(\@items, [151], "Fetching one item via claim_items");
is($q->queue_length, 10, "Queue len check 5");

@items = $q->claim_items(3);
is_deeply(\@items, [152..154], "Fetching three items via claim_items");
is($q->queue_length, 7, "Queue len check 6");

$q->enqueue_item({foo => "bar"});
is($q->queue_length, 8, "Queue len check 7");

@items = $q->claim_items(10);
is_deeply(\@items, [155..161, {foo => "bar"}, undef, undef], "Fetching items via claim_items");
is($q->queue_length, 0, "Queue len check 8");

$item = $q->claim_item();
ok(!defined($item));
is($q->queue_length, 0, "Queue len check 9");

done_testing();
