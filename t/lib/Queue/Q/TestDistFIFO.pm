package # Hide from PAUSE
    Queue::Q::TestDistFIFO;
use strict;
use warnings;
use Test::More;

sub test_dist_fifo {
    my $q = shift;

    # clean up so the tests make sense
    $q->flush_queue;
    is($q->queue_length, 0, "Flushed queue has no items");

    $q->enqueue_item($_) for 1..2;
    is($q->queue_length, 2, "Queue len check 1");

    my @items;
    push @items, $q->claim_item;
    is($q->queue_length, 1, "Queue len check 3");

    push @items, $q->claim_item();
    is($q->queue_length, 0, "Queue len check 4");

    $q->enqueue_items(151..161);
    is($q->queue_length, 11, "Queue len check 2");

    @items = sort {$a <=> $b} @items; 
    is(scalar(@items), 2);
    is($items[0], 1, "Fetched one item [1]");
    is($items[1], 2, "Fetched one item [2]");

    @items = $q->claim_items();
    is($q->queue_length, 10, "Queue len check 5");

    push @items, $q->claim_items(3);
    is($q->queue_length, 7, "Queue len check 6");

    push @items, grep defined, $q->claim_items(15);
    is($q->queue_length, 0, "Queue len check 7");
    @items = sort {$a <=> $b} @items;
    is_deeply(\@items, [151..161]);

    my @set1 = (1..10);
    my @set2 = (11..20);
    my @set3 = (21..30);

    $q->enqueue_items_strict_ordering(@set1);
    $q->enqueue_items_strict_ordering(@set2);
    $q->enqueue_items_strict_ordering(@set3);

    for (1..30) {
        my $item = $q->claim_item();
        for my $s (\@set1, \@set2, \@set3) {
            if (@$s and $s->[0] == $item) {
                pass("Strict ordering checks: $_");
                $item = undef;
                shift @$s;
                last;
            }
        }
        if (defined $item) {
            fail("Strict ordering checks: $_");
        }
    }
}


1;
