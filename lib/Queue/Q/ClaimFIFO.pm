package Queue::Q::ClaimFIFO;
use strict;
use warnings;

use Carp qw(croak);

# Note: items are generally Queue::Q::ClaimFIFO::Item's
use Queue::Q::ClaimFIFO::Item;

# enqueue_item($single_item)
sub enqueue_item { croak("Unimplemented") }
# enqueue_items(@list_of_items)
sub enqueue_items { croak("Unimplemented") }

# my $item_or_undef = claim_item()
sub claim_item { croak("Unimplemented") }
# my (@items_or_undefs) = claim_items($n)
sub claim_items { croak("Unimplemented") }

# mark_item_as_done($item_previously_claimed)
sub mark_item_as_done { croak("Unimplemented") }
# mark_item_as_done(@items_previously_claimed)
sub mark_items_as_done { croak("Unimplemented") }

1;
