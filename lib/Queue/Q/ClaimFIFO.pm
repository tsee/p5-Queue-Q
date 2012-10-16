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

sub flush_queue { croak("Unimplemented") }

# my $nitems = queue_length()
sub queue_length { croak("Unimplemented") }

# my $nclaimed_items = claimed_count()
sub claimed_count { croak("Unimplemented") }

1;
__END__

=head1 NAME

Queue::Q::ClaimFIFO - FIFO queue keeping track of claimed items

=head1 SYNOPSIS

  use Queue::Q::ClaimFIFO::Redis; # or ::Perl or ...

=head1 DESCRIPTION

Abstract interface class for a FIFO queue that keeps track
of all in-flight ("claimed") items. Implementations are
required to provide strict ordering and adhere to
the run-time complexities listed below (or better).

All ...

=head1 METHODS

=head2 enqueue_item

Given a data structure, that data structure is added to
the queue. Items enqueued with C<enqueue_item> in order
must be returned by C<claim_item> in the same order.

Complexity: O(1)

=head2 enqueue_items

Given a number data structures, enqueues them in order.
This is conceptually the same as calling C<enqueue_item> multiple
times, but may help save on network round-trips.

Complexity: O(n) where n is the number of items to enqueue.

=head2 claim_item

Returns the oldest item in the queue. Returns C<undef>
if there is none left.

Complexity: O(1)

=head2 claim_items

As C<enqueue_items> is to C<enqueue_item>, C<claim_items> is to
C<claim_item>. Takes one optional parameter (defaults to 1):
The number of items to fetch and return:

  my @items = $q->claim_items(20); # returns a batch of 20 items

If there are less than the desired number of items to be claimed,
returns a correspondingly shorter list.

Complexity: O(n) where n is the number of items claimed.

=head2 queue_length

Returns the number of items available in the queue.

Complexity: O(1)

=head2 flush_queue

Removes all content in the queue.

Complexity: O(n) where n is the number of items in the queue.

=head1 AUTHOR

Steffen Mueller, E<lt>smueller@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
