package Queue::Q::ClaimFIFO::Perl;
use strict;
use warnings;

use Queue::Q::ClaimFIFO;
use parent 'Queue::Q::ClaimFIFO';

use Carp qw(croak);
use Scalar::Util qw(refaddr);

# Note: items are generally Queue::Q::ClaimFIFO::Item's
use Queue::Q::ClaimFIFO::Item;

sub new {
    my $class = shift;
    my $self = bless {
        @_,
        queue => [],
        claimed => {},
    } => $class;
    return $self;
}

# enqueue_item($single_item)
sub enqueue_item {
    my $self = shift;
    my $item = shift;
    push @{$self->{queue}}, $item;
    return 1;
}

# enqueue_items(@list_of_items)
sub enqueue_items {
    my $self = shift;
    push @{$self->{queue}}, @_;
    return 1;
}

# my $item_or_undef = claim_item()
sub claim_item {
    my $self = shift;
    my $item = shift @{ $self->{queue} };
    return undef if not $item;
    $self->{claimed}{refaddr($item)} = $item;
    return $item;
}

# my (@items_or_undefs) = claim_items($n)
sub claim_items {
    my $self = shift;
    my $n = shift || 1;

    my @items = splice(@{ $self->{queue} }, 0, $n);

    my $cl = $self->{claimed};
    for (@items) {
        $cl->{refaddr($_)} = $_;
    }

    return @items;
}

# mark_item_as_done($item_previously_claimed)
sub mark_item_as_done {
    my $self = shift;
    my $item = shift;
    delete $self->{claimed}{refaddr($item)};
    return 1;
}

# mark_item_as_done(@items_previously_claimed)
sub mark_items_as_done {
    my $self = shift;

    foreach (@_) {
        next if not defined $_;
        delete $self->{claimed}{refaddr($_)};
    }

    return 1;
}

sub flush_queue {
    my $self = shift;
    @{ $self->{queue} }   = ();
    %{ $self->{claimed} } = ();
}

# my $nitems = queue_length()
sub queue_length {
    my $self = shift;
    return scalar( @{ $self->{queue} } );
}

# my $nclaimed_items = claimed_count()
sub claimed_count {
    my $self = shift;
    return scalar( keys %{ $self->{claimed} } );
}

1;
