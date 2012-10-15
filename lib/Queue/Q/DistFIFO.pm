package Queue::Q::DistFIFO;
use strict;
use warnings;
use Carp qw(croak);

use List::Util ();
use Scalar::Util qw(refaddr);

use Class::XSAccessor {
    getters => [qw(shards next_shard)],
};

sub new {
    my $class = shift;
    my $self = bless({
        @_,
        next_shard => 0,
    } => $class);

    if (not defined $self->{shards}
        or not ref($self->{shards}) eq 'ARRAY'
        or not @{$self->{shards}})
    {
        croak("Need 'shards' parameter being an array of shards");
    }

    $self->{shards_order} = [ List::Util::shuffle( @{$self->shards} ) ];

    return $self;
}

sub _next_shard {
    my $self = shift;
    my $ns = $self->{next_shard};
    my $so = $self->{shards_order};
    if ($ns > $#{$so}) {
        $ns = $self->{next_shard} = 0;
    }
    ++$self->{next_shard};
    return $so->[$ns];
}

sub enqueue_item {
    my $self = shift;
    croak("Need exactly one item to enqeue")
        if not @_ == 1;
    $self->_next_shard->enqueue_item($_[0]);
}

sub enqueue_items {
    my $self = shift;
    return if not @_;
    $self->_next_shard->enqueue_item($_) for @_;
}

sub enqueue_items_strict_ordering {
    my $self = shift;
    return if not @_;
    my $shard = $self->_next_shard;
    $shard->enqueue_items(@_);
}

sub claim_item {
    my $self = shift;
    # FIXME very inefficient!
    my $shard = $self->_next_shard;
    my $first_shard_addr = refaddr($shard);
    while (1) {
        my $item = $shard->claim_item;
        return $item if defined $item;
        $shard = $self->_next_shard;
        return undef if refaddr($shard) == $first_shard_addr;
    }
}

sub claim_items {
    my ($self, $n) = @_;
    $n ||= 1;
    my @elem;
    push @elem, $self->claim_item() for 1..$n;
    return @elem;
}

sub flush_queue {
    my $self = shift;
    my $shards = $self->{shards};
    for my $i (0..$#$shards) {
        $shards->[$i]->flush_queue;
    }
    return();
}

sub queue_length {
    my $self = shift;
    my $shards = $self->{shards};
    my $len = 0;
    for my $i (0..$#$shards) {
        $len += $shards->[$i]->queue_length;
    }
    return $len;
}

sub claimed_count {
    my $self = shift;
    my $shards = $self->{shards};
    my $ccount = 0;
    for my $i (0..$#$shards) {
        my $shard = $shards->[$i];
        my $meth = $shard->can("claimed_count");
        if (not $meth) {
            Carp::croak("Shard $i does not support claimed count. Is it of type NaiveFIFO?");
        }
        $ccount += $meth->($shard);
    }
    return $ccount;
}

1;
