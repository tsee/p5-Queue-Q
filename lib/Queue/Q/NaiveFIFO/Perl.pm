package Queue::Q::NaiveFIFO::Perl;
use strict;
use warnings;

use Carp qw(croak);
use Queue::Q::NaiveFIFO;
use parent 'Queue::Q::NaiveFIFO';

sub new {
    my $class = shift;
    my $self = bless {
        @_,
        queue => [],
    } => $class;
    return $self;
}

sub enqueue_item {
    my $self = shift;
    push @{$self->{queue}}, shift;
}

sub enqueue_items {
    my $self = shift;
    push @{$self->{queue}}, @_;
}

sub claim_item {
    my $self = shift;
    return shift @{$self->{queue}};
}

sub claim_items {
    my $self = shift;
    my $n = shift || 1;
    my @items;
    my $q = $self->{queue};
    for (1..$n) {
        push @items, shift @$q;
    }
    return @items;
}

sub flush_queue {
    my $self = shift;
    @{ $self->{queue} } = ();
}

sub queue_length {
    my $self = shift;
    return scalar(@{ $self->{queue} });
}

1;
