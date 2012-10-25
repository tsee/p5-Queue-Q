package Queue::Q::ReliableFIFO::Redis;
use strict;
use warnings;
use Carp qw(croak);

use Queue::Q::NaiveFIFO;
use parent 'Queue::Q::ReliableFIFO';
use Queue::Q::ReliableFIFO::Item;

use Redis;
use Sereal::Encoder;
use Sereal::Decoder;
use JSON::XS;
use Data::Dumper;

my  $DEFAULT_CLAIM_TIMEOUT = 1;

use Class::XSAccessor {
    getters => [qw(server port queue_name db _redis_conn _busy_queue
                   _burry_queue _time_queue busy_expiry_time)],
};

sub new {
    my ($class, %params) = @_;
    for (qw(server port queue_name)) {
        croak("Need '$_' parameter")
            if not exists $params{$_};
    }

    my $self = bless({
        (map {$_ => $params{$_}} qw(server port queue_name) ),
        db => $params{db} || 0,
        _redis_conn => undef,
    } => $class);
    $self->{_busy_queue} = $params{queue_name} . "_busy";
    $self->{_burry_queue} = $params{queue_name} . "_burry";
    $self->{_time_queue} = $params{queue_name} . "_time";

    $self->{_redis_conn} = Redis->new(
        %{$params{redis_options} || {}},
        encoding => undef, # force undef for binary data
        server => join(":", $self->server, $self->port),
    );

    $self->_redis_conn->select($self->db) if $self->db;

    return $self;
}

sub enqueue_item {
    my $self = shift;
    croak("Need exactly one item to enqeue")
        if not @_ == 1;
    my ($item) = Queue::Q::ReliableFIFO::Item->new(data => $_[0]);
    $self->_redis_conn->lpush($self->queue_name, $item->_serialized);
}

sub enqueue_items {
    my $self = shift;
    return if not @_;
    my $qn = $self->queue_name;
    my $conn = $self->_redis_conn;
    my @serial = 
        map { Queue::Q::ReliableFIFO::Item->new(data => $_)->_serialized } @_;
    $conn->lpush($qn, @serial);
}

sub claim_item {
    my ($self, $timeout) = @_;
    $timeout ||= $DEFAULT_CLAIM_TIMEOUT;
    # rpoplpush gives higher throughput than the blocking version
    # (i.e. brpoplpush). So use the blocked version only when we
    # need to wait.
    my $v = 
	    $self->_redis_conn->rpoplpush($self->queue_name, $self->_busy_queue)
        ||
        $self->_redis_conn->brpoplpush(
            $self->queue_name, $self->_busy_queue, $timeout);
    return if !$v;
    my ($item) = Queue::Q::ReliableFIFO::Item->new(_serialized => $v);
    return $item;
}

sub claim_items {
    my ($self, $n) = @_;
    $n ||= 1;
    my $conn = $self->_redis_conn;
    my $qn = $self->queue_name;
    my $bq = $self->_busy_queue;
    my @items;
    $conn->rpoplpush($qn, $bq, sub {
        if (defined $_[0]) {
            push @items, 
            Queue::Q::ReliableFIFO::Item->new(_serialized => $_[0])
        }
    }) for 1..$n;
    $conn->wait_all_responses;
    if (@items == 0) {
        # list seems empty, use the blocking version
        my $sereal = $conn->brpoplpush($qn, $bq, $DEFAULT_CLAIM_TIMEOUT);
        if (defined $sereal) {
            push(@items,
                Queue::Q::ReliableFIFO::Item->new(_serialized => $sereal));
            $conn->rpoplpush($qn, $bq, sub {
                if (defined $_[0]) {
                    push @items, 
                    Queue::Q::ReliableFIFO::Item->new(_serialized => $_[0])
                }
            }) for 1 .. ($n-1);
        }
    }
    return @items;
}
sub mark_item_as_done {
	my ($self, $item) = @_;
    return $self->_redis_conn->lrem($self->_busy_queue, 1, $item->_serialized);
}

sub mark_items_as_done {
	my $self = shift;
    my $conn = $self->_redis_conn;
	my $count = 0;
    $conn->lrem(
        $self->_busy_queue, 1, $_->_serialized, sub { $count += $_[0] })
            for @_;
	$conn->wait_all_responses;
	return $count;
}

sub flush_queue {
    my $self = shift;
    $self->_redis_conn->del($self->queue_name);
    $self->_redis_conn->del($self->_busy_queue);
}

sub queue_length       { $_[0]->_queue_length($_[0]->queue_name);   }
sub busy_queue_length  { $_[0]->_queue_length($_[0]->_busy_queue);  }
sub burry_queue_length { $_[0]->_queue_length($_[0]->_burry_queue); }
sub _queue_length {
    my ($self, $qn) = @_;
    my ($len) = $self->_redis_conn->llen($qn);
    return $len;
}

my %valid_options = map { $_ => 1 } (qw(OnError max_nr_requeues Chunk));
my %onerror  = (
    'die'     => sub { croak('Fatal error while consumming the queue'); },
    'ignore'  => sub {},
    'requeue' => sub { my ($self, $item) = @_; $self->_requeue($item); },
    'burry'   => sub { my ($self, $item) = @_; $self->_burry($item); },
);
sub consume {
    my ($self, $callback, %options) = @_;
    for (keys %options) {
        die "Unknown option $_\n" if !$valid_options{$_};
    }

    my $chunk = exists $options{Chunk} ? int($options{Chunk}) : 1;
    $chunk ||= 1;

    my $onerror = $onerror{die};
    if (exists $options{OnError}) {
        my $k = $options{OnError};
        if (exists $onerror{$k}) {
            $onerror = $onerror{$k};
        }
        else {
            die "Invalid option OnError=$k\n";
        }
    }
    my $max_nr_requeues = $options{max_nr_requeues};
    my $stop = 0;
    local $SIG{INT} = local $SIG{TERM} = sub { print "stopping\n"; $stop = 1; };
    if ($chunk == 1) {
        while(!$stop) {
            my $item = $self->claim_item();
            next if (!$item);
            eval { $callback->($item->data); 1; } 
            or do {
                warn;
                $onerror->($self, $item);
            };
            my $count = $self->mark_item_as_done($item);
            warn("item was already removed from queue") if $count == 0;
        }
    }
    else {
        while(!$stop) {
            my @items = $self->claim_items($chunk);
            next if (@items == 0);
            for my $item (@items) {
                eval { $callback->($item->data); 1; } 
                or do {
                    warn;
                    $onerror->($self, $item);
                };
            }
            my $count = $self->mark_items_as_done(@items);
            warn "not all items removed from busy queue ($count)\n"
                if $count != @items;
        }
    }
}
# methods to be used for cleanup script and Nagios checks
# the methods read or remove items from the busy queue
my %known_actions = map { $_ => undef } (qw(requeue burry drop));
sub handle_expired_items {
    my ($self, $timeout, $action) = @_;
    $timeout ||= 10;
    die "unknown action\n" if !exists $known_actions{$action};
    my $conn = $self->_redis_conn;
    my @sereal = $conn->lrange($self->_busy_queue, 0, -1);
    my $time = time;
    my %timetable = 
        map { reverse split /-/,$_,2 }
        $conn->lrange($self->_time_queue, 0, -1);
    my @match = grep { exists $timetable{$_} } @sereal;
    my %match = map { $_ => undef } @match;
    my @timedout = grep { $time - $timetable{$_} >= $timeout } @match;
    my @log;
    # handle timed out items
    if ($action eq 'requeue') {
        for my $sereal (@timedout) {
            my $n = $conn->lrem( $self->_busy_queue, 1, $sereal);
            if ($n) {
                my $item = Queue::Q::ReliableFIFO::Item->new(
                    _serialized => $sereal);
                $item->inc_nr_requeues();
                $conn->lpush($self->queue_name, $item->_serialized);
                push @log, $item;
            }
        }
    }
    elsif ($action eq 'burry') {
        for my $sereal (@timedout) {
            my $n = $conn->lrem( $self->_busy_queue, 1, $sereal);
            if ($n) {
                $conn->lpush($self->burry_queue, $sereal);
                push @log, 
                    Queue::Q::ReliableFIFO::Item->new(_serialized => $sereal);
            }
        }
    }
    elsif ($action eq 'drop') {
        for my $sereal (@timedout) {
            my $n = $conn->lrem( $self->_busy_queue, 1, $sereal);
            push @log, Queue::Q::ReliableFIFO::Item->new(
                                _serialized => $sereal) if ($n);
        }
    }

    # put in the items of older scans that did not timeout
    my %timedout = map { $_ => undef } @timedout;
    my %newtimetable = 
        map  { $_ => $timetable{$_} } 
        grep { ! exists $timedout{$_} }
        keys %timetable;
    # put in the items of latest scan that did not timeout
    $newtimetable{$_} = $time 
        for (grep { !exists $newtimetable{$_} } @sereal);
    $conn->multi;
    $conn->del($self->_time_queue);
    $conn->lpush($self->_time_queue, join('-',$newtimetable{$_},$_))
        for (keys %newtimetable);
    $conn->exec;
    return @log;
}
1;

__END__

=head1 NAME

Queue::Q::ReliableFIFO::Redis - In-memory Redis implementation of the ReliableFIFO queue

=head1 SYNOPSIS

  use Queue::Q::ReliableFIFO::Redis;
  my $q = Queue::Q::ReliableFIFO::Redis->new(
      server     => 'myredisserver',
      port       => 6379,
      queue_name => 'my_work_queue',
  );

  # Producer:
  $q->enqueue_item("foo");
  # You can pass any JSON/Sereal-serializable data structure
  $q->enqueue_item({ bar => "baz" }); 
  $q->flush_queue();    # get a clean state, removes queue

  # Consumer:
  $q->consumer(\&callback);
  $q->consumer(sub { my $data = shift; print 'Received: ', Dumper($data); });

  # Cleanup script
  while (1) {
      my @handled_items = $q->handle_expired_items($timeout, $action);
      for my $item (@handled_items) {
          printf "%s: item %s, in queue since %s, requeued % times\n", 
              $action, Dumper($item->data),
              scalar localtime $item->time,
              $item->rn_requeues;
      }
      sleep(60);
  }

  # Nagios?
  $q->get_length_queue();
  $q->get_length_busy_queue();

  # Obsolete (consumer)
  my $item = $q->claim_item;
  my $foo  = $item->data;
  $q->mark_item_as_done($item);

=head1 DESCRIPTION

Implements interface defined in L<Queue::Q::ReliableFIFO>:
an implementation based on Redis lists.

The data structures passed to C<enqueue_item> are serialized
using JSON or Sereal (cf. L<Sereal::Encoder>, L<Sereal::Decoder>), so
any data structures supported by that can be enqueued.


=head1 METHODS

All methods of L<Queue::Q::ReliableFIFO> plus:

=head2 new

Constructor. Takes named parameters. Required parameters are
the C<server> hostname or address, the Redis C<port>, and
the name of the Redis key to use as the C<queue_name>.

You may optionally specify a Redis C<db> number to use.
Since this module will establish the Redis connection,
you may pass in a hash reference of options that are valid
for the constructor of the L<Redis> module. This can be
passed in as the C<redis_options> parameter.

=head2 consume(\&callback, %options)

This method is called by the consumer to consume the items of a
queue. For each item in the queue, the callback function will be
called. The function will receive that data of the queued item
as parameter.

=head3 options B<OnError>

This method can handle failures (die's) of the function in four ways:

=over

=item * B<die>. Die on failures. The item will stay in the busy queue.
(If there is a cleanup job, the item might be rescheduled.)
This is the default.

=item * B<ignore>. Ignore the error, consider as processed.

=item * B<requeue>. Re-enqueu the item so that it can be tried again.

=item * B<burry>. Put the item aside in a burry queue. It requires
manual action to reschedule these items.

=back

To select a failure mode, use the OnError option, e.g.

    $q->consume(\&callback, OnError => 'die');

=head3 option B<max_nr_requeues>

In case of OnError => 'requeue', you can limit how often the item
will be requeued. Value "undef" means unlimited (Default).
When the maximum number is reached, the item will be considered as being
processed.

=head1 AUTHOR

Herald van der Breggen, E<lt>herald.vanderbreggen@booking.comE<gt>

Steffen Mueller, E<lt>smueller@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
