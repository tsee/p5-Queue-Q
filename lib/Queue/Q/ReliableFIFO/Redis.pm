package Queue::Q::ReliableFIFO::Redis;
use strict;
use warnings;
use Carp qw(croak cluck);

use Queue::Q::NaiveFIFO;
use parent 'Queue::Q::ReliableFIFO';
use Queue::Q::ReliableFIFO::Item;

use Redis;
use Data::Dumper;

my  $DEFAULT_CLAIM_TIMEOUT = 1;

use Class::XSAccessor {
    getters => [qw(
                server
                port
                queue_name
                db
                busy_expiry_time
                requeue_limit
                _redis_conn 
                _main_queue
                _busy_queue
                _failed_queue
                _time_queue
                )],
    setters => { set_requeue_limit => 'requeue_limit' }
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
    $self->{_main_queue}   = $params{queue_name} . "_main";
    $self->{_busy_queue}   = $params{queue_name} . "_busy";
    $self->{_failed_queue} = $params{queue_name} . "_failed";
    $self->{_time_queue}   = $params{queue_name} . "_time";
    $self->{requeue_limit} ||= 5;

    $self->{_redis_conn} = exists $params{_redis_conn} 
        ? $params{_redis_conn} 
        : Redis->new(
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
    $self->_redis_conn->lpush($self->_main_queue, $item->_serialized);
}

sub enqueue_items {
    my $self = shift;
    return if not @_;
    my $qn = $self->_main_queue;
    my $conn = $self->_redis_conn;
    my @serial = 
        map { Queue::Q::ReliableFIFO::Item->new(data => $_)->_serialized } @_;
    $conn->lpush($qn, @serial);
}

sub _requeue_at_tail  { my $self = shift; $self->__requeue('tail', @_); }
sub _requeue_at_front { my $self = shift; $self->__requeue('front', @_); }
sub __requeue {
    my $self = shift;
    my $mode = shift;
    my @serial = 
        map  { $_->_serialized }
        grep { $_->inc_nr_requeues <= $self->requeue_limit }
        @_;
    return if not @serial;
    if ($mode eq 'tail') {
        $self->_redis_conn->lpush($self->_main_queue, @serial);
    }
    else {
        $self->_redis_conn->rpush($self->_main_queue, @serial);
    }
}

sub requeue_failed_items {
    my $self = shift;
    my $limit = shift || 0;
    my $count = 0;
    while($self->rpoplpush(
        $self->_failed_queue,
        $self->_main_queue)) {
        $count++;
        last if $limit && $count >= $limit;
    }
    return $count;
}

sub mark_as_failed {
    my $self = shift;
    return if not @_;
    $self->_redis_conn->lpush($self->_failed_queue,
        map { $_->_serialized } @_);
}

sub claim_item {
    my ($self, $timeout) = @_;
    $timeout ||= $DEFAULT_CLAIM_TIMEOUT;
    # rpoplpush gives higher throughput than the blocking version
    # (i.e. brpoplpush). So use the blocked version only when we
    # need to wait.
    my $v = 
        $self->_redis_conn->rpoplpush($self->_main_queue, $self->_busy_queue)
        ||
        $self->_redis_conn->brpoplpush(
            $self->_main_queue, $self->_busy_queue, $timeout);
    return if !$v;
    my ($item) = Queue::Q::ReliableFIFO::Item->new(_serialized => $v);
    return $item;
}

sub claim_items {
    my ($self, $n) = @_;
    $n ||= 1;
    my $conn = $self->_redis_conn;
    my $qn = $self->_main_queue;
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
        my $serial = $conn->brpoplpush($qn, $bq, $DEFAULT_CLAIM_TIMEOUT);
        if (defined $serial) {
            push(@items,
                Queue::Q::ReliableFIFO::Item->new(_serialized => $serial));
            $conn->rpoplpush($qn, $bq, sub {
                if (defined $_[0]) {
                    push @items, 
                        Queue::Q::ReliableFIFO::Item->new(_serialized => $_[0]);
                }
            }) for 1 .. ($n-1);
            $conn->wait_all_responses;
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
    $self->_redis_conn->del($self->_main_queue);
    $self->_redis_conn->del($self->_busy_queue);
    $self->_redis_conn->del($self->_time_queue);
}

sub queue_length       { $_[0]->_queue_length($_[0]->_main_queue);   }
sub busy_queue_length  { $_[0]->_queue_length($_[0]->_busy_queue);  }
sub failed_queue_length { $_[0]->_queue_length($_[0]->_failed_queue); }
sub _queue_length {
    my ($self, $qn) = @_;
    my ($len) = $self->_redis_conn->llen($qn);
    return $len;
}

my %valid_options       = map { $_ => 1 } (qw(Chunk DieOnError));
my %valid_error_actions = map { $_ => 1 } (qw(drop requeue fail));

sub consume {
    my ($self, $callback, $error_action, $options) = @_;
    # validation of input
    $error_action ||= 'drop';
    die "Unknown error action\n"
        if ! exists $valid_error_actions{$error_action};
    my %error_subs = (
        'drop'    => sub {},
        'requeue' => sub { my ($self, $item) = @_;
                           $self->_requeue_at_tail($item); },
        'fail'    => sub { my ($self, $item) = @_;
                           $self->mark_as_failed($item); },
    );
    my $onerror = $error_subs{$error_action} 
        || die "no handler for $error_action\n";

    $options ||= {};
    my $chunk = delete $options->{Chunk} || 1;
    die "Chunk should be a number > 0\n" if (! $chunk > 0);
    my $die   = delete $options->{DieOnError} || 0;

    for (keys %$options) {
        die "Unknown option $_\n" if !$valid_options{$_};
    }

    # Now we can start...
    my $stop = 0;
    local $SIG{INT} = local $SIG{TERM} = sub { print "stopping\n"; $stop = 1; };
    if ($chunk == 1) {
        my $die_afterwards = 0;
        while(!$stop) {
            my $item = $self->claim_item();
            next if (!$item);
            my $ok = eval { $callback->($item->data); 1; };
            # check of we can delete the item from the busy queue
            # if $count happens to be 0, we suppose we don't own the item
            # anymore
            my $count = $self->mark_item_as_done($item);
            if (!$ok) {
                warn;
                $onerror->($self, $item) if ($count);
                if ($die) {
                    $stop = 1;
                    cluck("Stopping because of DieOnError\n");
                }
            };
        }
    }
    else {
        my $die_afterwards = 0;
        while(!$stop) {
            my @items = $self->claim_items($chunk);
            next if (@items == 0);
            my @done;
            while (my $item = shift @items) {
                my $ok = eval { $callback->($item->data); 1; };
                if ($ok) {
                    push @done, $item;
                }
                else {
                    warn;
                    my $count = $self->mark_item_as_done($item);
                    $onerror->($self, $item) if ($count);
                    if ($die) {
                        cluck("Stopping because of DieOnError\n");
                        $stop = 1;
                        last;
                    }
                };
            }
            my $count = $self->mark_items_as_done(@done);
            warn "not all items removed from busy queue ($count)\n"
                if $count != @done;

            # put back the claimed but not touched items
            if (@items > 0) { print "not all items done!\n"; }
            for my $item (@items) {
                my $count = $self->mark_item_as_done($item);
                $self->_requeue_at_front($item) if $count;
            }
        }
    }
}

# methods to be used for cleanup script and Nagios checks
# the methods read or remove items from the busy queue
my %known_actions = map { $_ => undef } (qw(requeue fail drop));
sub handle_expired_items {
    my ($self, $timeout, $action, $options) = @_;
    $options ||= {};
    $timeout ||= 10;
    die "timeout should be a number> 0\n" if !int($timeout);
    die "unknown action\n" if !exists $known_actions{$action};
    my $conn = $self->_redis_conn;
    my @serial = $conn->lrange($self->_busy_queue, 0, -1);
    my $time = time;
    my %timetable = 
        map { reverse split /-/,$_,2 }
        $conn->lrange($self->_time_queue, 0, -1);
    my @match = grep { exists $timetable{$_} } @serial;
    my %match = map { $_ => undef } @match;
    my @timedout = grep { $time - $timetable{$_} >= $timeout } @match;
    my @log;

    # handle timed out items
    local $SIG{INT} = local $SIG{TERM} = 
        sub { print "stopping\n"; @timedout=(); };
    if ($action eq 'requeue') {
        for my $serial (@timedout) {
            my $n = $conn->lrem( $self->_busy_queue, 1, $serial);
            if ($n) {
                my $item = Queue::Q::ReliableFIFO::Item->new(
                    _serialized => $serial);
                $item->inc_nr_requeues();
                $conn->lpush($self->_main_queue, $item->_serialized);
                push @log, $item;
            }
        }
    }
    elsif ($action eq 'fail') {
        for my $serial (@timedout) {
            my $n = $conn->lrem( $self->_busy_queue, 1, $serial);
            if ($n) {
                $conn->lpush($self->failed_queue, $serial);
                push @log, 
                    Queue::Q::ReliableFIFO::Item->new(_serialized => $serial);
            }
        }
    }
    elsif ($action eq 'drop') {
        for my $serial (@timedout) {
            my $n = $conn->lrem( $self->_busy_queue, 1, $serial);
            push @log, Queue::Q::ReliableFIFO::Item->new(
                                _serialized => $serial) if ($n);
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
        for (grep { !exists $newtimetable{$_} } @serial);
    $conn->multi;
    $conn->del($self->_time_queue);
    $conn->lpush($self->_time_queue, join('-',$newtimetable{$_},$_))
        for (keys %newtimetable);
    $conn->exec;
    #FIXME the log info should also show what is done with the items
    # (e.g. dropped after requeue limit).
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
  my $action = 'requeue';
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
  $q->requeue_failed_items();

  # Nagios?
  $q->get_length_queue();
  $q->get_length_busy_queue();

  # Obsolete (consumer)
  my $item = $q->claim_item;
  my @items= $q->claim_items(100);
  my $foo  = $item->data;
  $q->mark_item_as_done($item);
  $q->mark_items_as_done(@items);

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

=head2 consume(\&callback, $action, %options)

This method is called by the consumer to consume the items of a
queue. For each item in the queue, the callback function will be
called. The function will receive that data of the queued item
as parameter.

The $action parameter is applied when the callback function returns
a "die". Allowed values are:

=over

=item * B<requeue>. I.e. do it again, the item will be put at the
tail of the queue. The requeue_limit property is the queue indicates 
the limit to how many times an item can be requeued.
The default is 5 times. You can change that by setting by calling
the set_queue_limit() method or by passing the property to the
constructor. When the requeue limit is reached, the item will be dropped.

=item * B<fail>. Put the item aside in another queue. Needs manual
intervention to reschedule. Or a cronjob to do that.

=item * B<drop>. Forget about it.

=back

=head3 option B<Chunk>

The Chunk option is used to set a chunk size for number of items
to claim and to mark as done in one go. This helps to fight latency.

=head3 option B<DieOnError>

If this option has a true value, the consumer will stop when the 
callback function returns a "die" call. Default "false".

Example:

    $q->consume(\&callback, 'requeue');

=head2 @item_obj = $q->handle_expired_items($timeout, $action [, %options]);

This method can be used by a cleanup job that ensures that items don't
stick forever in the B<busy> status. When an item has been in this status 
for $timeout seconds, the action specified by the $action will be done.
The $action parameter is the same as with the consume() method.

The method returns item object of type L<Queue::Q::ReliableFIFO::Item>
which has the item data as well as the time when it was queued at the first
time, how often it was requeued.

The optional %options parameter can be used to limit the amount of requeues:

    $q->handle_expired_items(60, 'requeue');

In that case an item will be requeued 5 times at most. After that the item
will be dropped. The cleanup script can log this together with the item data
by using the returned item objects.

=head2 my $count = $q->requeue_failed_items([ $limit ]);

This method will move items from the failed queue to the
working queue. The $limit parameter is optional and can be used to move
only a subset to the working queue.
The number of items actually moved will be the return value.

=head1 AUTHOR

Herald van der Breggen, E<lt>herald.vanderbreggen@booking.comE<gt>

Steffen Mueller, E<lt>smueller@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
