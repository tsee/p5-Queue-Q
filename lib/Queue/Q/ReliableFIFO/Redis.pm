package Queue::Q::ReliableFIFO::Redis;
use strict;
use warnings;
use Carp qw(croak cluck);

use parent 'Queue::Q::ReliableFIFO';
use Queue::Q::ReliableFIFO::Item;
use Queue::Q::ReliableFIFO::Lua;
use Redis;
use Time::HiRes;
use Data::Dumper;

use Class::XSAccessor {
    getters => [qw(
                server
                port
                db
                queue_name
                busy_expiry_time
                claim_wait_timeout
                requeue_limit
                redis_conn 
                redis_options
                _main_queue
                _busy_queue
                _failed_queue
                _time_queue
                _script_cache
                )],
    setters => { 
        set_requeue_limit => 'requeue_limit',
        set_busy_expiry_time => 'busy_expiry_time',
        set_claim_wait_timeout => 'claim_wait_timeout',
    }
};
my %queue_type = map { $_ => undef } (qw(main busy failed time));

my %allowed_new_params = map { $_ => undef } (qw(
    server port db queue_name busy_expiry_time
    claim_wait_timeout requeue_limit redis_conn redis_options));
my %required_new_params = map { $_ => undef } (qw(server port queue_name));

sub new {
    my ($class, %params) = @_;
    for (keys %required_new_params) {
        croak("Need '$_' parameter")
            if not exists $params{$_};
    }
    for (keys %params) {
        croak("Unknown parameter $_")
            if not exists $allowed_new_params{$_};
    }

    my $self = bless({
        requeue_limit => 5,
        busy_expiry_time => 30,
        claim_wait_timeout => 1,
        db => 0,
        %params
    } => $class);
    $self->{"_$_" . '_queue'}   = $params{queue_name} . "_$_"
        for (keys %queue_type);

    $self->{redis_options} ||= { reconnect => 60 }; # auto reconnect
    $self->{redis_conn} ||= Redis->new(
            %{$self->{redis_options}},
            encoding => undef, # force undef for binary data
            server => join(":", $self->server, $self->port),
    );

    $self->redis_conn->select($self->db) if $self->db;

    $self->{_lua}
        = Queue::Q::ReliableFIFO::Lua->new(redis_conn => $self->redis_conn);

    return $self;
}

sub enqueue_item {
    my $self = shift;
    return if not @_;

    return $self->redis_conn->lpush(
        $self->_main_queue,
        map { Queue::Q::ReliableFIFO::Item->new(data => $_)->_serialized } @_
    );
}

sub claim_item {
    my ($self, $n) = @_;
    $n ||= 1;
    my $timeout = $self->claim_wait_timeout;
    if ($n == 1) {
        # rpoplpush gives higher throughput than the blocking version
        # (i.e. brpoplpush). So use the blocked version only when we
        # need to wait.
        my $v = 
            $self->redis_conn->rpoplpush($self->_main_queue, $self->_busy_queue)
            ||
            $self->redis_conn->brpoplpush(
                $self->_main_queue, $self->_busy_queue, $timeout);
        return if !$v;
        my $item;
        eval { ($item) = Queue::Q::ReliableFIFO::Item->new(_serialized => $v);};
        return $item;
    }
    else {
        my $conn = $self->redis_conn;
        my $qn = $self->_main_queue;
        my $bq = $self->_busy_queue;
        my @items;
        my $serial;
        if ($n > 100) {
            my ($l) = $self->redis_conn->llen($qn);
            $n = $l if $l < $n;
        }
        eval {
            $conn->rpoplpush($qn, $bq, sub {
                if (defined $_[0]) {
                    push @items, 
                    Queue::Q::ReliableFIFO::Item->new(_serialized => $_[0])
                }
            }) for 1..$n;
            $conn->wait_all_responses;
            if (@items == 0) {
                # list seems empty, use the blocking version
                $serial = $conn->brpoplpush($qn, $bq, $timeout);
                if (defined $serial) {
                    push(@items,
                        Queue::Q::ReliableFIFO::Item->new(_serialized => $serial));
                    undef $serial;
                    $conn->rpoplpush($qn, $bq, sub {
                        if (defined $_[0]) {
                            push @items, 
                                Queue::Q::ReliableFIFO::Item->new(
                                    _serialized => $_[0]);
                        }
                    }) for 1 .. ($n-1);
                    $conn->wait_all_responses;
                }
            }
            1;
        }
        or do {
            return @items;  # return with whatever we have...
        };
        return @items;
    }
}

sub mark_item_as_done {
    my $self = shift;
    if (@_ == 1) {
        return $self->redis_conn->lrem(
            $self->_busy_queue, -1, $_[0]->_serialized);
    }
    else {
        my $conn = $self->redis_conn;
        my $count = 0;
        $conn->lrem(
            $self->_busy_queue, -1, $_->_serialized, sub { $count += $_[0] })
                for @_;
        $conn->wait_all_responses;
        return $count;
    }
}

sub unclaim  { 
    my $self = shift;
    return $self->__requeue_busy(1, undef, @_);
}

sub requeue_busy_item {
    my ($self, $raw) = @_;
    return $self->__requeue_busy(0, undef, $raw);
}
sub requeue_busy {
    my $self = shift;
    return $self->__requeue_busy(0, undef, @_);
}
sub requeue_busy_error {
    my $self = shift;
    my $error= shift;
    return $self->__requeue_busy(0, $error, @_);
}

sub __requeue_busy  { 
    my $self = shift; 
    my $place = shift;  # 0: producer side, 1: consumer side
    my $error = shift;  # error message
    my $n = 0;
    eval {
        $n += $self->{_lua}->call(
            'requeue_busy',
            3, 
            $self->_busy_queue,
            $self->_main_queue,
            $self->_failed_queue,
            time(),
            $_->_serialized, 
            $self->requeue_limit,
            $place,
            $error || '',
        ) for @_;
        1;
    }
    or do {
        cluck("lua call went wrong! $@");
    };
    return $n;
}

sub requeue_failed_item {
    my $self = shift;
    my $n = 0;
    eval {
        $n += $self->{_lua}->call(
            'requeue_failed_item',
            2, 
            $self->_failed_queue,
            $self->_main_queue,
            time(),
            $_->_serialized, 
            $self->requeue_limit,
        ) for @_;
        1;
    }
    or do {
        cluck("lua call went wrong! $@");
    };
    return $n;
}

sub requeue_failed_items {
    my $self = shift;
    my $limit = shift || 0;
    return $self->{_lua}->call(
        'requeue_failed',
        2,
        $self->_failed_queue,
        $self->_main_queue,
        time(),
        $limit);
}

sub get_and_flush_failed_items {
    my ($self, %options) = @_;
    my $min_age = delete $options{MinAge} || 0;
    my $min_fc = delete $options{MinFailCount} || 0;
    my $now = time();
    die "Unsupported option(s): " . join(', ', keys %options) . "\n"
        if (keys %options);
    my @failures = 
        grep { $_->time_created < ($now-$min_age) 
                || $_->fail_count >= $min_fc }
        $self->raw_items_failed();
    my $conn = $self->redis_conn;
    my $name = $self->_failed_queue;
    $conn->multi;
    $conn->lrem($name, -1, $_->_serialized) for (@failures);
    $conn->exec;
    return @failures;
}

sub flush_queue {
    my $self = shift;
    my $conn = $self->redis_conn;
    $conn->multi;
    $conn->del($_)
        for ($self->_main_queue, $self->_busy_queue,
             $self->_failed_queue, $self->_time_queue);
    $conn->exec;
    return;
}

sub queue_length {
    my ($self, $type) = @_;
    __validate_type(\$type);
    my $qn = $self->queue_name . "_$type";
    my ($len) = $self->redis_conn->llen($qn);
    return $len;
}

sub age {
    my ($self, $type) = @_;
    # this function returns age of oldest item in the queue (in seconds)
    __validate_type(\$type);
    my $qn = $self->queue_name . "_$type";

    # take oldest item
    my ($serial) = $self->redis_conn->lrange($qn,-1,-1);
    return 0 if ! $serial;    # empty queue, so age 0

    my $item = Queue::Q::ReliableFIFO::Item->new(_serialized => $serial);
    return time() - $item->time_queued;
}

sub raw_items_main {
    my $self = shift;
    return $self->_raw_items('main', @_);
}
sub raw_items_busy {
    my $self = shift;
    return $self->_raw_items('busy', @_);
}
sub raw_items_failed {
    my $self = shift;
    return $self->_raw_items('failed', @_);
}
sub _raw_items {
    my ($self, $type, $n) = @_;
    __validate_type(\$type);
    $n ||= 0;
    my $qn = $self->queue_name . "_$type";
    return 
        map { Queue::Q::ReliableFIFO::Item->new(_serialized => $_); }
        $self->redis_conn->lrange($qn, -$n, -1);
}

sub __validate_type {
    my $type = shift;
    $$type ||= 'main';
    croak("Unknown queue type $$type") if ! exists $queue_type{$$type};
}

sub memory_usage_perc {
    my $self = shift;
    my $conn = $self->redis_conn;
    my $info = $conn->info('memory');
    my $mem_used = $info->{used_memory};
    my (undef, $mem_avail) = $conn->config('get', 'maxmemory');
    return 100 if $mem_avail == 0; # if nothing is available, it's full!
    return $mem_used * 100 / $mem_avail;
}

my %valid_options       = map { $_ => 1 } (qw(
    Chunk DieOnError MaxItems MaxSeconds ProcessAll Pause ReturnWhenEmpty));
my %valid_error_actions = map { $_ => 1 } (qw(drop requeue ));

sub consume {
    my ($self, $callback, $error_action, $options) = @_;
    # validation of input
    $error_action ||= 'requeue';
    croak("Unknown error action")
        if ! exists $valid_error_actions{$error_action};
    my %error_subs = (
        'drop'    => sub { my ($self, $item) = @_;
                            $self->mark_item_as_done($item); },
        'requeue' => sub { my ($self, $item, $error) = @_;
                           $self->requeue_busy_error($error, $item); },
    );
    my $onerror = $error_subs{$error_action} 
        || croak("no handler for $error_action");

    $options ||= {};
    my $chunk       = delete $options->{Chunk} || 1;
    croak("Chunk should be a number > 0") if (! $chunk > 0);
    my $die         = delete $options->{DieOnError} || 0;
    my $maxitems    = delete $options->{MaxItems} || -1;
    my $maxseconds  = delete $options->{MaxSeconds} || 0;
    my $pause       = delete $options->{Pause} || 0;
    my $process_all = delete $options->{ProcessAll} || 0;
    my $return_when_empty= delete $options->{ReturnWhenEmpty} || 0;
    croak("Option ProcessAll without Chunks does not make sense")
        if $process_all && $chunk <= 1;
    croak("Option Pause does without Chunks does not make sense")
        if $pause && $chunk <= 1;

    for (keys %$options) {
        croak("Unknown option $_") if !$valid_options{$_};
    }
    my $stop_time = $maxseconds > 0 ? time() + $maxseconds : 0;

    # Now we can start...
    my $stop = 0;
    my $MAX_RECONNECT = 60;
    local $SIG{INT} = local $SIG{TERM} = sub { print "stopping\n"; $stop = 1; };
    if ($chunk == 1) {
        my $die_afterwards = 0;
        while(!$stop) {
            my $item = eval { $self->claim_item(); };
            if (!$item) {
                last if $return_when_empty
                            || ($stop_time > 0 && time() >= $stop_time);
                next;    # nothing claimed this time, try again
            }
            my $ok = eval { $callback->($item->data); 1; };
            if (!$ok) {
                my $error = _clean_error($@);
                for (1 .. $MAX_RECONNECT) {    # retry if connection is lost
                    eval { $onerror->($self, $item, $error); 1; }
                    or do {
                        last if $stop;
                        sleep 1;
                        next;
                    };
                    last;
                }
                if ($die) {
                    $stop = 1;
                    cluck("Stopping because of DieOnError\n");
                }
            } else {
                for (1 .. $MAX_RECONNECT) {    # retry if connection is lost
                    eval { $self->mark_item_as_done($item); 1; }
                    or do {
                        last if $stop;
                        sleep 1;
                        next;
                    };
                    last;
                }
            }
            $stop = 1 if ($maxitems > 0 && --$maxitems == 0) 
                            || ($stop_time > 0 && time() >= $stop_time);
        }
    }
    else {
        my $die_afterwards = 0;
        my $t0 = Time::HiRes::time();
        while(!$stop) {
            my @items;

            # give queue some time to grow
            if ($pause) {
                my $pt = ($pause - (Time::HiRes::time()-$t0))*1e6;
                Time::HiRes::usleep($pt) if $pt > 0;
            }

            eval { @items = $self->claim_item($chunk); 1; }
            or do {
                print "error with claim\n";
            };
            $t0 = Time::HiRes::time() if $pause; # only relevant for pause
            if (@items == 0) {
                last if $return_when_empty
                            || ($stop_time > 0 && time() >= $stop_time);
                next;    # nothing claimed this time, try again
            }
            my @done;
            if ($process_all) {
                # process all items in one call (option ProcessAll)
                my $ok = eval { $callback->(map { $_->data } @items); 1; };
                if ($ok) {
                    @done = splice @items;
                }
                else {
                    # we need to call onerror for all items now
                    @done = (); # consider all items failed
                    my $error = _clean_error($@);
                    while (my $item = shift @items) {
                        for (1 .. $MAX_RECONNECT) {
                            eval { $onerror->($self, $item, $error); 1; }
                            or do {
                                last if $stop; # items might stay in busy mode
                                sleep 1;
                                next;
                            };
                            last;
                        }
                        if ($die) {
                            cluck("Stopping because of DieOnError\n");
                            $stop = 1;
                            last;
                        }
                    }
                }
            }
            else {
                # normal case: process items one by one
                while (my $item = shift @items) {
                    my $ok = eval { $callback->($item->data); 1; };
                    if ($ok) {
                        push @done, $item;
                    }
                    else {
                        my $error = _clean_error($@);
                        # retry if connection is lost
                        for (1 .. $MAX_RECONNECT) {
                            eval { $onerror->($self, $item, $error); 1; }
                            or do {
                                last if $stop;
                                sleep 1;
                                next;
                            };
                            last;
                        }
                        if ($die) {
                            cluck("Stopping because of DieOnError\n");
                            $stop = 1;
                            last;
                        }
                    };
                }
            }
            my $count = 0;
            for (1 .. $MAX_RECONNECT) {
                eval { $count += $self->mark_item_as_done(@done); 1; }
                or do {
                    last if $stop;
                    sleep 1;
                    next;
                };
                last;
            }
            warn "not all items removed from busy queue ($count)\n"
                if $count != @done;

            # put back the claimed but not touched items
            if (@items > 0) {
                my $n = @items;
                print "unclaiming $n items\n";
                for (1 .. $MAX_RECONNECT) {
                    eval { $self->unclaim($_) for @items; 1; }
                    or do {
                        last if $stop;
                        sleep 1;
                        next;
                    };
                    last;
                }
            }
            $stop = 1 if ($maxitems > 0 && ($maxitems -= $chunk) <= 0)
                            || ($stop_time > 0 && time() >= $stop_time);
        }
    }
}

sub _clean_error {
    $_[0] =~ s/, <GEN0> line [0-9]+//;
    chomp $_[0];
    return $_[0];
}

# methods to be used for cleanup script and Nagios checks
# the methods read or remove items from the busy queue
my %known_actions = map { $_ => undef } (qw(requeue drop));
sub handle_expired_items {
    my ($self, $timeout, $action) = @_;
    $timeout ||= 10;
    die "timeout should be a number> 0\n" if !int($timeout);
    die "unknown action\n" if !exists $known_actions{$action};
    my $conn = $self->redis_conn;
    my @serial = $conn->lrange($self->_busy_queue, 0, -1);
    my $time = time;
    my %timetable = 
        map { reverse split /-/,$_,2 }
        $conn->lrange($self->_time_queue, 0, -1);
    my @match = grep { exists $timetable{$_} } @serial;
    my %match = map { $_ => undef } @match;
    my @timedout = grep { $time - $timetable{$_} >= $timeout } @match;
    my @log;

    if ($action eq 'requeue') {
        for my $serial (@timedout) {
            my $item = Queue::Q::ReliableFIFO::Item->new(
                _serialized => $serial);
            my $n = $self->requeue_busy_item($item);
            push @log, $item if $n;
        }
    }
    elsif ($action eq 'drop') {
        for my $serial (@timedout) {
            my $n = $conn->lrem( $self->_busy_queue, -1, $serial);
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
  # You can pass any JSON-serializable data structure
  $q->enqueue_item({ bar => "baz" }); 
  $q->enqueue_item({ id=> 12},{id =>34});   # two items
  # get rid of everything in the queue:
  $q->flush_queue();    # get a clean state, removes queue

  # Consumer:
  $q->consume(\&callback);
  $q->consume(
    sub { my $data = shift; print 'Received: ', Dumper($data); });

  # Cleanup script
  my $action = 'requeue';
  while (1) {
      my @handled_items = $q->handle_expired_items($timeout, $action);
      for my $item (@handled_items) {
          printf "%s: item %s, in queue since %s, requeued % times\n", 
              $action, Dumper($item->data),
              scalar localtime $item->time,
              $item->requeue_count;
      }
      sleep(60);
  }
  # retry items that failed before:
  $q->requeue_failed_items();
  $q->requeue_failed_items(10); # only the first 10 items

  # Nagios?
  $q->queue_length();
  $q->queue_length('failed');

  # Depricated (consumer)
  my $item = $q->claim_item;
  my @items= $q->claim_item(100);
  my $foo  = $item->data;
  $q->mark_item_as_done($item);     # single item
  $q->mark_item_as_done(@items);    # multiple items

=head1 DESCRIPTION

Implements interface defined in L<Queue::Q::ReliableFIFO>:
an implementation based on Redis.

The data structures passed to C<enqueue_item> are serialized
using JSON (cf. L<JSON::XS>), so
any data structures supported by that can be enqueued.
We use JSON because that is supported at the lua side as well (the cjson
library).

The implementation is kept very lightweight at the Redis level
in order to get a hight throughput. With this implementation it is
easy to get a throughput of 10,000 items per second on a single core.

At the Redis side this is basically done at the following events:

=over

=item putting an item: lput

=item getting an item: (b)rpoplpush

=item mark as done: lrem

=item mark an item as failed: lrem + lpush

=item requeue an item: lrem + lpush (or lrem + rpush)

=back

Note that only exceptions need multiple commands.

To detect hanging items, a cronjob is needed, looking at how long items
stay in the busy status.

The queues are implemented as list data structures in Redis. The lists
ave as name the queue name plus an extension. The extension is:

 _main for the working queue
 _busy for the list with items that are claimed but not finished
 _failed for the items that failed

There can also be a list with extension "_time" if a cronjob is monitoring
how long items are in the busy list (see method handle_expired_items()).

=head1 METHODS

B<Important note>:
At the Redis level a lost connection will always throw an
exception, even if auto-reconnect is switched on.
As consequence, the methods that do redis commands, like
enqueue_item(), claim_item() and 
mark_item_as_done(), will throw an exception when the connection to the
Redis server is lost. The consume() method handles these exceptions.
For other methods you need to catch and handle the exception.

All methods of L<Queue::Q::ReliableFIFO>. Other methods are:

=head2 new

Constructor. Takes named parameters. Required parameters are

=over

=item the B<server> hostname or address

=item the Redis B<port>, and

=item the name of the Redis key to use as the B<queue_name>.

=back

Optional parameters are

=over

=item a Redis B<db> number to use. C<Default value is 0>.

=item B<redis_options> for connection options

=item B<redis_connection> for reusing an existing redis connection

=item B<requeue_limit> to specify how often an item is allowed to
enter the queu again before ending up in the failed queue.
C<Default value is 5>.

=item B<claim_wait_timeout> (in seconds) to specify how long the
claim_item() method is allowed to wait before it returns.
This applies to the situation with an empty queue.
A value of "0" means "wait forever".
C<Default value is 1>.

=item B<busy_expiry_time> to specify the threshold (in seconds)
after which an item is supposed to get stuck. After this time a follow
up strategy should be applied. (Normally done by the handle_expired_items()
Method, typically done by a cronjob).
C<Default value is 30>.

=back

=head2 enqueue_item(@items)

Special for the Redis imlementation is that the C<return value> is 
the length of the queue after the items are added.

=head2 consume(\&callback, $action, %options)

This method is called by the consumer to consume the items of a
queue. For each item in the queue, the callback function will be
called. The function will receive that data of the queued item
as parameter. While the consume method deals with the queue related
actions, like claiming, "marking as done" etc, the callback function
only deals with processing the item.

The $action parameter is applied when the callback function returns
a "die". Allowed values are:

By default, the consume method will keep on reading the queue forever or
until the process receives a SIGINT or SIGTERM signal. You can make the
consume method return ealier by using one of the options MaxItems,
MaxSeconds or ReturnWhenEmpty. If you still want to have a "near real time"
behavior you need to make sure there are always consumers running,
which can be achived using cron and IPC::ConcurrencyLimit::WithStandby.

This method also uses B<claim_wait_timeout>.

=over

=item * B<requeue>. (C<Default>). I.e. do it again, the item will be put at the
tail of the queue. The requeue_limit property is the queue indicates 
the limit to how many times an item can be requeued.
The default is 5 times. You can change that by setting by calling
the set_queue_limit() method or by passing the property to the
constructor. When the requeue limit is reached, the item will go 
to the failed queue.

Note: by setting the queue_limit to "0" you can force the item to
go to the "failed" status right away (without being requeued).

=item * B<drop>. Forget about it.

=back


=head3 options

=over

=item * B<Chunk>.  The Chunk option is used to set a chunk size
for number of items to claim and to mark as done in one go.
This helps to fight latency.

=item * B<DieOnError>.  
If this option has a true value, the consumer will stop when the 
callback function returns a "die" call. Default "false".

=item * B<MaxItems>
This can be used to limit the consume method to process only a limited amount
of items, which can be useful in cases of memory leaks. When you use
this option, you will probably need to look into restarting 
strategies with cron. Of course this comes with delays in handling the
items.

=item * B<MaxSeconds>
This can be used to limit the consume method to process items for a limited
amount of time.

=item * B<ReturnWhenEmpty>
Use this when you want to let consume() return when the queue is empty.
Note that comsume will wait for 
claim_wait_timeout seconds until it can come to the conclusion
that the queue is empty.

=item * B<Pause>.
This can be used to give the queue some time to grow, so that more
items at the time can be claimed. The value of Pause is in seconds and 
can be a fraction of a second (e.g. 0.5).
Default value is 0. This option only makes sense if
larger Chunks are read from the queue (so together with option "Chunk").

=item * B<ProcessAll>.
This can be used to process all claimed items by one invocation of
the callback function. The value of ProcessAll should be a true or false 
value. Default value is "0". Note that this changes the @_ content of
callback: normally the callback function is called with one item
data structure, while in this case @_ will contain an array with item
data structures.
This  option only makes sense if larger Chunks are read from the queue
(so together with option "Chunk").

=back

Examples:

    $q->consume(\&callback,);
    $q->consume(\&callback, 'requeue'); # same, because 'requeue' is default

    # takes 50 items a time. Faster because less latency
    $q->consume(\&callback, 'requeue', { Chunk => 50 });

=head2 @item_obj = $q->handle_expired_items($timeout, $action);

This method can be used by a cleanup job to ensure that items don't
stick forever in the B<busy> status. When an item has been in this status 
for $timeout seconds, the action specified by the $action will be done.
The $action parameter is the same as with the consume() method.

The method returns item objects of type L<Queue::Q::ReliableFIFO::Item>
which has the item data as well as the time when it was queued at the first
time, how often it was requeued.

To set/change the limit of how often an item can be requeued, use the
requeue_limit parameter in the new() constructor or use the method
set_requeue_limit.

Once an item is moved to the failed queue, the counter is reset. The
item can be put back into the main queue by using the requeue_failed_items()
method (or via the CLI). Then it will be retried again up to 
requeue_limit times.

=head2 my $count = $q->unclaim(@items)

This method puts the items that are passed, back to the queue
at the consumer side,
so that they can be picked up a.s.a.p. This method is e.g. be used
when a chunk of items are claimed but the consumer aborts before all
items are processed.

=head2 my $count = $q->requeue_busy(@items)

This puts items that are claimed back to the queue so that other consumers
can pick this up. In this case the items are put at the back of the queue,
so depending the queue length it can take some time before it is 
available for consumers.

=head2 my $count = $q->requeue_failed_items([ $limit ]);

This method will move items from the failed queue to the
working queue. The $limit parameter is optional and can be used to move
only a subset to the working queue.
The number of items actually moved will be the return value.

=head2 my $count = $q->requeue_failed_item(@raw_items)

This method will move the specified item to the main queue so that it
will be processed again. It will return 0 or 1 (i.e. the number of items
moved).

=head2 my $count = $q->requeue_busy_item($raw_item)

Same as requeue_busy, it only accepts one value instead of a list.

=head2 my @raw_failed_items = $q->get_and_flush_failed_items(%options);

This method will read all existing failed items and remove all failed
items right after that.
Typical use could be a cronjob that warns about failed items
(e.g. via email) and cleans them up.

Supported options:

=over 2

=item * B<MaxAge> => $seconds
Only the failed items that are older then $seconds will be retrieved and
removed.

=item * B<MinFailCount> => $n
Only the items that failed at least $n times will be retrieved and removed.

=back

If both options are used, only one of the two needs to be true to retrieve and remove an item.

=head2 my $age = $q->age($queue_name [,$type]);

This methods returns maximum wait time of items in the queue. This
method will simply lookup the item in the head of the queue (i.e.
at the comsumer side of the queue) and will return the age of that item.
So this is a relatively cheap method.

=head2 my @raw_items = $q->raw_items_busy( [$max_number] );

Returns objects of type Queue::Q::RelaibleFIFO::Item from the busy list.
You can limit the number of items by passing the limit to the method.

=head2 my @raw_items = $q->raw_items_failed( [$max_number] );

Similar to raw_items_busy() but for failed items.

=head2 my @raw_items = $q->raw_items_main( [$max_number] );

Similar to raw_items_busy() but for items in the working queue. Note that
the main queue can be large, so a limit is strongly recommended here.

=head2 my $memory_usage = $q->memory_usage_perc();

Returns the memory usage percentage of the redis instance where the queue
is located.

=head1 AUTHOR

Herald van der Breggen, E<lt>herald.vanderbreggen@booking.comE<gt>

Steffen Mueller, E<lt>smueller@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
