#!/usr/bin/env perl
package Queue::Q::ReliableFIFO::CLI;
use strict;
use Redis;

use Queue::Q::ReliableFIFO::Redis;
use Class::XSAccessor {
    constructor => 'new',
    getters => [qw(conn level prompt queue type redis server port)],
    setters => { 
        set_conn => 'conn',
        set_prompt => 'prompt',
        set_queue => 'queue',
        set_type => 'type',
        set_redis => 'redis',
        set_server => 'server',
        set_port => 'port',
    },
};

# level 0: not connected
# level 1: connected
# level 2: in queue context
# level 3: in list (redis level) context

my @all_types = (qw(main busy failed ));
my %type = map { $_ => undef } @all_types;

sub open {
    my ($self, %params) = @_;
    $params{server} || die "missing server\n";
    $params{port}   ||= 6379;
    $self->set_conn( Redis->new(
        server => $params{server} . ':' . $params{port}));
    $self->set_server($params{server});
    $self->set_port($params{port});
    $self->set_level($self->conn ? 1 : 0);
}
sub close {
    my $self = shift;
    $self->set_conn(undef);
    $self->set_level(0);
}
sub ls {
    my $self = shift;
    if ($self->level == 0) {
        die "not connected";
    }
    elsif ($self->level == 1) {
        my %q = map { $_ => undef } 
                map { s/_(?:main|busy|failed|time)$//; $_ } 
                grep{ /_/ }
                map { $self->conn->keys('*' . '_' . $_) }
                @all_types;
        my @q = sort keys %q;
        return @q;
    }
    elsif ($self->level == 2) {
        return 
            map {  sprintf "%7s: %d items", 
                        $_, int($self->conn->llen($self->queue . '_' . $_)) 
            }
            @all_types;
    }
    elsif ($self->level == 3) {
        my $start = shift || -10;
        my $n = shift || 9;
        return reverse $self->conn->lrange(
            $self->_redisname, $start, $start + $n);
    }
}
sub cd {
    my $self = shift;
    die "not connected" if $self->level < 1;

    if ($self->level == 1) {
        my $queue = shift;
        my @q; push @q, "$queue$_" for (qw(_main _busy _time));
        my $found = 0;
        for (@q) {
            if ($self->conn->keys($_)) {
                $found = 1;
                last;
            }
        }
        if ($found) {
            $self->set_queue($queue);
            $self->set_redis(Queue::Q::ReliableFIFO::Redis->new(
                _redis_conn => $self->conn,
                server      => $self->server,
                port        => $self->port,
                queue_name  => $queue));
            $self->set_level(2);
        }
        else {
            die "$queue not found";
        }
    }
    elsif ($self->level == 2) {
        my $type = shift;
        if ($type eq '..') {
            $self->set_level(1);
            return;
        }
        if (exists $type{$type}) {
            $self->set_type($type);
            $self->set_level(3);
        }
        else { die "expected main|busy|time|failed" }
    }
    elsif ($self->level == 3) {
        my $dir = shift;
        if ($dir eq '..') {
            $self->set_level(2);
        }
        else {
            die "only cd .. allowed";
        }
    }
}
sub mv {
    my ($self, $from, $to, $limit) = @_;
    if ($self->level == 2) {
        my @d = split /\//,$from;
        die "$d[0]? expected main|busy|time|failed" if !exists $type{$d[0]};
        die "$to? expected main|busy|time|failed" if !exists $type{$to};
        return $self->_move($d[0], $to, $limit);
    }
    else {
        die "mv command not available at this level";
    }
}
sub _move {
    my ($self, $from, $to, $limit) = @_;
    my $conn = $self->conn;
    my $count = 0;
    $from = $self->_redisname($from);
    $to   = $self->_redisname($to);
    while ($conn->rpoplpush($from, $to)) {
        $count++;
        last if ($limit && $count >= $limit);
    }
    return $count;
}
sub rm {
    my ($self, $limit) = @_;
    my $conn = $self->conn;
    my $count = 0;
    while ($conn->rpop($self->_redisname)) {
        $count++;
        last if ($limit && $count >= $limit);
    }
    return $count;
}
sub cleanup {
    my ($self, $timeout, $action) = @_;
    die if $self->level < 2;
    my @items = $self->{redis}->handle_expired_items($timeout, $action);
    return scalar @items;
}
sub _redisname {
    my ($self, $type) = @_;
    $type ||= $self->type;
    return $self->queue . '_' . $type;
}
sub set_level {
    my ($self, $level) = @_;
    my $l = $self->{level} = $level;
    my $s = $self->server;
    my $p = $self->port;
    my $q = $self->queue;
    my $t = $self->type;
    my $prompt = '';
    if    ($l == 0) { $prompt = ''; }
    elsif ($l == 1) { $prompt = "$s:$p"; }
    elsif ($l == 2) { $prompt = "$self->{server}:$self->{port} \[/$q\]"; }
    elsif ($l == 3) { $prompt = "$self->{server}:$self->{port} \[/$q/$t\]"; }
    else { die "unknown level\n" }
    $self->set_prompt($prompt);
}
sub show_prompt {
    my $self = shift;
    return 'ReliableFIFO-', $self->prompt, " > ";
}
1;

package main;
use strict;
use Term::ReadKey;
use Data::Dumper;
use JSON::XS;
use File::Slurp;

my $cli = Queue::Q::ReliableFIFO::CLI->new();
$| = 1;

my @history;
my $his_pos = 0;

# take settings from previous session
my $conf_file = "$ENV{HOME}/.reliablefifo";
my %conf = ();
if (-f $conf_file) {
    %conf = %{decode_json(read_file($conf_file))};
    $cli->open(server => $conf{server},  port => $conf{port})
        if exists $conf{server} && exists $conf{port};
    $cli->cd($conf{queue}) if exists $conf{queue};
    $cli->cd($conf{type}) if exists $conf{type};
    push(@history, @{$conf{history}}) if exists $conf{history};
}

my %commands = map { $_ => undef } (qw(
    open
    ls
    cd
    close
    mv
    rm
    quit
    exit
    hist
    cleanup
    ?
));
my %help = (
    0 => ["open <server> [<port>]"],
    1 => ["ls", "cd <name>"],
    2 => ["ls", "cd <name>", "mv <name-from> <name-to> [<limit>]",
            "cleanup <timeout> <(requeue|fail|drop)>"],
    3 => ["ls", "cd ..", "rm [<limit>]"],
);
push(@{$help{$_}}, ("?", "hist", "quit")) for (0 .. 3);

ReadMode 4;
print "Type '?' for help\n";
print $cli->show_prompt;

while(1) {
    my $line;
    push @history, '';
    $his_pos = $#history;

    # deal with the keyboard
    while (1) {
        my $c = ReadKey(1);
        next if ! defined $c;
        my $ord = ord $c;

        if ($ord == 3 || $ord == 4) {   # ctrl-c, ctrl-d
            print "\n";
            quit();
        }
        elsif ($ord == 127) {
            print "\r" , $cli->show_prompt , ' ' x length($line);
            $line = substr($line, 0, length($line)-1);
            print "\r", $cli->show_prompt, $line;
        }
        elsif ($ord == 27) {
            my $a = ord(ReadKey (1));
            if ($a == 91) {
                my $b = ord(ReadKey (1));
                if ($b == 65 || $b == 66) {
                    # arrow keys up/down
                    print "\r" , $cli->show_prompt , ' ' x length($line);
                    if ($b == 65) {  # up
                        $history[$his_pos] = $line if $his_pos == $#history;
                        $his_pos-- if $his_pos > 0;
                    }
                    else {
                        $his_pos++ if $his_pos < $#history;;
                    }
                    $line = $history[$his_pos];
                    print "\r" ,  $cli->show_prompt , $line;
                }
            } # else ignore
        }
        elsif ($ord == 10) { #LF
            $his_pos = $#history;
            $history[$his_pos] = $line;
            print $c;
            last;
        }
        else {
            $line .= $c;
            print $c;
        }
    }

    # deal with the command
    my ($cmd, @args) = split /\s+/, $line;
    if ($cmd) {
        if (exists $commands{$cmd}) {
            eval {
                if ($cmd eq "open") {
                    $cli->open(server => $args[0], port => $args[1]);
                }
                elsif ($cmd eq "cd") {
                    $cli->cd(@args);
                }
                elsif ($cmd eq "rm") {
                    printf "%d items removed\n", $cli->rm(@args);
                }
                elsif ($cmd eq "mv") {
                    printf "%d items moved\n", $cli->mv(@args);
                }
                elsif ($cmd eq 'ls') {
                    print join("\n", $cli->ls(@args)), "\n";
                }
                elsif ($cmd eq 'cleanup') {
                    my $n = $cli->cleanup(@args);
                    printf "%d items affected\n", $n;
                    print "Try again after <timeout> seconds\n" if ($n ==0);
                }
                elsif ($cmd eq 'close') {
                    $cli->close(), "\n";
                }
                elsif ($cmd eq 'quit' || $cmd eq 'exit') {
                    quit();
                }
                elsif ($cmd eq 'hist') {
                    print "\t", join("\n\t", @history), "\n";
                }
                elsif ($cmd eq '?') {
                    print "available commands at this level:\n\t",
                        join("\n\t", @{$help{$cli->level||0}}), "\n";
                }
                1;
            }
            or do {
                print "$@\n";
            }
        }
        else {
            print "unknown command $cmd\n";
        }
    }
    print $cli->show_prompt;
}
quit();

sub quit {
    ReadMode 0;

    # save setting for next session
    %conf = ();
    if ($cli->level >= 1) {
        $conf{server} = $cli->server;
        $conf{port} = $cli->port;
        $conf{queue} = $cli->queue if $cli->level >= 2;
        $conf{type} = $cli->type if $cli->level >= 3;
    }
    splice(@history, 0, $#history-20) if $#history > 20; # limit history
    $conf{history} = \@history;
    write_file($conf_file, encode_json(\%conf));

    exit;
}

__END__

