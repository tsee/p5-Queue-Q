#!/usr/bin/perl 
#
# Script to be used by Nagios to check queue length
#
use strict;

use Redis;
use Getopt::Std;
use Data::Dumper;

use constant {
    OK => 1,
    WARNING => 1,
    CRITICAL => 2,
    UNKNOWN => 3
};

my %opts;
getopts('H:p:q:w:c:?hv', \%opts);

#print Dumper(\%opts);
usage() if ( exists $opts{h} || exists $opts{'?'}
    || !exists $opts{H}) || !exists $opts{q};

my $VERBOSE = exists $opts{v};

# Defaults
$opts{p} ||= '6379';
$opts{w} ||= '100';
$opts{c} ||= '500';
for (qw(p w c)) {
    if ($opts{$_} =~ m/^[^0-9]+$/) {
        warn "Invalid value for option $_\n";
        exit UNKNOWN;
    }
}

if ($opts{w} > $opts{c}) {
    warn "warn level ($opts{w}) should be lower than"
    . " critical level ($opts{c})\n";
    usage();
}

my $service = "$opts{H}:$opts{p}";

my $conn = Redis->new( server => $service, reconnect => 3);
    
if (!$conn) {
    warn "can't connect to $service\n";
    exit CRITICAL;
}

my $len = $conn->llen($opts{q});

print "$opts{q} has length $len\n" if $VERBOSE;

if ($len < $opts{w}) {
    print "$len is OK\n" if $VERBOSE;
    exit OK;
}
elsif ($len < $opts{c}) {
    print "$len is WARNING\n" if $VERBOSE;
    exit WARNING;
}
else {
    print "$len is CRITICAL\n" if $VERBOSE;
    exit CRITICAL;
}

sub usage {
    print "usage $0 -H host -q queue_name [-p port] [-w len] [-c len] [-v]\n";
    exit;
}
