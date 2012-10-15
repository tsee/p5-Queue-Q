package Queue::Q::NaiveFIFO::Redis;
use strict;
use warnings;
use Carp qw(croak);

use Queue::Q::NaiveFIFO;
use parent 'Queue::Q::NaiveFIFO';

use Redis;
use Sereal::Encoder;
use Sereal::Decoder;

our $SerealEncoder;
our $SerealDecoder;

use Class::XSAccessor {
    getters => [qw(server port queue_name db _redis_conn)],
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
    my ($blob) = $self->_serialize($_[0]);
    $self->_redis_conn->lpush($self->queue_name, $blob);
}

sub enqueue_items {
    my $self = shift;
    return if not @_;
    my $qn = $self->queue_name;
    my $conn = $self->_redis_conn;
    my @blobs = $self->_serialize(@_);
    $conn->lpush($qn, @blobs);
}

sub claim_item {
    my $self = shift;
    my ($rv) = $self->_deserialize( $self->_redis_conn->rpop($self->queue_name) );
    return $rv;
}

sub claim_items {
    my ($self, $n) = @_;
    $n ||= 1;
    my $conn = $self->_redis_conn;
    my $qn = $self->queue_name;
    my @elem;
    $conn->rpop($qn, sub {push @elem, $_[0]}) for 1..$n;
    $conn->wait_all_responses;
    return $self->_deserialize( @elem );
}

sub flush_queue {
    my $self = shift;
    $self->_redis_conn->del($self->queue_name);
}

sub queue_len {
    my $self = shift;
    my ($len) = $self->_redis_conn->llen($self->queue_name);
    return $len;
}

sub _serialize {
    my $self = shift;
    $SerealEncoder ||= Sereal::Encoder->new({stringify_undef => 1, warn_undef => 1});
    return map $SerealEncoder->encode($_), @_;
}

sub _deserialize {
    my $self = shift;
    $SerealDecoder ||= Sereal::Decoder->new();
    return map defined($_) ? $SerealDecoder->decode($_) : $_, @_;
}

1;
