package Queue::Q::ClaimFIFO::Item;
use strict;
use warnings;
use Sereal::Decoder;
use Sereal::Encoder;
use Digest::MD5 (); # much faster than sha1 and good enough

use Exporter;
use parent 'Exporter';
our @EXPORT_OK = qw(make_item);
our %EXPORT_TAGS = (all => \@EXPORT_OK);

use Class::XSAccessor {
    constructor => 'new',
    getters => [qw(item_data)],
};

our $SerealEncoder = Sereal::Encoder->new;
our $SerealDecoder = Sereal::Decoder->new;
our $MD5 = Digest::MD5->new;

# not a method
sub make_item {
    return Queue::Q::ClaimFIFO::Item->new(
        item_data => shift()
    );
}

# for "friends" only
sub _key {
    my $self = shift;
    return $self->{_key}
        if defined $self->{_key};
    $MD5->add($self->_serialized_data);
    $MD5->add(rand(), time());
    return( $self->{_key} = $MD5->digest );
}

# for "friends" only
sub _serialized_data {
    my $self = shift;
    return $self->{_serialized_data}
        if defined $self->{_serialized_data};
    return( $self->{_serialized_data} = $self->_serialize_data($self->item_data) );
}

# for "friends" only
sub _serialize_data {
    my $self = shift;
    return $SerealEncoder->encode($_[0]);
}

# for "friends" only
sub _deserialize_data {
    my $self = shift;
    return $SerealDecoder->decode($_[0]);
}

