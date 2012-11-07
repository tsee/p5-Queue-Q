package Queue::Q::ReliableFIFO::Lua;
use Redis;
use File::Slurp;
use Digest::SHA1;
use Carp qw(croak);
use Class::XSAccessor {
    getters => [qw(redis_conn script_dir)]
};

sub new {
    my $class = shift;
    my $self = bless { @_ }, $class;
    $self->{script_dir} ||= $ENV{LUA_SCRIPT_DIR} || 
        croak("need a script dir, set env LUA_SCRIPT_DIR");
    $self->redis_conn || croak("need a redis connection");
    $self->register;
    return $self;
}

sub register {
    my $self = shift;
    my $name = shift || '*';
    for my $file (glob("$self->{script_dir}/$name.lua")) {
        my $script = read_file($file);
        my $sha1 = Digest::SHA1::sha1_hex($script);
        my ($found) = @{$self->redis_conn->script_exists($sha1)};
        if (!$found) {
            print "registering $file\n";
            my $rv = $self->redis_conn->script_load($script);
            croak("returned sha1 is different from ours!") if ($rv ne $sha1);
        }
        (my $call = $file) =~ s/\.lua$//;
        $call =~ s/^.*\///;
        $self->{call}{$call} = $sha1;
    }
}
sub call {
    my $self = shift;
    my $name = shift;
    $self->register($name) if not exists $self->{call}{$name};
    my $sha1 = $self->{call}{$name};
    croak("Unknown script $name") if ! $sha1;
    return $self->redis_conn->evalsha($sha1, @_);
}

1;
