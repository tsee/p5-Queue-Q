package Queue::Q::ReliableFIFO::Lua;
use Redis;
use File::Slurp;
use Digest::SHA1;
use Carp qw(croak);
use Class::XSAccessor {
    getters => [qw(redis_conn script_dir)]
};
my %scripts;

sub new {
    my $class = shift;
    my $self = bless { @_ }, $class;
    $self->redis_conn || croak("need a redis connection");
    $self->{script_dir} ||= $ENV{LUA_SCRIPT_DIR};
    $self->{call} ||= {};
    $self->register;
    return $self;
}

sub register {
    my $self = shift;
    my $name = shift;
    if ($self->script_dir) {
        $name ||= '*';
        for my $file (glob("$self->{script_dir}/$name.lua")) {
            my $script = read_file($file);
            my $sha1 = Digest::SHA1::sha1_hex($script);
            my ($found) = @{$self->redis_conn->script_exists($sha1)};
            if (!$found) {
                print "registering $file\n";
                my $rv = $self->redis_conn->script_load($script);
                croak("returned sha1 is different from ours!")
                    if ($rv ne $sha1);
            }
            (my $call = $file) =~ s/\.lua$//;
            $call =~ s/^.*\///;
            $self->{call}{$call} = $sha1;
        }
    }
    else {
        croak("script $name not found") if $name && !exists $scripts{$name};
        my @names = $name ? ($name) : (keys %script);
        for my $scr_name (@names) {
            my $script = $scripts{$scr_name};
            my $sha1 = Digest::SHA1::sha1_hex($script);
            my ($found) = @{$self->redis_conn->script_exists($sha1)};
            if (!$found) {
                my $rv = $self->redis_conn->script_load($script);
                croak("returned sha1 is different from ours!") 
                    if ($rv ne $sha1);
            }
            $self->{call}{$scr_name} = $sha1;
        }
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

%scripts = (
requeue_busy => q{
-- Requeue_busy_items
-- # KEYS[1] queue name
-- # ARGV[1] item
-- # ARGV[2] requeue limit
-- # ARGV[3] place to requeue in dest-queue:
--      0: at producer side, 1: consumer side
--      Note: failed items will always go to the tail of the failed queue
-- # ARGV[4] OPTIONAL error message
--
--redis.log(redis.LOG_WARNING, "requeue_tail")
if #KEYS ~= 1 then error('requeue_busy requires 1 key') end
redis.log(redis.LOG_NOTICE, "nr keys: " .. #KEYS)
local queue = assert(KEYS[1], 'queue name missing')
local item  = assert(ARGV[1], 'item missing')
local limit = assert(tonumber(ARGV[2]), 'requeue limit missing')
local place = tonumber(ARGV[3])
assert(place == 0 or place == 1, 'requeue place should be 0 or 1')

local from = queue .. "_busy"
local dest = queue .. "_main"
local fail = queue .. "_failed"

local n = redis.call('lrem', from, 1, item)

if n > 0 then
    local i= cjson.decode(item)
    if i.rc == nil then
        i.rc=1
    else
        i.rc=i.rc+1
    end

    if i.rc <= limit then
        local v=cjson.encode(i)
        if place == 0 then 
            redis.call('lpush', dest, v)
        else
            redis.call('rpush', dest, v)
        end
    else
        -- reset requeue counter and increase fail counter
        i.rc = 0
        if i.fc == nil then
            i.fc = 1
        else
            i.fc = i.fc + 1
        end
        if #ARGV == 4 then
            i.error = ARGV[4]
        else
            i[error] = nil
        end
        local v=cjson.encode(i)
        redis.call('lpush', fail, v)
    end
end
return n
},
requeue_failed => q{
-- KEYS[1]: queue name
-- ARGV[1]: number of items to requeue. Value "0" means "all items"

if #KEYS ~= 1 then error('requeue_failed requires 1 key') end
local queue = assert(KEYS[1], 'queue name missing')
local num   = assert(tonumber(ARGV[1]), 'number of items missing')
local dest = queue .. "_main"
local fail = queue .. "_failed"

local count = 0
if num == 0 then
    while redis.call('rpoplpush', fail, dest) do count = count + 1 end
else
    for i = 1, num do
        if not redis.call('rpoplpush', fail, dest) then break end
        count = count + 1
    end
end
return count
}
);

1;

__END__

=head1 NAME

Queue::Q::ReliableFIFO::Lua - Load lua scripts into Redis

=head1 SYNOPSIS

  use Queue::Q::ReliableFIFO::Lua;
  my $lua = Queue::Q::ReliableFIFO::Lua->new(
    script_dir => /some/path
    redis_conn => $redis_conn);

  $lua->call('myscript', $n, @keys, @argv);

=head1 DESCRIPTION

This module offers two ways of loading/running lua scripts.

One way
is with separate lua scripts, which live at a location as indicated
by the script_dir parameter (passed to the constructor) or as 
indicated by the LUA_SCRIPT_DIR environment variable.

The other way is by putting the source code of the lua scripts in
this module, in the %scripts hash.

Which way is actually used depends on whether or not passing info
about a path to lua scripts. If a lua script location is known, those
script will be used, otherwise the %scripts code is used.

During development it is more conveniant to use the separate lua files
of course. But for deploying it is less error prone if the lua code
is inside the perl module. So that is why this is done this way.

The scripts are loaded when the constructor is called.

