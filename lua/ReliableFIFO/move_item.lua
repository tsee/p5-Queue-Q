-- KEYS[1]: source queue name
-- KEYS[2]: dest queue name
-- ARGV[1]: item to requeue

if #KEYS ~= 2 then error('requeue_failed requires 2 keys') end
local source = assert(KEYS[1], 'from queue name missing')
local dest   = assert(KEYS[2], 'dest. queue name missing')
local item   = assert(ARGV[1], 'item is missing')

local count = redis.call('lrem', source, -1, item)
if count > 0 then
    redis.call('lpush', dest, item)
end
return count
