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
