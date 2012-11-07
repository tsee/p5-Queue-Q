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
