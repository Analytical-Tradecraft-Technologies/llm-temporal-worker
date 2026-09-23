-- Bounded compare-and-swap for one field in a configuration-scoped hash.
-- Current state deliberately has no TTL: expiry must not clear sticky incidents.
local previous = redis.call('HGET', KEYS[1], ARGV[1]) or ''
if previous ~= ARGV[2] then return 0 end
local size = tonumber(redis.call('HGET', KEYS[1], '__bytes') or '0')
if not size or size < 0 then return redis.error_reply('invalid provider state size') end
local next_size = size - string.len(previous) + string.len(ARGV[3])
if next_size < 0 or next_size > 33554432 or string.len(ARGV[3]) > 4194304 then return -1 end
if previous == '' and redis.call('HLEN', KEYS[1]) >= 4097 then return -1 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3], '__bytes', tostring(next_size))
return 1
