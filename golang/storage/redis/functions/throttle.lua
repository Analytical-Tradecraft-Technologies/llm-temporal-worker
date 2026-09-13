-- llmtw_throttle_v1 / throttle-v1
-- Atomic operational request/token/concurrency reservations. Monetary budget
-- accounting remains in admission.lua; this Function has no financial fields.
local ACTION = ARGV[1]
local MAX_SAFE = 9007199254740991

local function integer(value)
    local result = tonumber(value)
    if result == nil or result < 0 or result > MAX_SAFE or result ~= math.floor(result) then
        return nil
    end
    return result
end

local function reservation()
    local value = redis.call('GET', KEYS[1])
    if not value then return nil, nil end
    local ok, decoded = pcall(cjson.decode, value)
    if not ok or type(decoded) ~= 'table' or decoded.schema ~= 'throttle/v1' then
        return nil, 'invalid_record'
    end
    return decoded, value
end

if ACTION == 'acquire' or ACTION == 'acquire_fair' then
    local fair = ACTION == 'acquire_fair'
    local ok, incoming = pcall(cjson.decode, ARGV[2])
    if not ok or type(incoming) ~= 'table' or incoming.schema ~= 'throttle/v1' or type(incoming.limits) ~= 'table' then
        return {'invalid_request', ''}
    end
    local limit_key_count = fair and (#KEYS - 4) or (#KEYS - 1)
    if #incoming.limits ~= limit_key_count then return {'invalid_request', ''} end
    local queue_key = fair and KEYS[#KEYS - 2] or nil
    local sequence_key = fair and KEYS[#KEYS - 1] or nil
    local deadline_key = fair and KEYS[#KEYS] or nil
    local existing, encoded = reservation()
    if existing then
        if fair then redis.call('ZREM', queue_key, incoming.id); redis.call('ZREM', deadline_key, incoming.id) end
        if existing.digest ~= incoming.digest then return {'conflict', ''} end
        return {'existing', encoded}
    elseif encoded == 'invalid_record' then
        return {'state_unavailable', ''}
    end
    local ttl = integer(ARGV[3])
    if not ttl or ttl <= 0 then return {'invalid_request', ''} end
    if fair then
        local queue_ttl = integer(ARGV[4])
        if not queue_ttl or queue_ttl <= 0 then return {'invalid_request', ''} end
        local redis_time = redis.call('TIME')
        local now_micros = tonumber(redis_time[1]) * 1000000 + tonumber(redis_time[2])
        local expired = redis.call('ZRANGEBYSCORE', deadline_key, '-inf', tostring(now_micros))
        for _, member in ipairs(expired) do redis.call('ZREM', queue_key, member) end
        redis.call('ZREMRANGEBYSCORE', deadline_key, '-inf', tostring(now_micros))
        local score = redis.call('INCR', sequence_key)
        if not integer(score) then return {'state_unavailable', ''} end
        redis.call('ZADD', queue_key, 'NX', tostring(score), incoming.id)
        redis.call('ZADD', deadline_key, 'NX', tostring(now_micros + queue_ttl * 1000000), incoming.id)
        redis.call('EXPIRE', queue_key, tostring(queue_ttl))
        redis.call('EXPIRE', sequence_key, tostring(queue_ttl))
        redis.call('EXPIRE', deadline_key, tostring(queue_ttl))
        local head = redis.call('ZRANGE', queue_key, 0, 0)
        if #head ~= 1 or head[1] ~= incoming.id then return {'queued', ''} end
    end
    for index, limit in ipairs(incoming.limits) do
        local amount = integer(limit.amount)
        local window = integer(limit.window_seconds)
        if not amount or amount <= 0 or not window or window <= 0 or not limit.key_digest or not limit.kind then
            if fair then redis.call('ZREM', queue_key, incoming.id); redis.call('ZREM', deadline_key, incoming.id) end
            return {'invalid_request', ''}
        end
        local current = redis.call('GET', KEYS[index + 1])
        local parsed = current and integer(current) or 0
        if parsed == nil or parsed > MAX_SAFE - amount then return {'state_unavailable', ''} end
        local limit_value = integer(limit.limit)
        if limit_value and parsed + amount > limit_value then return {fair and 'queued' or 'denied', ''} end
    end
    for index, limit in ipairs(incoming.limits) do
        local amount = integer(limit.amount)
        local next_value = redis.call('INCRBY', KEYS[index + 1], tostring(amount))
        if integer(next_value) == nil then return {'state_unavailable', ''} end
        local current_ttl = redis.call('TTL', KEYS[index + 1])
        local window = integer(limit.window_seconds)
        if current_ttl == -2 or current_ttl < window then redis.call('EXPIRE', KEYS[index + 1], tostring(window)) end
    end
    local encoded_incoming = cjson.encode(incoming)
    redis.call('SET', KEYS[1], encoded_incoming, 'EX', tostring(ttl), 'NX')
    if fair then redis.call('ZREM', queue_key, incoming.id); redis.call('ZREM', deadline_key, incoming.id) end
    return {'created', encoded_incoming}
end

if ACTION == 'cancel_fair' then
    if #KEYS ~= 2 or not ARGV[2] or ARGV[2] == '' then return {'invalid_request', ''} end
    local removed = redis.call('ZREM', KEYS[1], ARGV[2])
    redis.call('ZREM', KEYS[2], ARGV[2])
    if removed == 0 then return {'not_found', ''} end
    return {'cancelled', ''}
end

if ACTION == 'renew' then
    local existing, encoded = reservation()
    if not existing then
        if encoded == 'invalid_record' then return {'state_unavailable', ''} end
        return {'not_found', ''}
    end
    local ttl = integer(ARGV[3])
    if existing.digest ~= ARGV[2] or not ttl or ttl <= 0 or type(existing.limits) ~= 'table' or #existing.limits ~= (#KEYS - 1) then
        return {'conflict', ''}
    end
    for index, limit in ipairs(existing.limits) do
        if limit.kind == 'concurrency' then
            local current = integer(redis.call('GET', KEYS[index + 1]) or '')
            local amount = integer(limit.amount)
            if not current or not amount or current < amount then return {'state_unavailable', ''} end
        end
    end
    redis.call('EXPIRE', KEYS[1], tostring(ttl))
    for index, limit in ipairs(existing.limits) do
        if limit.kind == 'concurrency' then redis.call('EXPIRE', KEYS[index + 1], tostring(ttl)) end
    end
    return {'renewed', ''}
end

if ACTION == 'release' then
    local existing, encoded = reservation()
    if not existing then
        if encoded == 'invalid_record' then return {'state_unavailable', ''} end
        return {'not_found', ''}
    end
    if existing.digest ~= ARGV[2] or type(existing.limits) ~= 'table' or #existing.limits ~= (#KEYS - 1) or (#ARGV - 2) ~= #existing.limits then
        return {'conflict', ''}
    end
    for index, limit in ipairs(existing.limits) do
        local amount = integer(ARGV[index + 2])
        local expected = integer(limit.amount)
        if not amount or not expected or amount ~= expected then return {'conflict', ''} end
        if limit.kind == 'concurrency' then
            local current = integer(redis.call('GET', KEYS[index + 1]) or '0')
            if not current or current < amount then return {'state_unavailable', ''} end
        end
    end
    for index, limit in ipairs(existing.limits) do
        local amount = integer(ARGV[index + 2])
        if limit.kind == 'concurrency' then
            redis.call('DECRBY', KEYS[index + 1], tostring(amount))
        end
    end
    redis.call('DEL', KEYS[1])
    return {'released', ''}
end

return {'invalid_request', ''}
