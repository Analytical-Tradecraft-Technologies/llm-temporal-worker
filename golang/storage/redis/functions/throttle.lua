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

-- Lua 5.1 tostring() formats numbers with %.14g and switches to scientific
-- notation at 1e14, which INCRBY/DECRBY/EXPIRE reject. Values here are integers
-- of at most MAX_SAFE, which %.0f prints exactly.
local function decimal(value)
    return string.format('%.0f', value)
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

if ACTION == 'acquire' then
    local ok, incoming = pcall(cjson.decode, ARGV[2])
    if not ok or type(incoming) ~= 'table' or incoming.schema ~= 'throttle/v1' or type(incoming.limits) ~= 'table' then
        return {'invalid_request', ''}
    end
    local existing, encoded = reservation()
    if existing then
        if existing.digest ~= incoming.digest then return {'conflict', ''} end
        return {'existing', encoded}
    elseif encoded == 'invalid_record' then
        return {'state_unavailable', ''}
    end
    if #incoming.limits ~= (#KEYS - 1) then return {'invalid_request', ''} end
    local ttl = integer(ARGV[3])
    if not ttl or ttl <= 0 then return {'invalid_request', ''} end
    for index, limit in ipairs(incoming.limits) do
        local amount = integer(limit.amount)
        if not amount or amount <= 0 or not limit.key_digest or not limit.kind then
            return {'invalid_request', ''}
        end
        local current = redis.call('GET', KEYS[index + 1])
        local parsed = current and integer(current) or 0
        if parsed == nil or parsed > MAX_SAFE - amount then return {'state_unavailable', ''} end
        local limit_value = integer(limit.limit)
        if limit_value and parsed + amount > limit_value then return {'denied', ''} end
    end
    for index, limit in ipairs(incoming.limits) do
        local amount = integer(limit.amount)
        local next_value = redis.call('INCRBY', KEYS[index + 1], decimal(amount))
        if integer(next_value) == nil then return {'state_unavailable', ''} end
        local current_ttl = redis.call('TTL', KEYS[index + 1])
        if current_ttl == -2 or current_ttl < ttl then redis.call('EXPIRE', KEYS[index + 1], decimal(ttl)) end
    end
    local encoded_incoming = cjson.encode(incoming)
    redis.call('SET', KEYS[1], encoded_incoming, 'EX', decimal(ttl), 'NX')
    return {'created', encoded_incoming}
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
        local current = integer(redis.call('GET', KEYS[index + 1]) or '0')
        if not current or current < amount then return {'state_unavailable', ''} end
    end
    for index, limit in ipairs(existing.limits) do
        local amount = integer(ARGV[index + 2])
        redis.call('DECRBY', KEYS[index + 1], decimal(amount))
    end
    redis.call('DEL', KEYS[1])
    return {'released', ''}
end

return {'invalid_request', ''}
