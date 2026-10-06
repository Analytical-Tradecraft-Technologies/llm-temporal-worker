-- llmtw_throttle_v1 / throttle-v1
-- Counters are fixed windows that start at the first acquire.
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

-- Redis server time in milliseconds. Window ends are absolute so a release
-- can tell whether the counter it charged is still the same window.
local function now_ms()
    local clock = redis.call('TIME')
    return tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
end

-- Each counter is a fixed window: its TTL is set once, when the first acquire
-- creates it, so it resets every window instead of being extended by every
-- acquire. Returns the window's absolute end in milliseconds.
local function window_end(key, ttl, now)
    local pttl = redis.call('PTTL', key)
    if pttl < 0 then
        redis.call('PEXPIRE', key, decimal(ttl * 1000))
        pttl = ttl * 1000
    end
    return now + pttl
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
    local now = now_ms()
    local window_ends = {}
    for index, limit in ipairs(incoming.limits) do
        local amount = integer(limit.amount)
        local next_value = redis.call('INCRBY', KEYS[index + 1], decimal(amount))
        if integer(next_value) == nil then return {'state_unavailable', ''} end
        window_ends[index] = window_end(KEYS[index + 1], ttl, now)
    end
    incoming.window_ends = window_ends
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
    -- Return capacity only to the window the reservation was charged in. Once
    -- that window has ended its counter was reset, so the capacity is already
    -- free and decrementing a newer window would over-admit. Reservations
    -- recorded before window ends were kept decrement as before.
    local now = now_ms()
    local charged = {}
    for index, limit in ipairs(existing.limits) do
        local amount = integer(ARGV[index + 2])
        local expected = integer(limit.amount)
        if not amount or not expected or amount ~= expected then return {'conflict', ''} end
        charged[index] = true
        if type(existing.window_ends) == 'table' then
            local recorded = tonumber(existing.window_ends[index])
            local pttl = redis.call('PTTL', KEYS[index + 1])
            charged[index] = recorded ~= nil and pttl > 0 and math.abs((now + pttl) - recorded) <= 2
        end
        if charged[index] then
            local current = integer(redis.call('GET', KEYS[index + 1]) or '0')
            if not current or current < amount then return {'state_unavailable', ''} end
        end
    end
    for index, limit in ipairs(existing.limits) do
        if charged[index] then
            redis.call('DECRBY', KEYS[index + 1], decimal(integer(ARGV[index + 2])))
        end
    end
    redis.call('DEL', KEYS[1])
    return {'released', ''}
end

return {'invalid_request', ''}
