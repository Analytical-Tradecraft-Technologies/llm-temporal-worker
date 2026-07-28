-- llmtw_admission_v1 / admission-v1
--
-- The caller supplies every key touched by a transaction. All admission keys
-- include the same literal hash tag, so this script is safe on Redis Cluster.
-- Values are JSON records; monetary and bucket values are decimal integers.

local ACTION = ARGV[1]
local MAX_SAFE = 9007199254740991
-- Keep malformed direct FCALL/EVALSHA invocations bounded before any Redis
-- read or mutation. Production callers already enforce smaller record limits,
-- but the Function is a public shared-state boundary and must not rely on the
-- Go caller to supply a safe key/argument shape.
local MAX_KEYS = 512
local MAX_KEY_BYTES = 1024
local MAX_ARGUMENTS = 16
local MAX_ARGUMENT_BYTES = 2 * 1024 * 1024
-- A begin call consumes three operation keys; continuation/finalization calls
-- consume two plus old/new reservation keys. Keep the reservation vector
-- within Redis' declared-key ceiling, and reject larger requests before any
-- read or mutation.
local MAX_RESERVATIONS = 253
local MAX_ATTEMPT_NUMBER = 1000000

local function valid_invocation(min_keys, max_keys, argument_count)
    if #KEYS < min_keys or (max_keys and #KEYS > max_keys) or #KEYS > MAX_KEYS then
        return false
    end
    if #ARGV ~= argument_count or #ARGV > MAX_ARGUMENTS then
        return false
    end
    for index = 1, #KEYS do
        if type(KEYS[index]) ~= 'string' or #KEYS[index] == 0 or #KEYS[index] > MAX_KEY_BYTES then
            return false
        end
    end
    for index = 1, #ARGV do
        if type(ARGV[index]) ~= 'string' or #ARGV[index] > MAX_ARGUMENT_BYTES then
            return false
        end
    end
    return true
end

local function bounded_string(value, maximum, required)
    if value == nil and not required then
        return true
    end
    return type(value) == 'string' and (not required or #value > 0) and #value <= maximum
end

local function valid_dispatch(value)
    return value == nil or value == '' or value == 'not_dispatched' or
        value == 'rejected' or value == 'accepted' or value == 'ambiguous'
end

local function number(value)
    local result = tonumber(value)
    if result == nil or result < 0 or result > MAX_SAFE then
        return nil
    end
    return result
end

local function integer(value)
    local result = number(value)
    if result == nil or result ~= math.floor(result) then
        return nil
    end
    return result
end

local function valid_attempt(value)
    if type(value) ~= 'table' or
        not bounded_string(value.route_id, 256, false) or
        not bounded_string(value.endpoint_id, 256, false) or
        not bounded_string(value.provider, 128, false) or
        not bounded_string(value.resolved_model, 256, false) or
        not bounded_string(value.provider_request_id, 512, false) or
        not bounded_string(value.service_class, 64, false) or
        not valid_dispatch(value.dispatch) then
        return false
    end
    local attempt_number = value.attempt_number
    if attempt_number == nil then
        attempt_number = 0
    end
    attempt_number = integer(attempt_number)
    return attempt_number ~= nil and attempt_number <= MAX_ATTEMPT_NUMBER
end

local function now_micros()
    local clock = redis.call('TIME')
    return integer(clock[1]) * 1000000 + integer(clock[2])
end

local function now_string()
    local clock = redis.call('TIME')
    return clock[1] .. ':' .. clock[2]
end

local function get_record(key)
    local encoded = redis.call('GET', key)
    if not encoded then
        return nil, nil
    end
    local ok, record = pcall(cjson.decode, encoded)
    if not ok or type(record) ~= 'table' or record.schema ~= 'admission/v1' or
        not bounded_string(record.id, 256, true) or
        not bounded_string(record.scope_key, 1024, true) or
        not bounded_string(record.request_digest, 64, true) or
        #record.request_digest ~= 64 or
        not bounded_string(record.dispatch_token, 512, true) or
        not bounded_string(record.state, 32, true) or
        not valid_attempt(record.attempt) then
        return nil, 'invalid_record'
    end
    return record, encoded
end

local function operation_status(key, token)
    local record, encoded = get_record(key)
    if not record then
        if encoded == 'invalid_record' then
            return nil, {'state_unavailable', ''}
        end
        return nil, {'not_found', ''}
    end
    if token and record.dispatch_token ~= token then
        return nil, {'invalid_token', ''}
    end
    return record, encoded
end

local function amount(record, field)
    local value = integer(record[field])
    if value == nil then
        return nil
    end
    return value
end

local function reservation_fields(reservation)
    if type(reservation) ~= 'table' or
        not bounded_string(reservation.policy_id, 256, true) or
        not bounded_string(reservation.window_id, 256, true) then
        return nil
    end
    local limit = integer(reservation.limit)
    local amount_value = integer(reservation.amount)
    local bucket = integer(reservation.bucket)
    local bucket_ns = integer(reservation.bucket_nanos)
    local duration_ns = integer(reservation.duration_nanos)
    if not limit or not amount_value or not bucket or not bucket_ns or not duration_ns or limit <= 0 or bucket_ns <= 0 or duration_ns <= 0 then
        return nil
    end
    return limit, amount_value, bucket, bucket_ns, duration_ns
end

-- The scalar amount is the request-wide reservation envelope. Every bucket
-- mutation must carry that same amount, and a bucket identity may appear only
-- once. Reject malformed vectors before any read or mutation so callers
-- cannot under-reserve or debit a bucket twice in one transaction.
local function valid_reservation_envelope(reservations, expected)
    local expected_value = integer(expected)
	if not expected_value then
        return false
    end
    local seen = {}
    for _, reservation in ipairs(reservations) do
        local _, amount_value, bucket = reservation_fields(reservation)
        if not amount_value or amount_value ~= expected_value then
            return false
        end
        local identity = reservation.policy_id .. '\0' .. reservation.window_id .. '\0' .. tostring(bucket)
        if seen[identity] then
            return false
        end
        seen[identity] = true
    end
    return true
end

local function active_for(reservation, budget_key, now)
    local limit, _, _, bucket_ns, duration_ns = reservation_fields(reservation)
    if not limit then
        return nil, 'invalid_reservation'
    end
    -- Redis TIME has microsecond precision. Rounding the configured nanosecond
    -- bounds up keeps this check conservative for normal sub-second windows.
    local bucket_us = math.ceil(bucket_ns / 1000)
    local duration_us = math.ceil(duration_ns / 1000)
    if bucket_us <= 0 or duration_us <= 0 then
        return nil, 'invalid_reservation'
    end
    local first = math.floor((now - duration_us) / bucket_us)
    local last = math.floor(now / bucket_us)
    local active = 0
    for index = first, last do
        local value = redis.call('HGET', budget_key, tostring(index))
        if value then
            local parsed = integer(value)
            if not parsed then
                return nil, 'state_unavailable'
            end
            active = active + parsed
            if active > MAX_SAFE then
                return nil, 'state_unavailable'
            end
        end
    end
    return active, nil
end

local function denial(reservation, active, requested)
    return cjson.encode({
        retry_after_nanos = 0,
        policy_id = reservation.policy_id,
        window_id = reservation.window_id,
        limit = integer(reservation.limit),
        active = active or 0,
        requested = requested,
    })
end

local function check_reservations(reservations, key_offset, requested, now)
    local requested_value = integer(requested)
    if not requested_value then
        return nil, {'invalid_request', ''}
    end
    for index, reservation in ipairs(reservations) do
        local key = KEYS[key_offset + index - 1]
        local active, err = active_for(reservation, key, now)
        if err then
            return nil, {err, ''}
        end
        local limit = integer(reservation.limit)
        if active > limit or requested_value > limit - active then
            return nil, {'denied', denial(reservation, active, requested_value)}
        end
    end
    return true, nil
end

local function expire_budget(key, duration_ns, ttl)
    local ttl_value = integer(ttl)
    if ttl_value and ttl_value > 0 then
        local window_seconds = math.ceil(duration_ns / 1000000000)
        local desired = ttl_value + window_seconds
        local current = redis.call('TTL', key)
        -- A longer-lived operation must never be shortened by a later write
        -- to the same shared budget hash. -1 means persistent by policy.
        if current == -2 or (current >= 0 and current < desired) then
            redis.call('EXPIRE', key, tostring(desired))
        end
    end
end

local function can_increment_reservations(reservations, key_offset)
    for index, reservation in ipairs(reservations) do
        local _, amount_value, bucket = reservation_fields(reservation)
        if not amount_value then
            return false
        end
        local current = redis.call('HGET', KEYS[key_offset + index - 1], tostring(bucket))
        local parsed = current and integer(current) or 0
        if parsed == nil or parsed > MAX_SAFE - amount_value then
            return false
        end
    end
    return true
end

local function increment_reservations(reservations, key_offset, ttl)
    for index, reservation in ipairs(reservations) do
        local _, amount_value, bucket = reservation_fields(reservation)
        if not amount_value then
            return false
        end
        local next_value = redis.call('HINCRBY', KEYS[key_offset + index - 1], tostring(bucket), tostring(amount_value))
        if integer(next_value) == nil then
            return false
        end
        expire_budget(KEYS[key_offset + index - 1], reservation.duration_nanos, ttl)
    end
    return true
end

local function can_reconcile(reservations, key_offset, actual)
    for index, reservation in ipairs(reservations) do
        local _, amount_value, bucket = reservation_fields(reservation)
        if not amount_value then
            return false
        end
        local current = redis.call('HGET', KEYS[key_offset + index - 1], tostring(bucket))
        local parsed = current and integer(current) or 0
		local actual_value = integer(actual)
		if parsed == nil or parsed < amount_value or not actual_value or actual_value > MAX_SAFE - (parsed - amount_value) then
            return false
        end
    end
    return true
end

local function reconcile(reservations, key_offset, actual, ttl)
    local actual_value = integer(actual)
	if not actual_value or not can_reconcile(reservations, key_offset, actual) then
        return false
    end
    for index, reservation in ipairs(reservations) do
        local _, amount_value, bucket = reservation_fields(reservation)
        local key = KEYS[key_offset + index - 1]
        local delta = actual_value - amount_value
        local next_value = redis.call('HINCRBY', key, tostring(bucket), tostring(delta))
        if integer(next_value) == nil or integer(next_value) < 0 then
            return false
        end
        expire_budget(key, reservation.duration_nanos, ttl)
    end
    return true
end

local function set_record(key, record, ttl)
    local encoded = cjson.encode(record)
    -- SET clears an existing expiry. Capture it first, then restore the
    -- longer of the existing and requested retention windows. A zero TTL is
    -- used by terminal/dispatch updates and must preserve the record's
    -- current expiry rather than making it persistent.
    local current_ttl = redis.call('TTL', key)
    redis.call('SET', key, encoded)
    local ttl_value = integer(ttl)
    local restore_ttl = nil
    if current_ttl >= 0 then
        restore_ttl = current_ttl
    end
    if ttl_value and ttl_value > 0 then
        -- A persistent existing record (TTL -1) is intentionally not
        -- shortened by a later update that happens to carry an expiry.
        if current_ttl == -2 or (current_ttl >= 0 and current_ttl < ttl_value) then
            restore_ttl = ttl_value
        end
    end
    if restore_ttl and restore_ttl >= 0 then
        redis.call('EXPIRE', key, tostring(restore_ttl))
    end
    return encoded
end

if ACTION == 'begin' then
    if not valid_invocation(3, nil, 3) then
        return {'invalid_request', ''}
    end
    local ttl = integer(ARGV[3])
    if not ttl or ttl <= 0 then
        return {'invalid_request', ''}
    end
    local incoming_ok, incoming = pcall(cjson.decode, ARGV[2])
    if not incoming_ok or type(incoming) ~= 'table' or incoming.schema ~= 'admission/v1' or
        not bounded_string(incoming.id, 256, true) or
        not bounded_string(incoming.scope_key, 1024, true) or
        not bounded_string(incoming.request_digest, 64, true) or
        #incoming.request_digest ~= 64 or
        not bounded_string(incoming.dispatch_token, 512, true) or
        not bounded_string(incoming.lease_until, 64, false) or
        not bounded_string(incoming.expires_at, 64, false) then
        return {'invalid_request', ''}
    end
    local existing_key = redis.call('GET', KEYS[1])
    if existing_key then
        local existing, encoded = get_record(existing_key)
        if existing then
            if existing.request_digest ~= incoming.request_digest then
                return {'conflict', ''}
            end
            return {'existing', encoded}
        end
        if encoded == 'invalid_record' then
            return {'state_unavailable', ''}
        end
        redis.call('DEL', KEYS[1])
    end
    local reservations = incoming.reservations
    if type(reservations) ~= 'table' or #reservations > MAX_RESERVATIONS or #KEYS ~= 3 + #reservations then
        return {'invalid_request', ''}
    end
    local requested = integer(incoming.reserved_micro_usd)
    if not requested or requested < 0 or not valid_reservation_envelope(reservations, requested) then
        return {'invalid_request', ''}
    end
    local accepted, response = check_reservations(reservations, 4, requested, now_micros())
    if not accepted then
        return response
    end
    if not can_increment_reservations(reservations, 4) or not increment_reservations(reservations, 4, ttl) then
        return {'state_unavailable', ''}
    end
    local now = now_string()
    incoming.created_at = now
    incoming.updated_at = now
    local encoded = set_record(KEYS[3], incoming, ttl)
    redis.call('SET', KEYS[1], KEYS[3])
    redis.call('SET', KEYS[2], KEYS[3])
    redis.call('EXPIRE', KEYS[1], tostring(ttl))
    redis.call('EXPIRE', KEYS[2], tostring(ttl))
    return {'created', encoded}
end

if ACTION == 'mark_dispatching' then
    if not valid_invocation(2, 2, 5) or not bounded_string(ARGV[2], 512, true) or
        not bounded_string(ARGV[4], 64, false) then
        return {'invalid_request', ''}
    end
    local record, response = operation_status(KEYS[2], ARGV[2])
    if not record then
        return response
    end
    if record.state ~= 'reserved' then
        if record.state == 'completed' or record.state == 'definite_failed' or record.state == 'ambiguous' or record.state == 'canceled' then
            return {'ok', cjson.encode(record)}
        end
        return {'invalid_transition', ''}
    end
    local ok, attempt = pcall(cjson.decode, ARGV[3])
    if not ok or not valid_attempt(attempt) then
        return {'invalid_request', ''}
    end
    attempt.attempt_number = integer(attempt.attempt_number) or 0
    if attempt.attempt_number >= MAX_ATTEMPT_NUMBER then
        return {'invalid_request', ''}
    end
    attempt.attempt_number = attempt.attempt_number + 1
    record.state = 'dispatching'
    record.attempt = attempt
    record.lease_until = ARGV[4]
    record.updated_at = now_string()
    return {'ok', set_record(KEYS[2], record, ARGV[5])}
end

if ACTION == 'continue' then
    if not valid_invocation(2, nil, 8) or not bounded_string(ARGV[2], 512, true) or
        not bounded_string(ARGV[6], 64, false) or not bounded_string(ARGV[7], 64, false) then
        return {'invalid_request', ''}
    end
    local record, response = operation_status(KEYS[2], ARGV[2])
    if not record then
        return response
    end
    if record.state ~= 'dispatching' then
        return {'invalid_transition', ''}
    end
    local outcome_ok, outcome = pcall(cjson.decode, ARGV[3])
    local reservations_ok, reservations = pcall(cjson.decode, ARGV[5])
    if not outcome_ok or type(outcome) ~= 'table' or not reservations_ok or type(reservations) ~= 'table' then
        return {'invalid_request', ''}
    end
    if outcome.certainty ~= 'not_dispatched' and outcome.certainty ~= 'rejected' then
        return {'invalid_request', ''}
    end
    local remaining = integer(ARGV[4])
    local incurred = integer(outcome.incurred)
    if not remaining or not incurred then
        return {'invalid_request', ''}
    end
    local old = record.reservations
    if type(old) ~= 'table' or #old > MAX_RESERVATIONS or #reservations > MAX_RESERVATIONS or #KEYS ~= 2 + #old + #reservations or not valid_reservation_envelope(old, record.reserved_micro_usd) or not valid_reservation_envelope(reservations, remaining) or not reconcile(old, 3, incurred, ARGV[8]) then
        return {'state_unavailable', ''}
    end
    local accepted, denial_response = check_reservations(reservations, 3 + #old, remaining, now_micros())
    if not accepted then
        if denial_response[1] == 'denied' then
            record.state = 'definite_failed'
            record.incurred_micro_usd = tostring(incurred)
            record.final_micro_usd = tostring(incurred)
            record.reserved_micro_usd = '0'
            record.updated_at = now_string()
            local encoded = set_record(KEYS[2], record, ARGV[8])
            return {'denied', encoded, denial_response[2]}
        end
        return denial_response
    end
    if not can_increment_reservations(reservations, 3 + #old) or not increment_reservations(reservations, 3 + #old, ARGV[8]) then
        return {'state_unavailable', ''}
    end
    local attempt = outcome.attempt
    if not valid_attempt(attempt) then
        attempt = {}
    end
    attempt.dispatch = 'not_dispatched'
    attempt.attempt_number = integer(attempt.attempt_number) or 0
    record.state = 'reserved'
    record.reservations = reservations
    record.reserved_micro_usd = tostring(remaining)
    record.attempt = attempt
    record.dispatch_token = record.dispatch_token .. '-' .. tostring(attempt.attempt_number + 1)
    record.lease_until = ARGV[6]
    record.expires_at = ARGV[7]
    record.updated_at = now_string()
    return {'ok', set_record(KEYS[2], record, ARGV[8])}
end

if ACTION == 'complete' then
    if not valid_invocation(2, nil, 6) or not bounded_string(ARGV[2], 512, true) then
        return {'invalid_request', ''}
    end
    local record, response = operation_status(KEYS[2], ARGV[2])
    if not record then
        return response
    end
    if record.state == 'completed' then
        return {'ok', cjson.encode(record)}
    end
    if record.state ~= 'dispatching' then
        return {'invalid_transition', ''}
    end
    local actual = integer(ARGV[3])
    local attempt_ok, attempt = pcall(cjson.decode, ARGV[5])
    local result_ok, result = pcall(cjson.decode, ARGV[4])
    if not actual or actual < 0 or not attempt_ok or not valid_attempt(attempt) or not result_ok or
        type(record.reservations) ~= 'table' or #record.reservations > MAX_RESERVATIONS or #KEYS ~= 2 + #record.reservations then
        return {'invalid_request', ''}
    end
    if not reconcile(record.reservations, 3, actual, ARGV[6]) then
        return {'state_unavailable', ''}
    end
    record.state = 'completed'
    record.incurred_micro_usd = tostring(actual)
    record.final_micro_usd = tostring(actual)
    record.reserved_micro_usd = '0'
    record.result_ref = result
    attempt.dispatch = 'accepted'
    record.attempt = attempt
    record.updated_at = now_string()
    return {'ok', set_record(KEYS[2], record, ARGV[6])}
end

if ACTION == 'fail' then
    if not valid_invocation(2, nil, 7) or not bounded_string(ARGV[2], 512, true) then
        return {'invalid_request', ''}
    end
    local record, response = operation_status(KEYS[2], ARGV[2])
    if not record then
        return response
    end
    if record.state == 'completed' or record.state == 'definite_failed' or record.state == 'ambiguous' or record.state == 'canceled' then
        return {'ok', cjson.encode(record)}
    end
    if record.state ~= 'dispatching' then
        return {'invalid_transition', ''}
    end
    local incurred = integer(ARGV[4])
    local attempt_ok, attempt = pcall(cjson.decode, ARGV[5])
    if not incurred or not attempt_ok or not valid_attempt(attempt) or
        type(record.reservations) ~= 'table' or #record.reservations > MAX_RESERVATIONS or #KEYS ~= 2 + #record.reservations then
        return {'invalid_request', ''}
    end
    local certainty = ARGV[3]
    if certainty ~= 'not_dispatched' and certainty ~= 'rejected' and certainty ~= 'accepted' and certainty ~= 'ambiguous' then
        return {'invalid_request', ''}
    end
    local retain = certainty == 'accepted' or certainty == 'ambiguous'
    if not retain then
        if not reconcile(record.reservations, 3, incurred, ARGV[6]) then
            return {'state_unavailable', ''}
        end
        record.state = 'definite_failed'
        record.final_micro_usd = tostring(incurred)
        record.reserved_micro_usd = '0'
    else
        record.state = 'ambiguous'
        record.final_micro_usd = record.reserved_micro_usd
    end
    record.incurred_micro_usd = tostring(incurred)
    attempt.dispatch = certainty
    record.attempt = attempt
    record.updated_at = now_string()
    return {'ok', set_record(KEYS[2], record, ARGV[6])}
end

-- Durable v1 budget materialization uses the same immutable Function/Lua
-- deployment as admission, but a separate key family and nano-USD values.
-- Operation records retain the generation/incarnation fence and event
-- fingerprints. Bucket hashes hold aggregate totals while a per-bucket
-- expiry ZSET lets the Function remove expired reservations atomically.
local DURABLE_SCHEMA = 'durable-budget/v1'
local MAX_DURABLE_RESERVATIONS = 250
local MAX_DURABLE_EVENTS = 250

local function durable_int(value)
    local parsed = integer(value)
    if not parsed then return nil end
    return parsed
end

local function durable_bucket_field(bucket)
    return 'sum:' .. bucket
end

local function durable_limit_field(bucket)
    return 'limit:' .. bucket
end

local function durable_expiry_member(fingerprint, bucket, amount)
    return fingerprint .. '|' .. bucket .. '|' .. amount
end

-- Remove expired members and their aggregate contribution. The bounded read
-- prevents a pathological expiry backlog from blocking all admission; any
-- remaining expired members are conservatively left in the aggregate until a
-- subsequent mutation cleans them up.
local function durable_cleanup(bucket_key, expiry_key, now)
    local members = redis.call('ZRANGEBYSCORE', expiry_key, '-inf', tostring(now), 'LIMIT', 0, 1024)
    for _, member in ipairs(members) do
        local fingerprint, bucket, amount = string.match(member, '^([^|]+)|([^|]+)|([^|]+)$')
        local value = durable_int(amount)
        if not fingerprint or not bucket or not value then
            return false
        end
        local field = durable_bucket_field(bucket)
        local current = durable_int(redis.call('HGET', bucket_key, field) or '0')
        if not current or current < value then
            return false
        end
        local next_value = redis.call('HINCRBY', bucket_key, field, tostring(-value))
        if durable_int(next_value) == 0 then
            redis.call('HDEL', bucket_key, field, durable_limit_field(bucket))
        end
        redis.call('ZREM', expiry_key, member)
    end
    return true
end

local function durable_encode(record)
    return cjson.encode(record)
end

local function durable_restore_record(key, encoded, ttl)
    local current = redis.call('TTL', key)
    redis.call('SET', key, encoded)
    if current >= 0 then
        redis.call('EXPIRE', key, tostring(current))
    elseif ttl and ttl > 0 then
        redis.call('EXPIRE', key, tostring(ttl))
    end
    return encoded
end

if ACTION == 'durable_reserve' then
    if #KEYS < 3 or #KEYS > (1 + 2 * MAX_DURABLE_RESERVATIONS) or #ARGV ~= 8 then
        return {'invalid_request', ''}
    end
    local generation = ARGV[2]
    local incarnation = ARGV[3]
    local operation_id = ARGV[4]
    local fingerprint = ARGV[5]
    local ttl = durable_int(ARGV[6])
    local occurred_at = ARGV[7]
    if not bounded_string(generation, 128, true) or not bounded_string(incarnation, 128, true) or
        not bounded_string(operation_id, 128, true) or not bounded_string(fingerprint, 64, true) or
        #fingerprint ~= 64 or not ttl or ttl <= 0 or not bounded_string(occurred_at, 64, true) then
        return {'invalid_request', ''}
    end
    local reservation_ok, reservations = pcall(cjson.decode, ARGV[8])
    if not reservation_ok or type(reservations) ~= 'table' or #reservations == 0 or
        #reservations > MAX_DURABLE_RESERVATIONS or #KEYS ~= 1 + 2 * #reservations then
        return {'invalid_request', ''}
    end
    local existing_raw = redis.call('GET', KEYS[1])
    if existing_raw then
        local existing_ok, existing = pcall(cjson.decode, existing_raw)
        if not existing_ok or type(existing) ~= 'table' or existing.schema ~= DURABLE_SCHEMA then
            return {'state_unavailable', ''}
        end
        if existing.generation_id ~= generation then return {'generation_mismatch', ''} end
        if existing.incarnation_id ~= incarnation then return {'incarnation_mismatch', ''} end
        if existing.operation_id ~= operation_id or existing.fingerprint ~= fingerprint then
            return {'conflict', ''}
        end
        return {'existing', existing_raw}
    end
    local now = redis.call('TIME')
    local now_seconds = durable_int(now[1])
    local now_micros = durable_int(now[2])
    if not now_seconds or not now_micros then return {'state_unavailable', ''} end
    local now_millis = now_seconds * 1000 + math.floor(now_micros / 1000)
    local seen = {}
    for index, reservation in ipairs(reservations) do
        if type(reservation) ~= 'table' or not bounded_string(reservation.policy_id, 128, true) or
            not bounded_string(reservation.window_id, 128, true) or not bounded_string(reservation.bucket, 64, true) or
            not bounded_string(reservation.amount_nano, 64, true) or not bounded_string(reservation.limit_nano, 64, true) or
            not bounded_string(reservation.bucket_start_nanos, 64, true) or not bounded_string(reservation.event_id, 128, true) then
            return {'invalid_request', ''}
        end
        local bucket = durable_int(reservation.bucket)
        local amount = durable_int(reservation.amount_nano)
        local limit = durable_int(reservation.limit_nano)
        local expires = durable_int(reservation.expires_millis)
        if not bucket or not amount or not limit or not expires or bucket < 0 or amount <= 0 or limit <= 0 or amount > limit or expires <= now_millis then
            return {'invalid_request', ''}
        end
        local identity = reservation.policy_id .. '\0' .. reservation.window_id .. '\0' .. reservation.bucket
        if seen[identity] then return {'invalid_request', ''} end
        seen[identity] = true
        local bucket_key = KEYS[1 + (index - 1) * 2 + 1]
        local expiry_key = KEYS[1 + (index - 1) * 2 + 2]
        if not durable_cleanup(bucket_key, expiry_key, now_millis) then return {'state_unavailable', ''} end
        local limit_field = durable_limit_field(reservation.bucket)
        local prior_limit = redis.call('HGET', bucket_key, limit_field)
        if prior_limit and prior_limit ~= reservation.limit_nano then return {'conflict', ''} end
        local active = durable_int(redis.call('HGET', bucket_key, durable_bucket_field(reservation.bucket)) or '0')
        if not active then return {'state_unavailable', ''} end
        if active > limit or amount > limit - active then
            local denial = {
                schema = DURABLE_SCHEMA, operation_id = operation_id, generation_id = generation,
                incarnation_id = incarnation, fingerprint = fingerprint, status = 'denied',
                occurred_at = occurred_at, reservations = reservations,
                denial = {policy_id = reservation.policy_id, window_id = reservation.window_id,
                    limit_nano = reservation.limit_nano, active_nano = tostring(active or 0), requested_nano = reservation.amount_nano},
                events = {},
            }
            local encoded_denial = durable_encode(denial)
            redis.call('SET', KEYS[1], encoded_denial, 'EX', tostring(ttl))
            return {'created', encoded_denial}
        end
    end
    local record = {
        schema = DURABLE_SCHEMA, operation_id = operation_id, generation_id = generation,
        incarnation_id = incarnation, fingerprint = fingerprint, status = 'accepted',
        occurred_at = occurred_at, reservations = reservations, events = {},
    }
    for index, reservation in ipairs(reservations) do
        local bucket_key = KEYS[1 + (index - 1) * 2 + 1]
        local expiry_key = KEYS[1 + (index - 1) * 2 + 2]
        local bucket = reservation.bucket
        local amount = durable_int(reservation.amount_nano)
        local expires = durable_int(reservation.expires_millis)
        redis.call('HSET', bucket_key, durable_limit_field(bucket), reservation.limit_nano)
        redis.call('HINCRBY', bucket_key, durable_bucket_field(bucket), tostring(amount))
        redis.call('ZADD', expiry_key, tostring(expires), durable_expiry_member(fingerprint, bucket, tostring(amount)))
        reservation.reserved_nano = reservation.amount_nano
        reservation.accounted_nano = '0'
        reservation.reservation_revision = 1
        reservation.status = 'reserved'
    end
    local encoded = durable_encode(record)
    redis.call('SET', KEYS[1], encoded, 'EX', tostring(ttl))
    return {'created', encoded}
end

if ACTION == 'durable_reconcile' then
    if #KEYS < 3 or #KEYS > (1 + 2 * MAX_DURABLE_RESERVATIONS) or #ARGV ~= 5 then
        return {'invalid_request', ''}
    end
    local generation = ARGV[2]
    local incarnation = ARGV[3]
    local operation_id = ARGV[4]
    local events_ok, events = pcall(cjson.decode, ARGV[5])
    if not bounded_string(generation, 128, true) or not bounded_string(incarnation, 128, true) or
        not bounded_string(operation_id, 128, true) or not events_ok or type(events) ~= 'table' or
        #events == 0 or #events > MAX_DURABLE_EVENTS then
        return {'invalid_request', ''}
    end
    local raw = redis.call('GET', KEYS[1])
    if not raw then return {'not_found', ''} end
    local decoded_ok, record = pcall(cjson.decode, raw)
    if not decoded_ok or type(record) ~= 'table' or record.schema ~= DURABLE_SCHEMA then return {'state_unavailable', ''} end
    if record.generation_id ~= generation then return {'generation_mismatch', ''} end
    if record.incarnation_id ~= incarnation then return {'incarnation_mismatch', ''} end
    if record.operation_id ~= operation_id or record.status ~= 'accepted' then return {'not_found', ''} end
    if type(record.reservations) ~= 'table' or #record.reservations == 0 or #record.reservations > MAX_DURABLE_RESERVATIONS then
        return {'state_unavailable', ''}
    end
    if type(record.events) ~= 'table' then record.events = {} end
    -- Validate and stage the complete batch before mutating any aggregate. A
    -- later conflict must not leave earlier events in the batch applied.
    local staged = {}
    local staged_reservations = {}
    local staged_event_ids = {}
    for _, event in ipairs(events) do
        if type(event) ~= 'table' or not bounded_string(event.event_id, 128, true) or
            not bounded_string(event.window_id, 128, true) or not bounded_string(event.bucket_start_nanos, 64, true) or
            not bounded_string(event.reserved_decrease_nano, 64, true) or not bounded_string(event.accounted_increase_nano, 64, true) or
            not bounded_string(event.accounted_decrease_nano, 64, true) or not bounded_string(event.fingerprint, 64, true) or
            #event.fingerprint ~= 64 then return {'invalid_request', ''} end
        local batch_fingerprint = staged_event_ids[event.event_id]
        if batch_fingerprint then
            if batch_fingerprint ~= event.fingerprint then return {'conflict', ''} end
        else
            staged_event_ids[event.event_id] = event.fingerprint
            local prior_fingerprint = record.events[event.event_id]
            if prior_fingerprint then
                if prior_fingerprint ~= event.fingerprint then return {'conflict', ''} end
            else
                local reservation_index = nil
                for index, reservation in ipairs(record.reservations) do
                    if reservation.window_id == event.window_id and reservation.bucket_start_nanos == event.bucket_start_nanos then
                        if reservation_index then return {'not_found', ''} end
                        reservation_index = index
                    end
                end
                if not reservation_index or staged_reservations[reservation_index] then return {'conflict', ''} end
                local reservation = record.reservations[reservation_index]
                local revision = durable_int(event.reservation_revision)
                local reserved_decrease = durable_int(event.reserved_decrease_nano)
                local accounted_increase = durable_int(event.accounted_increase_nano)
                local accounted_decrease = durable_int(event.accounted_decrease_nano)
                local old_reserved = durable_int(reservation.reserved_nano or '0')
                local old_accounted = durable_int(reservation.accounted_nano or '0')
                if not revision or not reserved_decrease or not accounted_increase or not accounted_decrease or
                    not old_reserved or not old_accounted or revision <= (durable_int(reservation.reservation_revision or '0') or 0) or
                    reserved_decrease > old_reserved or accounted_decrease > old_accounted then
                    return {'conflict', ''}
                end
                local status = reservation.status or 'reserved'
                if status == 'finalized' then return {'finalized', ''} end
                if event.kind == 'retain_ambiguous' then
                    if status ~= 'reserved' or reserved_decrease ~= 0 or accounted_increase ~= 0 or accounted_decrease ~= 0 then return {'conflict', ''} end
                elseif event.kind == 'resolve_unknown_exact' then
                    if status ~= 'ambiguous' then return {'conflict', ''} end
                elseif event.kind == 'finalize_exact' or event.kind == 'finalize_unknown' or event.kind == 'release' then
                    if status ~= 'reserved' then return {'conflict', ''} end
                else
                    return {'invalid_request', ''}
                end
                local old_total = old_reserved + old_accounted
                local new_reserved = old_reserved - reserved_decrease
                local new_accounted = old_accounted + accounted_increase - accounted_decrease
                if new_accounted < 0 then return {'conflict', ''} end
                local new_total = new_reserved + new_accounted
                local bucket_key = KEYS[1 + (reservation_index - 1) * 2 + 1]
                local expiry_key = KEYS[1 + (reservation_index - 1) * 2 + 2]
                local bucket = reservation.bucket
                local current = durable_int(redis.call('HGET', bucket_key, durable_bucket_field(bucket)) or '0')
                if not current or current < old_total then return {'not_found', ''} end
                staged_reservations[reservation_index] = true
                staged[#staged + 1] = {
                    event = event, reservation_index = reservation_index, old_total = old_total,
                    new_reserved = new_reserved, new_accounted = new_accounted, new_total = new_total,
                    bucket_key = bucket_key, expiry_key = expiry_key, bucket = bucket,
                }
            end
        end
    end
    for _, item in ipairs(staged) do
        local event = item.event
        local reservation = record.reservations[item.reservation_index]
        if item.old_total > 0 then
            local decreased = redis.call('HINCRBY', item.bucket_key, durable_bucket_field(item.bucket), tostring(-item.old_total))
            if durable_int(decreased) == 0 then
                redis.call('HDEL', item.bucket_key, durable_bucket_field(item.bucket), durable_limit_field(item.bucket))
            end
            redis.call('ZREM', item.expiry_key, durable_expiry_member(record.fingerprint, item.bucket, tostring(item.old_total)))
        end
        if item.new_total > 0 then
            redis.call('HINCRBY', item.bucket_key, durable_bucket_field(item.bucket), tostring(item.new_total))
            redis.call('ZADD', item.expiry_key, tostring(reservation.expires_millis), durable_expiry_member(record.fingerprint, item.bucket, tostring(item.new_total)))
        end
        reservation.reserved_nano = tostring(item.new_reserved)
        reservation.accounted_nano = tostring(item.new_accounted)
        reservation.reservation_revision = durable_int(event.reservation_revision)
        if event.kind == 'retain_ambiguous' then reservation.status = 'ambiguous' else reservation.status = 'finalized' end
        record.events[event.event_id] = event.fingerprint
    end
    local encoded = durable_encode(record)
    durable_restore_record(KEYS[1], encoded, 0)
    return {'ok', encoded}
end

return {'invalid_request', ''}
