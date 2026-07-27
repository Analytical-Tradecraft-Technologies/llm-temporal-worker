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
local MAX_KEYS = 256
local MAX_KEY_BYTES = 1024
local MAX_ARGUMENTS = 16
local MAX_ARGUMENT_BYTES = 2 * 1024 * 1024
-- A begin call consumes three operation keys; continuation/finalization calls
-- consume two plus old/new reservation keys. Keep the reservation vector
-- within Redis' declared-key ceiling, and reject larger requests before any
-- read or mutation.
local MAX_RESERVATIONS = 253

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
    return attempt_number ~= nil and attempt_number <= 1000000
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
    if not requested or requested < 0 then
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
    if type(old) ~= 'table' or #old > MAX_RESERVATIONS or #reservations > MAX_RESERVATIONS or #KEYS ~= 2 + #old + #reservations or not reconcile(old, 3, incurred, ARGV[8]) then
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

return {'invalid_request', ''}
