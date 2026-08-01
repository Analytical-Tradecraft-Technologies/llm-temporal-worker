package redis

// This file contains the read-only v2 budget-status boundary. It is kept
// separate from the durable materializer because v1 aggregate hashes are not
// a coherent query snapshot and must never be reinterpreted as one.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	redisclient "github.com/redis/go-redis/v9"
)

const (
	// BudgetStatusWindowSchema is intentionally distinct from the durable v1
	// record schema. A hash without this exact marker is unsupported.
	BudgetStatusWindowSchema    = "budget-window/v2"
	BudgetStatusFunctionLibrary = "llmtw_budget_status_v2"
	BudgetStatusFunctionVersion = "budget_status_v2"
	BudgetStatusAction          = "read"
	BudgetStatusMaxExpiryDrain  = 1024
	maxBudgetStatusMembers      = 4096
)

var (
	ErrBudgetStatusUnavailable   = errors.New("Redis budget status is unavailable")
	ErrBudgetHistoryNotAvailable = errors.New("budget_history_not_available")
	budgetStatusStreamID         = regexp.MustCompile(`^[0-9]+-[0-9]+$`)
)

// BudgetStatusWindowRecord is the storage-neutral DTO returned by the
// versioned Redis Function. All values remain decimal strings at this
// boundary; conversion to exact USD happens only after validation.
type BudgetStatusWindowRecord struct {
	Schema           string    `json:"schema"`
	GenerationID     string    `json:"generation_id"`
	IncarnationID    string    `json:"incarnation_id"`
	ManifestDigest   string    `json:"manifest_digest"`
	MemberKey        string    `json:"member_key"`
	LimitNanoUSD     string    `json:"limit_nano_usd"`
	ReservedNanoUSD  string    `json:"reserved_nano_usd"`
	AccountedNanoUSD string    `json:"accounted_nano_usd"`
	CoverageStart    time.Time `json:"coverage_start"`
	CoverageEnd      time.Time `json:"coverage_end"`
}

// BudgetStatusRead is the complete, coherent Function result. Member order
// is the immutable manifest order and therefore cannot be interpreted as a
// second, independently fetched catalog.
type BudgetStatusRead struct {
	GenerationID        string                     `json:"generation_id"`
	IncarnationID       string                     `json:"incarnation_id"`
	ManifestDigest      string                     `json:"manifest_digest"`
	StreamHighWaterMark string                     `json:"stream_high_water_mark"`
	Members             []BudgetStatusWindowRecord `json:"members"`
}

// BudgetStatusReaderOptions configures one snapshot-owned reader. Generation
// and Keys must belong to the same immutable Redis snapshot as the worker.
type BudgetStatusReaderOptions struct {
	Client          redisclient.Scripter
	Invoker         FunctionInvoker
	Generation      BudgetGenerationPort
	Keys            BudgetKeySpace
	Mode            AdmissionMode
	FunctionVersion string
	Clock           func() time.Time
	MaxExpiryDrain  int
}

type budgetStatusInvoker struct {
	client  redisclient.Scripter
	mode    AdmissionMode
	version string
}

func (invoker budgetStatusInvoker) Run(ctx context.Context, name string, keys []string, args ...string) ([]any, error) {
	if name != invoker.version || name != BudgetStatusFunctionVersion {
		return nil, fmt.Errorf("unsupported Redis budget status function %q", name)
	}
	if invoker.client == nil {
		return nil, errors.New("Redis budget status Function client is required")
	}
	values := make([]interface{}, len(args))
	for i, value := range args {
		values[i] = value
	}
	var result interface{}
	var err error
	switch invoker.mode {
	case AdmissionModeFunction:
		caller, ok := invoker.client.(interface {
			FCall(context.Context, string, []string, ...interface{}) *redisclient.Cmd
		})
		if !ok {
			return nil, errors.New("Redis budget status client does not support FCALL")
		}
		result, err = caller.FCall(ctx, name, keys, values...).Result()
	case AdmissionModeLua:
		result, err = invoker.client.EvalSha(ctx, budgetStatusScript.Hash(), keys, values...).Result()
	default:
		return nil, fmt.Errorf("unsupported Redis budget status mode %q", invoker.mode)
	}
	if err != nil {
		return nil, err
	}
	array, ok := result.([]interface{})
	if !ok {
		return nil, errors.New("Redis budget status Function returned an invalid result")
	}
	return array, nil
}

// RedisBudgetStatusReader implements internal/runtime.BudgetStatusReader by
// method shape, without importing the internal runtime package.
type RedisBudgetStatusReader struct {
	generation BudgetGenerationPort
	keys       BudgetKeySpace
	invoke     FunctionInvoker
	function   string
	clock      func() time.Time
	maxExpiry  int
}

func NewRedisBudgetStatusReader(options BudgetStatusReaderOptions) (*RedisBudgetStatusReader, error) {
	if options.Generation == nil {
		return nil, errors.New("Redis budget status generation port is required")
	}
	if options.Keys.space.prefix == "" {
		return nil, errors.New("Redis budget status key space is required")
	}
	function := options.FunctionVersion
	if function == "" {
		function = BudgetStatusFunctionVersion
	}
	if function != BudgetStatusFunctionVersion {
		return nil, fmt.Errorf("unsupported Redis budget status function %q", function)
	}
	invoke := options.Invoker
	if invoke == nil {
		if options.Client == nil {
			return nil, errors.New("Redis budget status Function client is required")
		}
		invoke = budgetStatusInvoker{client: options.Client, mode: options.Mode, version: function}
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.MaxExpiryDrain <= 0 || options.MaxExpiryDrain > BudgetStatusMaxExpiryDrain {
		options.MaxExpiryDrain = BudgetStatusMaxExpiryDrain
	}
	return &RedisBudgetStatusReader{generation: options.Generation, keys: options.Keys, invoke: invoke, function: function, clock: options.Clock, maxExpiry: options.MaxExpiryDrain}, nil
}

var _ interface {
	ReadBudgetStatus(context.Context, control.BudgetStatusQuery, time.Time) (control.BudgetStatusResult, error)
} = (*RedisBudgetStatusReader)(nil)

func (reader *RedisBudgetStatusReader) ReadBudgetStatus(ctx context.Context, query control.BudgetStatusQuery, activeAt time.Time) (control.BudgetStatusResult, error) {
	if reader == nil {
		return control.BudgetStatusResult{}, ErrBudgetStatusUnavailable
	}
	if ctx == nil {
		return control.BudgetStatusResult{}, errors.New("Redis budget status context is nil")
	}
	if err := ctx.Err(); err != nil {
		return control.BudgetStatusResult{}, err
	}
	activeAt = activeAt.UTC()
	if activeAt.IsZero() {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: active_at is required", ErrBudgetStatusUnavailable)
	}
	// The query is current-only. A materially old instant is a typed history
	// miss, never a request to replay a covered generation.
	now := reader.clock().UTC()
	if activeAt.Before(now.Add(-time.Second)) {
		return control.BudgetStatusResult{}, ErrBudgetHistoryNotAvailable
	}
	pointer, err := reader.generation.ActiveGeneration(ctx)
	if err != nil {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: active generation: %v", ErrBudgetStatusUnavailable, err)
	}
	manifest, err := reader.generation.LoadManifest(ctx, pointer)
	if err != nil {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: manifest: %v", ErrBudgetStatusUnavailable, err)
	}
	if err := manifest.Validate(); err != nil {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: manifest: %v", ErrBudgetStatusUnavailable, err)
	}
	digest, err := manifest.ManifestDigestHex()
	if err != nil || digest != pointer.ManifestDigest {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: manifest digest mismatch", ErrBudgetStatusUnavailable)
	}
	if activeAt.Before(manifest.CoverageStart) || !activeAt.Before(manifest.CoverageEnd) {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: active_at outside coverage", ErrBudgetStatusUnavailable)
	}
	if len(manifest.Members) == 0 || len(manifest.Members) > maxBudgetStatusMembers {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: member catalog is incomplete", ErrBudgetStatusUnavailable)
	}
	if query.PolicyKey != nil && string(*query.PolicyKey) == "" {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: policy key is empty", ErrBudgetStatusUnavailable)
	}
	keys := []string{reader.keys.EventsKey()}
	for _, member := range manifest.Members {
		if member.LimitNanoUSD == "" {
			return control.BudgetStatusResult{}, fmt.Errorf("%w: member %q has no v2 limit", ErrBudgetStatusUnavailable, member.Key())
		}
		if _, err := parseNano(member.LimitNanoUSD); err != nil {
			return control.BudgetStatusResult{}, fmt.Errorf("%w: member %q limit: %v", ErrBudgetStatusUnavailable, member.Key(), err)
		}
		keys = append(keys, reader.keys.BudgetStatusWindowKey(manifest.GenerationID, member), reader.keys.BudgetStatusExpiryKey(manifest.GenerationID, member))
	}
	if query.PolicyKey != nil {
		found := false
		for _, member := range manifest.Members {
			if string(*query.PolicyKey) == member.PolicyID {
				found = true
				break
			}
		}
		if !found {
			return control.BudgetStatusResult{}, fmt.Errorf("%w: policy key is unknown", ErrBudgetStatusUnavailable)
		}
	}
	args := []string{BudgetStatusAction, string(pointer.GenerationID), string(pointer.IncarnationID), digest, strconv.FormatInt(now.UnixMilli(), 10), strconv.Itoa(len(manifest.Members)), strconv.Itoa(reader.maxExpiry)}
	raw, err := reader.invoke.Run(ctx, reader.function, keys, args...)
	if err != nil {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: coherent Redis read: %v", ErrBudgetStatusUnavailable, err)
	}
	status, payload, err := budgetStatusFunctionResult(raw)
	if err != nil || status != "ok" {
		if err == nil {
			err = errors.New("Function rejected the snapshot")
		}
		return control.BudgetStatusResult{}, fmt.Errorf("%w: %v", ErrBudgetStatusUnavailable, err)
	}
	var read BudgetStatusRead
	if err := json.Unmarshal([]byte(payload), &read); err != nil {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: decode snapshot: %v", ErrBudgetStatusUnavailable, err)
	}
	if err := validateBudgetStatusRead(read, pointer, manifest, digest); err != nil {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: %v", ErrBudgetStatusUnavailable, err)
	}
	result := control.BudgetStatusResult{ActiveAt: activeAt, GenerationID: control.BudgetGenerationID(read.GenerationID), ManifestDigest: control.ManifestDigest(read.ManifestDigest), StreamHighWaterMark: control.StreamHighWaterMark(read.StreamHighWaterMark)}
	include := query.IncludeWindows == nil || *query.IncludeWindows
	if include {
		result.Windows = make([]control.BudgetWindow, 0, len(read.Members))
	}
	for index, value := range read.Members {
		member := manifest.Members[index]
		if query.PolicyKey != nil && string(*query.PolicyKey) != member.PolicyID {
			continue
		}
		limit, _ := parseNano(value.LimitNanoUSD)
		reserved, _ := parseNano(value.ReservedNanoUSD)
		accounted, _ := parseNano(value.AccountedNanoUSD)
		available := limit - reserved - accounted
		limitUSD, _ := pricing.USDFromNano(pricing.NanoUSD(limit))
		reservedUSD, _ := pricing.USDFromNano(pricing.NanoUSD(reserved))
		accountedUSD, _ := pricing.USDFromNano(pricing.NanoUSD(accounted))
		availableUSD, _ := pricing.USDFromNano(pricing.NanoUSD(available))
		if include {
			result.Windows = append(result.Windows, control.BudgetWindow{PolicyKey: control.PolicyKey(member.PolicyID), WindowKey: control.WindowKey(member.WindowID), CoverageStart: member.CoverageStart, CoverageEnd: member.CoverageEnd, LimitUSD: control.DecimalUSD(limitUSD.String()), ReservedCostUSD: control.DecimalUSD(reservedUSD.String()), AccountedCostUSD: control.DecimalUSD(accountedUSD.String()), AvailableUSD: control.DecimalUSD(availableUSD.String())})
		}
	}
	return result, nil
}

func budgetStatusFunctionResult(values []any) (string, string, error) {
	if len(values) != 2 {
		return "", "", errors.New("Function result must contain status and payload")
	}
	status, ok := values[0].(string)
	if !ok {
		return "", "", errors.New("Function status is malformed")
	}
	payload, ok := values[1].(string)
	if !ok {
		return "", "", errors.New("Function payload is malformed")
	}
	return status, payload, nil
}

func validateBudgetStatusRead(read BudgetStatusRead, pointer ActiveBudgetGeneration, manifest BudgetManifest, digest string) error {
	if read.GenerationID != string(pointer.GenerationID) || read.IncarnationID != string(pointer.IncarnationID) || read.ManifestDigest != digest {
		return errors.New("snapshot provenance mismatch")
	}
	if !budgetStatusStreamID.MatchString(read.StreamHighWaterMark) {
		return errors.New("stream high-water mark is malformed")
	}
	if len(read.Members) != len(manifest.Members) {
		return errors.New("snapshot member count mismatch")
	}
	seen := make(map[string]struct{}, len(read.Members))
	for index, value := range read.Members {
		member := manifest.Members[index]
		if value.Schema != BudgetStatusWindowSchema || value.GenerationID != string(pointer.GenerationID) || value.IncarnationID != string(pointer.IncarnationID) || value.ManifestDigest != digest || value.MemberKey != member.Key() {
			return fmt.Errorf("member %d provenance mismatch", index)
		}
		if value.CoverageStart.IsZero() || value.CoverageEnd.IsZero() || !value.CoverageStart.Equal(member.CoverageStart) || !value.CoverageEnd.Equal(member.CoverageEnd) {
			return fmt.Errorf("member %d coverage mismatch", index)
		}
		if _, duplicate := seen[value.MemberKey]; duplicate {
			return errors.New("duplicate snapshot member")
		}
		seen[value.MemberKey] = struct{}{}
		limit, err := parseNano(value.LimitNanoUSD)
		if err != nil {
			return fmt.Errorf("member %d limit: %v", index, err)
		}
		reserved, err := parseNano(value.ReservedNanoUSD)
		if err != nil {
			return fmt.Errorf("member %d reserved: %v", index, err)
		}
		accounted, err := parseNano(value.AccountedNanoUSD)
		if err != nil {
			return fmt.Errorf("member %d accounted: %v", index, err)
		}
		if limit != int64FromManifest(member.LimitNanoUSD) {
			return fmt.Errorf("member %d limit provenance mismatch", index)
		}
		if reserved > limit || accounted > limit-reserved {
			return fmt.Errorf("member %d exceeds limit", index)
		}
	}
	return nil
}

func int64FromManifest(value string) int64 { parsed, _ := parseNano(value); return parsed }

func parseNano(value string) (int64, error) {
	if value == "" || strings.TrimSpace(value) != value || (len(value) > 1 && value[0] == '0') {
		return 0, errors.New("integer is malformed")
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 || parsed > int64(pricing.NanoUSDSafeLimit) {
		return 0, errors.New("integer is outside Redis safe range")
	}
	return parsed, nil
}

// BudgetStatusFunctionDigest is the immutable library identity used by
// readiness/deployment tooling.
func BudgetStatusFunctionDigest() string {
	digest := sha256.Sum256([]byte(BudgetStatusFunctionLibrarySource()))
	return hex.EncodeToString(digest[:])
}

func BudgetStatusFunctionLibrarySource() string {
	return "#!lua name=" + BudgetStatusFunctionLibrary + "\nredis.register_function('" + BudgetStatusFunctionVersion + "', function(KEYS, ARGV)\n" + budgetStatusFunctionSource + "\nend)\n"
}

var budgetStatusScript = redisclient.NewScript(budgetStatusFunctionSource)

const budgetStatusFunctionSource = `
local MAX_SAFE = 9007199254740991
local SCHEMA = 'budget-window/v2'
local function integer(value)
  if type(value) ~= 'string' or string.match(value, '^0[0-9]') then return nil end
  local parsed = tonumber(value)
  if not parsed or parsed < 0 or parsed > MAX_SAFE or parsed ~= math.floor(parsed) then return nil end
  return parsed
end
local function stream_id(value) return type(value) == 'string' and string.match(value, '^[0-9]+%-[0-9]+$') ~= nil end
if ARGV[1] ~= 'read' or #ARGV ~= 7 then return {'invalid_request', ''} end
local generation, incarnation, digest = ARGV[2], ARGV[3], ARGV[4]
local requested_now, count, expiry_limit = integer(ARGV[5]), integer(ARGV[6]), integer(ARGV[7])
if not generation or not incarnation or #generation == 0 or #incarnation == 0 or type(digest) ~= 'string' or #digest ~= 64 or not requested_now or not count or count == 0 or count > 4096 or not expiry_limit or expiry_limit == 0 or #KEYS ~= 1 + 2 * count then return {'invalid_request', ''} end
local redis_time = redis.call('TIME')
local now = integer(redis_time[1]) * 1000 + math.floor(integer(redis_time[2]) / 1000)
if not now or math.abs(now - requested_now) > 60000 then return {'state_unavailable', ''} end
local members = {}
for i = 1, count do
  local window_key = KEYS[1 + (i - 1) * 2 + 1]
  local expiry_key = KEYS[1 + (i - 1) * 2 + 2]
  local expired = redis.call('ZRANGEBYSCORE', expiry_key, '-inf', tostring(now), 'LIMIT', 0, expiry_limit)
  for _, item in ipairs(expired) do
    local item_generation, amount = string.match(item, '^([^|]+)|([0-9]+)$')
    local delta = integer(amount)
    if item_generation ~= generation or not delta or delta == 0 then return {'state_unavailable', ''} end
    local reserved = integer(redis.call('HGET', window_key, 'reserved_nano_usd') or '')
    if not reserved or delta > reserved then return {'state_unavailable', ''} end
    redis.call('HINCRBY', window_key, 'reserved_nano_usd', tostring(-delta))
    redis.call('ZREM', expiry_key, item)
  end
  local values = redis.call('HMGET', window_key, 'schema', 'generation_id', 'incarnation_id', 'manifest_digest', 'member_key', 'limit_nano_usd', 'reserved_nano_usd', 'accounted_nano_usd', 'coverage_start', 'coverage_end')
  for _, value in ipairs(values) do if value == false or value == nil then return {'state_unavailable', ''} end end
  local limit, reserved, accounted = integer(values[6]), integer(values[7]), integer(values[8])
	if values[1] ~= SCHEMA or values[2] ~= generation or values[3] ~= incarnation or values[4] ~= digest or not limit or not reserved or not accounted or reserved > limit or accounted > limit - reserved then return {'state_unavailable', ''} end
  members[i] = {schema=values[1], generation_id=values[2], incarnation_id=values[3], manifest_digest=values[4], member_key=values[5], limit_nano_usd=values[6], reserved_nano_usd=values[7], accounted_nano_usd=values[8], coverage_start=values[9], coverage_end=values[10]}
end
local info = redis.call('XINFO', 'STREAM', KEYS[1])
local high_water = nil
for i = 1, #info, 2 do if info[i] == 'last-generated-id' then high_water = info[i + 1] end end
if not stream_id(high_water) then return {'state_unavailable', ''} end
return {'ok', cjson.encode({generation_id=generation, incarnation_id=incarnation, manifest_digest=digest, stream_high_water_mark=high_water, members=members})}
`
