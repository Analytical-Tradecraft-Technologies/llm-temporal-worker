package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/pricing"
	redisclient "github.com/redis/go-redis/v9"
)

const (
	BudgetColdStartReceiptSchema = "budget-cold-start-receipt/v1"
	BudgetColdStartStreamID      = "0-1"
)

var (
	ErrBudgetColdStartFunctionMismatch = errors.New("budget cold-start Redis function mismatch")
	ErrBudgetColdStartStateConflict    = errors.New("budget cold-start state conflict")
)

// BudgetColdStartReceipt is deliberately limited to a content-addressed
// identity. Created and replayed bootstraps return the same receipt, so the
// deployment Job cannot expose mutable Redis state or distinguish a replay.
type BudgetColdStartReceipt struct {
	Schema    string `json:"schema"`
	ReceiptID string `json:"receipt_id"`
}

func (receipt BudgetColdStartReceipt) Validate() error {
	if receipt.Schema != BudgetColdStartReceiptSchema || !sha256HexPattern.MatchString(receipt.ReceiptID) {
		return errors.New("budget cold-start receipt is invalid")
	}
	return nil
}

// BudgetColdStartFunctionClient is the read-only Function inspection boundary
// shared by deployment bootstrap and worker readiness.
type BudgetColdStartFunctionClient interface {
	FunctionList(context.Context, redisclient.FunctionListQuery) *redisclient.FunctionListCmd
}

// BudgetColdStartRedisClient adds the one atomic mutation used only by
// deployment bootstrap.
type BudgetColdStartRedisClient interface {
	BudgetColdStartFunctionClient
	Eval(context.Context, string, []string, ...interface{}) *redisclient.Cmd
}

type BudgetColdStartOptions struct {
	Client   BudgetColdStartRedisClient
	Keys     BudgetKeySpace
	Manifest BudgetManifest
}

// ValidateBudgetColdStartManifest applies the stricter deployment bootstrap
// contract without reading or mutating Redis.
func ValidateBudgetColdStartManifest(manifest BudgetManifest) error {
	_, _, _, _, _, err := canonicalColdStartState(manifest)
	return err
}

// Redis scripts are atomic with respect to other clients. The script creates
// the immutable manifest, active pointer, initial coordination record, and
// budget windows only when the complete key set is absent. Replay preserves
// the immutable bytes while accepting counters changed by legitimate budget
// activity and later, schema-valid coordination records for the same active
// generation.
const budgetColdStartScript = `
local function keytype(key)
  local value = redis.call('TYPE', key)
  if type(value) == 'table' then return value.ok end
  return value
end
local max_safe = tonumber(ARGV[6])
local max_event_bytes = tonumber(ARGV[7])
local function integer(value)
  if type(value) ~= 'string' or string.match(value, '^0[0-9]') then return nil end
  local parsed = tonumber(value)
  if not parsed or parsed < 0 or parsed > max_safe or parsed ~= math.floor(parsed) then return nil end
  return parsed
end
local function digest(value)
  return type(value) == 'string' and #value == 64 and string.match(value, '^[0-9a-f]+$') ~= nil
end
local allowed_kinds = {
  reserve=true, reconcile=true, release=true, policy_refresh=true,
  horizon_advance=true, generation_switch=true, denial=true
}
local function valid_later_event(payload, generation)
  if type(payload) ~= 'string' or #payload == 0 or #payload > max_event_bytes then return false end
  local decoded, event = pcall(cjson.decode, payload)
  if not decoded or type(event) ~= 'table' or event.schema ~= 'budget-event/v1' or not allowed_kinds[event.kind] then return false end
  if event.generation_id ~= generation then return false end
  if type(event.revision) ~= 'number' or event.revision < 0 or event.revision > max_safe or event.revision ~= math.floor(event.revision) then return false end
  if type(event.nano_delta) ~= 'number' or event.nano_delta < 0 or event.nano_delta > max_safe or event.nano_delta ~= math.floor(event.nano_delta) then return false end
  if type(event.occurred_at) ~= 'string' or not string.match(event.occurred_at, '^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%d') then return false end
  if event.operation_hash ~= nil and not digest(event.operation_hash) then return false end
  if event.member_hash ~= nil and not digest(event.member_hash) then return false end
  if event.kind == 'reserve' or event.kind == 'reconcile' or event.kind == 'release' then
    if not digest(event.operation_hash) or not digest(event.member_hash) then return false end
  end
  return true
end
local windows = cjson.decode(ARGV[5])
if #windows ~= #KEYS - 3 then return 0 end
local absent = keytype(KEYS[1]) == 'none' and keytype(KEYS[2]) == 'none' and keytype(KEYS[3]) == 'none'
for index = 4, #KEYS do
  if keytype(KEYS[index]) ~= 'none' then absent = false end
end
if absent then
  for index, fields in ipairs(windows) do
    redis.call('HSET', KEYS[index + 3], unpack(fields))
  end
  redis.call('SET', KEYS[2], ARGV[1])
  redis.call('SET', KEYS[1], ARGV[2])
  redis.call('XADD', KEYS[3], ARGV[4], 'event', ARGV[3])
  return 1
end
if keytype(KEYS[1]) ~= 'string' or keytype(KEYS[2]) ~= 'string' or keytype(KEYS[3]) ~= 'stream' then return 0 end
if redis.call('GET', KEYS[1]) ~= ARGV[2] or redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
for index, fields in ipairs(windows) do
  local key = KEYS[index + 3]
  if keytype(key) ~= 'hash' or redis.call('HLEN', key) ~= #fields / 2 then return 0 end
  local mutable = {}
  for field = 1, #fields, 2 do
    local name = fields[field]
    local actual = redis.call('HGET', key, name)
    if name == 'reserved_nano_usd' or name == 'accounted_nano_usd' then
      mutable[name] = integer(actual)
      if not mutable[name] then return 0 end
    elseif actual ~= fields[field + 1] then
      return 0
    end
  end
  local limit = integer(redis.call('HGET', key, 'limit_nano_usd'))
  local reserved = mutable.reserved_nano_usd
  local accounted = mutable.accounted_nano_usd
  if not limit or reserved > limit or accounted > limit - reserved then return 0 end
end
local bootstrap = redis.call('XRANGE', KEYS[3], ARGV[4], ARGV[4], 'COUNT', 1)
if #bootstrap ~= 1 or bootstrap[1][1] ~= ARGV[4] then return 0 end
local bootstrap_fields = bootstrap[1][2]
if #bootstrap_fields ~= 2 or bootstrap_fields[1] ~= 'event' or bootstrap_fields[2] ~= ARGV[3] then return 0 end
local pointer = cjson.decode(ARGV[2])
local cursor = '(' .. ARGV[4]
while true do
  local later = redis.call('XRANGE', KEYS[3], cursor, '+', 'COUNT', 256)
  if #later == 0 then break end
  for _, row in ipairs(later) do
    local fields = row[2]
    if #fields ~= 2 or fields[1] ~= 'event' or not valid_later_event(fields[2], pointer.generation_id) then return 0 end
  end
  cursor = '(' .. later[#later][1]
end
return 2
`

func BootstrapBudgetColdStart(ctx context.Context, options BudgetColdStartOptions) (BudgetColdStartReceipt, error) {
	if ctx == nil {
		return BudgetColdStartReceipt{}, errors.New("budget cold-start context is required")
	}
	if err := ctx.Err(); err != nil {
		return BudgetColdStartReceipt{}, err
	}
	if options.Client == nil {
		return BudgetColdStartReceipt{}, errors.New("budget cold-start Redis client is required")
	}
	if options.Keys.space.prefix == "" {
		return BudgetColdStartReceipt{}, errors.New("budget cold-start key space is required")
	}
	manifestBytes, pointerBytes, eventBytes, windowBytes, receipt, err := canonicalColdStartState(options.Manifest)
	if err != nil {
		return BudgetColdStartReceipt{}, err
	}
	if err := VerifyBudgetColdStartFunctions(ctx, options.Client); err != nil {
		return BudgetColdStartReceipt{}, err
	}
	keys := []string{options.Keys.ActiveGenerationKey(), options.Keys.ManifestKey(options.Manifest.GenerationID), options.Keys.EventsKey()}
	for _, member := range options.Manifest.Members {
		keys = append(keys, options.Keys.BudgetStatusWindowKey(options.Manifest.GenerationID, member))
	}
	result, err := options.Client.Eval(ctx, budgetColdStartScript, keys,
		string(manifestBytes), string(pointerBytes), string(eventBytes), BudgetColdStartStreamID, string(windowBytes),
		int64(pricing.NanoUSDSafeLimit), MaxBudgetStreamEventBytes).Int64()
	if err != nil {
		return BudgetColdStartReceipt{}, fmt.Errorf("execute budget cold-start transition: %w", err)
	}
	switch result {
	case 1, 2:
		return receipt, nil
	case 0:
		return BudgetColdStartReceipt{}, ErrBudgetColdStartStateConflict
	default:
		return BudgetColdStartReceipt{}, errors.New("budget cold-start returned an invalid result")
	}
}

func canonicalColdStartState(manifest BudgetManifest) ([]byte, []byte, []byte, []byte, BudgetColdStartReceipt, error) {
	if manifest.StreamHighWaterMark != BudgetColdStartStreamID || manifest.JournalHighWaterMark != 0 {
		return nil, nil, nil, nil, BudgetColdStartReceipt{}, fmt.Errorf("%w: initial stream and journal high-water marks are required", ErrBudgetManifestInvalid)
	}
	manifestBytes, err := manifest.Canonical()
	if err != nil {
		return nil, nil, nil, nil, BudgetColdStartReceipt{}, err
	}
	pointer, err := manifest.Pointer()
	if err != nil {
		return nil, nil, nil, nil, BudgetColdStartReceipt{}, err
	}
	pointerBytes, err := json.Marshal(pointer)
	if err != nil {
		return nil, nil, nil, nil, BudgetColdStartReceipt{}, errors.New("marshal budget cold-start pointer")
	}
	event := BudgetStreamEvent{
		Schema: budgetStreamEventSchema, Kind: BudgetEventGenerationSwitch,
		GenerationID: manifest.GenerationID, Revision: 0, NanoDelta: 0,
		OccurredAt: manifest.CoverageStart.UTC(),
	}
	eventBytes, err := event.Marshal()
	if err != nil {
		return nil, nil, nil, nil, BudgetColdStartReceipt{}, err
	}
	windowFields := make([][]string, 0, len(manifest.Members))
	for _, member := range manifest.Members {
		if member.LimitNanoUSD == "" {
			return nil, nil, nil, nil, BudgetColdStartReceipt{}, fmt.Errorf("%w: initial member budget is required", ErrBudgetManifestInvalid)
		}
		if _, err := parseNano(member.LimitNanoUSD); err != nil {
			return nil, nil, nil, nil, BudgetColdStartReceipt{}, fmt.Errorf("%w: initial member budget is invalid", ErrBudgetManifestInvalid)
		}
		windowFields = append(windowFields, []string{
			"schema", BudgetStatusWindowSchema,
			"generation_id", string(manifest.GenerationID),
			"incarnation_id", string(manifest.IncarnationID),
			"manifest_digest", pointer.ManifestDigest,
			"member_key", member.Key(),
			"limit_nano_usd", member.LimitNanoUSD,
			"reserved_nano_usd", "0",
			"accounted_nano_usd", "0",
			"coverage_start", member.CoverageStart.UTC().Format(time.RFC3339Nano),
			"coverage_end", member.CoverageEnd.UTC().Format(time.RFC3339Nano),
		})
	}
	windowBytes, err := json.Marshal(windowFields)
	if err != nil {
		return nil, nil, nil, nil, BudgetColdStartReceipt{}, errors.New("marshal budget cold-start windows")
	}
	identity := struct {
		Schema               string `json:"schema"`
		ManifestDigest       string `json:"manifest_digest"`
		AdmissionDigest      string `json:"admission_digest"`
		ThrottleDigest       string `json:"throttle_digest"`
		BudgetStatusDigest   string `json:"budget_status_digest"`
		CoordinationStreamID string `json:"coordination_stream_id"`
	}{
		Schema: BudgetColdStartReceiptSchema, ManifestDigest: pointer.ManifestDigest,
		AdmissionDigest: AdmissionFunctionDigest(), ThrottleDigest: ThrottleFunctionDigest(),
		BudgetStatusDigest: BudgetStatusFunctionDigest(), CoordinationStreamID: BudgetColdStartStreamID,
	}
	identityBytes, err := json.Marshal(identity)
	if err != nil {
		return nil, nil, nil, nil, BudgetColdStartReceipt{}, errors.New("marshal budget cold-start receipt identity")
	}
	digest := sha256.Sum256(identityBytes)
	receipt := BudgetColdStartReceipt{Schema: BudgetColdStartReceiptSchema, ReceiptID: hex.EncodeToString(digest[:])}
	return manifestBytes, pointerBytes, eventBytes, windowBytes, receipt, nil
}

type requiredColdStartFunction struct {
	library string
	version string
	digest  string
}

// VerifyBudgetColdStartFunctions proves that every Function used by admission,
// throttling, and coherent budget reads is the exact checked-in library. It is
// read-only so worker readiness can use the same proof without invoking the
// deployment-owned cold-start mutation.
func VerifyBudgetColdStartFunctions(ctx context.Context, client BudgetColdStartFunctionClient) error {
	if ctx == nil {
		return errors.New("budget cold-start context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if client == nil {
		return errors.New("budget cold-start Redis client is required")
	}
	required := []requiredColdStartFunction{
		{library: AdmissionFunctionLibrary, version: AdmissionFunctionVersion, digest: AdmissionFunctionDigest()},
		{library: ThrottleFunctionLibrary, version: ThrottleFunctionVersion, digest: ThrottleFunctionDigest()},
		{library: BudgetStatusFunctionLibrary, version: BudgetStatusFunctionVersion, digest: BudgetStatusFunctionDigest()},
	}
	libraries, err := client.FunctionList(ctx, redisclient.FunctionListQuery{WithCode: true}).Result()
	if err != nil {
		return fmt.Errorf("inspect budget cold-start Redis functions: %w", err)
	}
	byName := make(map[string][]redisclient.Library, len(libraries))
	for _, library := range libraries {
		byName[library.Name] = append(byName[library.Name], library)
	}
	for _, expected := range required {
		matches := byName[expected.library]
		if len(matches) != 1 {
			return ErrBudgetColdStartFunctionMismatch
		}
		library := matches[0]
		digest := sha256.Sum256([]byte(library.Code))
		if library.Engine != "LUA" ||
			hex.EncodeToString(digest[:]) != expected.digest ||
			!libraryContainsExactFunction(library, expected.version) {
			return ErrBudgetColdStartFunctionMismatch
		}
	}
	return nil
}

func libraryContainsExactFunction(library redisclient.Library, version string) bool {
	if len(library.Functions) != 1 {
		return false
	}
	function := library.Functions[0]
	return function.Name == version && function.Description == "" && len(function.Flags) == 0
}
