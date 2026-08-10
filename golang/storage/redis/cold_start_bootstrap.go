package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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
// the immutable manifest, active pointer, and first coordination record only
// when all three keys are absent. Any other shape is accepted only when all
// bytes match the canonical requested state exactly.
const budgetColdStartScript = `
local function keytype(key)
  local value = redis.call('TYPE', key)
  if type(value) == 'table' then return value.ok end
  return value
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
  for field = 1, #fields, 2 do
    if redis.call('HGET', key, fields[field]) ~= fields[field + 1] then return 0 end
  end
end
local info = redis.call('XINFO', 'STREAM', KEYS[3])
local function info_value(name)
  for index = 1, #info, 2 do
    if info[index] == name then return info[index + 1] end
  end
  return nil
end
if tonumber(info_value('length')) ~= 1 or tonumber(info_value('entries-added')) ~= 1 or tonumber(info_value('groups')) ~= 0 then return 0 end
if info_value('last-generated-id') ~= ARGV[4] or info_value('max-deleted-entry-id') ~= '0-0' then return 0 end
local rows = redis.call('XRANGE', KEYS[3], '-', '+', 'COUNT', 2)
if #rows ~= 1 or rows[1][1] ~= ARGV[4] then return 0 end
local fields = rows[1][2]
if #fields ~= 2 or fields[1] ~= 'event' or fields[2] ~= ARGV[3] then return 0 end
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
		string(manifestBytes), string(pointerBytes), string(eventBytes), BudgetColdStartStreamID, string(windowBytes)).Int64()
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
