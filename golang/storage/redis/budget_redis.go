package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	redisclient "github.com/redis/go-redis/v9"
)

// ErrBudgetActiveGenerationMissing distinguishes an intentionally empty
// dataset from a malformed pointer while preserving ErrBudgetManifestInvalid
// for callers that only need the fail-closed umbrella error.
var (
	ErrBudgetActiveGenerationMissing   = errors.New("active budget generation is missing")
	ErrBudgetActiveGenerationMalformed = errors.New("active budget generation is malformed")
)

// ErrBudgetStreamInvalid indicates that Redis returned a malformed or
// unverifiable coordination record. Stream hints are never authorization
// state, but accepting a malformed record could poison local readiness hints.
var ErrBudgetStreamInvalid = errors.New("invalid budget stream state")

// ErrBudgetStreamGap indicates that the requested cursor is older than the
// retained Stream prefix. Callers must discard local hints and reload the
// active generation; a Stream gap is never repaired from PostgreSQL online.
var ErrBudgetStreamGap = errors.New("budget stream cursor gap")

const (
	maxActiveBudgetGenerationBytes = 1024
	budgetStreamEventField         = "event"

	// This script stores a generation manifest and switches the active pointer
	// as one Redis operation. A generation ID is immutable: a retry with the
	// same canonical manifest is idempotent, while a changed payload is a
	// conflict. Both keys are in the configured hash tag and therefore one
	// Redis Cluster slot.
	budgetGenerationPublishScript = `
local existing = redis.call('GET', KEYS[2])
if existing then
  if existing ~= ARGV[1] then
    return 0
  end
else
  redis.call('SET', KEYS[2], ARGV[1])
end
redis.call('SET', KEYS[1], ARGV[2])
return 1
`
)

// BudgetGenerationRedisClient is the minimal Redis command seam required by
// RedisBudgetGenerationPort. redis.Client and redis.ClusterClient satisfy it.
// Keeping the seam small also permits deterministic command-level tests.
type BudgetGenerationRedisClient interface {
	Get(context.Context, string) *redisclient.StringCmd
	Eval(context.Context, string, []string, ...interface{}) *redisclient.Cmd
}

// RedisBudgetGenerationPort is the production implementation of
// BudgetGenerationPort. Manifests are immutable content-addressed values and
// the active pointer is switched only after the manifest has been stored in
// the same atomic Redis script invocation.
type RedisBudgetGenerationPort struct {
	client BudgetGenerationRedisClient
	keys   BudgetKeySpace
}

var _ BudgetGenerationPort = (*RedisBudgetGenerationPort)(nil)

func NewRedisBudgetGenerationPort(client BudgetGenerationRedisClient, keys BudgetKeySpace) (*RedisBudgetGenerationPort, error) {
	if client == nil {
		return nil, fmt.Errorf("Redis budget generation client is required")
	}
	if keys.space.prefix == "" {
		return nil, fmt.Errorf("Redis budget key space is required")
	}
	return &RedisBudgetGenerationPort{client: client, keys: keys}, nil
}

func (port *RedisBudgetGenerationPort) ActiveGeneration(ctx context.Context) (ActiveBudgetGeneration, error) {
	if err := ctx.Err(); err != nil {
		return ActiveBudgetGeneration{}, err
	}
	raw, err := port.client.Get(ctx, port.keys.ActiveGenerationKey()).Result()
	if errors.Is(err, redisclient.Nil) {
		return ActiveBudgetGeneration{}, fmt.Errorf("%w: %w", ErrBudgetManifestInvalid, ErrBudgetActiveGenerationMissing)
	}
	if err != nil {
		return ActiveBudgetGeneration{}, fmt.Errorf("read active budget generation: %w", err)
	}
	pointer, err := decodeActiveBudgetGeneration(raw)
	if err != nil {
		return ActiveBudgetGeneration{}, err
	}
	return pointer, nil
}

func (port *RedisBudgetGenerationPort) LoadManifest(ctx context.Context, pointer ActiveBudgetGeneration) (BudgetManifest, error) {
	if err := ctx.Err(); err != nil {
		return BudgetManifest{}, err
	}
	if err := validateActiveBudgetGeneration(pointer); err != nil {
		return BudgetManifest{}, err
	}
	raw, err := port.client.Get(ctx, port.keys.ManifestKey(pointer.GenerationID)).Result()
	if errors.Is(err, redisclient.Nil) {
		return BudgetManifest{}, fmt.Errorf("%w: manifest is missing", ErrBudgetManifestInvalid)
	}
	if err != nil {
		return BudgetManifest{}, fmt.Errorf("read budget manifest: %w", err)
	}
	if len(raw) > MaxBudgetManifestBytes {
		return BudgetManifest{}, fmt.Errorf("%w: manifest exceeds %d bytes", ErrBudgetManifestInvalid, MaxBudgetManifestBytes)
	}
	var manifest BudgetManifest
	if err := json.Unmarshal([]byte(raw), &manifest); err != nil {
		return BudgetManifest{}, fmt.Errorf("%w: decode manifest: %v", ErrBudgetManifestInvalid, err)
	}
	canonical, err := manifest.Canonical()
	if err != nil {
		return BudgetManifest{}, err
	}
	if !bytes.Equal([]byte(raw), canonical) {
		return BudgetManifest{}, fmt.Errorf("%w: manifest is not canonical", ErrBudgetManifestInvalid)
	}
	if err := pointer.ValidateAgainst(manifest); err != nil {
		return BudgetManifest{}, err
	}
	return manifest, nil
}

func (port *RedisBudgetGenerationPort) PublishGeneration(ctx context.Context, manifest BudgetManifest) (ActiveBudgetGeneration, error) {
	if err := ctx.Err(); err != nil {
		return ActiveBudgetGeneration{}, err
	}
	canonical, err := manifest.Canonical()
	if err != nil {
		return ActiveBudgetGeneration{}, err
	}
	pointer, err := manifest.Pointer()
	if err != nil {
		return ActiveBudgetGeneration{}, err
	}
	pointerJSON, err := json.Marshal(pointer)
	if err != nil {
		return ActiveBudgetGeneration{}, fmt.Errorf("marshal active budget generation: %w", err)
	}
	result, err := port.client.Eval(ctx, budgetGenerationPublishScript,
		[]string{port.keys.ActiveGenerationKey(), port.keys.ManifestKey(manifest.GenerationID)},
		string(canonical), string(pointerJSON),
	).Int64()
	if err != nil {
		return ActiveBudgetGeneration{}, fmt.Errorf("publish budget generation: %w", err)
	}
	if result == 0 {
		return ActiveBudgetGeneration{}, ErrBudgetGenerationConflict
	}
	if result != 1 {
		return ActiveBudgetGeneration{}, fmt.Errorf("publish budget generation returned unexpected result %d", result)
	}
	return pointer, nil
}

func validateActiveBudgetGeneration(pointer ActiveBudgetGeneration) error {
	if pointer.GenerationID == "" || pointer.IncarnationID == "" || len(pointer.ManifestDigest) != 64 || !sha256HexPattern.MatchString(pointer.ManifestDigest) {
		return fmt.Errorf("%w: active pointer is malformed", ErrBudgetManifestInvalid)
	}
	if err := validateOpaqueID("generation_id", string(pointer.GenerationID)); err != nil {
		return fmt.Errorf("%w: %v", ErrBudgetManifestInvalid, err)
	}
	if err := validateOpaqueID("incarnation_id", string(pointer.IncarnationID)); err != nil {
		return fmt.Errorf("%w: %v", ErrBudgetManifestInvalid, err)
	}
	return nil
}

func decodeActiveBudgetGeneration(raw string) (ActiveBudgetGeneration, error) {
	if len(raw) == 0 || len(raw) > maxActiveBudgetGenerationBytes {
		return ActiveBudgetGeneration{}, fmt.Errorf("%w: %w: active pointer is oversized or empty", ErrBudgetManifestInvalid, ErrBudgetActiveGenerationMalformed)
	}
	var pointer ActiveBudgetGeneration
	if err := json.Unmarshal([]byte(raw), &pointer); err != nil {
		return ActiveBudgetGeneration{}, fmt.Errorf("%w: %w: decode active pointer: %v", ErrBudgetManifestInvalid, ErrBudgetActiveGenerationMalformed, err)
	}
	if err := validateActiveBudgetGeneration(pointer); err != nil {
		return ActiveBudgetGeneration{}, fmt.Errorf("%w: %w", err, ErrBudgetActiveGenerationMalformed)
	}
	return pointer, nil
}

// BudgetEventRedisClient is the minimal Redis command seam required by
// RedisBudgetEventPort. The port deliberately does not expose consumer groups
// or blocking reads: every worker independently tails the broadcast Stream.
type BudgetEventRedisClient interface {
	XAdd(context.Context, *redisclient.XAddArgs) *redisclient.StringCmd
	XInfoStream(context.Context, string) *redisclient.XInfoStreamCmd
	XRead(context.Context, *redisclient.XReadArgs) *redisclient.XStreamSliceCmd
}

// RedisBudgetEventPort appends and independently reads bounded coordination
// events from the configured Redis Stream. The Stream is an optimization for
// wake-ups; the atomic admission Function remains the authorization source.
type RedisBudgetEventPort struct {
	client BudgetEventRedisClient
	keys   BudgetKeySpace
}

var _ BudgetEventPort = (*RedisBudgetEventPort)(nil)

func NewRedisBudgetEventPort(client BudgetEventRedisClient, keys BudgetKeySpace) (*RedisBudgetEventPort, error) {
	if client == nil {
		return nil, fmt.Errorf("Redis budget event client is required")
	}
	if keys.space.prefix == "" {
		return nil, fmt.Errorf("Redis budget key space is required")
	}
	return &RedisBudgetEventPort{client: client, keys: keys}, nil
}

// CurrentCursor returns the latest retained Stream ID. Tailer recovery reads
// this before reloading the manifest so records appended during the reload
// remain visible to the next XREAD rather than being skipped.
func (port *RedisBudgetEventPort) CurrentCursor(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	info, err := port.client.XInfoStream(ctx, port.keys.EventsKey()).Result()
	if errors.Is(err, redisclient.Nil) {
		return "", ErrBudgetStreamCursorUnavailable
	}
	if err != nil {
		return "", fmt.Errorf("inspect budget stream cursor: %w", err)
	}
	if info == nil || info.LastEntry.ID == "" || len(info.LastEntry.ID) > MaxBudgetStreamIDBytes {
		return "", ErrBudgetStreamCursorUnavailable
	}
	if _, _, err := parseRedisStreamID(info.LastEntry.ID); err != nil {
		return "", fmt.Errorf("%w: invalid Redis Stream cursor", ErrBudgetStreamCursorUnavailable)
	}
	return info.LastEntry.ID, nil
}

func (port *RedisBudgetEventPort) Append(ctx context.Context, event BudgetStreamEvent) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	payload, err := event.Marshal()
	if err != nil {
		return "", err
	}
	id, err := port.client.XAdd(ctx, &redisclient.XAddArgs{
		Stream: port.keys.EventsKey(),
		ID:     "*",
		Values: map[string]interface{}{budgetStreamEventField: string(payload)},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("append budget stream event: %w", err)
	}
	if _, _, err := parseRedisStreamID(id); err != nil || len(id) > MaxBudgetStreamIDBytes {
		return "", fmt.Errorf("%w: Redis returned invalid stream ID", ErrBudgetStreamInvalid)
	}
	return id, nil
}

func (port *RedisBudgetEventPort) Read(ctx context.Context, cursor string, limit int) ([]BudgetStreamRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 10_000 {
		return nil, fmt.Errorf("stream read limit must be between 1 and 10000")
	}
	afterMajor, afterMinor := uint64(0), uint64(0)
	if cursor != "" {
		var err error
		afterMajor, afterMinor, err = parseRedisStreamID(cursor)
		if err != nil {
			return nil, err
		}
		info, err := port.client.XInfoStream(ctx, port.keys.EventsKey()).Result()
		if errors.Is(err, redisclient.Nil) {
			return nil, ErrBudgetStreamGap
		}
		if err != nil {
			return nil, fmt.Errorf("inspect budget stream: %w", err)
		}
		if info == nil || info.FirstEntry.ID == "" {
			return nil, ErrBudgetStreamGap
		}
		firstMajor, firstMinor, err := parseRedisStreamID(info.FirstEntry.ID)
		if err != nil || len(info.FirstEntry.ID) > MaxBudgetStreamIDBytes {
			return nil, fmt.Errorf("%w: invalid Redis stream first ID", ErrBudgetStreamInvalid)
		}
		if firstMajor > afterMajor || (firstMajor == afterMajor && firstMinor > afterMinor) {
			return nil, ErrBudgetStreamGap
		}
	}
	streams, err := port.client.XRead(ctx, &redisclient.XReadArgs{
		Streams: []string{port.keys.EventsKey()},
		ID:      cursorOrZero(cursor),
		Count:   int64(limit),
		Block:   -1, // omit BLOCK; zero would block forever in Redis.
	}).Result()
	if errors.Is(err, redisclient.Nil) {
		return []BudgetStreamRecord{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read budget stream: %w", err)
	}
	result := make([]BudgetStreamRecord, 0, limit)
	lastMajor, lastMinor := afterMajor, afterMinor
	for _, stream := range streams {
		if stream.Stream != port.keys.EventsKey() {
			return nil, fmt.Errorf("%w: unexpected Redis stream key", ErrBudgetStreamInvalid)
		}
		for _, message := range stream.Messages {
			major, minor, err := parseRedisStreamID(message.ID)
			if err != nil || len(message.ID) > MaxBudgetStreamIDBytes || major < lastMajor || (major == lastMajor && minor <= lastMinor) {
				return nil, fmt.Errorf("%w: invalid or non-advancing Redis stream ID", ErrBudgetStreamInvalid)
			}
			lastMajor, lastMinor = major, minor
			event, err := decodeBudgetStreamEvent(message.Values[budgetStreamEventField])
			if err != nil {
				return nil, err
			}
			result = append(result, BudgetStreamRecord{ID: message.ID, Event: event})
			if len(result) == limit {
				return result, nil
			}
		}
	}
	return result, nil
}

func cursorOrZero(cursor string) string {
	if strings.TrimSpace(cursor) == "" {
		return "0-0"
	}
	return cursor
}

func decodeBudgetStreamEvent(value interface{}) (BudgetStreamEvent, error) {
	var payload []byte
	switch value := value.(type) {
	case string:
		payload = []byte(value)
	case []byte:
		payload = value
	default:
		return BudgetStreamEvent{}, fmt.Errorf("%w: event field has unsupported type", ErrBudgetStreamInvalid)
	}
	if len(payload) == 0 || len(payload) > MaxBudgetStreamEventBytes {
		return BudgetStreamEvent{}, fmt.Errorf("%w: event payload is oversized or empty", ErrBudgetStreamInvalid)
	}
	var event BudgetStreamEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return BudgetStreamEvent{}, fmt.Errorf("%w: decode event: %v", ErrBudgetStreamInvalid, err)
	}
	if err := event.Validate(); err != nil {
		return BudgetStreamEvent{}, fmt.Errorf("%w: %v", ErrBudgetStreamInvalid, err)
	}
	return event, nil
}
