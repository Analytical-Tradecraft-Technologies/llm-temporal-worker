package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

// BudgetStreamEventKind is the bounded coordination vocabulary for the Redis
// budget Stream. Events invalidate local hints and wake rebuild waiters; they
// never authorize a request (the atomic budget Function remains authoritative).
type BudgetStreamEventKind string

const (
	BudgetEventReserve          BudgetStreamEventKind = "reserve"
	BudgetEventReconcile        BudgetStreamEventKind = "reconcile"
	BudgetEventRelease          BudgetStreamEventKind = "release"
	BudgetEventPolicyRefresh    BudgetStreamEventKind = "policy_refresh"
	BudgetEventHorizonAdvance   BudgetStreamEventKind = "horizon_advance"
	BudgetEventGenerationSwitch BudgetStreamEventKind = "generation_switch"
	BudgetEventDenial           BudgetStreamEventKind = "denial"
)

var ErrBudgetGenerationConflict = errors.New("budget generation is immutable")

const (
	BudgetActiveGenerationSuffix = "budget:active-generation"
	BudgetEventsSuffix           = "budget:events"
	BudgetWorkersSuffix          = "budget:workers"
	MaxBudgetStreamEventBytes    = 8 << 10
)

var redisStreamIDPattern = regexp.MustCompile(`^[0-9]+-[0-9]+$`)

// BudgetStreamEvent contains only opaque digests and bounded accounting
// metadata. Raw tenant, operation, provider, and prompt identifiers must not
// enter Redis coordination values or observability output.
type BudgetStreamEvent struct {
	Schema        string                `json:"schema"`
	Kind          BudgetStreamEventKind `json:"kind"`
	GenerationID  BudgetGenerationID    `json:"generation_id"`
	OperationHash string                `json:"operation_hash,omitempty"`
	MemberHash    string                `json:"member_hash,omitempty"`
	Revision      int64                 `json:"revision"`
	NanoDelta     int64                 `json:"nano_delta"`
	OccurredAt    time.Time             `json:"occurred_at"`
}

const budgetStreamEventSchema = "budget-event/v1"

func (event BudgetStreamEvent) Validate() error {
	if event.Schema != budgetStreamEventSchema {
		return fmt.Errorf("unsupported budget stream event schema %q", event.Schema)
	}
	switch event.Kind {
	case BudgetEventReserve, BudgetEventReconcile, BudgetEventRelease, BudgetEventPolicyRefresh, BudgetEventHorizonAdvance, BudgetEventGenerationSwitch, BudgetEventDenial:
	default:
		return fmt.Errorf("unsupported budget stream event kind %q", event.Kind)
	}
	if err := validateOpaqueID("generation_id", string(event.GenerationID)); err != nil {
		return err
	}
	for name, value := range map[string]string{"operation_hash": event.OperationHash, "member_hash": event.MemberHash} {
		if value != "" && !sha256HexPattern.MatchString(value) {
			return fmt.Errorf("%s must be a lowercase SHA-256 digest", name)
		}
	}
	if event.Revision < 0 || event.NanoDelta < 0 || event.NanoDelta > int64(pricing.NanoUSDSafeLimit) {
		return errors.New("budget stream event counters must be non-negative")
	}
	if event.OccurredAt.IsZero() {
		return errors.New("budget stream event timestamp is required")
	}
	if event.Kind == BudgetEventReserve || event.Kind == BudgetEventReconcile || event.Kind == BudgetEventRelease {
		if event.OperationHash == "" || event.MemberHash == "" {
			return errors.New("accounting event requires operation and member digests")
		}
	}
	return nil
}

func (event BudgetStreamEvent) Marshal() ([]byte, error) {
	if err := event.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal budget stream event: %w", err)
	}
	if len(data) > MaxBudgetStreamEventBytes {
		return nil, fmt.Errorf("budget stream event exceeds %d bytes", MaxBudgetStreamEventBytes)
	}
	return data, nil
}

// BudgetGenerationPort is the explicit boundary used by readiness, adoption,
// and the future fenced bootstrap coordinator. It does not expose a PostgreSQL
// fallback: callers must prove an allowed rebuild condition before invoking a
// recovery implementation.
type BudgetGenerationPort interface {
	ActiveGeneration(context.Context) (ActiveBudgetGeneration, error)
	LoadManifest(context.Context, ActiveBudgetGeneration) (BudgetManifest, error)
	PublishGeneration(context.Context, BudgetManifest) (ActiveBudgetGeneration, error)
}

// BudgetEventPort is the broadcast coordination boundary. A cursor is a
// Redis Stream ID; each worker reads independently rather than using a shared
// consumer group.
type BudgetEventPort interface {
	Append(context.Context, BudgetStreamEvent) (string, error)
	Read(context.Context, string, int) ([]BudgetStreamRecord, error)
}

type BudgetStreamRecord struct {
	ID    string
	Event BudgetStreamEvent
}

// BudgetKeySpace exposes only the stable generation/Stream key families. The
// underlying HMAC secret remains private to keySpace, so callers cannot turn
// raw identifiers into Redis keys accidentally.
type BudgetKeySpace struct{ space keySpace }

func NewBudgetKeySpace(options KeyOptions) (BudgetKeySpace, error) {
	space, err := newKeySpace(options)
	if err != nil {
		return BudgetKeySpace{}, err
	}
	return BudgetKeySpace{space: space}, nil
}

func (keys BudgetKeySpace) ActiveGenerationKey() string {
	return keys.space.admissionPrefix() + BudgetActiveGenerationSuffix
}
func (keys BudgetKeySpace) EventsKey() string {
	return keys.space.admissionPrefix() + BudgetEventsSuffix
}
func (keys BudgetKeySpace) WorkersKey() string {
	return keys.space.admissionPrefix() + BudgetWorkersSuffix
}

func (keys BudgetKeySpace) ManifestKey(generation BudgetGenerationID) string {
	return keys.space.admissionPrefix() + "budget:g:" + keys.space.digest("budget-generation", string(generation)) + ":manifest"
}

// BudgetStatusWindowKey derives the v2 generation-scoped window hash. The
// generation is part of the HMAC input, so a stale member can never be read
// under a newly adopted pointer even when its policy/window identity matches.
func (keys BudgetKeySpace) BudgetStatusWindowKey(generation BudgetGenerationID, member BudgetManifestMember) string {
	return keys.space.admissionPrefix() + "budget:v2:" + keys.space.digest("budget-status-window", string(generation), member.Key())
}

// BudgetStatusExpiryKey is the bounded expiry index paired with one v2 window
// hash. It deliberately has the same Redis hash tag as the window key so one
// server-side Function invocation can drain and read the complete snapshot.
func (keys BudgetKeySpace) BudgetStatusExpiryKey(generation BudgetGenerationID, member BudgetManifestMember) string {
	return keys.space.admissionPrefix() + "budget:v2-expiry:" + keys.space.digest("budget-status-expiry", string(generation), member.Key())
}

// MemoryBudgetGenerationPort is a deterministic contract implementation used
// by unit tests and offline readiness checks. A production implementation must
// publish the manifest and pointer atomically in Redis; this implementation
// preserves the same validate-before-publish and pointer-provenance rules.
type MemoryBudgetGenerationPort struct {
	mu        sync.RWMutex
	active    ActiveBudgetGeneration
	manifests map[BudgetGenerationID]BudgetManifest
}

var _ BudgetGenerationPort = (*MemoryBudgetGenerationPort)(nil)

func (port *MemoryBudgetGenerationPort) ActiveGeneration(ctx context.Context) (ActiveBudgetGeneration, error) {
	if err := ctx.Err(); err != nil {
		return ActiveBudgetGeneration{}, err
	}
	port.mu.RLock()
	defer port.mu.RUnlock()
	if port.active.GenerationID == "" {
		return ActiveBudgetGeneration{}, fmt.Errorf("%w: %w", ErrBudgetManifestInvalid, ErrBudgetActiveGenerationMissing)
	}
	return port.active, nil
}

func (port *MemoryBudgetGenerationPort) LoadManifest(ctx context.Context, pointer ActiveBudgetGeneration) (BudgetManifest, error) {
	if err := ctx.Err(); err != nil {
		return BudgetManifest{}, err
	}
	port.mu.RLock()
	defer port.mu.RUnlock()
	manifest, ok := port.manifests[pointer.GenerationID]
	if !ok {
		return BudgetManifest{}, ErrBudgetManifestInvalid
	}
	if err := pointer.ValidateAgainst(manifest); err != nil {
		return BudgetManifest{}, err
	}
	return manifest, nil
}

func (port *MemoryBudgetGenerationPort) PublishGeneration(ctx context.Context, manifest BudgetManifest) (ActiveBudgetGeneration, error) {
	if err := ctx.Err(); err != nil {
		return ActiveBudgetGeneration{}, err
	}
	pointer, err := manifest.Pointer()
	if err != nil {
		return ActiveBudgetGeneration{}, err
	}
	port.mu.Lock()
	defer port.mu.Unlock()
	if port.manifests == nil {
		port.manifests = make(map[BudgetGenerationID]BudgetManifest)
	}
	if existing, ok := port.manifests[manifest.GenerationID]; ok {
		existingDigest, digestErr := existing.ManifestDigestHex()
		if digestErr != nil || existingDigest != pointer.ManifestDigest {
			return ActiveBudgetGeneration{}, ErrBudgetGenerationConflict
		}
		port.active = pointer
		return pointer, nil
	}
	port.manifests[manifest.GenerationID] = manifest
	port.active = pointer
	return pointer, nil
}

// MemoryBudgetEventPort is a deterministic contract implementation for unit
// tests. Production uses a Redis Stream adapter; keeping this implementation in
// the same package lets conformance tests exercise the port without a server.
type MemoryBudgetEventPort struct {
	mu      sync.Mutex
	next    int64
	records []BudgetStreamRecord
}

var _ BudgetEventPort = (*MemoryBudgetEventPort)(nil)

func (port *MemoryBudgetEventPort) Append(ctx context.Context, event BudgetStreamEvent) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := event.Validate(); err != nil {
		return "", err
	}
	port.mu.Lock()
	defer port.mu.Unlock()
	port.next++
	id := fmt.Sprintf("%d-0", port.next)
	port.records = append(port.records, BudgetStreamRecord{ID: id, Event: event})
	return id, nil
}

// CurrentCursor mirrors Redis XINFO STREAM's last-entry position for the
// deterministic in-memory contract. An empty stream has no safe recovery
// position and therefore fails closed.
func (port *MemoryBudgetEventPort) CurrentCursor(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	port.mu.Lock()
	defer port.mu.Unlock()
	if len(port.records) == 0 {
		return "", ErrBudgetStreamCursorUnavailable
	}
	return port.records[len(port.records)-1].ID, nil
}

func (port *MemoryBudgetEventPort) Read(ctx context.Context, cursor string, limit int) ([]BudgetStreamRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var afterMajor, afterMinor uint64
	if cursor != "" {
		var err error
		afterMajor, afterMinor, err = parseRedisStreamID(cursor)
		if err != nil {
			return nil, err
		}
	}
	if limit <= 0 || limit > 10_000 {
		return nil, fmt.Errorf("stream read limit must be between 1 and 10000")
	}
	port.mu.Lock()
	defer port.mu.Unlock()
	result := make([]BudgetStreamRecord, 0, limit)
	for _, record := range port.records {
		major, minor, err := parseRedisStreamID(record.ID)
		if err != nil {
			return nil, err
		}
		if major < afterMajor || (major == afterMajor && minor <= afterMinor) {
			continue
		}
		result = append(result, record)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func parseRedisStreamID(value string) (uint64, uint64, error) {
	if !redisStreamIDPattern.MatchString(value) {
		return 0, 0, fmt.Errorf("invalid Redis Stream cursor")
	}
	parts := strings.SplitN(value, "-", 2)
	major, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Redis Stream cursor")
	}
	minor, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Redis Stream cursor")
	}
	return major, minor, nil
}
