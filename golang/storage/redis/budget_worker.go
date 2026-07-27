package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

// BudgetWorkerLeaseSchema identifies the bounded worker-coordination record.
// The lease and roster deliberately contain no tenant, provider, or prompt
// values; they are only liveness and Stream-cursor state.
const BudgetWorkerLeaseSchema = "budget-worker/v1"

const (
	maxBudgetWorkerRecordBytes    = 8 << 10
	maxBudgetWorkerSessionBytes   = 64
	maxBudgetWorkerLeaseTTL       = 24 * time.Hour
	budgetWorkerLeaseFieldPrefix  = "lease:"
	budgetWorkerRosterFieldPrefix = "roster:"
)

// These scripts make the lease transitions compare-and-swap operations. The
// serialized prior record is the CAS token, so a concurrent renew cannot
// overwrite a newer cursor and pruning cannot delete a lease that was just
// renewed after a scan observed it as expired.
const budgetWorkerRenewScript = `
local current = redis.call('HGET', KEYS[1], ARGV[1])
if not current then return -1 end
if current ~= ARGV[2] then return -2 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3], ARGV[4], ARGV[5])
return 1
`

const budgetWorkerPruneScript = `
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current == ARGV[2] then
  redis.call('HDEL', KEYS[1], ARGV[1])
  return 1
end
return 0
`

var (
	ErrBudgetWorkerLeaseMissing     = errors.New("budget worker lease is missing")
	ErrBudgetWorkerLeaseConflict    = errors.New("budget worker lease ownership conflict")
	ErrBudgetWorkerLeaseExpired     = errors.New("budget worker lease is expired")
	ErrBudgetWorkerCursorRegression = errors.New("budget worker cursor regressed")
	ErrBudgetWorkerRecordInvalid    = errors.New("invalid budget worker record")
)

// BudgetWorkerLease is the expiring liveness record for one process session.
// SessionID remains stable in the owning process across Redis reconnects. A
// caller must persist Cursor in the lease after a successful tailer poll.
type BudgetWorkerLease struct {
	Schema         string             `json:"schema"`
	SessionID      string             `json:"session_id"`
	GenerationID   BudgetGenerationID `json:"generation_id"`
	Cursor         string             `json:"cursor,omitempty"`
	LeaseExpiresAt time.Time          `json:"lease_expires_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

// BudgetWorkerRoster is intentionally persistent after a liveness lease
// expires. It proves that a process session existed across a Redis outage;
// maintenance may remove stale entries only after generation replacement.
type BudgetWorkerRoster struct {
	Schema       string             `json:"schema"`
	SessionID    string             `json:"session_id"`
	GenerationID BudgetGenerationID `json:"generation_id"`
	RegisteredAt time.Time          `json:"registered_at"`
	LastSeenAt   time.Time          `json:"last_seen_at"`
}

// BudgetWorkerLeaseOptions controls deterministic construction in tests and
// allows the runtime to inject a clock. SessionID is optional; production
// callers should leave it empty so a cryptographically random value is made
// once per process.
type BudgetWorkerLeaseOptions struct {
	Clock     func() time.Time
	SessionID string
}

// BudgetWorkerLeaseRedisClient is the minimal command seam used by the
// worker-coordination store. redis.Client and redis.ClusterClient both satisfy
// it, while command-level fakes can test all state transitions without Redis.
type BudgetWorkerLeaseRedisClient interface {
	HGet(context.Context, string, string) *redisclient.StringCmd
	HSet(context.Context, string, ...interface{}) *redisclient.IntCmd
	HGetAll(context.Context, string) *redisclient.MapStringStringCmd
	HScan(context.Context, string, uint64, string, int64) *redisclient.ScanCmd
	HDel(context.Context, string, ...string) *redisclient.IntCmd
	Eval(context.Context, string, []string, ...interface{}) *redisclient.Cmd
}

// BudgetWorkerLeaseStore persists one process session's roster and liveness
// lease in the configured budget:workers hash. A lease is represented by an
// expiring timestamp rather than a Redis key TTL so roster data survives an
// outage and can distinguish reconnecting sessions from a new process.
type BudgetWorkerLeaseStore struct {
	client      BudgetWorkerLeaseRedisClient
	keys        BudgetKeySpace
	clock       func() time.Time
	sessionID   string
	pruneMu     sync.Mutex
	pruneCursor uint64
}

var _ BudgetWorkerLeaseStorePort = (*BudgetWorkerLeaseStore)(nil)

// BudgetWorkerLeaseStorePort is the runtime-facing boundary. It keeps Redis
// details out of the tailer and recovery state machine.
type BudgetWorkerLeaseStorePort interface {
	SessionID() string
	Register(context.Context, BudgetGenerationID, string, time.Duration) (BudgetWorkerLease, error)
	Renew(context.Context, BudgetGenerationID, string, time.Duration) (BudgetWorkerLease, error)
	Release(context.Context) error
	Live(context.Context, BudgetGenerationID, time.Time) ([]BudgetWorkerLease, error)
	Roster(context.Context, BudgetGenerationID) ([]BudgetWorkerRoster, error)
	PruneExpired(context.Context, BudgetGenerationID, time.Time, int) (int, error)
}

func NewBudgetWorkerLeaseStore(client BudgetWorkerLeaseRedisClient, keys BudgetKeySpace, options BudgetWorkerLeaseOptions) (*BudgetWorkerLeaseStore, error) {
	if client == nil {
		return nil, errors.New("Redis budget worker lease client is required")
	}
	if keys.space.prefix == "" {
		return nil, errors.New("Redis budget key space is required")
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	sessionID := options.SessionID
	if sessionID == "" {
		var entropy [16]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			return nil, fmt.Errorf("generate budget worker session: %w", err)
		}
		sessionID = hex.EncodeToString(entropy[:])
	}
	if err := validateWorkerSessionID(sessionID); err != nil {
		return nil, err
	}
	return &BudgetWorkerLeaseStore{client: client, keys: keys, clock: clock, sessionID: sessionID}, nil
}

// SessionID is stable for this store and therefore for the owning process.
func (store *BudgetWorkerLeaseStore) SessionID() string {
	if store == nil {
		return ""
	}
	return store.sessionID
}

// Register creates or renews this process's lease and writes its persistent
// roster entry in one HSET. Existing entries with a different generation are
// rejected rather than silently allowing one session to span generations.
func (store *BudgetWorkerLeaseStore) Register(ctx context.Context, generation BudgetGenerationID, cursor string, ttl time.Duration) (BudgetWorkerLease, error) {
	if err := store.validateRequest(ctx, generation, cursor, ttl); err != nil {
		return BudgetWorkerLease{}, err
	}
	now := store.clock().UTC()
	lease := BudgetWorkerLease{Schema: BudgetWorkerLeaseSchema, SessionID: store.sessionID, GenerationID: generation, Cursor: cursor, LeaseExpiresAt: now.Add(ttl), UpdatedAt: now}
	roster := BudgetWorkerRoster{Schema: BudgetWorkerLeaseSchema, SessionID: store.sessionID, GenerationID: generation, RegisteredAt: now, LastSeenAt: now}
	if raw, err := store.client.HGet(ctx, store.keys.WorkersKey(), store.leaseField()).Result(); err == nil {
		prior, decodeErr := decodeBudgetWorkerLease(raw)
		if decodeErr != nil {
			return BudgetWorkerLease{}, decodeErr
		}
		if prior.SessionID != store.sessionID || prior.GenerationID != generation {
			return BudgetWorkerLease{}, ErrBudgetWorkerLeaseConflict
		}
		lease.Cursor = prior.Cursor
		if cursor != "" {
			if !budgetStreamIDAdvances(prior.Cursor, cursor) && prior.Cursor != cursor {
				return BudgetWorkerLease{}, ErrBudgetWorkerCursorRegression
			}
			lease.Cursor = cursor
		}
		if rosterRaw, rosterErr := store.client.HGet(ctx, store.keys.WorkersKey(), store.rosterField()).Result(); rosterErr == nil {
			priorRoster, rosterDecodeErr := decodeBudgetWorkerRoster(rosterRaw)
			if rosterDecodeErr != nil {
				return BudgetWorkerLease{}, rosterDecodeErr
			}
			roster.RegisteredAt = priorRoster.RegisteredAt
		} else if !errors.Is(rosterErr, redisclient.Nil) {
			return BudgetWorkerLease{}, fmt.Errorf("read existing budget worker roster: %w", rosterErr)
		}
	} else if !errors.Is(err, redisclient.Nil) {
		return BudgetWorkerLease{}, fmt.Errorf("read existing budget worker lease: %w", err)
	}
	if err := store.writeRecords(ctx, lease, roster, ""); err != nil {
		return BudgetWorkerLease{}, err
	}
	return lease, nil
}

// Renew requires an existing lease owned by this session. Cursor advancement
// is monotonic, preventing a stale reconnect callback from moving the stored
// Stream position backwards and defeating retention safety.
func (store *BudgetWorkerLeaseStore) Renew(ctx context.Context, generation BudgetGenerationID, cursor string, ttl time.Duration) (BudgetWorkerLease, error) {
	if err := store.validateRequest(ctx, generation, cursor, ttl); err != nil {
		return BudgetWorkerLease{}, err
	}
	raw, err := store.client.HGet(ctx, store.keys.WorkersKey(), store.leaseField()).Result()
	if errors.Is(err, redisclient.Nil) {
		return BudgetWorkerLease{}, ErrBudgetWorkerLeaseMissing
	}
	if err != nil {
		return BudgetWorkerLease{}, fmt.Errorf("read budget worker lease: %w", err)
	}
	prior, err := decodeBudgetWorkerLease(raw)
	if err != nil {
		return BudgetWorkerLease{}, err
	}
	now := store.clock().UTC()
	if prior.SessionID != store.sessionID || prior.GenerationID != generation {
		return BudgetWorkerLease{}, ErrBudgetWorkerLeaseConflict
	}
	if !prior.LeaseExpiresAt.After(now) {
		// A process may reconnect after its liveness TTL elapsed. The persistent
		// roster proves this is the same in-memory session, so it may renew
		// without a PostgreSQL read; a missing/mismatched roster fails closed.
		rawRoster, rosterErr := store.client.HGet(ctx, store.keys.WorkersKey(), store.rosterField()).Result()
		if errors.Is(rosterErr, redisclient.Nil) {
			return BudgetWorkerLease{}, ErrBudgetWorkerLeaseExpired
		}
		if rosterErr != nil {
			return BudgetWorkerLease{}, fmt.Errorf("read budget worker roster: %w", rosterErr)
		}
		roster, rosterDecodeErr := decodeBudgetWorkerRoster(rawRoster)
		if rosterDecodeErr != nil {
			return BudgetWorkerLease{}, rosterDecodeErr
		}
		if roster.SessionID != store.sessionID || roster.GenerationID != generation {
			return BudgetWorkerLease{}, ErrBudgetWorkerLeaseConflict
		}
	}
	if !budgetStreamIDAdvances(prior.Cursor, cursor) && prior.Cursor != cursor {
		return BudgetWorkerLease{}, ErrBudgetWorkerCursorRegression
	}
	lease := BudgetWorkerLease{Schema: BudgetWorkerLeaseSchema, SessionID: store.sessionID, GenerationID: generation, Cursor: cursor, LeaseExpiresAt: now.Add(ttl), UpdatedAt: now}
	roster := BudgetWorkerRoster{Schema: BudgetWorkerLeaseSchema, SessionID: store.sessionID, GenerationID: generation, RegisteredAt: prior.UpdatedAt, LastSeenAt: now}
	if rawRoster, rosterErr := store.client.HGet(ctx, store.keys.WorkersKey(), store.rosterField()).Result(); rosterErr == nil {
		priorRoster, rosterDecodeErr := decodeBudgetWorkerRoster(rawRoster)
		if rosterDecodeErr != nil {
			return BudgetWorkerLease{}, rosterDecodeErr
		}
		roster.RegisteredAt = priorRoster.RegisteredAt
	} else if !errors.Is(rosterErr, redisclient.Nil) {
		return BudgetWorkerLease{}, fmt.Errorf("read budget worker roster: %w", rosterErr)
	}
	if err := store.writeRecords(ctx, lease, roster, raw); err != nil {
		return BudgetWorkerLease{}, err
	}
	return lease, nil
}

// Release removes only liveness. The roster deliberately remains available to
// distinguish a reconnecting process from a newly started process.
func (store *BudgetWorkerLeaseStore) Release(ctx context.Context) error {
	if store == nil || store.client == nil {
		return errors.New("Redis budget worker lease store is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := store.client.HDel(ctx, store.keys.WorkersKey(), store.leaseField()).Result(); err != nil {
		return fmt.Errorf("release budget worker lease: %w", err)
	}
	return nil
}

// Live returns non-expired leases for the requested generation. Expired
// records are ignored but not deleted; call PruneExpired with a bounded limit
// from maintenance to avoid unbounded hash work in the readiness path.
func (store *BudgetWorkerLeaseStore) Live(ctx context.Context, generation BudgetGenerationID, now time.Time) ([]BudgetWorkerLease, error) {
	if store == nil || store.client == nil {
		return nil, errors.New("Redis budget worker lease store is nil")
	}
	if err := validateGeneration(generation); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	values, err := store.client.HGetAll(ctx, store.keys.WorkersKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("list budget worker leases: %w", err)
	}
	result := make([]BudgetWorkerLease, 0, len(values))
	for field, raw := range values {
		if !strings.HasPrefix(field, budgetWorkerLeaseFieldPrefix) {
			continue
		}
		lease, err := decodeBudgetWorkerLease(raw)
		if err != nil {
			return nil, err
		}
		if lease.GenerationID == generation && lease.LeaseExpiresAt.After(now.UTC()) {
			result = append(result, lease)
		}
	}
	return result, nil
}

// Roster returns persistent process sessions for a generation, including
// sessions whose liveness lease has expired. Recovery uses this view to avoid
// mistaking an empty lease set for a cold fleet while a reconnecting process
// can still present its in-memory session ID.
func (store *BudgetWorkerLeaseStore) Roster(ctx context.Context, generation BudgetGenerationID) ([]BudgetWorkerRoster, error) {
	if store == nil || store.client == nil {
		return nil, errors.New("Redis budget worker lease store is nil")
	}
	if err := validateGeneration(generation); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	values, err := store.client.HGetAll(ctx, store.keys.WorkersKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("list budget worker roster: %w", err)
	}
	result := make([]BudgetWorkerRoster, 0, len(values))
	for field, raw := range values {
		if !strings.HasPrefix(field, budgetWorkerRosterFieldPrefix) {
			continue
		}
		roster, err := decodeBudgetWorkerRoster(raw)
		if err != nil {
			return nil, err
		}
		if roster.GenerationID == generation {
			result = append(result, roster)
		}
	}
	return result, nil
}

// PruneExpired removes at most limit expired lease fields. It does not touch
// roster fields, which are intentionally retained for recovery classification.
func (store *BudgetWorkerLeaseStore) PruneExpired(ctx context.Context, generation BudgetGenerationID, now time.Time, limit int) (int, error) {
	if store == nil || store.client == nil {
		return 0, errors.New("Redis budget worker lease store is nil")
	}
	if err := validateGeneration(generation); err != nil {
		return 0, err
	}
	if limit <= 0 || limit > 10_000 {
		return 0, errors.New("budget worker prune limit must be between 1 and 10000")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	// HSCAN is incremental, unlike HGETALL, and the cursor is retained by this
	// process so each maintenance call inspects only one bounded page.
	store.pruneMu.Lock()
	defer store.pruneMu.Unlock()
	values, cursor, err := store.client.HScan(ctx, store.keys.WorkersKey(), store.pruneCursor, budgetWorkerLeaseFieldPrefix+"*", int64(limit)).Result()
	if err != nil {
		return 0, fmt.Errorf("scan budget worker leases: %w", err)
	}
	store.pruneCursor = cursor
	removed := 0
	inspected := 0
	for index := 0; index+1 < len(values) && inspected < limit; index += 2 {
		inspected++
		field, raw := values[index], values[index+1]
		lease, decodeErr := decodeBudgetWorkerLease(raw)
		if decodeErr != nil {
			return 0, decodeErr
		}
		if lease.GenerationID == generation && !lease.LeaseExpiresAt.After(now.UTC()) {
			result, evalErr := store.client.Eval(ctx, budgetWorkerPruneScript, []string{store.keys.WorkersKey()}, field, raw).Int64()
			if evalErr != nil {
				return 0, fmt.Errorf("prune budget worker lease: %w", evalErr)
			}
			if result == 1 {
				removed++
			}
		}
	}
	return removed, nil
}

func (store *BudgetWorkerLeaseStore) validateRequest(ctx context.Context, generation BudgetGenerationID, cursor string, ttl time.Duration) error {
	if store == nil || store.client == nil {
		return errors.New("Redis budget worker lease store is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateGeneration(generation); err != nil {
		return err
	}
	if cursor != "" {
		if len(cursor) > MaxBudgetStreamIDBytes {
			return fmt.Errorf("%w: cursor exceeds bounded length", ErrBudgetWorkerRecordInvalid)
		}
		if _, _, err := parseRedisStreamID(cursor); err != nil {
			return fmt.Errorf("%w: invalid cursor", ErrBudgetWorkerRecordInvalid)
		}
	}
	if ttl <= 0 || ttl > maxBudgetWorkerLeaseTTL {
		return fmt.Errorf("%w: lease TTL is outside the bounded range", ErrBudgetWorkerRecordInvalid)
	}
	return nil
}

func validateGeneration(generation BudgetGenerationID) error {
	if err := validateOpaqueID("generation_id", string(generation)); err != nil {
		return fmt.Errorf("%w: %v", ErrBudgetWorkerRecordInvalid, err)
	}
	return nil
}

func validateWorkerSessionID(sessionID string) error {
	if sessionID == "" || len(sessionID) > maxBudgetWorkerSessionBytes || strings.TrimSpace(sessionID) != sessionID || strings.ContainsAny(sessionID, "\x00\r\n") {
		return fmt.Errorf("%w: session ID is empty, oversized, or unsafe", ErrBudgetWorkerRecordInvalid)
	}
	return nil
}

func (store *BudgetWorkerLeaseStore) leaseField() string {
	return budgetWorkerLeaseFieldPrefix + store.keys.space.digest("budget-worker-session", store.sessionID)
}

func (store *BudgetWorkerLeaseStore) rosterField() string {
	return budgetWorkerRosterFieldPrefix + store.keys.space.digest("budget-worker-session", store.sessionID)
}

func (store *BudgetWorkerLeaseStore) writeRecords(ctx context.Context, lease BudgetWorkerLease, roster BudgetWorkerRoster, expectedLeaseRaw string) error {
	leaseJSON, err := marshalBudgetWorkerRecord(lease)
	if err != nil {
		return err
	}
	rosterJSON, err := marshalBudgetWorkerRecord(roster)
	if err != nil {
		return err
	}
	if expectedLeaseRaw != "" {
		result, evalErr := store.client.Eval(ctx, budgetWorkerRenewScript, []string{store.keys.WorkersKey()}, store.leaseField(), expectedLeaseRaw, leaseJSON, store.rosterField(), rosterJSON).Int64()
		if errors.Is(evalErr, redisclient.Nil) {
			return ErrBudgetWorkerLeaseMissing
		}
		if evalErr != nil {
			return fmt.Errorf("renew budget worker lease: %w", evalErr)
		}
		switch result {
		case -1:
			return ErrBudgetWorkerLeaseMissing
		case -2:
			return ErrBudgetWorkerLeaseConflict
		case 1:
			return nil
		default:
			return fmt.Errorf("renew budget worker lease: unexpected result %d", result)
		}
	}
	if _, err := store.client.HSet(ctx, store.keys.WorkersKey(), store.leaseField(), leaseJSON, store.rosterField(), rosterJSON).Result(); err != nil {
		return fmt.Errorf("write budget worker records: %w", err)
	}
	return nil
}

func marshalBudgetWorkerRecord(value interface{}) (string, error) {
	data, err := json.Marshal(value)
	if err != nil || len(data) > maxBudgetWorkerRecordBytes {
		return "", fmt.Errorf("%w: worker record exceeds bounded JSON size", ErrBudgetWorkerRecordInvalid)
	}
	return string(data), nil
}

func decodeBudgetWorkerLease(raw string) (BudgetWorkerLease, error) {
	if raw == "" || len(raw) > maxBudgetWorkerRecordBytes {
		return BudgetWorkerLease{}, fmt.Errorf("%w: lease payload is empty or oversized", ErrBudgetWorkerRecordInvalid)
	}
	var lease BudgetWorkerLease
	if err := json.Unmarshal([]byte(raw), &lease); err != nil {
		return BudgetWorkerLease{}, fmt.Errorf("%w: decode lease: %v", ErrBudgetWorkerRecordInvalid, err)
	}
	if lease.Schema != BudgetWorkerLeaseSchema || validateWorkerSessionID(lease.SessionID) != nil || validateGeneration(lease.GenerationID) != nil || lease.LeaseExpiresAt.IsZero() || lease.UpdatedAt.IsZero() || !lease.LeaseExpiresAt.After(lease.UpdatedAt) {
		return BudgetWorkerLease{}, fmt.Errorf("%w: lease fields are invalid", ErrBudgetWorkerRecordInvalid)
	}
	if lease.Cursor != "" {
		if _, _, err := parseRedisStreamID(lease.Cursor); err != nil {
			return BudgetWorkerLease{}, fmt.Errorf("%w: lease cursor is invalid", ErrBudgetWorkerRecordInvalid)
		}
	}
	return lease, nil
}

func decodeBudgetWorkerRoster(raw string) (BudgetWorkerRoster, error) {
	if raw == "" || len(raw) > maxBudgetWorkerRecordBytes {
		return BudgetWorkerRoster{}, fmt.Errorf("%w: roster payload is empty or oversized", ErrBudgetWorkerRecordInvalid)
	}
	var roster BudgetWorkerRoster
	if err := json.Unmarshal([]byte(raw), &roster); err != nil {
		return BudgetWorkerRoster{}, fmt.Errorf("%w: decode roster: %v", ErrBudgetWorkerRecordInvalid, err)
	}
	if roster.Schema != BudgetWorkerLeaseSchema || validateWorkerSessionID(roster.SessionID) != nil || validateGeneration(roster.GenerationID) != nil || roster.RegisteredAt.IsZero() || roster.LastSeenAt.IsZero() || roster.LastSeenAt.Before(roster.RegisteredAt) {
		return BudgetWorkerRoster{}, fmt.Errorf("%w: roster fields are invalid", ErrBudgetWorkerRecordInvalid)
	}
	return roster, nil
}
