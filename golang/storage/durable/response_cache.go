package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

var (
	// ErrResponseCacheInvalid marks incomplete bindings or invalid preparation inputs.
	ErrResponseCacheInvalid = errors.New("response cache preparation is invalid")
	// ErrResponseCacheCorrupt marks cache data that does not match its lookup or origin.
	ErrResponseCacheCorrupt = errors.New("response cache preparation data is corrupt")
)

// ResponseCache adapts cloud cache preparation to the Generate and Compact
// runners. Construct one per immutable runtime snapshot, using its repositories
// and trusted clock. It contains no per-call mutable state.
//
// Replay/recovery and authorized route selection must precede Prepare. Persist
// the proposed lease first and reuse every field on retries. Its Attempt must
// be the Redis generation ID: a new paid attempt needs a new budget generation.
// This adapter does not construct fingerprints or publish checkpoints. Its
// finalization helpers order result publication and fill completion around a
// caller-supplied, idempotent Redis budget settlement.
type ResponseCache struct {
	responses cache.ResponseRepository
	fills     cache.FillRepository
	now       func() time.Time
}

func NewResponseCache(responses cache.ResponseRepository, fills cache.FillRepository, now func() time.Time) (*ResponseCache, error) {
	if nilCacheRepository(responses) || nilCacheRepository(fills) || now == nil {
		return nil, fmt.Errorf("%w: repositories and a clock are required", ErrResponseCacheInvalid)
	}
	return &ResponseCache{responses: responses, fills: fills, now: now}, nil
}

func nilCacheRepository(repository any) bool {
	if repository == nil {
		return true
	}
	value := reflect.ValueOf(repository)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// PrepareGenerate is a CacheLookup port implementation after the caller has
// resolved the authenticated scope, exact route, semantic key and stable lease.
// Nil maxAge permits any successful completion age. No activity sleeps here.
func (c *ResponseCache) PrepareGenerate(ctx context.Context, lease cache.FillLease, maxAge *time.Duration) (CacheDecision, error) {
	p, err := c.prepare(ctx, lease, maxAge, cache.OperationGenerate)
	if err != nil {
		return CacheDecision{}, err
	}
	d := CacheDecision{Disposition: p.disposition, preparation: p}
	if p.entry != nil {
		var response llm.GenerateResponseV1
		if err := json.Unmarshal(p.entry.Response, &response); err != nil {
			return CacheDecision{}, fmt.Errorf("%w: invalid cached Generate response", ErrResponseCacheCorrupt)
		}
		if response.OperationID != string(p.entry.OriginOperationID) || int64(response.Cache.Variant) != lease.Key.RequestIndex ||
			(response.Status != llm.ResponseStatusCompleted && response.Status != llm.ResponseStatusToolCalls) || response.Checkpoint.Kind != "generation" || response.Cache.Disposition == "hit" {
			return CacheDecision{}, fmt.Errorf("%w: cached Generate response does not match its origin", ErrResponseCacheCorrupt)
		}
		d.Response = &response
	}
	if err := d.Validate(); err != nil {
		return CacheDecision{}, fmt.Errorf("%w: invalid Generate cache decision", ErrResponseCacheCorrupt)
	}
	return d, nil
}

// PrepareCompact uses the compaction cache domain and requested sample index.
// Its fingerprint must cover source content and compaction policy versions.
func (c *ResponseCache) PrepareCompact(ctx context.Context, lease cache.FillLease, maxAge *time.Duration) (CompactCacheDecision, error) {
	p, err := c.prepare(ctx, lease, maxAge, cache.OperationCompact)
	if err != nil {
		return CompactCacheDecision{}, err
	}
	d := CompactCacheDecision{Disposition: p.disposition, preparation: p}
	if p.entry != nil {
		var response llm.CompactResponseV1
		if err := json.Unmarshal(p.entry.Response, &response); err != nil {
			return CompactCacheDecision{}, fmt.Errorf("%w: invalid cached Compact response", ErrResponseCacheCorrupt)
		}
		if response.OperationID != string(p.entry.OriginOperationID) || int64(response.Cache.Variant) != lease.Key.RequestIndex || response.Checkpoint.Kind != "compaction" || response.Cache.Disposition == "hit" {
			return CompactCacheDecision{}, fmt.Errorf("%w: cached Compact response does not match its origin", ErrResponseCacheCorrupt)
		}
		d.Response = &response
	}
	if err := d.Validate(); err != nil {
		return CompactCacheDecision{}, fmt.Errorf("%w: invalid Compact cache decision", ErrResponseCacheCorrupt)
	}
	return d, nil
}

func (c *ResponseCache) prepare(ctx context.Context, lease cache.FillLease, maxAge *time.Duration, kind cache.OperationKind) (*responseCachePreparation, error) {
	if ctx == nil || c == nil || c.now == nil || nilCacheRepository(c.responses) || nilCacheRepository(c.fills) ||
		lease.Key.Operation != kind || lease.Key.ScopeID == "" || lease.Key.RequestIndex < 0 ||
		lease.Key.Fingerprint == (cache.Fingerprint{}) ||
		lease.OperationID == "" || lease.Attempt == "" || lease.AcquiredAt.IsZero() || !lease.ExpiresAt.After(lease.AcquiredAt) || lease.ExpiresAt.Sub(lease.AcquiredAt) > cache.MaxFillLease ||
		(maxAge != nil && *maxAge <= 0) {
		return nil, ErrResponseCacheInvalid
	}
	route := lease.Key.Route
	if route.Provider == "" || route.Endpoint == "" || route.Model == "" || route.Revision == "" || route.Compiler == "" {
		return nil, fmt.Errorf("%w: a complete resolved route is required", ErrResponseCacheInvalid)
	}
	// The lease was stamped by the worker that quoted the attempt. A clock that
	// lags it is late, not outside the lease: look up as of the acquisition.
	now := c.now().UTC()
	if now.Before(lease.AcquiredAt) {
		now = lease.AcquiredAt.UTC()
	}
	if !now.Before(lease.ExpiresAt) {
		return nil, fmt.Errorf("%w: clock is outside the lease", ErrResponseCacheInvalid)
	}
	result, err := cache.Prepare(ctx, c.responses, c.fills, cache.ResponseLookup{Key: lease.Key, Now: now, MaxAge: maxAge}, lease)
	if err != nil {
		return nil, err
	}
	p := &responseCachePreparation{cache: c, lease: lease, fill: result.Fill, preparedAt: now}
	if result.Entry != nil {
		entry := result.Entry
		if entry.Key != lease.Key || entry.ID == "" || entry.OriginOperationID == "" || entry.OriginCheckpointID == "" ||
			entry.CompletedAt.IsZero() || entry.CompletedAt.After(now) || (maxAge != nil && now.Sub(entry.CompletedAt) > *maxAge) {
			return nil, fmt.Errorf("%w: cached response does not match lookup", ErrResponseCacheCorrupt)
		}
		p.entry = cloneResponseEntry(entry)
		p.disposition = CacheHit
		return p, nil
	}
	if result.Fill.Record.Lease.Key != lease.Key {
		return nil, fmt.Errorf("%w: cache fill does not match lookup", ErrResponseCacheCorrupt)
	}
	switch result.Fill.Disposition {
	case cache.FillOwned:
		if !sameCacheLease(lease, result.Fill.Record.Lease) || result.Fill.Record.State != cache.FillHeld {
			return nil, fmt.Errorf("%w: cache ownership does not match proposed lease", ErrResponseCacheCorrupt)
		}
		p.disposition = CacheMiss
	case cache.FillWait:
		if result.Fill.Record.State != cache.FillHeld || !result.Fill.Record.Lease.ExpiresAt.After(now) {
			return nil, fmt.Errorf("%w: invalid cache wait decision", ErrResponseCacheCorrupt)
		}
		p.disposition = CacheWait
	case cache.FillRecoveryNeeded, cache.FillAttemptFinished:
		p.disposition = CacheRecoveryRequired
	default:
		return nil, fmt.Errorf("%w: unknown cache fill decision", ErrResponseCacheCorrupt)
	}
	return p, nil
}

// Entry returns an independent copy of the successful origin template's
// metadata, for checkpoint provenance and the idempotent use receipt. A miss
// returns nil. FinalizeCache must still create a distinct zero-cost child.
func (d CacheDecision) Entry() *cache.ResponseEntry        { return d.preparation.origin() }
func (d CompactCacheDecision) Entry() *cache.ResponseEntry { return d.preparation.origin() }

func (p *responseCachePreparation) origin() *cache.ResponseEntry {
	if p == nil {
		return nil
	}
	return cloneResponseEntry(p.entry)
}

func cloneResponseEntry(entry *cache.ResponseEntry) *cache.ResponseEntry {
	if entry == nil {
		return nil
	}
	copy := *entry
	copy.Response = append(json.RawMessage(nil), entry.Response...)
	return &copy
}

func sameCacheLease(a, b cache.FillLease) bool {
	return a.Key == b.Key && a.OperationID == b.OperationID && a.Attempt == b.Attempt && a.AcquiredAt.Equal(b.AcquiredAt) && a.ExpiresAt.Equal(b.ExpiresAt)
}
