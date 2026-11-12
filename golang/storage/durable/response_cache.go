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
	// ErrResponseCacheLeaseExpired marks a lookup whose clock has reached the
	// fill lease's expiry. The caller renews the lease and retries.
	ErrResponseCacheLeaseExpired = errors.New("response cache fill lease has expired")
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
	return c.prepareGenerate(ctx, lease, maxAge, false)
}

// PrepareGenerateAfterMiss is PrepareGenerate after Lookup of lease.Key found
// no entry earlier in the same invocation. It skips that repeated read.
func (c *ResponseCache) PrepareGenerateAfterMiss(ctx context.Context, lease cache.FillLease, maxAge *time.Duration) (CacheDecision, error) {
	return c.prepareGenerate(ctx, lease, maxAge, true)
}

func (c *ResponseCache) prepareGenerate(ctx context.Context, lease cache.FillLease, maxAge *time.Duration, missed bool) (CacheDecision, error) {
	p, err := c.prepare(ctx, lease, maxAge, cache.OperationGenerate, missed)
	if err != nil {
		return CacheDecision{}, err
	}
	d := CacheDecision{Disposition: p.disposition, preparation: p}
	if p.entry != nil {
		response, err := cachedGenerateResponse(*p.entry, lease.Key)
		if err != nil {
			return CacheDecision{}, err
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
	return c.prepareCompact(ctx, lease, maxAge, false)
}

// PrepareCompactAfterMiss is PrepareGenerateAfterMiss for compaction.
func (c *ResponseCache) PrepareCompactAfterMiss(ctx context.Context, lease cache.FillLease, maxAge *time.Duration) (CompactCacheDecision, error) {
	return c.prepareCompact(ctx, lease, maxAge, true)
}

func (c *ResponseCache) prepareCompact(ctx context.Context, lease cache.FillLease, maxAge *time.Duration, missed bool) (CompactCacheDecision, error) {
	p, err := c.prepare(ctx, lease, maxAge, cache.OperationCompact, missed)
	if err != nil {
		return CompactCacheDecision{}, err
	}
	d := CompactCacheDecision{Disposition: p.disposition, preparation: p}
	if p.entry != nil {
		response, err := cachedCompactResponse(*p.entry, lease.Key)
		if err != nil {
			return CompactCacheDecision{}, err
		}
		d.Response = &response
	}
	if err := d.Validate(); err != nil {
		return CompactCacheDecision{}, fmt.Errorf("%w: invalid Compact cache decision", ErrResponseCacheCorrupt)
	}
	return d, nil
}

func cachedGenerateResponse(entry cache.ResponseEntry, key cache.ResponseKey) (llm.GenerateResponseV1, error) {
	var response llm.GenerateResponseV1
	if err := json.Unmarshal(entry.Response, &response); err != nil {
		return llm.GenerateResponseV1{}, fmt.Errorf("%w: invalid cached Generate response", ErrResponseCacheCorrupt)
	}
	if response.OperationID != string(entry.OriginOperationID) || int64(response.Cache.Variant) != key.RequestIndex ||
		(response.Status != llm.ResponseStatusCompleted && response.Status != llm.ResponseStatusToolCalls) || response.Checkpoint.Kind != "generation" || response.Cache.Disposition == "hit" {
		return llm.GenerateResponseV1{}, fmt.Errorf("%w: cached Generate response does not match its origin", ErrResponseCacheCorrupt)
	}
	return response, nil
}

func cachedCompactResponse(entry cache.ResponseEntry, key cache.ResponseKey) (llm.CompactResponseV1, error) {
	var response llm.CompactResponseV1
	if err := json.Unmarshal(entry.Response, &response); err != nil {
		return llm.CompactResponseV1{}, fmt.Errorf("%w: invalid cached Compact response", ErrResponseCacheCorrupt)
	}
	if response.OperationID != string(entry.OriginOperationID) || int64(response.Cache.Variant) != key.RequestIndex || response.Checkpoint.Kind != "compaction" || response.Cache.Disposition == "hit" {
		return llm.CompactResponseV1{}, fmt.Errorf("%w: cached Compact response does not match its origin", ErrResponseCacheCorrupt)
	}
	return response, nil
}

// Lookup reads the successful entry for key without acquiring, starting or
// releasing a fill, so a caller can answer a hit before it persists a new
// attempt. It validates the entry exactly as PrepareGenerate and
// PrepareCompact do. A nil entry is a miss; errors are never misses. A caller
// that then prepares the same key may use the AfterMiss variants.
func (c *ResponseCache) Lookup(ctx context.Context, key cache.ResponseKey, maxAge *time.Duration) (*cache.ResponseEntry, error) {
	if ctx == nil || c == nil || c.now == nil || nilCacheRepository(c.responses) ||
		(key.Operation != cache.OperationGenerate && key.Operation != cache.OperationCompact) || key.ScopeID == "" || key.RequestIndex < 0 ||
		key.Fingerprint == (cache.Fingerprint{}) || (maxAge != nil && *maxAge <= 0) || !completeCacheRoute(key.Route) {
		return nil, ErrResponseCacheInvalid
	}
	now := c.now().UTC()
	entry, err := c.responses.Lookup(ctx, cache.ResponseLookup{Key: key, Now: now, MaxAge: maxAge})
	if err != nil || entry == nil {
		return nil, err
	}
	if err := validCachedEntry(*entry, key, now, maxAge); err != nil {
		return nil, err
	}
	if key.Operation == cache.OperationGenerate {
		_, err = cachedGenerateResponse(*entry, key)
	} else {
		_, err = cachedCompactResponse(*entry, key)
	}
	if err != nil {
		return nil, err
	}
	return cloneResponseEntry(entry), nil
}

func completeCacheRoute(route cache.RouteIdentity) bool {
	return route.Provider != "" && route.Endpoint != "" && route.Model != "" && route.Revision != "" && route.Compiler != ""
}

func validCachedEntry(entry cache.ResponseEntry, key cache.ResponseKey, now time.Time, maxAge *time.Duration) error {
	if entry.Key != key || entry.ID == "" || entry.OriginOperationID == "" || entry.OriginCheckpointID == "" ||
		entry.CompletedAt.IsZero() || entry.CompletedAt.After(now.Add(cache.MaxCompletionSkew)) || (maxAge != nil && now.Sub(entry.CompletedAt) > *maxAge) {
		return fmt.Errorf("%w: cached response does not match lookup", ErrResponseCacheCorrupt)
	}
	return nil
}

func (c *ResponseCache) prepare(ctx context.Context, lease cache.FillLease, maxAge *time.Duration, kind cache.OperationKind, missed bool) (*responseCachePreparation, error) {
	if ctx == nil || c == nil || c.now == nil || nilCacheRepository(c.responses) || nilCacheRepository(c.fills) ||
		lease.Key.Operation != kind || lease.Key.ScopeID == "" || lease.Key.RequestIndex < 0 ||
		lease.Key.Fingerprint == (cache.Fingerprint{}) ||
		lease.OperationID == "" || lease.Attempt == "" || lease.AcquiredAt.IsZero() || !lease.ExpiresAt.After(lease.AcquiredAt) || lease.ExpiresAt.Sub(lease.AcquiredAt) > cache.MaxFillLease ||
		(maxAge != nil && *maxAge <= 0) {
		return nil, ErrResponseCacheInvalid
	}
	if !completeCacheRoute(lease.Key.Route) {
		return nil, fmt.Errorf("%w: a complete resolved route is required", ErrResponseCacheInvalid)
	}
	// The lease was stamped by the worker that quoted the attempt. A clock that
	// lags it is late, not outside the lease: look up as of the acquisition.
	now := c.now().UTC()
	if now.Before(lease.AcquiredAt) {
		now = lease.AcquiredAt.UTC()
	}
	if !now.Before(lease.ExpiresAt) {
		return nil, fmt.Errorf("%w: clock is outside the lease", ErrResponseCacheLeaseExpired)
	}
	lookup := cache.ResponseLookup{Key: lease.Key, Now: now, MaxAge: maxAge}
	var result cache.Preparation
	var err error
	if missed {
		result, err = cache.PrepareAfterMiss(ctx, c.responses, c.fills, lookup, lease)
	} else {
		result, err = cache.Prepare(ctx, c.responses, c.fills, lookup, lease)
	}
	if err != nil {
		return nil, err
	}
	p := &responseCachePreparation{cache: c, lease: lease, fill: result.Fill, preparedAt: now}
	if result.Entry != nil {
		if err := validCachedEntry(*result.Entry, lease.Key, now, maxAge); err != nil {
			return nil, err
		}
		p.entry = cloneResponseEntry(result.Entry)
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
		p.stalled = cache.StalledFill(result.Fill.Record, lease, now)
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

// StalledFill reports a recovery decision on another attempt's started fill
// older than cache.StalledFillAfter. The caller may then stop waiting and make
// its own paid call without the cache; it never takes that fill over.
func (d CacheDecision) StalledFill() bool        { return d.preparation.stalledFill() }
func (d CompactCacheDecision) StalledFill() bool { return d.preparation.stalledFill() }

func (p *responseCachePreparation) stalledFill() bool {
	return p != nil && p.disposition == CacheRecoveryRequired && p.stalled
}

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
