package cloudstate

import (
	"context"
	"errors"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

func TestCloudCacheFinalizationRetriesPublicationSettlementAndFillReceipt(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	ctx, lease := context.Background(), fillLease(entry)
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillOwned)
	mustStartFill(t, r.ResponseFills(), lease, lease.AcquiredAt)
	completion := cache.FillCompletion{Outcome: cache.FillPublished, EntryID: entry.ID, CompletedAt: entry.CompletedAt}
	newFinalizer := func() *durable.ResponseCache {
		t.Helper()
		opened := reopen(t, table, blobs)
		result := mustResponseCache(t, opened.Responses(), opened.ResponseFills(), func() time.Time { return completion.CompletedAt })
		return result
	}
	settlements := 0
	settle := func(context.Context) error { settlements++; return nil }
	table.hook = failTableWrites("", "/cache/success/", contracts.ErrUnavailable, nil)
	if err := newFinalizer().CompleteAttempt(ctx, lease, completion, &entry, settle); !errors.Is(err, contracts.ErrUnavailable) || settlements != 0 {
		t.Fatalf("published before visibility: %v settlements=%d", err, settlements)
	}
	table.hook = nil
	settlementFailure := errors.New("Redis settlement temporarily unavailable")
	testassert.ErrorIs(t, newFinalizer().CompleteAttempt(ctx, lease, completion, &entry, func(context.Context) error {
		settlements++
		return settlementFailure
	}), settlementFailure)
	requireResponseHit(t, r, entry, entry.CompletedAt, "committed success not visible after settlement failure")
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillRecoveryNeeded)
	table.hook = failTableWrites("replace", "/cache/fill/", nil, contracts.ErrOutcomeUnknown)
	testassert.ErrorIs(t, newFinalizer().CompleteAttempt(ctx, lease, completion, &entry, settle), contracts.ErrOutcomeUnknown)
	table.hook = nil
	testassert.NoError(t, newFinalizer().CompleteAttempt(ctx, lease, completion, &entry, settle), "restart completion")
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillAttemptFinished)
	if settlements != 3 {
		t.Fatalf("settlement retries=%d", settlements)
	}
}

func TestCloudCacheFinalizationUnknownOutcomeDoesNotInvalidateSuccess(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	ctx := context.Background()
	mustPublishResponse(t, r, entry)
	next := fillLease(entry)
	next.Attempt = "unknown-attempt"
	next.OperationID = "unknown-operation"
	next.AcquiredAt = entry.CompletedAt.Add(time.Second)
	next.ExpiresAt = next.AcquiredAt.Add(time.Minute)
	mustAcquireFill(t, r.ResponseFills(), next, cache.FillOwned)
	mustStartFill(t, r.ResponseFills(), next, next.AcquiredAt)
	finalizer := mustResponseCache(t, reopen(t, table, blobs).Responses(), reopen(t, table, blobs).ResponseFills(), func() time.Time { return next.ExpiresAt })
	settled := false
	completion := cache.FillCompletion{Outcome: cache.FillUnknown, CompletedAt: next.ExpiresAt}
	if err := finalizer.CompleteAttempt(ctx, next, completion, nil, func(context.Context) error {
		settled = true // Charge the original unknown attempt; no refund.
		return nil
	}); err != nil || !settled {
		t.Fatalf("unknown outcome: %v settled=%t", err, settled)
	}
	requireResponseHit(t, r, entry, next.ExpiresAt, "unknown outcome displaced older success")
	mustAcquireFill(t, r.ResponseFills(), next, cache.FillAttemptFinished)
	if err := finalizer.CompleteAttempt(ctx, next, completion, &entry, func(context.Context) error { return nil }); err == nil {
		t.Fatal("accepted cache entry for unknown outcome")
	}
}

func TestCloudCacheFinalizationUseRequiresCommittedConsumer(t *testing.T) {
	r, table, blobs, entry, origin := responseCacheFixture(t)
	ctx := context.Background()
	child := publishEntryWithReplayChild(t, r, entry, origin)
	use := replayUse(origin, child, entry, entry.CompletedAt.Add(time.Hour))
	finalizer := mustResponseCache(t, r.Responses(), r.ResponseFills(), func() time.Time { return use.CompletedAt })
	testassert.ErrorIs(t, finalizer.RecordUse(ctx, entry, use), contracts.ErrNotFound, "uncommitted consumer counted")
	mustPublishCheckpoint(t, r.Checkpoints(), child)
	table.hook = failTableWrites("", "/cache/use/", nil, contracts.ErrOutcomeUnknown)
	testassert.ErrorIs(t, finalizer.RecordUse(ctx, entry, use), contracts.ErrOutcomeUnknown)
	table.hook = nil
	finalizer = mustResponseCache(t, reopen(t, table, blobs).Responses(), reopen(t, table, blobs).ResponseFills(), func() time.Time { return use.CompletedAt })
	testassert.NoError(t, finalizer.RecordUse(ctx, entry, use), "restart use")
	got, err := r.Responses().ReadUse(ctx, use.ScopeID, use.OperationID)
	if err != nil || got != use {
		t.Fatalf("use receipt=%+v error=%v", got, err)
	}
	bad := use
	bad.OperationID = origin.OriginOperationID
	if err := finalizer.RecordUse(ctx, entry, bad); err == nil {
		t.Fatal("origin operation used its own cache entry")
	}
}

// A worker whose clock lags the publisher's sees a success stamped slightly in
// its future. Waiters and new requests must use it rather than taking over the
// finished fill and paying again (#1361); entries far ahead stay ineligible.
func TestCloudCacheLaggingClockUsesPublishedSuccess(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	ctx, origin := context.Background(), fillLease(entry)
	waiter := origin
	waiter.OperationID, waiter.Attempt = "waiting-operation", "waiting-attempt"
	mustAcquireFill(t, r.ResponseFills(), origin, cache.FillOwned)
	mustAcquireFill(t, r.ResponseFills(), waiter, cache.FillWait)
	mustStartFill(t, r.ResponseFills(), origin, origin.AcquiredAt)
	completion := cache.FillCompletion{Outcome: cache.FillPublished, EntryID: entry.ID, CompletedAt: entry.CompletedAt}
	finalizer := mustResponseCache(t, r.Responses(), r.ResponseFills(), func() time.Time { return completion.CompletedAt })
	testassert.NoError(t, finalizer.CompleteAttempt(ctx, origin, completion, &entry, func(context.Context) error { return nil }))
	lag := 150 * time.Millisecond
	fresh := origin
	fresh.OperationID, fresh.Attempt = "new-operation", "new-attempt"
	fresh.AcquiredAt = entry.CompletedAt.Add(-lag)
	fresh.ExpiresAt = fresh.AcquiredAt.Add(time.Minute)
	prepare := func(lease cache.FillLease, now time.Time) durable.CacheDisposition {
		t.Helper()
		opened := reopen(t, table, blobs)
		c := mustResponseCache(t, opened.Responses(), opened.ResponseFills(), func() time.Time { return now })
		decision, err := c.PrepareGenerate(ctx, lease, nil)
		testassert.NoError(t, err)
		if decision.Disposition == durable.CacheHit && decision.Entry().ID != entry.ID {
			t.Fatalf("hit on %q", decision.Entry().ID)
		}
		return decision.Disposition
	}
	if got := prepare(waiter, entry.CompletedAt.Add(-lag)); got != durable.CacheHit {
		t.Fatalf("lagging waiter = %v; want hit", got)
	}
	if got := prepare(fresh, fresh.AcquiredAt); got != durable.CacheHit {
		t.Fatalf("lagging new request = %v; want hit", got)
	}
	if got := lookupResponse(t, r, entry.Key, entry.CompletedAt.Add(-cache.MaxCompletionSkew-time.Nanosecond)); got != nil {
		t.Fatal("served an entry beyond the skew tolerance")
	}
}
