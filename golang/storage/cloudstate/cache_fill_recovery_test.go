package cloudstate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

func TestCloudFillLostCompletionSurvivesTakeover(t *testing.T) {
	for _, terminal := range []cache.FillState{cache.FillReleased, cache.FillFinished} {
		t.Run(string(terminal), func(t *testing.T) {
			r, table, blobs, entry, _ := responseCacheFixture(t)
			ctx, lease := context.Background(), fillLease(entry)
			mustAcquireFill(t, r.ResponseFills(), lease, cache.FillOwned)
			if terminal == cache.FillFinished {
				mustStartFill(t, r.ResponseFills(), lease, lease.AcquiredAt)
			}
			completion := cache.FillCompletion{Outcome: cache.FillUnknown, CompletedAt: entry.CompletedAt}
			finish := func(store cache.FillRepository) error {
				if terminal == cache.FillReleased {
					return store.Release(ctx, lease, entry.CompletedAt)
				}
				return store.Complete(ctx, lease, completion)
			}
			table.hook = failTableWrites("replace", "/cache/fill/", nil, contracts.ErrOutcomeUnknown)
			testassert.ErrorIs(t, finish(r.ResponseFills()), contracts.ErrOutcomeUnknown)
			table.hook = nil
			next := lease
			next.Attempt, next.AcquiredAt, next.ExpiresAt = "replacement", entry.CompletedAt.Add(time.Second), entry.CompletedAt.Add(time.Minute)
			// Takeover must preserve the terminal witness before changing head.
			table.hook = failTableWrites("", "/cache/fill-receipt/", contracts.ErrUnavailable, nil)
			testassert.ErrorIs(t, testassert.Err(r.ResponseFills().Acquire(ctx, next, next.AcquiredAt)), contracts.ErrUnavailable, "takeover discarded receipt")
			table.hook = nil
			mustAcquireFill(t, r.ResponseFills(), next, cache.FillOwned)
			mustStartFill(t, r.ResponseFills(), next, next.AcquiredAt)
			if err := finish(reopen(t, table, blobs).ResponseFills()); err != nil {
				t.Fatal("lost finalization after takeover", err)
			}
			mustAcquireFill(t, r.ResponseFills(), next, cache.FillRecoveryNeeded)
			mustAcquireFill(t, r.ResponseFills(), lease, cache.FillAttemptFinished)
			changed := lease
			changed.OperationID = "different-operation"
			testassert.ErrorIs(t, testassert.Err(r.ResponseFills().Acquire(ctx, changed, changed.AcquiredAt)), contracts.ErrConflict, "reused completed attempt identity")
		})
	}
}

func TestCloudFillWaitsForCachePublicationAndPreservesOldSuccess(t *testing.T) {
	r, table, _, entry, _ := responseCacheFixture(t)
	ctx, lease := context.Background(), fillLease(entry)
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillOwned)
	mustStartFill(t, r.ResponseFills(), lease, lease.AcquiredAt)
	table.hook = failTableWrites("", "/cache/success/", contracts.ErrUnavailable, nil)
	testassert.ErrorIs(t, r.Responses().Publish(ctx, entry), contracts.ErrUnavailable)
	table.hook = nil
	completion := cache.FillCompletion{Outcome: cache.FillPublished, EntryID: entry.ID, CompletedAt: entry.CompletedAt}
	testassert.ErrorIs(t, r.ResponseFills().Complete(ctx, lease, completion), contracts.ErrConflict, "finished before visible publication")
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillRecoveryNeeded)
	mustPublishResponse(t, r, entry)
	testassert.NoError(t, r.ResponseFills().Complete(ctx, lease, completion))
	next := lease
	next.Attempt, next.AcquiredAt, next.ExpiresAt = "failed-refresh", entry.CompletedAt.Add(time.Second), entry.CompletedAt.Add(time.Minute)
	mustAcquireFill(t, r.ResponseFills(), next, cache.FillOwned)
	mustStartFill(t, r.ResponseFills(), next, next.AcquiredAt)
	testassert.NoError(t, r.ResponseFills().Complete(ctx, next, cache.FillCompletion{Outcome: cache.FillFailed, CompletedAt: next.ExpiresAt}))
	requireResponseHit(t, r, entry, next.ExpiresAt, "failure invalidated success")
}

func TestCloudFillCompletionRejectsOtherOrigins(t *testing.T) {
	for _, mutate := range []func(*cache.FillLease){
		func(l *cache.FillLease) { l.OperationID = "another-operation" },
		func(l *cache.FillLease) { l.Key.RequestIndex++ },
		func(l *cache.FillLease) {
			l.AcquiredAt = l.AcquiredAt.Add(2 * time.Minute)
			l.ExpiresAt = l.AcquiredAt.Add(time.Minute)
		},
	} {
		r, _, _, entry, _ := responseCacheFixture(t)
		ctx, lease := context.Background(), fillLease(entry)
		mustPublishResponse(t, r, entry)
		mutate(&lease)
		mustAcquireFill(t, r.ResponseFills(), lease, cache.FillOwned)
		mustStartFill(t, r.ResponseFills(), lease, lease.AcquiredAt)
		testassert.ErrorIs(t, r.ResponseFills().Complete(ctx, lease, cache.FillCompletion{Outcome: cache.FillPublished, EntryID: entry.ID, CompletedAt: lease.ExpiresAt}), ErrInvalid, "bound wrong origin")
	}
}

func TestCloudFillFinishesOlderPublishedEntryAfterNewerSuccess(t *testing.T) {
	r, _, _, entry, _ := responseCacheFixture(t)
	ctx, lease := context.Background(), fillLease(entry)
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillOwned)
	mustStartFill(t, r.ResponseFills(), lease, lease.AcquiredAt)
	mustPublishResponse(t, r, entry)
	newer := entry
	newer.ID, newer.CompletedAt = "newer-success", entry.CompletedAt.Add(time.Hour)
	mustPublishResponse(t, r, newer)
	if err := r.ResponseFills().Complete(ctx, lease, cache.FillCompletion{Outcome: cache.FillPublished, EntryID: entry.ID, CompletedAt: entry.CompletedAt}); err != nil {
		t.Fatal("new success blocked older finalizer recovery", err)
	}
}

func TestCloudFillMalformedStateAndBoundedContention(t *testing.T) {
	r, table, _, entry, _ := responseCacheFixture(t)
	ctx, lease := context.Background(), fillLease(entry)
	// Persistent definite conflicts do not create an unbounded activity loop.
	calls := 0
	table.hook = func(_ string, item kv.KeyValueItem) (error, error) {
		if strings.Contains(item.PartitionKey, "/cache/fill/") {
			calls++
			return contracts.ErrConflict, nil
		}
		return nil, nil
	}
	if _, err := r.ResponseFills().Acquire(ctx, lease, lease.AcquiredAt); !errors.Is(err, contracts.ErrConflict) || calls != 16 {
		t.Fatalf("unbounded contention: %d %v", calls, err)
	}
	table.hook = nil
	store := r.ResponseFills().(*cacheFills)
	bad := cache.FillRecord{Lease: lease, State: "invented", UpdatedAt: lease.AcquiredAt}
	testassert.NoError(t, store.write(ctx, bad, ""))
	testassert.ErrorIs(t, testassert.Err(store.Acquire(ctx, lease, lease.AcquiredAt)), ErrCorrupt, "unknown stored state")
}
