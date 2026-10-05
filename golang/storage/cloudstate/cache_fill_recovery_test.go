package cloudstate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/mfow/llm-temporal-worker/golang/cache"
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
			table.hook = func(op string, item kv.KeyValueItem) (error, error) {
				if op == "replace" && strings.Contains(item.PartitionKey, "/cache/fill/") {
					return nil, contracts.ErrOutcomeUnknown
				}
				return nil, nil
			}
			if err := finish(r.ResponseFills()); !errors.Is(err, contracts.ErrOutcomeUnknown) {
				t.Fatal(err)
			}
			table.hook = nil
			next := lease
			next.Attempt, next.AcquiredAt, next.ExpiresAt = "replacement", entry.CompletedAt.Add(time.Second), entry.CompletedAt.Add(time.Minute)
			// Takeover must preserve the terminal witness before changing head.
			table.hook = func(_ string, item kv.KeyValueItem) (error, error) {
				if strings.Contains(item.PartitionKey, "/cache/fill-receipt/") {
					return contracts.ErrUnavailable, nil
				}
				return nil, nil
			}
			if _, err := r.ResponseFills().Acquire(ctx, next, next.AcquiredAt); !errors.Is(err, contracts.ErrUnavailable) {
				t.Fatal("takeover discarded receipt", err)
			}
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
			if _, err := r.ResponseFills().Acquire(ctx, changed, changed.AcquiredAt); !errors.Is(err, contracts.ErrConflict) {
				t.Fatal("reused completed attempt identity", err)
			}
		})
	}
}

func TestCloudFillWaitsForCachePublicationAndPreservesOldSuccess(t *testing.T) {
	r, table, _, entry, _ := responseCacheFixture(t)
	ctx, lease := context.Background(), fillLease(entry)
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillOwned)
	mustStartFill(t, r.ResponseFills(), lease, lease.AcquiredAt)
	table.hook = func(_ string, item kv.KeyValueItem) (error, error) {
		if strings.Contains(item.PartitionKey, "/cache/success/") {
			return contracts.ErrUnavailable, nil
		}
		return nil, nil
	}
	if err := r.Responses().Publish(ctx, entry); !errors.Is(err, contracts.ErrUnavailable) {
		t.Fatal(err)
	}
	table.hook = nil
	completion := cache.FillCompletion{Outcome: cache.FillPublished, EntryID: entry.ID, CompletedAt: entry.CompletedAt}
	if err := r.ResponseFills().Complete(ctx, lease, completion); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("finished before visible publication", err)
	}
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillRecoveryNeeded)
	if err := r.Responses().Publish(ctx, entry); err != nil {
		t.Fatal(err)
	}
	if err := r.ResponseFills().Complete(ctx, lease, completion); err != nil {
		t.Fatal(err)
	}
	next := lease
	next.Attempt, next.AcquiredAt, next.ExpiresAt = "failed-refresh", entry.CompletedAt.Add(time.Second), entry.CompletedAt.Add(time.Minute)
	mustAcquireFill(t, r.ResponseFills(), next, cache.FillOwned)
	mustStartFill(t, r.ResponseFills(), next, next.AcquiredAt)
	if err := r.ResponseFills().Complete(ctx, next, cache.FillCompletion{Outcome: cache.FillFailed, CompletedAt: next.ExpiresAt}); err != nil {
		t.Fatal(err)
	}
	if hit := lookupResponse(t, r, entry.Key, next.ExpiresAt); hit == nil || hit.ID != entry.ID {
		t.Fatal("failure invalidated success")
	}
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
		if err := r.Responses().Publish(ctx, entry); err != nil {
			t.Fatal(err)
		}
		mutate(&lease)
		mustAcquireFill(t, r.ResponseFills(), lease, cache.FillOwned)
		mustStartFill(t, r.ResponseFills(), lease, lease.AcquiredAt)
		if err := r.ResponseFills().Complete(ctx, lease, cache.FillCompletion{Outcome: cache.FillPublished, EntryID: entry.ID, CompletedAt: lease.ExpiresAt}); !errors.Is(err, ErrInvalid) {
			t.Fatal("bound wrong origin", err)
		}
	}
}

func TestCloudFillFinishesOlderPublishedEntryAfterNewerSuccess(t *testing.T) {
	r, _, _, entry, _ := responseCacheFixture(t)
	ctx, lease := context.Background(), fillLease(entry)
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillOwned)
	mustStartFill(t, r.ResponseFills(), lease, lease.AcquiredAt)
	if err := r.Responses().Publish(ctx, entry); err != nil {
		t.Fatal(err)
	}
	newer := entry
	newer.ID, newer.CompletedAt = "newer-success", entry.CompletedAt.Add(time.Hour)
	if err := r.Responses().Publish(ctx, newer); err != nil {
		t.Fatal(err)
	}
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
	if err := store.write(ctx, bad, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(ctx, lease, lease.AcquiredAt); !errors.Is(err, ErrCorrupt) {
		t.Fatal("unknown stored state", err)
	}
}
