package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/mfow/llm-temporal-worker/golang/cache"
)

func fillLease(entry cache.ResponseEntry) cache.FillLease {
	return cache.FillLease{Key: entry.Key, OperationID: entry.OriginOperationID, Attempt: "private-attempt-1", AcquiredAt: entry.CompletedAt.Add(-time.Minute), ExpiresAt: entry.CompletedAt.Add(14 * time.Minute)}
}

func mustAcquireFill(t *testing.T, store cache.FillRepository, lease cache.FillLease, want cache.FillDisposition) cache.FillRecord {
	t.Helper()
	decision, err := store.Acquire(context.Background(), lease, lease.AcquiredAt)
	if err != nil || decision.Disposition != want {
		t.Fatalf("acquire = %v, %v; want %s", decision, err, want)
	}
	return decision.Record
}

func mustStartFill(t *testing.T, store cache.FillRepository, lease cache.FillLease, now time.Time) {
	t.Helper()
	started, err := store.Start(context.Background(), lease, now)
	if err != nil || !started {
		t.Fatalf("start = %t, %v", started, err)
	}
}

func TestCloudFillsHundredIndependentMissesDispatchOnce(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	lease := fillLease(entry)
	var wg sync.WaitGroup
	var dispatches atomic.Int32
	errs := make(chan error, 100)
	for i := range 100 {
		store := reopen(t, table, blobs).ResponseFills()
		candidate := lease
		candidate.Attempt = fmt.Sprintf("attempt-%d", i)
		wg.Go(func() {
			prepared, err := cache.Prepare(context.Background(), r.Responses(), store, cache.ResponseLookup{Key: lease.Key, Now: lease.AcquiredAt}, candidate)
			if err != nil {
				errs <- err
				return
			}
			if prepared.Entry != nil {
				errs <- errors.New("unexpected cache hit")
				return
			}
			if prepared.Fill.Disposition != cache.FillOwned {
				return
			}
			started, err := store.Start(context.Background(), candidate, lease.AcquiredAt)
			if err != nil {
				errs <- err
				return
			}
			if started {
				dispatches.Add(1)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if dispatches.Load() != 1 {
		t.Fatalf("dispatch count: %d", dispatches.Load())
	}
	// Once work may have started, years of lease expiry cannot authorize more.
	lease.Attempt = "later-attempt"
	lease.AcquiredAt, lease.ExpiresAt = lease.AcquiredAt.AddDate(1, 0, 0), lease.ExpiresAt.AddDate(1, 0, 0)
	mustAcquireFill(t, reopen(t, table, blobs).ResponseFills(), lease, cache.FillRecoveryNeeded)
}

func TestCloudFillsHundredRetriesOfSameOwnerStartOnce(t *testing.T) {
	_, table, blobs, entry, _ := responseCacheFixture(t)
	lease := fillLease(entry)
	var wg sync.WaitGroup
	var dispatches atomic.Int32
	errs := make(chan error, 100)
	for range 100 {
		store := reopen(t, table, blobs).ResponseFills()
		wg.Go(func() {
			_, err := store.Acquire(context.Background(), lease, lease.AcquiredAt)
			if err != nil {
				errs <- err
				return
			}
			started, err := store.Start(context.Background(), lease, lease.AcquiredAt)
			if err != nil {
				errs <- err
				return
			}
			if started {
				dispatches.Add(1)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if dispatches.Load() != 1 {
		t.Fatalf("dispatches = %d", dispatches.Load())
	}
}

func TestCloudFillsExpiryFencesPreviousOwner(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	ctx, lease := context.Background(), fillLease(entry)
	store := r.ResponseFills()
	mustAcquireFill(t, store, lease, cache.FillOwned)
	next := lease
	next.Attempt = "next"
	next.AcquiredAt, next.ExpiresAt = lease.ExpiresAt, lease.ExpiresAt.Add(time.Minute)
	before := next
	before.AcquiredAt = next.AcquiredAt.Add(-time.Nanosecond)
	mustAcquireFill(t, store, before, cache.FillWait)
	if ok, err := store.Start(ctx, lease, lease.ExpiresAt); ok || !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("expired start = %v %v", ok, err)
	}
	mustAcquireFill(t, reopen(t, table, blobs).ResponseFills(), next, cache.FillOwned)
	// A stale process supplies its original time; CAS ownership still fences it.
	if ok, err := store.Start(ctx, lease, lease.AcquiredAt); ok || !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("stale start = %v %v", ok, err)
	}
	if err := store.Release(ctx, lease, next.AcquiredAt); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal(err)
	}
	mustStartFill(t, store, next, next.AcquiredAt)
	if err := store.Release(ctx, next, next.AcquiredAt); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("released started work", err)
	}
}

func TestCloudFillsWaiterTakesOverExpiredOwnerAtAcquisitionTime(t *testing.T) {
	r, _, _, entry, _ := responseCacheFixture(t)
	ctx, lease := context.Background(), fillLease(entry)
	store := r.ResponseFills()
	mustAcquireFill(t, store, lease, cache.FillOwned)
	// The waiter's stable lease predates the owner's expiry, so it waits
	// while the owner is live and takes over once the owner has expired.
	waiter := lease
	waiter.Attempt, waiter.AcquiredAt, waiter.ExpiresAt = "waiter", lease.AcquiredAt.Add(time.Minute), lease.AcquiredAt.Add(time.Minute+cache.MaxFillLease)
	for _, now := range []time.Time{waiter.AcquiredAt, lease.ExpiresAt.Add(-time.Nanosecond)} {
		if decision, err := store.Acquire(ctx, waiter, now); err != nil || decision.Disposition != cache.FillWait {
			t.Fatalf("acquire at %s = %v, %v", now, decision, err)
		}
	}
	if _, err := store.Acquire(ctx, waiter, waiter.AcquiredAt.Add(-time.Nanosecond)); !errors.Is(err, ErrInvalid) {
		t.Fatal("acquisition before the lease", err)
	}
	if decision, err := store.Acquire(ctx, waiter, lease.ExpiresAt); err != nil || decision.Disposition != cache.FillOwned || decision.Record.Lease != waiter {
		t.Fatalf("acquire at expiry = %v, %v", decision, err)
	}
	if ok, err := store.Start(ctx, lease, lease.ExpiresAt); ok || !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("fenced start = %v %v", ok, err)
	}
	mustStartFill(t, store, waiter, lease.ExpiresAt)
}

func TestCloudFillsTakeoverRacesStart(t *testing.T) {
	// A paused owner with a stale clock can race an expiry-based takeover.
	// The shared CAS permits only one: either started work remains pinned or
	// the replacement lease fences the original owner's dispatch.
	for range 30 {
		r, table, blobs, entry, _ := responseCacheFixture(t)
		lease := fillLease(entry)
		mustAcquireFill(t, r.ResponseFills(), lease, cache.FillOwned)
		next := lease
		next.Attempt, next.AcquiredAt, next.ExpiresAt = "replacement", lease.ExpiresAt, lease.ExpiresAt.Add(time.Minute)
		var oldStarted bool
		var oldErr error
		var acquired cache.FillDecision
		var newErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			oldStarted, oldErr = reopen(t, table, blobs).ResponseFills().Start(context.Background(), lease, lease.AcquiredAt)
		})
		wg.Go(func() {
			acquired, newErr = reopen(t, table, blobs).ResponseFills().Acquire(context.Background(), next, next.AcquiredAt)
		})
		wg.Wait()
		if newErr != nil {
			t.Fatal(newErr)
		}
		if oldStarted {
			if acquired.Disposition != cache.FillRecoveryNeeded {
				t.Fatal("takeover of started owner")
			}
		} else {
			if !errors.Is(oldErr, contracts.ErrConflict) || acquired.Disposition != cache.FillOwned {
				t.Fatalf("race: %v %v", oldErr, acquired)
			}
			mustStartFill(t, r.ResponseFills(), next, next.AcquiredAt)
		}
	}
}

func TestCloudFillsCompletionAndTombstone(t *testing.T) {
	for _, outcome := range []cache.FillOutcome{cache.FillPublished, cache.FillNotCacheable, cache.FillFailed, cache.FillUnknown} {
		t.Run(string(outcome), func(t *testing.T) {
			r, table, blobs, entry, _ := responseCacheFixture(t)
			lease, ctx := fillLease(entry), context.Background()
			store := r.ResponseFills()
			mustAcquireFill(t, store, lease, cache.FillOwned)
			completion := cache.FillCompletion{Outcome: outcome, CompletedAt: entry.CompletedAt}
			if err := store.Complete(ctx, lease, completion); err == nil {
				t.Fatal("completed unstarted work")
			}
			mustStartFill(t, store, lease, lease.AcquiredAt)
			if outcome == cache.FillPublished {
				completion.EntryID = entry.ID
				if err := store.Complete(ctx, lease, completion); err == nil {
					t.Fatal("completed unpublished success")
				}
				if err := r.Responses().Publish(ctx, entry); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Complete(ctx, lease, completion); err != nil {
				t.Fatal(err)
			}
			store = reopen(t, table, blobs).ResponseFills()
			if err := store.Complete(ctx, lease, completion); err != nil {
				t.Fatal("retry", err)
			}
			mustAcquireFill(t, store, lease, cache.FillAttemptFinished)
			if started, err := store.Start(ctx, lease, lease.AcquiredAt); started || !errors.Is(err, contracts.ErrConflict) {
				t.Fatal("resurrected completed owner")
			}
			changed := completion
			changed.CompletedAt = changed.CompletedAt.Add(time.Second)
			if err := store.Complete(ctx, lease, changed); !errors.Is(err, contracts.ErrConflict) {
				t.Fatal("mutable completion", err)
			}
			// Even unknown paid work needs an explicit completion before a NEW
			// attempt can own the key. This does not grant that attempt budget.
			next := lease
			next.Attempt, next.AcquiredAt, next.ExpiresAt = "new-paid-attempt", completion.CompletedAt.Add(time.Second), completion.CompletedAt.Add(time.Minute)
			mustAcquireFill(t, store, next, cache.FillOwned)
			if err := store.Complete(ctx, lease, completion); err != nil {
				t.Fatal("completed attempt lost after new owner", err)
			}
			if err := store.Release(ctx, next, next.AcquiredAt); err != nil {
				t.Fatal(err)
			}
			mustAcquireFill(t, store, lease, cache.FillAttemptFinished)
		})
	}
}

func TestCloudFillsWaiterOwnsAfterUnpublishedEnd(t *testing.T) {
	for _, outcome := range []cache.FillOutcome{"", cache.FillNotCacheable, cache.FillFailed, cache.FillUnknown} {
		t.Run("end="+string(outcome), func(t *testing.T) {
			r, _, _, entry, _ := responseCacheFixture(t)
			lease, ctx := fillLease(entry), context.Background()
			store := r.ResponseFills()
			mustAcquireFill(t, store, lease, cache.FillOwned)
			// The waiter's attempt predates the owner's end.
			waiter := lease
			waiter.Attempt, waiter.AcquiredAt, waiter.ExpiresAt = "waiter", lease.AcquiredAt.Add(time.Second), lease.AcquiredAt.Add(time.Second+cache.MaxFillLease)
			mustAcquireFill(t, store, waiter, cache.FillWait)
			ended := entry.CompletedAt
			if outcome == "" {
				if err := store.Release(ctx, lease, ended); err != nil {
					t.Fatal(err)
				}
			} else {
				mustStartFill(t, store, lease, lease.AcquiredAt)
				if err := store.Complete(ctx, lease, cache.FillCompletion{Outcome: outcome, CompletedAt: ended}); err != nil {
					t.Fatal(err)
				}
			}
			// A lease that had expired by the end stays fenced and sees the record.
			expired := waiter
			expired.Attempt, expired.AcquiredAt, expired.ExpiresAt = "expired", ended.Add(-time.Minute), ended
			if decision, err := store.Acquire(ctx, expired); !errors.Is(err, contracts.ErrConflict) || decision.Record.Lease != lease {
				t.Fatalf("expired acquisition = %v, %v", decision, err)
			}
			mustAcquireFill(t, store, waiter, cache.FillOwned)
			mustAcquireFill(t, store, waiter, cache.FillOwned)
			mustAcquireFill(t, store, lease, cache.FillAttemptFinished)
			mustStartFill(t, store, waiter, ended.Add(time.Second))
		})
	}
}

func TestCloudFillsLostAcknowledgements(t *testing.T) {
	for _, operation := range []string{"acquire", "start", "release", "complete"} {
		for _, boundary := range []string{"blob", "row", "receipt"} {
			if boundary == "receipt" && (operation == "acquire" || operation == "start") {
				continue
			}
			for _, committed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/committed=%t", operation, boundary, committed), func(t *testing.T) {
					r, table, blobs, entry, _ := responseCacheFixture(t)
					ctx, lease := context.Background(), fillLease(entry)
					store := r.ResponseFills()
					if operation != "acquire" {
						mustAcquireFill(t, store, lease, cache.FillOwned)
					}
					if operation == "complete" {
						mustStartFill(t, store, lease, lease.AcquiredAt)
					}
					complete := cache.FillCompletion{Outcome: cache.FillUnknown, CompletedAt: entry.CompletedAt}
					invoke := func(store cache.FillRepository) (bool, error) {
						switch operation {
						case "acquire":
							_, err := store.Acquire(ctx, lease, lease.AcquiredAt)
							return false, err
						case "start":
							return store.Start(ctx, lease, lease.AcquiredAt)
						case "release":
							return false, store.Release(ctx, lease, entry.CompletedAt)
						default:
							return false, store.Complete(ctx, lease, complete)
						}
					}
					failure := func() (error, error) {
						// A provider may expose both classifications; uncertainty wins.
						err := errors.Join(contracts.ErrOutcomeUnknown, contracts.ErrConflict)
						if committed {
							return nil, err
						}
						return err, nil
					}
					var mutations int
					if boundary == "row" || boundary == "receipt" {
						table.hook = func(_ string, item kv.KeyValueItem) (error, error) {
							kind := "/cache/fill/"
							if boundary == "receipt" {
								kind = "/cache/fill-receipt/"
							}
							if strings.Contains(item.PartitionKey, kind) {
								mutations++
								return failure()
							}
							return nil, nil
						}
					} else {
						blobs.hook = func(blob.BlobKey) (error, error) { mutations++; return failure() }
					}
					if started, err := invoke(store); started || !errors.Is(err, contracts.ErrOutcomeUnknown) {
						t.Fatalf("uncertain write: %t %v", started, err)
					}
					if mutations != 1 {
						t.Fatalf("retried unknown mutation %d times", mutations)
					}
					table.hook, blobs.hook = nil, nil
					started, err := invoke(reopen(t, table, blobs).ResponseFills())
					if err != nil {
						t.Fatal("restart reconciliation", err)
					}
					if operation == "start" && started == (boundary == "row" && committed) {
						t.Fatalf("unsafe start recovery: %t", started)
					}
				})
			}
		}
	}
}

func TestCloudFillsCorruptionAndIsolation(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	lease, ctx := fillLease(entry), context.Background()
	mustAcquireFill(t, r.ResponseFills(), lease, cache.FillOwned)
	for _, edit := range []func(*cache.ResponseKey){
		func(k *cache.ResponseKey) { k.ScopeID += "-other" }, func(k *cache.ResponseKey) { k.Operation = cache.OperationCompact },
		func(k *cache.ResponseKey) { k.RequestIndex++ }, func(k *cache.ResponseKey) { k.Fingerprint[1]++ },
		func(k *cache.ResponseKey) { k.Route.Provider += "-other" }, func(k *cache.ResponseKey) { k.Route.Endpoint += "-other" },
		func(k *cache.ResponseKey) { k.Route.Account += "-other" }, func(k *cache.ResponseKey) { k.Route.Region += "-other" },
		func(k *cache.ResponseKey) { k.Route.Model += "-other" }, func(k *cache.ResponseKey) { k.Route.Revision += "-other" }, func(k *cache.ResponseKey) { k.Route.Compiler += "-other" },
	} {
		other := lease
		edit(&other.Key)
		mustAcquireFill(t, r.ResponseFills(), other, cache.FillOwned)
	}
	for _, row := range table.rows {
		data, _ := row.Item.Fields.MarshalBinary()
		if bytes.Contains(data, []byte("private")) || strings.Contains(row.Item.PartitionKey, "private") {
			t.Fatal("plaintext fill metadata")
		}
	}
	for _, data := range blobs.values {
		if bytes.Contains(data, []byte(lease.Attempt)) {
			t.Fatal("plaintext attempt")
		}
	}
	key := r.Responses().(*responseCache).key("fill", lease.Key)
	row := table.rows[key]
	var pointer cachePointer
	if err := json.Unmarshal(row.Item.Fields["cache"].(kv.KeyValueBytes), &pointer); err != nil {
		t.Fatal(err)
	}
	data := blobs.values[blob.BlobKey(pointer.Blob)]
	delete(blobs.values, blob.BlobKey(pointer.Blob))
	if _, err := r.ResponseFills().Acquire(ctx, lease, lease.AcquiredAt); !errors.Is(err, ErrCorrupt) {
		t.Fatal("dangling fill treated as miss", err)
	}
	blobs.values[blob.BlobKey(pointer.Blob)] = append([]byte(nil), data...)
	blobs.values[blob.BlobKey(pointer.Blob)][0] ^= 1
	if ok, err := r.ResponseFills().Start(ctx, lease, lease.AcquiredAt); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatal("corrupt fill granted start", err)
	}
}

func TestCloudFillsInvalidInputCannotMutate(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	lease, ctx := fillLease(entry), context.Background()
	rows, objects := len(table.rows), len(blobs.values)
	for _, edit := range []func(*cache.FillLease){
		func(l *cache.FillLease) { l.Key.ScopeID = "" }, func(l *cache.FillLease) { l.Key.RequestIndex = -1 },
		func(l *cache.FillLease) { l.OperationID = "" }, func(l *cache.FillLease) { l.Attempt = "" },
		func(l *cache.FillLease) { l.AcquiredAt = time.Time{} }, func(l *cache.FillLease) { l.ExpiresAt = l.AcquiredAt },
		func(l *cache.FillLease) { l.ExpiresAt = l.AcquiredAt.Add(cache.MaxFillLease + time.Nanosecond) },
	} {
		bad := lease
		edit(&bad)
		if _, err := r.ResponseFills().Acquire(ctx, bad, bad.AcquiredAt); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := r.ResponseFills().Acquire(nil, lease, lease.AcquiredAt); err == nil {
		t.Fatal("nil context")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := r.ResponseFills().Acquire(cancelled, lease, lease.AcquiredAt); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if ok, err := r.ResponseFills().Start(ctx, lease, lease.AcquiredAt.Add(-time.Second)); ok || !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := r.ResponseFills().Complete(ctx, lease, cache.FillCompletion{Outcome: cache.FillFailed, EntryID: "unexpected", CompletedAt: entry.CompletedAt}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if len(table.rows) != rows || len(blobs.values) != objects {
		t.Fatal("invalid input wrote storage")
	}
}
