package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/mfow/llm-temporal-worker/golang/cache"
)

func fillPointerBlob(t *testing.T, table *memoryTable, key kv.KeyValueKey) blob.BlobKey {
	t.Helper()
	var pointer cachePointer
	if err := json.Unmarshal(table.rows[key].Item.Fields["cache"].(kv.KeyValueBytes), &pointer); err != nil || pointer.Blob == "" {
		t.Fatal("missing fill pointer", err)
	}
	return blob.BlobKey(pointer.Blob)
}

// A fill record blob that cannot be read right now is not a corrupt fill:
// only a definitely missing or unauthenticated blob is. Both the mutable head
// and the sealed terminal receipt are covered.
func TestCloudFillsTransientBlobReadIsNotCorruption(t *testing.T) {
	transient := map[string]error{
		"unavailable": contracts.ErrUnavailable,
		"throttled":   &contracts.StorageError{Kind: contracts.ErrThrottled, Operation: "blob.open"},
		"timeout":     &contracts.StorageError{Kind: contracts.ErrDeadlineExceeded, Operation: "blob.open", Cause: context.DeadlineExceeded},
		"canceled":    context.Canceled,
	}
	for name, fault := range transient {
		t.Run(name, func(t *testing.T) {
			r, table, blobs, entry, _ := responseCacheFixture(t)
			ctx, lease := context.Background(), fillLease(entry)
			store := r.ResponseFills().(*cacheFills)
			mustAcquireFill(t, store, lease, cache.FillOwned)
			mustStartFill(t, store, lease, lease.AcquiredAt)
			completion := cache.FillCompletion{Outcome: cache.FillFailed, CompletedAt: lease.AcquiredAt.Add(1)}
			check := func(stage string, target blob.BlobKey) {
				t.Helper()
				blobs.open = func(key blob.BlobKey) error {
					if key == target {
						return fault
					}
					return nil
				}
				defer func() { blobs.open = nil }()
				_, acquireErr := store.Acquire(ctx, lease, lease.AcquiredAt)
				failures := map[string]error{"acquire": acquireErr, "complete": store.Complete(ctx, lease, completion)}
				if stage == "head" {
					// Start reads only the head; the receipt is not on its path.
					_, failures["start"] = store.Start(ctx, lease, lease.AcquiredAt)
				}
				for operation, err := range failures {
					if !errors.Is(err, fault) || errors.Is(err, ErrCorrupt) {
						t.Fatalf("%s %s with transient blob read = %v, want %v and not corrupt", stage, operation, err, fault)
					}
				}
			}
			check("head", fillPointerBlob(t, table, store.responses.key("fill", lease.Key)))
			if err := store.Complete(ctx, lease, completion); err != nil {
				t.Fatal("retry after transient head read", err)
			}
			check("receipt", fillPointerBlob(t, table, store.terminalKey(lease)))
			decision, err := store.Acquire(ctx, lease, lease.AcquiredAt)
			if err != nil || decision.Disposition != cache.FillAttemptFinished || decision.Record.State != cache.FillFinished || decision.Record.Completion != completion {
				t.Fatalf("retry after transient receipt read = %+v, %v", decision, err)
			}
		})
	}
}

func TestCloudFillsMissingBlobIsCorruption(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	ctx, lease := context.Background(), fillLease(entry)
	store := r.ResponseFills().(*cacheFills)
	mustAcquireFill(t, store, lease, cache.FillOwned)
	mustStartFill(t, store, lease, lease.AcquiredAt)
	completion := cache.FillCompletion{Outcome: cache.FillFailed, CompletedAt: lease.AcquiredAt.Add(1)}
	head := fillPointerBlob(t, table, store.responses.key("fill", lease.Key))
	saved := blobs.values[head]
	delete(blobs.values, head)
	if _, err := store.Acquire(ctx, lease, lease.AcquiredAt); !errors.Is(err, ErrCorrupt) {
		t.Fatal("dangling fill head treated as absent or transient", err)
	}
	if err := store.Complete(ctx, lease, completion); !errors.Is(err, ErrCorrupt) {
		t.Fatal("dangling fill head completed", err)
	}
	blobs.values[head] = saved
	if err := store.Complete(ctx, lease, completion); err != nil {
		t.Fatal(err)
	}
	// The sealed receipt binds the terminal head's blob.
	delete(blobs.values, fillPointerBlob(t, table, store.terminalKey(lease)))
	if _, err := store.Acquire(ctx, lease, lease.AcquiredAt); !errors.Is(err, ErrCorrupt) {
		t.Fatal("dangling fill receipt treated as absent or transient", err)
	}
	if err := store.Complete(ctx, lease, completion); !errors.Is(err, ErrCorrupt) {
		t.Fatal("dangling fill receipt replayed", err)
	}
}
