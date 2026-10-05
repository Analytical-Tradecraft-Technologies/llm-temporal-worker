package cloudstate

import (
	"context"
	"errors"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// lostBlobs is a blob store whose objects can be lost (every Open is a
// definite miss, as after a deletion or lifecycle rule) or briefly unreachable.
type lostBlobs struct {
	blob.BlobStore
	fail  error
	opens int
}

func (s *lostBlobs) Open(ctx context.Context, key blob.BlobKey) (blob.BlobReadResult, error) {
	if s.fail != nil {
		s.opens++
		return blob.BlobReadResult{}, s.fail
	}
	return s.BlobStore.Open(ctx, key)
}

// requireDanglingIsCorrupt reads through a committed reference three ways: with
// the blob lost, with the blob store briefly unavailable, and intact.
func requireDanglingIsCorrupt(t *testing.T, blobs *lostBlobs, read func() error) {
	t.Helper()
	blobs.fail, blobs.opens = contracts.ErrNotFound, 0
	err := read()
	if blobs.opens == 0 {
		t.Fatalf("the reader opened no blob (error %v)", err)
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("lost blob behind a committed reference = %v, want ErrCorrupt", err)
	}
	if errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("lost blob still reads as an absent record: %v", err)
	}
	blobs.fail = &contracts.StorageError{Kind: contracts.ErrUnavailable, Operation: "blob.open"}
	if err := read(); !errors.Is(err, contracts.ErrUnavailable) || errors.Is(err, ErrCorrupt) || errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("transient blob read = %v, want the retryable storage error unchanged", err)
	}
	blobs.fail = nil
	if err := read(); err != nil {
		t.Fatalf("intact blob: %v", err)
	}
}

func TestRequestRecordDanglingBlobIsCorruptNotAbsent(t *testing.T) {
	r, table, memory, request := fixture(t)
	ctx := context.Background()
	created, err := r.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	blobs := &lostBlobs{BlobStore: memory}
	r = reopen(t, table, blobs)
	requireDanglingIsCorrupt(t, blobs, func() error {
		_, err := r.Read(ctx, request.Scope, request.ID)
		return err
	})
	requireDanglingIsCorrupt(t, blobs, func() error {
		_, err := r.ReadForRecovery(ctx, request.ID)
		return err
	})
	// An update loads the previous revision through the same reference. The
	// token is fixed, so the intact pass commits and later passes replay it.
	update := change(created, StatusRunning, "start")
	requireDanglingIsCorrupt(t, blobs, func() error {
		_, err := r.TryUpdate(ctx, request.Scope, request.ID, update)
		return err
	})
	// A request that was never created is still simply absent.
	absent, err := NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	blobs.fail = contracts.ErrNotFound
	if _, err := r.Read(ctx, request.Scope, absent); !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("absent request = %v, want ErrNotFound", err)
	}
	if _, err := r.Read(ctx, Scope{Tenant: "other", Project: "other"}, request.ID); !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("cross-scope request = %v, want ErrNotFound", err)
	}
}

func TestOperationDanglingRecordBlobIsNotANewOperation(t *testing.T) {
	r, table, memory, request := fixture(t)
	ctx := context.Background()
	operation := Operation{Scope: request.Scope, Kind: request.Kind, Key: "operation", Manifest: request.Manifest, Now: request.CreatedAt}
	if _, err := r.BeginOperation(ctx, operation); err != nil {
		t.Fatal(err)
	}
	blobs := &lostBlobs{BlobStore: memory}
	r = reopen(t, table, blobs)
	requireDanglingIsCorrupt(t, blobs, func() error {
		_, err := r.LookupOperation(ctx, operation)
		return err
	})
	requireDanglingIsCorrupt(t, blobs, func() error {
		_, err := r.BeginOperation(ctx, operation)
		return err
	})
	other := operation
	other.Key = "never-started"
	blobs.fail = contracts.ErrNotFound
	if _, err := r.LookupOperation(ctx, other); !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("absent operation = %v, want ErrNotFound", err)
	}
}

func TestResponseCacheDanglingBlobIsCorruptNotAbsent(t *testing.T) {
	r, table, memory, entry, origin := responseCacheFixture(t)
	ctx := context.Background()
	if err := r.Responses().Publish(ctx, entry); err != nil {
		t.Fatal(err)
	}
	child := childCheckpoint(origin, "consumer")
	child.Kind, child.OriginCacheEntryID = state.CheckpointCacheReplay, &entry.ID
	if err := publishCheckpoint(ctx, r.Checkpoints(), child); err != nil {
		t.Fatal(err)
	}
	use := cache.ResponseUse{ScopeID: origin.ScopeID, OperationID: child.OriginOperationID, EntryID: entry.ID, CheckpointID: child.ID, CompletedAt: entry.CompletedAt.Add(time.Hour)}
	if err := r.Responses().RecordUse(ctx, use); err != nil {
		t.Fatal(err)
	}
	blobs := &lostBlobs{BlobStore: memory}
	r = reopen(t, table, blobs)
	// A cached success whose payload is lost is not an expired or absent
	// entry: Lookup reports it instead of returning a miss.
	requireDanglingIsCorrupt(t, blobs, func() error {
		_, err := r.Responses().Lookup(ctx, cache.ResponseLookup{Key: entry.Key, Now: entry.CompletedAt})
		return err
	})
	requireDanglingIsCorrupt(t, blobs, func() error {
		_, err := r.Responses().ReadUse(ctx, use.ScopeID, use.OperationID)
		return err
	})
	// Republishing an entry or receipt whose ID is bound re-reads the blob.
	requireDanglingIsCorrupt(t, blobs, func() error { return r.Responses().Publish(ctx, entry) })
	requireDanglingIsCorrupt(t, blobs, func() error { return r.Responses().RecordUse(ctx, use) })

	blobs.fail = contracts.ErrNotFound
	other := entry.Key
	other.Fingerprint[1]++
	if got, err := r.Responses().Lookup(ctx, cache.ResponseLookup{Key: other, Now: entry.CompletedAt}); got != nil || err != nil {
		t.Fatalf("absent cache entry = %v, %v, want a miss", got, err)
	}
	if _, err := r.Responses().ReadUse(ctx, use.ScopeID, "never-used"); !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("absent use receipt = %v, want ErrNotFound", err)
	}
}

func TestCheckpointDanglingBlobIsCorruptNotAbsent(t *testing.T) {
	r, table, memory, cp := checkpointFixture(t)
	ctx := context.Background()
	if err := publishCheckpoint(ctx, r.Checkpoints(), cp); err != nil {
		t.Fatal(err)
	}
	blobs := &lostBlobs{BlobStore: memory}
	store := reopen(t, table, blobs).Checkpoints()
	requireDanglingIsCorrupt(t, blobs, func() error {
		_, err := store.Get(ctx, cp.ScopeID, cp.ID)
		return err
	})
	// A signed blob reference is issued only after its object was written.
	requireDanglingIsCorrupt(t, blobs, func() error {
		_, err := store.Read(ctx, cp.ScopeID, cp.DeltaBlob)
		return err
	})
	blobs.fail = contracts.ErrNotFound
	if _, err := store.Get(ctx, cp.ScopeID, state.CheckpointID("00000000-0000-4000-8000-000000000000")); !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("absent checkpoint = %v, want ErrNotFound", err)
	}
	if _, err := store.Read(ctx, "other-scope", cp.DeltaBlob); !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("cross-scope blob reference = %v, want ErrNotFound", err)
	}
}
