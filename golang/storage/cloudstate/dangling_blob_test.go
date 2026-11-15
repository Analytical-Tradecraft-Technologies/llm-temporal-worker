package cloudstate

import (
	"context"
	"errors"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
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
// requireAbsentNotCorrupt requires err to report a missing record rather
// than corruption, failing with "<what> = <err>, want ErrNotFound".
func requireAbsentNotCorrupt(t *testing.T, err error, what string) {
	t.Helper()
	if !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("%s = %v, want ErrNotFound", what, err)
	}
}

func requireDanglingIsCorrupt(t *testing.T, blobs *lostBlobs, read func() error) {
	t.Helper()
	blobs.fail, blobs.opens = contracts.ErrNotFound, 0
	err := read()
	if blobs.opens == 0 {
		t.Fatalf("the reader opened no blob (error %v)", err)
	}
	testassert.ErrorIs(t, err, ErrCorrupt, "lost blob behind a committed reference")
	testassert.ErrorIsNot(t, err, contracts.ErrNotFound, "lost blob still reads as an absent record")
	blobs.fail = &contracts.StorageError{Kind: contracts.ErrUnavailable, Operation: "blob.open"}
	if err := read(); !errors.Is(err, contracts.ErrUnavailable) || errors.Is(err, ErrCorrupt) || errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("transient blob read = %v, want the retryable storage error unchanged", err)
	}
	blobs.fail = nil
	testassert.NoError(t, read(), "intact blob")
}

func TestRequestRecordDanglingBlobIsCorruptNotAbsent(t *testing.T) {
	r, table, memory, request := fixture(t)
	ctx := context.Background()
	created := mustCreate(t, r, request)
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
	testassert.NoError(t, err)
	blobs.fail = contracts.ErrNotFound
	requireAbsentNotCorrupt(t, testassert.Err(r.Read(ctx, request.Scope, absent)), "absent request")
	requireAbsentNotCorrupt(t, testassert.Err(r.Read(ctx, Scope{Tenant: "other", Project: "other"}, request.ID)), "cross-scope request")
}

func TestOperationDanglingRecordBlobIsNotANewOperation(t *testing.T) {
	r, table, memory, request := fixture(t)
	ctx := context.Background()
	operation := Operation{Scope: request.Scope, Kind: request.Kind, Key: "operation", Manifest: request.Manifest, Now: request.CreatedAt}
	mustBeginOperation(t, r, operation)
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
	requireAbsentNotCorrupt(t, testassert.Err(r.LookupOperation(ctx, other)), "absent operation")
}

func TestResponseCacheDanglingBlobIsCorruptNotAbsent(t *testing.T) {
	r, table, memory, entry, origin := responseCacheFixture(t)
	ctx := context.Background()
	child := publishEntryWithReplayChild(t, r, entry, origin)
	mustPublishCheckpoint(t, r.Checkpoints(), child)
	use := replayUse(origin, child, entry, entry.CompletedAt.Add(time.Hour))
	mustRecordUse(t, r, use)
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
	requireAbsentNotCorrupt(t, testassert.Err(r.Responses().ReadUse(ctx, use.ScopeID, "never-used")), "absent use receipt")
}

func TestCheckpointDanglingBlobIsCorruptNotAbsent(t *testing.T) {
	r, table, memory, cp := checkpointFixture(t)
	ctx := context.Background()
	mustPublishCheckpoint(t, r.Checkpoints(), cp)
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
	requireAbsentNotCorrupt(t, testassert.Err(store.Get(ctx, cp.ScopeID, state.CheckpointID("00000000-0000-4000-8000-000000000000"))), "absent checkpoint")
	requireAbsentNotCorrupt(t, testassert.Err(store.Read(ctx, "other-scope", cp.DeltaBlob)), "cross-scope blob reference")
}
