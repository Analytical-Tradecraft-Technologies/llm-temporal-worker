package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
	memoryadmission "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/memory"
)

type testBlobStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newTestBlobStore() *testBlobStore { return &testBlobStore{data: make(map[string][]byte)} }

func (store *testBlobStore) Put(ctx context.Context, request blob.PutRequest) (blob.Ref, error) {
	if err := ctx.Err(); err != nil {
		return blob.Ref{}, err
	}
	digest := blob.Digest(request.Data)
	tenantPrefix, err := blob.TenantPrefix(request.Tenant)
	if err != nil {
		return blob.Ref{}, err
	}
	ref := blob.Ref{Store: "memory", Locator: "results/" + tenantPrefix + "/" + digest, Digest: digest, ByteLength: int64(len(request.Data)), MediaType: request.MediaType, ExpiresAt: request.ExpiresAt}
	if err := ref.Validate(time.Now()); err != nil {
		return blob.Ref{}, err
	}
	store.mu.Lock()
	store.data[ref.Locator] = append([]byte(nil), request.Data...)
	store.mu.Unlock()
	return ref, nil
}

func (store *testBlobStore) Get(ctx context.Context, _ string, ref blob.Ref) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ref.Validate(time.Now()); err != nil {
		return nil, err
	}
	store.mu.Lock()
	data, ok := store.data[ref.Locator]
	store.mu.Unlock()
	if !ok {
		return nil, blob.ErrNotFound
	}
	if blob.Digest(data) != ref.Digest {
		return nil, blob.ErrDigestMismatch
	}
	return append([]byte(nil), data...), nil
}

func TestBlobResultStoreReplaysAfterRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	admissions := memoryadmission.NewAdmissionStore(memoryadmission.AdmissionOptions{Clock: func() time.Time { return now }})
	begin, err := admissions.Begin(ctx, admission.BeginRequest{
		ID:            "operation-1",
		ScopeKey:      "tenant-a\x00request-1",
		RequestDigest: admission.Digest([]byte("request")),
		Reservation:   pricing.MicroUSD(1),
		LeaseUntil:    now.Add(time.Minute),
		ExpiresAt:     now.Add(time.Hour),
	})
	testassert.NoError(t, err, "Begin() error")
	testassert.NoError(t, admissions.MarkDispatching(ctx, admission.DispatchRequest{OperationID: begin.Operation.ID, DispatchToken: begin.Operation.DispatchToken, LeaseUntil: now.Add(time.Minute)}), "MarkDispatching() error")
	blobStore := newTestBlobStore()
	resolver, err := NewContentAddressedBlobRefResolver("memory", "results")
	testassert.NoError(t, err, "NewContentAddressedBlobRefResolver() error")
	first, err := NewBlobResultStore(blobStore, admissions, resolver, func() time.Time { return now })
	testassert.NoError(t, err, "NewBlobResultStore(first) error")
	want := llm.Response{OperationKey: "request-1", Status: llm.ResponseStatusCompleted}
	stateRef, err := first.Put(ctx, begin.Operation.ID, want)
	testassert.NoError(t, err, "Put() error")
	testassert.NoError(t, admissions.Complete(ctx, admission.CompleteRequest{OperationID: begin.Operation.ID, DispatchToken: begin.Operation.DispatchToken, Actual: pricing.MicroUSD(0), ResultRef: &stateRef}), "Complete() error")
	got, err := first.Get(ctx, begin.Operation.ID)
	testassert.NoError(t, err, "Get(first) error")
	if got.OperationKey != want.OperationKey || got.Status != want.Status {
		t.Fatalf("first response = %#v, want %#v", got, want)
	}
	second, err := NewBlobResultStore(blobStore, admissions, resolver, func() time.Time { return now })
	testassert.NoError(t, err, "NewBlobResultStore(second) error")
	got, err = second.Get(ctx, begin.Operation.ID)
	testassert.NoError(t, err, "Get(second) error")
	if got.OperationKey != want.OperationKey || got.Status != want.Status {
		t.Fatalf("replayed response = %#v, want %#v", got, want)
	}
}

func TestBlobResultStoreMissingReferenceIsNotFound(t *testing.T) {
	store, err := NewBlobResultStore(newTestBlobStore(), memoryadmission.NewAdmissionStore(memoryadmission.AdmissionOptions{}), nil, time.Now)
	testassert.NoError(t, err, "NewBlobResultStore() error")
	_, err = store.Get(context.Background(), "missing")
	testassert.ErrorIs(t, err, engine.ErrResultNotFound)
}

func TestContentAddressedFileBlobResolverUsesTenantRelativeLocator(t *testing.T) {
	resolver, err := NewContentAddressedBlobRefResolver("file", "")
	testassert.NoError(t, err, "NewContentAddressedBlobRefResolver() error")
	value := state.BlobRef{Digest: [32]byte{1}, Size: 42, Media: "application/json"}
	expiresAt := time.Now().Add(time.Hour)
	ref, err := resolver(context.Background(), "tenant-a", value, expiresAt)
	testassert.NoError(t, err, "resolver() error")
	prefix, err := blob.TenantPrefix("tenant-a")
	testassert.NoError(t, err)
	if ref.Store != "file" || ref.Locator != prefix+"/"+value.DigestHex() {
		t.Fatalf("file ref = %#v", ref)
	}
}

type resultOperationsFake struct {
	admission.AdmissionStore
	mu         sync.Mutex
	operations map[string]admission.Operation
}

func (fake *resultOperationsFake) Get(_ context.Context, id string) (admission.Operation, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	operation, ok := fake.operations[id]
	if !ok {
		return admission.Operation{}, admission.ErrOperationNotFound
	}
	return operation, nil
}

func (fake *resultOperationsFake) put(id string, expiresAt time.Time) {
	fake.mu.Lock()
	fake.operations[id] = admission.Operation{ID: id, ScopeKey: "tenant-a\x00" + id, ExpiresAt: expiresAt}
	fake.mu.Unlock()
}

func (fake *resultOperationsFake) clear() {
	fake.mu.Lock()
	fake.operations = make(map[string]admission.Operation)
	fake.mu.Unlock()
}

func cachedResultRefCount(store *BlobResultStore) int {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return len(store.refs)
}

func TestBlobResultStoreReferenceCacheDropsExpiredAndMissingOperations(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	operations := &resultOperationsFake{operations: make(map[string]admission.Operation)}
	store, err := NewBlobResultStore(newTestBlobStore(), operations, nil, func() time.Time { return now })
	testassert.NoError(t, err)
	putAll := func(prefix string, count int) {
		t.Helper()
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("%s-%d", prefix, i)
			operations.put(id, now.Add(time.Hour))
			if _, err := store.Put(ctx, id, llm.Response{OperationKey: id, Status: llm.ResponseStatusCompleted}); err != nil {
				t.Fatalf("Put(%s) error = %v", id, err)
			}
		}
	}

	putAll("missing", 100)
	if got := cachedResultRefCount(store); got != 100 {
		t.Fatalf("cached refs = %d, want 100", got)
	}
	operations.clear()
	for i := 0; i < 100; i++ {
		testassert.ErrorIs(t, testassert.Err(store.Get(ctx, fmt.Sprintf("missing-%d", i))), engine.ErrResultNotFound, "Get(missing)")
	}
	if got := cachedResultRefCount(store); got != 0 {
		t.Fatalf("cached refs after missing-operation reads = %d, want 0", got)
	}

	putAll("expired", 100)
	now = now.Add(2 * time.Hour)
	putAll("fresh", 1)
	if got := cachedResultRefCount(store); got != 1 {
		t.Fatalf("cached refs after expiry and one new result = %d, want 1", got)
	}
}

func TestBlobResultStoreReferenceCacheIsCappedWhenResolvable(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	operations := &resultOperationsFake{operations: make(map[string]admission.Operation)}
	resolver, err := NewContentAddressedBlobRefResolver("memory", "results")
	testassert.NoError(t, err)
	store, err := NewBlobResultStore(newTestBlobStore(), operations, resolver, func() time.Time { return now })
	testassert.NoError(t, err)
	for i := 0; i < maxCachedResultRefs+10; i++ {
		id := fmt.Sprintf("operation-%d", i)
		operations.put(id, now.Add(time.Hour))
		if _, err := store.Put(ctx, id, llm.Response{OperationKey: id, Status: llm.ResponseStatusCompleted}); err != nil {
			t.Fatalf("Put(%s) error = %v", id, err)
		}
	}
	if got := cachedResultRefCount(store); got != maxCachedResultRefs {
		t.Fatalf("cached refs = %d, want cap %d", got, maxCachedResultRefs)
	}
}
