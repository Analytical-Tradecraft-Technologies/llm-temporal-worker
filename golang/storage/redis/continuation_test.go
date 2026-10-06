package redis

import (
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/redis/go-redis/v9"
)

type continuationHarness struct {
	mu       sync.Mutex
	values   map[string]string
	indexes  map[string]string
	lastKeys []string
}

func newContinuationHarness() *continuationHarness {
	return &continuationHarness{values: make(map[string]string), indexes: make(map[string]string)}
}

func (h *continuationHarness) Get(_ context.Context, key string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if value, ok := h.values[key]; ok {
		return value, nil
	}
	if value, ok := h.indexes[key]; ok {
		return value, nil
	}
	return "", redis.Nil
}

func (h *continuationHarness) Put(_ context.Context, keys []string, value, handle, _ string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(keys) != 2 && len(keys) != 3 {
		return "", errors.New("invalid fake continuation keys")
	}
	h.lastKeys = append(h.lastKeys[:0], keys...)
	if len(keys) == 3 {
		operationKey := keys[2]
		if existing, ok := h.indexes[operationKey]; ok {
			return "existing\x00" + existing, nil
		}
	}
	if _, exists := h.indexes[keys[0]]; exists {
		return "conflict", nil
	}
	h.values[keys[1]] = value
	h.indexes[keys[0]] = keys[1]
	if len(keys) == 3 {
		h.indexes[keys[2]] = handle
	}
	return "created", nil
}

func TestContinuationKeysShareAdmissionHashSlot(t *testing.T) {
	now := time.Unix(100, 0)
	harness := newContinuationHarness()
	store, err := NewContinuationStore(ContinuationOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Keyring: testKeyring(t), Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.CreateRoot(context.Background(), testContinuation(t, now))
	if err != nil {
		t.Fatal(err)
	}
	if len(harness.lastKeys) != 2 {
		t.Fatalf("root continuation keys = %d, want 2", len(harness.lastKeys))
	}
	for _, key := range harness.lastKeys {
		if !strings.HasPrefix(key, "test:{admission}") {
			t.Fatalf("root key is outside admission hash slot: %q", key)
		}
	}
	child := testContinuation(t, now)
	child.ParentID = root.String()
	child.Depth = 1
	if _, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: root, Child: child, OperationKey: "operation"}); err != nil {
		t.Fatal(err)
	}
	if len(harness.lastKeys) != 3 {
		t.Fatalf("child continuation keys = %d, want 3", len(harness.lastKeys))
	}
	for _, key := range harness.lastKeys {
		if !strings.HasPrefix(key, "test:{admission}") {
			t.Fatalf("child key is outside admission hash slot: %q", key)
		}
	}
}

func testKeyring(t *testing.T) *state.Keyring {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	keyring, err := state.NewKeyring([]state.Key{{ID: "primary", Secret: secret, Primary: true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return keyring
}

func testContinuation(t *testing.T, now time.Time) state.Continuation {
	_, digest, err := state.CanonicalTranscript(nil)
	if err != nil {
		t.Fatal(err)
	}
	return state.Continuation{Tenant: "tenant-a", TranscriptDigest: digest, TranscriptComplete: true, CapabilityVersion: "cap-v1", PriceVersion: "price-v1", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
}

func TestContinuationCreateGetAndTenantBinding(t *testing.T) {
	now := time.Unix(100, 0)
	harness := newContinuationHarness()
	store, err := NewContinuationStore(ContinuationOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Keyring: testKeyring(t), Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.CreateRoot(context.Background(), testContinuation(t, now))
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(context.Background(), root)
	if err != nil || got.Tenant != "tenant-a" || got.ID != root.String() {
		t.Fatalf("root = %#v, %v", got, err)
	}
	if _, err := store.GetForTenant(context.Background(), "tenant-b", root); !errors.Is(err, state.ErrInvalidHandle) {
		t.Fatalf("tenant mismatch = %v", err)
	}
	if got, err := store.GetForTenant(context.Background(), "tenant-a", root); err != nil || got.ID != root.String() {
		t.Fatalf("tenant read = %#v, %v", got, err)
	}
}

func TestContinuationStoreRoundTripsNilTranscriptForRootAndChild(t *testing.T) {
	now := time.Unix(100, 0)
	harness := newContinuationHarness()
	store, err := NewContinuationStore(ContinuationOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Keyring: testKeyring(t), Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	rootValue := testContinuation(t, now)
	if rootValue.Transcript != nil {
		t.Fatal("test must exercise nil transcript persistence")
	}
	root, err := store.CreateRoot(context.Background(), rootValue)
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	gotRoot, err := store.Get(context.Background(), root)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	if gotRoot.ID != root.String() {
		t.Fatalf("get root returned handle %q, want %q", gotRoot.ID, root)
	}
	space, err := newKeySpace(testKeyOptions())
	if err != nil {
		t.Fatal(err)
	}
	rootRecord := harness.values[space.continuationKey("tenant-a", root.String())]
	if !strings.Contains(rootRecord, `"Transcript":[]`) {
		t.Fatalf("root record transcript = %s, want empty array", rootRecord)
	}

	childValue := testContinuation(t, now)
	childValue.ParentID = root.String()
	childValue.Depth = 1
	child, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: root, Child: childValue, OperationKey: "nil-transcript-operation"})
	if err != nil {
		t.Fatalf("create child: %v", err)
	}
	gotChild, err := store.Get(context.Background(), child)
	if err != nil {
		t.Fatalf("get child: %v", err)
	}
	if gotChild.ID != child.String() {
		t.Fatalf("get child returned handle %q, want %q", gotChild.ID, child)
	}
	childRecord := harness.values[space.continuationKey("tenant-a", child.String())]
	if !strings.Contains(childRecord, `"Transcript":[]`) {
		t.Fatalf("child record transcript = %s, want empty array", childRecord)
	}
}

func TestContinuationChildIsImmutableAndOperationIdempotent(t *testing.T) {
	now := time.Unix(100, 0)
	harness := newContinuationHarness()
	store, err := NewContinuationStore(ContinuationOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Keyring: testKeyring(t), Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := store.CreateRoot(context.Background(), testContinuation(t, now))
	if err != nil {
		t.Fatal(err)
	}
	child := testContinuation(t, now)
	child.ParentID = parent.String()
	child.Depth = 1
	first, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child, OperationKey: "op-1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child, OperationKey: "op-1"})
	if err != nil || second != first {
		t.Fatalf("idempotent child = %q, %q, %v", first, second, err)
	}
	if _, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: state.Continuation{Tenant: "other", ParentID: parent.String(), Depth: 1, ExpiresAt: now.Add(time.Hour)}, OperationKey: "op-2"}); !errors.Is(err, state.ErrConflict) {
		t.Fatalf("cross-tenant child = %v", err)
	}
	if got, err := store.Get(context.Background(), first); err != nil || got.ParentID != parent.String() {
		t.Fatalf("child read = %#v, %v", got, err)
	}
}

func TestContinuationOperationKeyIsScopedToTenantAndParent(t *testing.T) {
	now := time.Unix(100, 0)
	harness := newContinuationHarness()
	store, err := NewContinuationStore(ContinuationOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Keyring: testKeyring(t), Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	parentA, err := store.CreateRoot(context.Background(), testContinuation(t, now))
	if err != nil {
		t.Fatal(err)
	}
	rootB := testContinuation(t, now)
	rootB.Tenant = "tenant-b"
	parentB, err := store.CreateRoot(context.Background(), rootB)
	if err != nil {
		t.Fatal(err)
	}

	childA := testContinuation(t, now)
	childA.ParentID = parentA.String()
	childA.Depth = 1
	first, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parentA, Child: childA, OperationKey: "shared-op"})
	if err != nil {
		t.Fatal(err)
	}

	childB := testContinuation(t, now)
	childB.Tenant = "tenant-b"
	childB.ParentID = parentB.String()
	childB.Depth = 1
	second, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parentB, Child: childB, OperationKey: "shared-op"})
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatalf("operation key was shared across tenants/parents: %q", first)
	}
	got, err := store.Get(context.Background(), second)
	if err != nil || got.Tenant != "tenant-b" || got.ParentID != parentB.String() {
		t.Fatalf("scoped child = %#v, %v", got, err)
	}
}

func TestContinuationExpiryAndBounds(t *testing.T) {
	now := time.Unix(100, 0)
	harness := newContinuationHarness()
	store, err := NewContinuationStore(ContinuationOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Keyring: testKeyring(t), Clock: func() time.Time { return now }, MaxBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateRoot(context.Background(), testContinuation(t, now)); err == nil {
		t.Fatal("oversized continuation was accepted")
	}
	large := testContinuation(t, now)
	large.ExpiresAt = now.Add(-time.Second)
	store, err = NewContinuationStore(ContinuationOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Keyring: testKeyring(t), Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateRoot(context.Background(), large); !errors.Is(err, state.ErrExpired) {
		t.Fatalf("expired continuation = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Get(ctx, "handle"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled get = %v", err)
	}
}
