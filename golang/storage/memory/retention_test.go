package memory

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

// newTestContinuationStore builds a store with a fixed test key whose clock
// reads *now. random seeds the keyring; nil uses the default source.
func newTestContinuationStore(t *testing.T, now *time.Time, random io.Reader) *ContinuationStore {
	t.Helper()
	keyring, err := state.NewKeyring([]state.Key{{ID: "k1", Secret: bytes.Repeat([]byte{3}, 32), Primary: true}}, random)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewContinuationStore(ContinuationOptions{Keyring: keyring, Clock: func() time.Time { return *now }, MaxDepth: 4})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// deterministicKeyringRandom is a fixed keyring random source with enough
// bytes for a root and a few children.
func deterministicKeyringRandom() io.Reader {
	return bytes.NewReader(append(bytes.Repeat([]byte{4}, 16), bytes.Repeat([]byte{5}, 16)...))
}

// textTranscript is a one-message transcript and its canonical digest.
func textTranscript(t *testing.T, actor llm.Actor, text string) ([]llm.Item, [32]byte) {
	t.Helper()
	items := []llm.Item{llm.Message{Actor: actor, Content: []llm.Part{llm.TextPart{Text: text}}}}
	return items, canonicalDigest(t, items)
}

func canonicalDigest(t *testing.T, items []llm.Item) [32]byte {
	t.Helper()
	_, digest, err := state.CanonicalTranscript(items)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func retentionChild(t *testing.T, parent state.Continuation, parentHandle state.Handle, text string, expiresAt time.Time) state.Continuation {
	t.Helper()
	child := parent.Clone()
	child.ParentID = parentHandle.String()
	child.Depth = parent.Depth + 1
	child.LastOperationID = "op"
	child.Transcript = append(child.Transcript, llm.ModelText(text))
	child.TranscriptDigest = canonicalDigest(t, child.Transcript)
	child.ExpiresAt = expiresAt
	return child
}

// createTextRoot stores a complete root continuation for "tenant" holding
// one human message with text, returning its handle and transcript.
func createTextRoot(t *testing.T, store *ContinuationStore, text string, expiresAt time.Time) (state.Handle, []llm.Item) {
	t.Helper()
	items, digest := textTranscript(t, llm.ActorHuman, text)
	handle, err := store.CreateRoot(context.Background(), state.Continuation{Tenant: "tenant", Transcript: items, TranscriptDigest: digest, TranscriptComplete: true, ExpiresAt: expiresAt, LastOperationID: "root"})
	if err != nil {
		t.Fatal(err)
	}
	return handle, items
}

func retentionRoot(t *testing.T, store *ContinuationStore, expiresAt time.Time) (state.Handle, state.Continuation) {
	t.Helper()
	handle, _ := createTextRoot(t, store, "hello", expiresAt)
	root, err := store.Get(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}
	return handle, root
}

// putShortLivedRetentionChild stores a child that expires a minute after now
// under a root that lives for an hour, returning the request and its handle.
func putShortLivedRetentionChild(t *testing.T, store *ContinuationStore, now time.Time) (state.PutChildRequest, state.Handle) {
	t.Helper()
	rootHandle, root := retentionRoot(t, store, now.Add(time.Hour))
	child := retentionChild(t, root, rootHandle, "world", now.Add(time.Minute))
	request := state.PutChildRequest{Parent: rootHandle, Child: child, OperationKey: "op-1"}
	first, err := store.PutChild(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return request, first
}

// assertRetryStoresNewChild retries request with a fresh expiry and requires
// a resolvable child other than the stale (swept or expired) first handle.
func assertRetryStoresNewChild(t *testing.T, store *ContinuationStore, request state.PutChildRequest, first state.Handle, now time.Time, stale string) {
	t.Helper()
	request.Child.ExpiresAt = now.Add(time.Minute)
	retry, err := store.PutChild(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if retry == first {
		t.Fatalf("PutChild returned the %s child handle %q", stale, retry)
	}
	if _, err := store.Get(context.Background(), retry); err != nil {
		t.Fatalf("Get(new child) = %v", err)
	}
}

// A retry of PutChild with the same parent and operation key after the child
// expired and was swept must not return the deleted handle.
func TestContinuationPutChildAfterChildSweepCreatesResolvableChild(t *testing.T) {
	now := time.Unix(100, 0)
	store := newTestContinuationStore(t, &now, nil)
	request, first := putShortLivedRetentionChild(t, store, now)
	now = now.Add(2 * time.Minute)
	if removed := store.Sweep(now); removed != 1 {
		t.Fatalf("Sweep removed %d continuations, want 1", removed)
	}
	store.mu.RLock()
	indexed := len(store.byOp)
	store.mu.RUnlock()
	if indexed != 0 {
		t.Fatalf("Sweep retained %d operation-index entries for deleted children", indexed)
	}
	assertRetryStoresNewChild(t, store, request, first, now, "swept")
}

// Even without an explicit Sweep, an indexed child that has expired must be
// treated as absent rather than returned as an unresolvable handle.
func TestContinuationPutChildAfterChildExpiryWithoutSweep(t *testing.T) {
	now := time.Unix(100, 0)
	store := newTestContinuationStore(t, &now, nil)
	request, first := putShortLivedRetentionChild(t, store, now)
	now = now.Add(2 * time.Minute)
	assertRetryStoresNewChild(t, store, request, first, now, "expired")
	if _, err := store.Get(context.Background(), first); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("Get(expired child) = %v, want ErrNotFound", err)
	}
}

// Writes reclaim expired continuations without an external lifecycle owner.
func TestContinuationWritesReclaimExpiredRecords(t *testing.T) {
	now := time.Unix(100, 0)
	store := newTestContinuationStore(t, &now, nil)
	for range 3 {
		retentionRoot(t, store, now.Add(time.Minute))
	}
	now = now.Add(2 * time.Minute)
	retentionRoot(t, store, now.Add(time.Minute))
	store.mu.RLock()
	retained := len(store.records)
	store.mu.RUnlock()
	if retained != 1 {
		t.Fatalf("retained %d continuation records, want 1", retained)
	}
}

func retentionBegin(t *testing.T, store *AdmissionStore, id string, now time.Time) admission.Operation {
	t.Helper()
	bucketNanos := int64(time.Second)
	reservation := admission.WindowReservation{PolicyID: "p", WindowID: "w", Bucket: now.UnixNano() / bucketNanos, Amount: 1, Limit: 1000, BucketNanos: bucketNanos, DurationNanos: int64(10 * time.Second)}
	result, err := store.Begin(context.Background(), admission.BeginRequest{ID: id, ScopeKey: "scope/" + id, RequestDigest: admission.Digest([]byte(id)), Reservation: 1, Reservations: []admission.WindowReservation{reservation}, LeaseUntil: now.Add(time.Minute), ExpiresAt: now.Add(time.Minute)})
	if err != nil || result.Denied != nil {
		t.Fatalf("Begin(%s) = %#v %v", id, result, err)
	}
	return result.Operation
}

func TestAdmissionSweepReclaimsOnlyExpiredTerminalState(t *testing.T) {
	now := time.Unix(1000, 0)
	store := NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }})
	ctx := context.Background()

	completed := retentionBegin(t, store, "completed", now)
	if err := store.MarkDispatching(ctx, admission.DispatchRequest{OperationID: completed.ID, DispatchToken: completed.DispatchToken, LeaseUntil: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkProviderPending(ctx, admission.ProviderPendingRequest{OperationID: completed.ID, DispatchToken: completed.DispatchToken, ProviderOperationID: "provider-op", EndpointID: "endpoint"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, admission.CompleteRequest{OperationID: completed.ID, DispatchToken: completed.DispatchToken, Actual: 1}); err != nil {
		t.Fatal(err)
	}
	reserved := retentionBegin(t, store, "reserved", now)
	dispatching := retentionBegin(t, store, "dispatching", now)
	if err := store.MarkDispatching(ctx, admission.DispatchRequest{OperationID: dispatching.ID, DispatchToken: dispatching.DispatchToken, LeaseUntil: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}

	// Long after every retention and admission window has passed.
	now = now.Add(time.Hour)
	if removed := store.Sweep(now); removed != 1 {
		t.Fatalf("Sweep removed %d operations, want 1", removed)
	}
	if _, err := store.Get(ctx, completed.ID); !errors.Is(err, admission.ErrOperationNotFound) {
		t.Fatalf("Get(completed) = %v, want not found", err)
	}
	for _, id := range []string{reserved.ID, dispatching.ID} {
		if _, err := store.Get(ctx, id); err != nil {
			t.Fatalf("in-flight operation %s was reclaimed: %v", id, err)
		}
	}
	store.mu.Lock()
	scopes, providers, polls := len(store.byScope), len(store.providerIDs), len(store.pollAfter)
	buckets := len(store.buckets[budgetKey{policy: "p", window: "w"}])
	store.mu.Unlock()
	if scopes != 2 || providers != 0 || polls != 0 {
		t.Fatalf("retained scopes=%d providers=%d polls=%d, want 2/0/0", scopes, providers, polls)
	}
	// The single bucket is still referenced by the in-flight reservations.
	if buckets != 1 {
		t.Fatalf("retained %d buckets, want the 1 referenced by in-flight work", buckets)
	}

	// In-flight operations can still reconcile against their buckets.
	if err := store.MarkDispatching(ctx, admission.DispatchRequest{OperationID: reserved.ID, DispatchToken: reserved.DispatchToken, LeaseUntil: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []admission.Operation{reserved, dispatching} {
		if err := store.Complete(ctx, admission.CompleteRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Actual: 1}); err != nil {
			t.Fatalf("Complete(%s) after sweep = %v", operation.ID, err)
		}
	}
}

// Begin reclaims expired terminal operations and out-of-window buckets without
// an external lifecycle owner, and a reclaimed scope can be admitted again.
func TestAdmissionBeginReclaimsExpiredState(t *testing.T) {
	now := time.Unix(1000, 0)
	store := NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }})
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		operation := retentionBegin(t, store, id, now)
		if err := store.MarkDispatching(ctx, admission.DispatchRequest{OperationID: id, DispatchToken: operation.DispatchToken, LeaseUntil: now.Add(time.Minute)}); err != nil {
			t.Fatal(err)
		}
		if err := store.Fail(ctx, admission.FailRequest{OperationID: id, DispatchToken: operation.DispatchToken, Certainty: admission.Rejected}); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(time.Hour)
	retentionBegin(t, store, "a", now)
	store.mu.Lock()
	operations, scopes := len(store.operations), len(store.byScope)
	buckets := len(store.buckets[budgetKey{policy: "p", window: "w"}])
	store.mu.Unlock()
	if operations != 1 || scopes != 1 || buckets != 1 {
		t.Fatalf("retained operations=%d scopes=%d buckets=%d, want 1/1/1", operations, scopes, buckets)
	}
}
