package memory

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func retentionContinuationStore(t *testing.T, now *time.Time) *ContinuationStore {
	t.Helper()
	keyring, err := state.NewKeyring([]state.Key{{ID: "k1", Secret: bytes.Repeat([]byte{3}, 32), Primary: true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewContinuationStore(ContinuationOptions{Keyring: keyring, Clock: func() time.Time { return *now }, MaxDepth: 4})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func retentionChild(t *testing.T, parent state.Continuation, parentHandle state.Handle, text string, expiresAt time.Time) state.Continuation {
	t.Helper()
	child := parent.Clone()
	child.ParentID = parentHandle.String()
	child.Depth = parent.Depth + 1
	child.LastOperationID = "op"
	child.Transcript = append(child.Transcript, llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: text}}})
	var err error
	_, child.TranscriptDigest, err = state.CanonicalTranscript(child.Transcript)
	if err != nil {
		t.Fatal(err)
	}
	child.ExpiresAt = expiresAt
	return child
}

func retentionRoot(t *testing.T, store *ContinuationStore, expiresAt time.Time) (state.Handle, state.Continuation) {
	t.Helper()
	items := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}
	_, digest, err := state.CanonicalTranscript(items)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := store.CreateRoot(context.Background(), state.Continuation{Tenant: "tenant", Transcript: items, TranscriptDigest: digest, TranscriptComplete: true, ExpiresAt: expiresAt, LastOperationID: "root"})
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.Get(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}
	return handle, root
}

// A retry of PutChild with the same parent and operation key after the child
// expired and was swept must not return the deleted handle.
func TestContinuationPutChildAfterChildSweepCreatesResolvableChild(t *testing.T) {
	now := time.Unix(100, 0)
	store := retentionContinuationStore(t, &now)
	rootHandle, root := retentionRoot(t, store, now.Add(time.Hour))
	child := retentionChild(t, root, rootHandle, "world", now.Add(time.Minute))
	request := state.PutChildRequest{Parent: rootHandle, Child: child, OperationKey: "op-1"}
	first, err := store.PutChild(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
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
	request.Child.ExpiresAt = now.Add(time.Minute)
	retry, err := store.PutChild(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if retry == first {
		t.Fatalf("PutChild returned the swept child handle %q", retry)
	}
	if _, err := store.Get(context.Background(), retry); err != nil {
		t.Fatalf("Get(new child) = %v", err)
	}
}

// Even without an explicit Sweep, an indexed child that has expired must be
// treated as absent rather than returned as an unresolvable handle.
func TestContinuationPutChildAfterChildExpiryWithoutSweep(t *testing.T) {
	now := time.Unix(100, 0)
	store := retentionContinuationStore(t, &now)
	rootHandle, root := retentionRoot(t, store, now.Add(time.Hour))
	child := retentionChild(t, root, rootHandle, "world", now.Add(time.Minute))
	request := state.PutChildRequest{Parent: rootHandle, Child: child, OperationKey: "op-1"}
	first, err := store.PutChild(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	request.Child.ExpiresAt = now.Add(time.Minute)
	retry, err := store.PutChild(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if retry == first {
		t.Fatalf("PutChild returned the expired child handle %q", retry)
	}
	if _, err := store.Get(context.Background(), retry); err != nil {
		t.Fatalf("Get(new child) = %v", err)
	}
	if _, err := store.Get(context.Background(), first); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("Get(expired child) = %v, want ErrNotFound", err)
	}
}

// Writes reclaim expired continuations without an external lifecycle owner.
func TestContinuationWritesReclaimExpiredRecords(t *testing.T) {
	now := time.Unix(100, 0)
	store := retentionContinuationStore(t, &now)
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
