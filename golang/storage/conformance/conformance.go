// Package conformance holds the black-box shared-state contract exercised by
// every StoreFactory. It is imported only by storage tests, so implementations
// cannot satisfy the suite through package-private seams.
package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

// ContinuationStore is the public continuation surface exercised by the
// shared suite. CreateRoot and GetForTenant are intentionally included in
// addition to state.ContinuationStore because they establish the handle MAC
// and tenant-binding contract.
type ContinuationStore interface {
	state.ContinuationStore
	CreateRoot(context.Context, state.Continuation) (state.Handle, error)
	GetForTenant(context.Context, string, state.Handle) (state.Continuation, error)
}

// Stores are the public shared-state ports supplied by a StoreFactory.
type Stores struct {
	Admission     admission.AdmissionStore
	Continuations ContinuationStore
	Now           func() time.Time
}

// StoreFactory creates isolated state stores for one conformance case.
// Implementations must not share state between calls: each case deliberately
// uses a fresh factory so its outcome is independent of test order.
type StoreFactory struct {
	Name string
	New  func(testing.TB) Stores
}

// Run executes the same black-box shared-state contract against a factory.
func Run(t *testing.T, factory StoreFactory) {
	t.Helper()
	if factory.Name == "" || factory.New == nil {
		t.Fatal("conformance StoreFactory is incomplete")
	}
	t.Run("ledger transitions, replay, refunds, excess, and clock boundaries", func(t *testing.T) {
		testLedger(t, factory)
	})
	t.Run("ambiguous reservations are retained", func(t *testing.T) {
		testAmbiguity(t, factory)
	})
	t.Run("continuations are immutable and integrity bound", func(t *testing.T) {
		testContinuations(t, factory)
	})
	t.Run("one hundred concurrent admissions stay within every overlap", func(t *testing.T) {
		testConcurrentAdmissions(t, factory)
	})
}

func (factory StoreFactory) stores(t *testing.T) Stores {
	t.Helper()
	stores := factory.New(t)
	if stores.Admission == nil || stores.Continuations == nil || stores.Now == nil {
		t.Fatalf("%s StoreFactory returned incomplete ports", factory.Name)
	}
	return stores
}

func testLedger(t *testing.T, factory StoreFactory) {
	t.Helper()
	stores := factory.stores(t)
	ctx := context.Background()
	now := stores.Now()

	request := beginRequest("complete", "complete-policy", 10, 10, now)
	operation := begin(t, stores.Admission, request)
	if err := stores.Admission.Complete(ctx, admission.CompleteRequest{
		OperationID:   operation.ID,
		DispatchToken: operation.DispatchToken,
		Actual:        3,
	}); !errors.Is(err, admission.ErrInvalidTransition) {
		t.Fatalf("complete before dispatch = %v, want invalid transition", err)
	}
	if err := stores.Admission.MarkDispatching(ctx, admission.DispatchRequest{
		OperationID:   operation.ID,
		DispatchToken: "wrong-token",
		Attempt:       admission.AttemptFacts{RouteID: "primary"},
		LeaseUntil:    now.Add(time.Minute),
	}); !errors.Is(err, admission.ErrInvalidToken) {
		t.Fatalf("wrong dispatch token = %v, want invalid token", err)
	}
	dispatch(t, stores.Admission, operation, now)

	result := &state.BlobRef{Digest: [32]byte{1}, Size: 7, Media: "application/octet-stream"}
	if err := stores.Admission.Complete(ctx, admission.CompleteRequest{
		OperationID:   operation.ID,
		DispatchToken: operation.DispatchToken,
		Actual:        3,
		ResultRef:     result,
		Attempt:       admission.AttemptFacts{RouteID: "primary"},
	}); err != nil {
		t.Fatalf("complete %q: %v", operation.ID, err)
	}
	result.Media = "mutated-input"
	completed, err := stores.Admission.Get(ctx, operation.ID)
	if err != nil {
		t.Fatalf("get completed operation: %v", err)
	}
	if completed.State != admission.StateCompleted || completed.ReservedMicroUSD != 0 || completed.FinalMicroUSD != 3 || completed.ResultRef == nil || completed.ResultRef.Media != "application/octet-stream" {
		t.Fatalf("completed operation = %#v", completed)
	}
	if err := stores.Admission.Complete(ctx, admission.CompleteRequest{
		OperationID:   operation.ID,
		DispatchToken: operation.DispatchToken,
		Actual:        9,
	}); err != nil {
		t.Fatalf("complete replay: %v", err)
	}
	replay, err := stores.Admission.Begin(ctx, request)
	if err != nil || !replay.Existing || replay.Operation.State != admission.StateCompleted || replay.Operation.FinalMicroUSD != 3 {
		t.Fatalf("completed begin replay = %#v, %v", replay, err)
	}
	conflicting := request
	conflicting.RequestDigest = admission.Digest([]byte("different-complete"))
	if _, err := stores.Admission.Begin(ctx, conflicting); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("request digest conflict = %v, want operation conflict", err)
	}

	if replay := begin(t, stores.Admission, beginRequest("refund", "complete-policy", 7, 10, now)); replay.State != admission.StateReserved {
		t.Fatalf("refunded capacity operation = %#v", replay)
	}
	if denied, err := stores.Admission.Begin(ctx, beginRequest("refund-over", "complete-policy", 1, 10, now)); err != nil || denied.Denied == nil {
		t.Fatalf("refund limit boundary = %#v, %v; want denial", denied, err)
	}

	excess := begin(t, stores.Admission, beginRequest("excess", "excess-policy", 5, 10, now))
	dispatch(t, stores.Admission, excess, now)
	if err := stores.Admission.Complete(ctx, admission.CompleteRequest{
		OperationID:   excess.ID,
		DispatchToken: excess.DispatchToken,
		Actual:        8,
		Attempt:       admission.AttemptFacts{RouteID: "primary"},
	}); err != nil {
		t.Fatalf("complete excess-cost operation: %v", err)
	}
	if accepted := begin(t, stores.Admission, beginRequest("excess-within", "excess-policy", 2, 10, now)); accepted.State != admission.StateReserved {
		t.Fatalf("excess capacity operation = %#v", accepted)
	}
	if denied, err := stores.Admission.Begin(ctx, beginRequest("excess-over", "excess-policy", 1, 10, now)); err != nil || denied.Denied == nil {
		t.Fatalf("excess-cost limit = %#v, %v; want denial", denied, err)
	}

	failed := begin(t, stores.Admission, beginRequest("definite-fail", "definite-fail-policy", 10, 10, now))
	dispatch(t, stores.Admission, failed, now)
	if err := stores.Admission.Fail(ctx, admission.FailRequest{
		OperationID:   failed.ID,
		DispatchToken: failed.DispatchToken,
		Certainty:     admission.Rejected,
		Incurred:      2,
		Attempt:       admission.AttemptFacts{RouteID: "primary", AttemptNumber: 1},
	}); err != nil {
		t.Fatalf("definitely fail operation: %v", err)
	}
	failedValue, err := stores.Admission.Get(ctx, failed.ID)
	if err != nil || failedValue.State != admission.StateDefiniteFailed || failedValue.ReservedMicroUSD != 0 || failedValue.FinalMicroUSD != 2 {
		t.Fatalf("definite failed operation = %#v, %v", failedValue, err)
	}
	if accepted := begin(t, stores.Admission, beginRequest("after-definite-fail", "definite-fail-policy", 8, 10, now)); accepted.State != admission.StateReserved {
		t.Fatalf("definite failure refund = %#v", accepted)
	}
	if denied, err := stores.Admission.Begin(ctx, beginRequest("after-definite-fail-over", "definite-fail-policy", 1, 10, now)); err != nil || denied.Denied == nil {
		t.Fatalf("definite failure limit = %#v, %v; want denial", denied, err)
	}

	if accepted := begin(t, stores.Admission, beginRequest("exact-boundary", "clock-boundary", 10, 10, now)); accepted.State != admission.StateReserved {
		t.Fatalf("exact clock boundary admission = %#v", accepted)
	}
	if denied, err := stores.Admission.Begin(ctx, beginRequest("past-boundary", "clock-boundary", 1, 10, now)); err != nil || denied.Denied == nil {
		t.Fatalf("past clock boundary admission = %#v, %v; want denial", denied, err)
	}

	continued := begin(t, stores.Admission, beginRequest("continue", "continue-policy", 10, 10, now))
	dispatch(t, stores.Admission, continued, now)
	nextReservations := []admission.WindowReservation{reservation("continue-policy", 8, 10, now)}
	next, err := stores.Admission.Continue(ctx, admission.ContinueRequest{
		OperationID:   continued.ID,
		DispatchToken: continued.DispatchToken,
		Outcome: admission.AttemptOutcome{
			Certainty: admission.Rejected,
			Incurred:  2,
			Attempt:   admission.AttemptFacts{RouteID: "fallback", AttemptNumber: 2},
		},
		Remaining:    8,
		Reservations: nextReservations,
		LeaseUntil:   now.Add(time.Minute),
		ExpiresAt:    now.Add(2 * time.Hour),
	})
	if err != nil || next.Denied != nil {
		t.Fatalf("continue = %#v, %v", next, err)
	}
	if next.Operation.State != admission.StateReserved || next.Operation.ReservedMicroUSD != 8 || next.Operation.DispatchToken == continued.DispatchToken {
		t.Fatalf("continued operation = %#v", next.Operation)
	}
	if err := stores.Admission.MarkDispatching(ctx, admission.DispatchRequest{
		OperationID:   continued.ID,
		DispatchToken: continued.DispatchToken,
		Attempt:       admission.AttemptFacts{RouteID: "fallback", AttemptNumber: 3},
		LeaseUntil:    now.Add(time.Minute),
	}); !errors.Is(err, admission.ErrInvalidToken) {
		t.Fatalf("old dispatch token after continue = %v, want invalid token", err)
	}
	if err := stores.Admission.MarkDispatching(ctx, admission.DispatchRequest{
		OperationID:   continued.ID,
		DispatchToken: next.Operation.DispatchToken,
		Attempt:       admission.AttemptFacts{RouteID: "fallback", AttemptNumber: 3},
		LeaseUntil:    now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("dispatch continued operation: %v", err)
	}
	if err := stores.Admission.Complete(ctx, admission.CompleteRequest{
		OperationID:   continued.ID,
		DispatchToken: next.Operation.DispatchToken,
		Actual:        8,
		Attempt:       admission.AttemptFacts{RouteID: "fallback", AttemptNumber: 3},
	}); err != nil {
		t.Fatalf("complete continued operation: %v", err)
	}
	completedContinuation, err := stores.Admission.Get(ctx, continued.ID)
	if err != nil || completedContinuation.State != admission.StateCompleted || completedContinuation.FinalMicroUSD != 8 {
		t.Fatalf("completed continuation = %#v, %v", completedContinuation, err)
	}
}

func testAmbiguity(t *testing.T, factory StoreFactory) {
	t.Helper()
	stores := factory.stores(t)
	ctx := context.Background()
	now := stores.Now()
	operation := begin(t, stores.Admission, beginRequest("ambiguous", "ambiguous-policy", 6, 10, now))
	dispatch(t, stores.Admission, operation, now)
	if err := stores.Admission.Fail(ctx, admission.FailRequest{
		OperationID:   operation.ID,
		DispatchToken: operation.DispatchToken,
		Certainty:     admission.Ambiguous,
		Incurred:      0,
		Attempt:       admission.AttemptFacts{RouteID: "primary"},
	}); err != nil {
		t.Fatalf("mark ambiguous: %v", err)
	}
	value, err := stores.Admission.Get(ctx, operation.ID)
	if err != nil {
		t.Fatalf("get ambiguous operation: %v", err)
	}
	if value.State != admission.StateAmbiguous || value.ReservedMicroUSD != 6 || value.FinalMicroUSD != 6 {
		t.Fatalf("ambiguous retention = %#v", value)
	}
	if denied, err := stores.Admission.Begin(ctx, beginRequest("after-ambiguous", "ambiguous-policy", 5, 10, now)); err != nil || denied.Denied == nil {
		t.Fatalf("ambiguous retained budget = %#v, %v; want denial", denied, err)
	}
}

func testContinuations(t *testing.T, factory StoreFactory) {
	t.Helper()
	stores := factory.stores(t)
	ctx := context.Background()
	now := stores.Now()

	expired := continuation(t, now)
	expired.ExpiresAt = now
	if _, err := stores.Continuations.CreateRoot(ctx, expired); !errors.Is(err, state.ErrExpired) {
		t.Fatalf("continuation expiry boundary = %v, want expired", err)
	}
	badDigest := continuation(t, now)
	badDigest.TranscriptDigest[0] ^= 1
	if _, err := stores.Continuations.CreateRoot(ctx, badDigest); err == nil {
		t.Fatal("continuation with mismatched transcript digest was accepted")
	}

	root := continuation(t, now)
	rootDigest := root.TranscriptDigest
	rootHandle, err := stores.Continuations.CreateRoot(ctx, root)
	if err != nil {
		t.Fatalf("create continuation root: %v", err)
	}
	root.Transcript[0] = llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "mutated"}}}
	root.ProviderState[0].Data[0] = 99
	loaded, err := stores.Continuations.GetForTenant(ctx, "tenant-a", rootHandle)
	if err != nil {
		t.Fatalf("get root for tenant: %v", err)
	}
	if loaded.TranscriptDigest != rootDigest || !bytes.Equal(loaded.ProviderState[0].Data, []byte{1, 2, 3}) {
		t.Fatalf("continuation write was mutable: %#v", loaded)
	}
	loaded.Transcript[0] = llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "mutated read"}}}
	loaded.ProviderState[0].Data[0] = 98
	again, err := stores.Continuations.Get(ctx, rootHandle)
	if err != nil {
		t.Fatalf("get immutable root: %v", err)
	}
	if again.TranscriptDigest != rootDigest || !bytes.Equal(again.ProviderState[0].Data, []byte{1, 2, 3}) {
		t.Fatalf("continuation read was mutable: %#v", again)
	}

	child := again.Clone()
	child.ParentID = rootHandle.String()
	child.Depth = again.Depth + 1
	child.LastOperationID = "child-operation"
	child.Transcript = append(child.Transcript, llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "child"}}})
	_, child.TranscriptDigest, err = state.CanonicalTranscript(child.Transcript)
	if err != nil {
		t.Fatal(err)
	}
	childHandle, err := stores.Continuations.PutChild(ctx, state.PutChildRequest{Parent: rootHandle, Child: child, OperationKey: "child-operation"})
	if err != nil {
		t.Fatalf("put immutable child: %v", err)
	}
	replay, err := stores.Continuations.PutChild(ctx, state.PutChildRequest{Parent: rootHandle, Child: child, OperationKey: "child-operation"})
	if err != nil || replay != childHandle {
		t.Fatalf("child replay = %q, %v; want %q", replay, err, childHandle)
	}
	otherRoot := continuation(t, now)
	otherRoot.Tenant = "tenant-b"
	otherRootHandle, err := stores.Continuations.CreateRoot(ctx, otherRoot)
	if err != nil {
		t.Fatalf("create second continuation root: %v", err)
	}
	otherChild := otherRoot.Clone()
	otherChild.ParentID = otherRootHandle.String()
	otherChild.Depth = otherRoot.Depth + 1
	otherChild.LastOperationID = "child-operation"
	otherChild.Transcript = append(otherChild.Transcript, llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "other child"}}})
	_, otherChild.TranscriptDigest, err = state.CanonicalTranscript(otherChild.Transcript)
	if err != nil {
		t.Fatal(err)
	}
	otherChildHandle, err := stores.Continuations.PutChild(ctx, state.PutChildRequest{Parent: otherRootHandle, Child: otherChild, OperationKey: "child-operation"})
	if err != nil {
		t.Fatalf("put second child: %v", err)
	}
	if otherChildHandle == childHandle {
		t.Fatalf("operation key was shared across tenant/parent scope: %q", childHandle)
	}
	parent, err := stores.Continuations.Get(ctx, rootHandle)
	if err != nil || len(parent.Transcript) != 1 || parent.TranscriptDigest != rootDigest {
		t.Fatalf("child mutated parent = %#v, %v", parent, err)
	}
	if _, err := stores.Continuations.GetForTenant(ctx, "tenant-b", rootHandle); !errors.Is(err, state.ErrInvalidHandle) {
		t.Fatalf("wrong tenant continuation read = %v, want invalid handle", err)
	}
	tampered := state.Handle(tamper(rootHandle.String()))
	if _, err := stores.Continuations.GetForTenant(ctx, "tenant-a", tampered); !errors.Is(err, state.ErrInvalidHandle) {
		t.Fatalf("tampered MAC continuation read = %v, want invalid handle", err)
	}
}

func testConcurrentAdmissions(t *testing.T, factory StoreFactory) {
	t.Helper()
	stores := factory.stores(t)
	now := stores.Now()
	const workers = 100
	const limit = 60
	reservations := []admission.WindowReservation{
		reservation("concurrent-policy-a", 1, limit, now),
		reservation("concurrent-policy-b", 1, limit, now),
	}
	type outcome struct {
		accepted bool
		denied   bool
		err      error
	}
	ready := make(chan struct{}, workers)
	start := make(chan struct{})
	results := make(chan outcome, workers)
	var group sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for index := 0; index < workers; index++ {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			ready <- struct{}{}
			<-start
			request := beginRequest(fmt.Sprintf("concurrent-%03d", index), "unused", 1, limit, now)
			request.Reservations = append([]admission.WindowReservation(nil), reservations...)
			result, err := stores.Admission.Begin(ctx, request)
			results <- outcome{accepted: err == nil && result.Denied == nil, denied: err == nil && result.Denied != nil, err: err}
		}()
	}
	for index := 0; index < workers; index++ {
		<-ready
	}
	close(start)
	group.Wait()
	close(results)

	accepted := 0
	denied := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent Begin error: %v", result.err)
		}
		if result.accepted {
			accepted++
		}
		if result.denied {
			denied++
		}
	}
	if accepted+denied != workers {
		t.Fatalf("concurrent outcomes accepted=%d denied=%d workers=%d", accepted, denied, workers)
	}
	for _, value := range reservations {
		if accepted*int(value.Amount) > int(value.Limit) {
			t.Fatalf("accepted %d microUSD exceeds %s/%s limit %d", accepted*int(value.Amount), value.PolicyID, value.WindowID, value.Limit)
		}
	}
	if accepted != limit || denied != workers-limit {
		t.Fatalf("concurrent capacity accepted=%d denied=%d; want %d/%d", accepted, denied, limit, workers-limit)
	}
}

func begin(t *testing.T, store admission.AdmissionStore, request admission.BeginRequest) admission.Operation {
	t.Helper()
	result, err := store.Begin(context.Background(), request)
	if err != nil || result.Denied != nil {
		t.Fatalf("begin %q = %#v, %v", request.ID, result, err)
	}
	return result.Operation
}

func dispatch(t *testing.T, store admission.AdmissionStore, operation admission.Operation, now time.Time) {
	t.Helper()
	if err := store.MarkDispatching(context.Background(), admission.DispatchRequest{
		OperationID:   operation.ID,
		DispatchToken: operation.DispatchToken,
		Attempt:       admission.AttemptFacts{RouteID: "primary", AttemptNumber: 1},
		LeaseUntil:    now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("mark dispatching %q: %v", operation.ID, err)
	}
}

func beginRequest(id, policy string, amount, limit int, now time.Time) admission.BeginRequest {
	return admission.BeginRequest{
		ID:            id,
		ScopeKey:      "tenant/" + id,
		RequestDigest: admission.Digest([]byte("request/" + id)),
		Reservation:   admissionAmount(amount),
		Reservations:  []admission.WindowReservation{reservation(policy, amount, limit, now)},
		LeaseUntil:    now.Add(time.Minute),
		ExpiresAt:     now.Add(2 * time.Hour),
	}
}

func reservation(policy string, amount, limit int, now time.Time) admission.WindowReservation {
	return admission.WindowReservation{
		PolicyID:      policy,
		WindowID:      "hour",
		Bucket:        now.UnixNano() / int64(time.Hour),
		Amount:        admissionAmount(amount),
		Limit:         admissionAmount(limit),
		BucketNanos:   int64(time.Hour),
		DurationNanos: int64(2 * time.Hour),
	}
}

func admissionAmount(value int) pricing.MicroUSD { return pricing.MicroUSD(value) }

func continuation(t *testing.T, now time.Time) state.Continuation {
	t.Helper()
	items := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "root"}}}}
	_, digest, err := state.CanonicalTranscript(items)
	if err != nil {
		t.Fatal(err)
	}
	return state.Continuation{
		Tenant:             "tenant-a",
		Transcript:         items,
		TranscriptDigest:   digest,
		TranscriptComplete: true,
		Pinning:            state.Pinning{Provider: "provider", EndpointID: "endpoint", Family: "family", ModelLineage: "lineage"},
		ProviderState: []state.OpaqueStateRef{{
			Provider: "provider", EndpointID: "endpoint", Family: "family", ModelLineage: "lineage",
			Media: "application/octet-stream", Data: []byte{1, 2, 3},
		}},
		CapabilityVersion: "cap-v1",
		PriceVersion:      "price-v1",
		CreatedAt:         now,
		ExpiresAt:         now.Add(time.Hour),
	}
}

func tamper(value string) string {
	parts := strings.Split(value, ".")
	if len(parts) == 4 && len(parts[3]) > 0 {
		replacement := byte('A')
		if parts[3][0] == replacement {
			replacement = 'B'
		}
		// Change the first encoded MAC sextet. Unlike altering the final
		// base64url character, this always changes decoded MAC bytes rather
		// than only potentially-unused padding bits.
		parts[3] = string(replacement) + parts[3][1:]
		return strings.Join(parts, ".")
	}
	return value + "x"
}
