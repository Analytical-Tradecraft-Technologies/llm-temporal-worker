package conformance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// OperationStore is the one-shot operation subset used by durable operation
// repositories. It is deliberately separate from Stores: Redis remains a
// budget/throttle implementation and is not required to emulate PostgreSQL's
// encrypted request/result and attempt tables.
type OperationStore interface {
	admission.AdmissionStore
}

type providerPendingStore interface {
	MarkProviderPending(context.Context, admission.ProviderPendingRequest) error
}

// RunOperation exercises replay, digest conflict, compare-and-set dispatch,
// result persistence, and terminal expiry semantics for memory and
// PostgreSQL operation stores. Callers should provide an isolated store.
func RunOperation(t *testing.T, store OperationStore, now time.Time) {
	t.Helper()
	if store == nil {
		t.Fatal("operation store is nil")
	}
	request := admission.BeginRequest{ID: "operation-conformance", OperationKey: "operation-conformance", Actor: "postgres-test", ScopeKey: "tenant/project", RequestDigest: admission.Digest([]byte("request")), ReservationUSD: pricing.MustUSD("0.000000000000000000"), ExpiresAt: now.Add(time.Hour)}
	first, err := store.Begin(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := store.Begin(context.Background(), request); err != nil || !replay.Existing {
		t.Fatalf("replay = %#v, %v", replay, err)
	}
	request.RequestDigest = admission.Digest([]byte("different"))
	if _, err := store.Begin(context.Background(), request); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("digest conflict = %v", err)
	}
	if err := store.MarkDispatching(context.Background(), admission.DispatchRequest{OperationID: first.Operation.ID, DispatchToken: first.Operation.DispatchToken, Attempt: admission.AttemptFacts{RouteID: "primary"}, LeaseUntil: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if pending, ok := store.(providerPendingStore); ok {
		if err := pending.MarkProviderPending(context.Background(), admission.ProviderPendingRequest{OperationID: first.Operation.ID, DispatchToken: first.Operation.DispatchToken, ProviderOperationID: "provider-operation-1", EndpointID: "endpoint-1", Provider: "fixture"}); err != nil {
			t.Fatalf("provider pending: %v", err)
		}
		if err := pending.MarkProviderPending(context.Background(), admission.ProviderPendingRequest{OperationID: first.Operation.ID, DispatchToken: first.Operation.DispatchToken, ProviderOperationID: "provider-operation-2", EndpointID: "endpoint-1", Provider: "fixture"}); !errors.Is(err, admission.ErrOperationConflict) {
			t.Fatalf("divergent provider pending replay = %v", err)
		}
	}
	ref := &state.BlobRef{Digest: [32]byte{1}, Size: 1, Media: "application/octet-stream"}
	if err := store.Complete(context.Background(), admission.CompleteRequest{OperationID: first.Operation.ID, DispatchToken: first.Operation.DispatchToken, ResultRef: ref}); err != nil {
		t.Fatalf("complete replay contract: %v", err)
	}
	completed, err := store.Get(context.Background(), first.Operation.ID)
	if err != nil || completed.State != admission.StateCompleted || completed.ResultRef == nil || completed.ResultRef.Digest != ref.Digest {
		t.Fatalf("completed operation = %#v, %v", completed, err)
	}
}
