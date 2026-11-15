package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	memoryadmission "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/memory"
)

// TestBlobResultStoreRewritingAStoredResultReturnsItsReference covers the
// legacy engine's recovery path, which writes a response read back from the
// store to obtain its reference: the content-addressed write must return the
// same reference as the original.
func TestBlobResultStoreRewritingAStoredResultReturnsItsReference(t *testing.T) {
	ctx := context.Background()
	now, admissions, begin, store := beginStoredResult(t, "operation-rewrite", "request-1")
	actual := pricing.MustUSD("0.000002000000000001")
	response := llm.Response{OperationKey: "request-1", Status: llm.ResponseStatusCompleted, Output: []llm.Item{llm.ModelText("world")}, Usage: llm.Usage{InputTokens: 3, OutputTokens: 1}, Cost: llm.Cost{Status: llm.CostStatusKnown, ActualCostUSD: &actual, Method: "usage"}, Diagnostics: []llm.Diagnostic{}}
	first, err := store.Put(ctx, begin.Operation.ID, response)
	if err != nil {
		t.Fatal(err)
	}
	if err := admissions.MarkDispatching(ctx, admission.DispatchRequest{OperationID: begin.Operation.ID, DispatchToken: begin.Operation.DispatchToken, LeaseUntil: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if err := admissions.Complete(ctx, admission.CompleteRequest{OperationID: begin.Operation.ID, DispatchToken: begin.Operation.DispatchToken, Actual: pricing.MicroUSD(3), ActualCostUSD: actual, ResultRef: &first}); err != nil {
		t.Fatal(err)
	}
	stored, err := store.Get(ctx, begin.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.Put(ctx, begin.Operation.ID, stored)
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Fatalf("rewritten reference = %+v, want %+v", again, first)
	}
}

// TestBlobResultStoreReadsAStoredResultBeforeCompletion covers the window the
// legacy engine recovers from: Put succeeded, the ledger completion failed,
// and the operation is still dispatching without a ResultRef.
func TestBlobResultStoreReadsAStoredResultBeforeCompletion(t *testing.T) {
	ctx := context.Background()
	now, admissions, begin, store := beginStoredResult(t, "operation-pending", "request-2")
	response := llm.Response{OperationKey: "request-2", Status: llm.ResponseStatusCompleted, Output: []llm.Item{}, Diagnostics: []llm.Diagnostic{}}
	if _, err := store.Put(ctx, begin.Operation.ID, response); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, begin.Operation.ID); !errors.Is(err, engine.ErrResultNotFound) {
		t.Fatalf("reserved operation Get error = %v, want ErrResultNotFound", err)
	}
	if err := admissions.MarkDispatching(ctx, admission.DispatchRequest{OperationID: begin.Operation.ID, DispatchToken: begin.Operation.DispatchToken, LeaseUntil: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, begin.Operation.ID)
	if err != nil {
		t.Fatalf("dispatching operation Get error = %v, want the stored result", err)
	}
	if got.OperationKey != response.OperationKey {
		t.Fatalf("stored result = %+v, want %+v", got, response)
	}
	restarted, err := NewBlobResultStore(newTestBlobStore(), admissions, nil, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Get(ctx, begin.Operation.ID); !errors.Is(err, engine.ErrResultNotFound) {
		t.Fatalf("another process read an uncompleted result: %v", err)
	}
}

// beginStoredResult reserves operation id for request in a fresh in-memory
// admission store and returns the clock reading, the store, the reservation
// and a blob result store over both.
func beginStoredResult(t *testing.T, id, request string) (time.Time, *memoryadmission.AdmissionStore, admission.BeginResult, *BlobResultStore) {
	t.Helper()
	now := time.Now().UTC()
	admissions := memoryadmission.NewAdmissionStore(memoryadmission.AdmissionOptions{Clock: func() time.Time { return now }})
	begin, err := admissions.Begin(context.Background(), admission.BeginRequest{ID: id, ScopeKey: "tenant-a\x00" + request, RequestDigest: admission.Digest([]byte("request")), Reservation: pricing.MicroUSD(1), LeaseUntil: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewBlobResultStore(newTestBlobStore(), admissions, nil, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return now, admissions, begin, store
}
