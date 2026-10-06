package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/memory"
)

func TestCompatibilityActualMicroUSDPreservesExactRecoveredCost(t *testing.T) {
	whole := pricing.MustUSD("2.000000000000000001")
	actual, err := compatibilityActualMicroUSD(whole)
	if err != nil {
		t.Fatalf("compatibilityActualMicroUSD() = %v", err)
	}
	if actual != 2_000_001 {
		t.Fatalf("recovered exact cost = %d, want 2000001 (ceil materialization)", actual)
	}

	subMicro := pricing.MustUSD("0.000000000000000001")
	actual, err = compatibilityActualMicroUSD(subMicro)
	if err != nil {
		t.Fatalf("compatibilityActualMicroUSD(sub-micro) = %v", err)
	}
	if actual != 1 {
		t.Fatalf("recovered sub-micro cost = %d, want one compatibility micro-dollar", actual)
	}
}

// failFirstCompleteAdmission fails the first ledger completion after the
// result has been stored, and records every completion it receives.
type failFirstCompleteAdmission struct {
	admission.AdmissionStore
	completes []admission.CompleteRequest
}

func (store *failFirstCompleteAdmission) Complete(ctx context.Context, request admission.CompleteRequest) error {
	store.completes = append(store.completes, request)
	if len(store.completes) == 1 {
		return errors.New("transient completion failure")
	}
	return store.AdmissionStore.Complete(ctx, request)
}

func TestGenerateRecoveryCompletesWithStoredResultReferenceAndCost(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	ledger := &failFirstCompleteAdmission{AdmissionStore: memory.NewAdmissionStore(memory.AdmissionOptions{Clock: func() time.Time { return now }})}
	adapter := &fakeAdapter{name: "stored-result-recovery", response: successfulResponse()}
	harness := newHarnessWithAdmission(t, adapter, ledger, func() time.Time { return now })
	request := baseRequest("stored-result-recovery")
	if _, err := harness.engine.Generate(context.Background(), request); err == nil {
		t.Fatal("first Generate succeeded despite the completion failure")
	}
	if _, err := harness.engine.Generate(context.Background(), request); err != nil {
		t.Fatalf("recovery Generate error = %v", err)
	}
	if adapter.invokes != 1 {
		t.Fatalf("provider invokes = %d, want 1", adapter.invokes)
	}
	if len(ledger.completes) != 2 {
		t.Fatalf("completions = %d, want 2", len(ledger.completes))
	}
	first, recovered := ledger.completes[0], ledger.completes[1]
	if recovered.ResultRef == nil || first.ResultRef == nil || *recovered.ResultRef != *first.ResultRef {
		t.Fatalf("recovery ResultRef = %v, want %v", recovered.ResultRef, first.ResultRef)
	}
	if recovered.Actual != first.Actual || recovered.ActualCostUSD.String() != first.ActualCostUSD.String() || recovered.CostStatus != first.CostStatus || recovered.CostMethod != first.CostMethod {
		t.Fatalf("recovery cost = %d/%s/%s/%s, want %d/%s/%s/%s", recovered.Actual, recovered.ActualCostUSD, recovered.CostStatus, recovered.CostMethod, first.Actual, first.ActualCostUSD, first.CostStatus, first.CostMethod)
	}
	if recovered.Attempt != first.Attempt {
		t.Fatalf("recovery attempt = %+v, want the finalization attempt %+v", recovered.Attempt, first.Attempt)
	}
}
