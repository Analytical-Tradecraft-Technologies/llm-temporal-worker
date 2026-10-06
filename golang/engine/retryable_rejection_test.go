package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// A definite, retryable rejection on the last route leaves the operation
// retryable: the identical request under the same key reserves again and
// dispatches instead of meeting a terminal operation conflict (#790).
func TestGenerateRetriesTheSameOperationAfterADefiniteRetryableRejection(t *testing.T) {
	adapter := &fakeAdapter{name: "reject-first", rejectFirst: true, response: successfulResponse()}
	harness := newHarness(t, adapter)
	request := baseRequest("retry-after-rejection")
	_, err := harness.engine.Generate(context.Background(), request)
	var failure *provider.Error
	if !errors.As(err, &failure) || failure.Retry == provider.RetryNever || failure.Dispatch != provider.DispatchRejected {
		t.Fatalf("first error = %#v, want a retryable definite rejection", err)
	}
	failed, err := harness.admission.Get(context.Background(), failure.OperationID)
	if err != nil || failed.State != admission.StateDefiniteFailed || !failed.Retryable || failed.ReservedMicroUSD != 0 {
		t.Fatalf("failed operation = %+v, %v; want a released, retryable definite failure", failed, err)
	}
	response, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatalf("same-key retry = %v, want a fresh dispatch", err)
	}
	if response.OperationID != failure.OperationID || adapter.invokes != 2 {
		t.Fatalf("retry operation = %q (first %q), provider invokes = %d", response.OperationID, failure.OperationID, adapter.invokes)
	}
	// The completed operation now replays without another dispatch.
	if _, err := harness.engine.Generate(context.Background(), request); err != nil || adapter.invokes != 2 {
		t.Fatalf("replay = %v, provider invokes = %d", err, adapter.invokes)
	}
}
