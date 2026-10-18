package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

// A call the worker's own HTTP timeout cuts off after the request was sent,
// on a route with no provider job and no idempotency recovery, cannot be
// learned by waiting out the start lease, and a replacement would run under
// the same timeout. It is a terminal, possibly billed deadline failure rather
// than an unknown outcome that buys a new attempt after every lease.
func TestCloudExecutionUnrecoverableDeadlineIsTerminal(t *testing.T) {
	for name, callErr := range map[string]error{
		"classified deadline": provider.NewError(provider.CodeDeadlineExceeded, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider request deadline exceeded"),
		"unclassified client timeout": &provider.Error{Code: provider.CodeProviderUnavailable, Phase: provider.PhaseDispatch, Dispatch: provider.DispatchAmbiguous, Retry: provider.RetrySameOperation,
			SafeMessage: "provider request failed before a response was classified", Cause: fmt.Errorf("read body: %w", context.DeadlineExceeded)},
	} {
		t.Run(name, func(t *testing.T) {
			f, budgets := toolArgumentsCloud(t, func(provider.Call) (provider.Result, error) { return provider.Result{}, callErr })
			ctx := context.Background()
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			if v.Retryable || v.FailureCode != "provider_error" || v.ErrorCode != string(provider.CodeDeadlineExceeded) || v.Dispatch != string(provider.DispatchAmbiguous) {
				t.Fatalf("failure=%+v", v)
			}
			execution := toolArgumentsExecution(t, f, v.RequestID)
			if execution.Stage != cloudstate.ExecutionFailed || execution.Failure == nil || execution.Failure.Retryable || execution.Settlement == nil || !execution.Settled || budgets.calls.Load() != 1 {
				t.Fatalf("execution stage=%s failure=%+v settled=%t reconciles=%d", execution.Stage, execution.Failure, execution.Settled, budgets.calls.Load())
			}
			// The provider may have billed the request: account the reservation.
			for _, event := range execution.Settlement.Events {
				if event.CostStatus != budget.CostUnknown || event.AccountedIncreaseUSD.String() != event.ReservedDecreaseUSD.String() {
					t.Fatalf("settlement event=%+v", event)
				}
			}
			f.now = f.now.Add(16 * time.Minute)
			f.restart(t)
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			if f.submits.Load() != 1 || budgets.calls.Load() != 1 {
				t.Fatalf("submits=%d reconciles=%d, want one each", f.submits.Load(), budgets.calls.Load())
			}
		})
	}
}

// Cancellation, such as a worker shutdown, is not the route's deadline: the
// attempt stays unknown and is recovered or replaced after the start lease.
func TestCloudExecutionCanceledCallStaysUnknown(t *testing.T) {
	f, _ := toolArgumentsCloud(t, func(provider.Call) (provider.Result, error) {
		return provider.Result{}, provider.NewError(provider.CodeCanceled, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider request canceled")
	})
	v, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	if execution := toolArgumentsExecution(t, f, v.RequestID); execution.Stage != cloudstate.ExecutionUnknown {
		t.Fatalf("execution stage=%s failure=%+v", execution.Stage, execution.Failure)
	}
}
