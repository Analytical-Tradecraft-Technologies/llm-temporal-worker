package runtime

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

type countingSettlement struct {
	durable.BudgetLeaser
	calls atomic.Int32
}

func (b *countingSettlement) Reconcile(ctx context.Context, request durable.ReconcileRequest) error {
	b.calls.Add(1)
	return b.BudgetLeaser.Reconcile(ctx, request)
}

func toolArgumentsCloud(t *testing.T, invoke func(provider.Call) (provider.Result, error)) (*boundedCloudFixture, *countingSettlement) {
	t.Helper()
	f := boundedCloud(t, false)
	budgets := &countingSettlement{BudgetLeaser: f.cap.Budgets.(durable.BudgetLeaser)}
	f.cap.Budgets = budgets
	f.restart(t)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return invoke(call)
	}
	return f, budgets
}

func toolArgumentsResult(call provider.Call, arguments string) provider.Result {
	result := executionResponse(call).Result
	result.Response.Status = llm.ResponseStatusToolCalls
	result.Response.Output = []llm.Item{llm.ToolCall{ID: "call_1", Name: "search", Arguments: json.RawMessage(arguments)}}
	return result
}

func toolArgumentsExecution(t *testing.T, f *boundedCloudFixture, requestID string) cloudstate.ProviderExecution {
	t.Helper()
	scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
	attempt, err := f.repository.LoadRequestAttempt(context.Background(), scope, cloudstate.RequestID(requestID))
	if err != nil {
		t.Fatal(err)
	}
	saved, err := f.repository.LoadProviderExecution(context.Background(), scope, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	return saved.Execution
}

// Provider key order differs from the canonical stored form. The settlement
// acknowledgement must still be accepted on the first Generate attempt.
func TestCloudExecutionToolArgumentsKeyOrderCompletesFirstAttempt(t *testing.T) {
	f, budgets := toolArgumentsCloud(t, func(call provider.Call) (provider.Result, error) {
		return toolArgumentsResult(call, `{"query":"x","limit":5}`), nil
	})
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	if f.submits.Load() != 1 || budgets.calls.Load() != 1 {
		t.Fatalf("submits=%d reconciles=%d, want one each", f.submits.Load(), budgets.calls.Load())
	}
	if execution := toolArgumentsExecution(t, f, v.RequestID); execution.Stage != cloudstate.ExecutionSucceeded || !execution.Settled {
		t.Fatalf("execution stage=%s settled=%t", execution.Stage, execution.Settled)
	}
	v, err = f.runtime.CompleteExecutionV1(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context})
	boundedState(t, v, err, llm.ExecutionCompleted)
	// Callers keep receiving the canonical arguments the saved response holds.
	call, ok := v.Generate.Output[0].(llm.ToolCall)
	if !ok || string(call.Arguments) != `{"limit":5,"query":"x"}` || f.submits.Load() != 1 || budgets.calls.Load() != 1 {
		t.Fatalf("output=%+v submits=%d reconciles=%d", v.Generate.Output, f.submits.Load(), budgets.calls.Load())
	}
}

// A response received in full but unusable is a terminal failure accounted at
// the reservation. It must not become an unknown outcome that stalls and then
// buys a second provider call.
func TestCloudExecutionDuplicateKeyToolArgumentsAreTerminal(t *testing.T) {
	for name, invoke := range map[string]func(provider.Call) (provider.Result, error){
		// What every adapter lift returns for duplicate-key arguments.
		"lift rejection": func(provider.Call) (provider.Result, error) {
			return provider.Result{}, provider.NewError(provider.CodeProviderInvalidResponse, provider.PhaseLift, provider.DispatchAccepted, provider.RetryNever, "tool call arguments are invalid JSON")
		},
		// An adapter that lets such a response through is caught before saving.
		"unsavable response": func(call provider.Call) (provider.Result, error) {
			return toolArgumentsResult(call, `{"q":"a","q":"b"}`), nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			f, budgets := toolArgumentsCloud(t, invoke)
			ctx := context.Background()
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			if v.Retryable || v.FailureCode != "provider_error" {
				t.Fatalf("failure=%+v", v)
			}
			execution := toolArgumentsExecution(t, f, v.RequestID)
			if execution.Stage != cloudstate.ExecutionFailed || execution.Failure == nil || execution.Failure.Code != provider.CodeProviderInvalidResponse ||
				execution.Failure.Dispatch != provider.DispatchAccepted || execution.Failure.Retryable || !execution.Settled || budgets.calls.Load() != 1 {
				t.Fatalf("execution stage=%s failure=%+v settled=%t reconciles=%d", execution.Stage, execution.Failure, execution.Settled, budgets.calls.Load())
			}
			// Past the recovery deadline the request is still terminal.
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
