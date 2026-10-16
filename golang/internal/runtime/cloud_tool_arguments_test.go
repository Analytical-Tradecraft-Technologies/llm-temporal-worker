package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
	v, err = f.runtime.CompleteExecutionV1(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability})
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

func nestedToolArguments(depth int) json.RawMessage {
	return json.RawMessage(strings.Repeat(`{"a":`, depth-1) + `{}` + strings.Repeat(`}`, depth-1))
}

// A resumable adapter's poll is held to the same depth: a completed job with
// too-deep arguments closes as a paid failure instead of being polled forever.
func TestCloudPollToolArgumentsPastDepthLimitFailPaid(t *testing.T) {
	f := boundedCloud(t, true)
	f.adapter.poll = func(_ context.Context, call provider.Call, _ string, _ provider.Observer) (provider.ResumableResult, error) {
		f.polls.Add(1)
		v := executionResponse(call)
		v.ProviderOperationID = "job"
		v.Result = toolArgumentsResult(call, string(nestedToolArguments(llm.MaxToolArgumentsDepth+1)))
		return v, nil
	}
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}
	f.now = f.now.Add(2 * time.Second)
	v, err = f.runtime.PollExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionFailed)
	execution := toolArgumentsExecution(t, f, v.RequestID)
	if v.Retryable || execution.Stage != cloudstate.ExecutionFailed || execution.Failure == nil || execution.Failure.Code != provider.CodeProviderInvalidResponse ||
		execution.Failure.Dispatch != provider.DispatchAccepted || !execution.Settled {
		t.Fatalf("result=%+v execution stage=%s failure=%+v settled=%t", v, execution.Stage, execution.Failure, execution.Settled)
	}
	f.now = f.now.Add(time.Minute)
	v, err = f.runtime.PollExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionFailed)
	if f.submits.Load() != 1 || f.polls.Load() != 1 {
		t.Fatalf("submits=%d polls=%d, want one each", f.submits.Load(), f.polls.Load())
	}
}

// A tool call at the adapter's depth limit is saved in every durable record
// it passes through: the child execution, the root finalization plan (with
// and without its cache entry), the checkpoint, and the parent of the next
// turn, which sends it back to the provider.
func TestCloudGenerateToolArgumentsAtDepthLimitContinue(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncached", true: "cached"}[cached], func(t *testing.T) {
			f := boundedCloud(t, false)
			if cached {
				f.request.Cache = &llm.CachePolicyV1{}
			}
			arguments := nestedToolArguments(llm.MaxToolArgumentsDepth)
			f.answerWith(
				[]llm.Item{llm.ToolCall{ID: "call_1", Name: "lookup", Arguments: arguments}},
				[]llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "done"}}}},
			)
			first := f.finish(t)
			call, ok := first.Generate.Output[0].(llm.ToolCall)
			if !ok || string(call.Arguments) != string(arguments) || f.submits.Load() != 1 {
				t.Fatalf("output=%+v submits=%d", first.Generate.Output, f.submits.Load())
			}
			// A retry of the same operation replays the saved call.
			if again := f.finish(t); string(again.Generate.Output[0].(llm.ToolCall).Arguments) != string(arguments) || f.submits.Load() != 1 {
				t.Fatalf("retry output=%+v submits=%d", again.Generate.Output, f.submits.Load())
			}
			f.continueFrom(first, "second", llm.ToolResult{CallID: call.ID, Content: []llm.Part{llm.TextPart{Text: "ok"}}})
			second := f.finish(t)
			if second.Generate.Checkpoint.Depth != 1 || f.submits.Load() != 2 {
				t.Fatalf("depth=%d submits=%d", second.Generate.Checkpoint.Depth, f.submits.Load())
			}
			inputs := f.adapter.planningAdapter.inputs
			sent := false
			for _, item := range inputs[len(inputs)-1].Request.Input {
				if echoed, ok := item.(llm.ToolCall); ok && echoed.ID == call.ID && string(echoed.Arguments) == string(arguments) {
					sent = true
				}
			}
			if !sent {
				t.Fatalf("follow-up turn did not send the parent's tool call: %+v", inputs[len(inputs)-1].Request.Input)
			}
		})
	}
}

// One level past the limit, a paid response settles as a terminal accepted
// failure with its claim released, and a caller cannot send such a call back.
func TestCloudGenerateToolArgumentsPastDepthLimitFailPaid(t *testing.T) {
	for name, invoke := range map[string]func(provider.Call) (provider.Result, error){
		// What every adapter lift returns for too-deep arguments.
		"lift rejection": func(provider.Call) (provider.Result, error) {
			return provider.Result{}, provider.NewError(provider.CodeProviderInvalidResponse, provider.PhaseLift, provider.DispatchAccepted, provider.RetryNever, "tool call arguments are invalid JSON")
		},
		// An adapter that lets such a response through is caught before saving.
		"unchecked adapter": func(call provider.Call) (provider.Result, error) {
			return toolArgumentsResult(call, string(nestedToolArguments(llm.MaxToolArgumentsDepth+1))), nil
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
				execution.Failure.Dispatch != provider.DispatchAccepted || execution.Claim == nil || execution.Settlement == nil || !execution.Settled || budgets.calls.Load() != 1 {
				t.Fatalf("execution stage=%s failure=%+v settled=%t reconciles=%d", execution.Stage, execution.Failure, execution.Settled, budgets.calls.Load())
			}
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}
			v, err = f.runtime.CompleteExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionFailed)
			// Past the recovery deadline the request is still terminal.
			f.now = f.now.Add(16 * time.Minute)
			f.restart(t)
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			if f.submits.Load() != 1 || budgets.calls.Load() != 1 {
				t.Fatalf("submits=%d reconciles=%d, want one each", f.submits.Load(), budgets.calls.Load())
			}
			// A caller echoing such a call is rejected before dispatch.
			f.request.OperationKey = "echo"
			f.request.Append = []llm.Item{
				llm.ToolCall{ID: "call_1", Name: "search", Arguments: nestedToolArguments(llm.MaxToolArgumentsDepth + 1)},
				llm.ToolResult{CallID: "call_1", Content: []llm.Part{llm.TextPart{Text: "ok"}}},
			}
			_, err = f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeInvalidArgument || f.submits.Load() != 1 {
				t.Fatalf("echoed too-deep call: err=%v submits=%d", err, f.submits.Load())
			}
		})
	}
}
