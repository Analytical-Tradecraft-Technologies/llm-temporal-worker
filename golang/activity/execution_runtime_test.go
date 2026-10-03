package activity

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"go.temporal.io/sdk/temporal"
)

const executionTestID = "llmtw_req_49c0cb63-15ed-4da7-aa74-17a7d711c9a5"

func pendingExecution() llm.ExecutionResultV1 {
	return llm.ExecutionResultV1{RequestID: executionTestID, Kind: "generate", State: llm.ExecutionPending, RetryAfterSeconds: 3}
}

type executionRuntimeStub struct {
	v1RuntimeStub
	calls  int
	result llm.ExecutionResultV1
	err    error
}

func (runtime *executionRuntimeStub) PrepareExecutionV1(context.Context, llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
	runtime.calls++
	return runtime.result, runtime.err
}
func (runtime *executionRuntimeStub) AcquireBudgetV1(context.Context, llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	runtime.calls++
	return runtime.result, runtime.err
}
func (runtime *executionRuntimeStub) GenerateStepV1(context.Context, llm.GenerateRequestV1) (llm.ExecutionResultV1, error) {
	runtime.calls++
	return runtime.result, runtime.err
}
func (runtime *executionRuntimeStub) CompactStepV1(context.Context, llm.CompactRequestV1) (llm.ExecutionResultV1, error) {
	runtime.calls++
	return runtime.result, runtime.err
}
func (runtime *executionRuntimeStub) PollExecutionV1(context.Context, llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	runtime.calls++
	return runtime.result, runtime.err
}
func (runtime *executionRuntimeStub) CompleteExecutionV1(context.Context, llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	runtime.calls++
	return runtime.result, runtime.err
}

func TestExecutionActivitiesDispatchOnceAndValidateBeforeDispatch(t *testing.T) {
	reference := llm.ExecutionReferenceV1{RequestID: executionTestID, Context: validGenerateV1Request().Context}
	generate := validGenerateV1Request()
	tests := []struct {
		name, kind string
		call       func(*Activities, context.Context) (*llm.ExecutionResultV1, error)
	}{
		{"prepare", "generate", func(a *Activities, c context.Context) (*llm.ExecutionResultV1, error) {
			return a.PrepareExecutionV1(c, llm.PrepareExecutionV1{Generate: &generate})
		}},
		{"budget", "generate", func(a *Activities, c context.Context) (*llm.ExecutionResultV1, error) {
			return a.AcquireBudgetV1(c, reference)
		}},
		{"generate", "generate", func(a *Activities, c context.Context) (*llm.ExecutionResultV1, error) {
			return a.GenerateStepV1(c, generate)
		}},
		{"compact", "compact", func(a *Activities, c context.Context) (*llm.ExecutionResultV1, error) {
			return a.CompactStepV1(c, validCompactV1Request())
		}},
		{"poll", "generate", func(a *Activities, c context.Context) (*llm.ExecutionResultV1, error) {
			return a.PollExecutionV1(c, reference)
		}},
		{"complete", "generate", func(a *Activities, c context.Context) (*llm.ExecutionResultV1, error) {
			return a.CompleteExecutionV1(c, reference)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := &executionRuntimeStub{result: pendingExecution()}
			runtime.result.Kind = test.kind
			a := &Activities{V1Runtime: runtime}
			result, err := test.call(a, context.Background())
			if err != nil || result == nil || runtime.calls != 1 {
				t.Fatalf("result=%#v err=%v calls=%d", result, err, runtime.calls)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if result, err := test.call(a, ctx); result != nil || err == nil || runtime.calls != 1 {
				t.Fatal("canceled request dispatched")
			}
			a.PayloadLimits.MaxInlineBytes = 8
			if result, err := test.call(a, context.Background()); result != nil || err == nil || runtime.calls != 1 {
				t.Fatal("oversized request dispatched")
			}
			if result, err := test.call(nil, context.Background()); result != nil || err == nil {
				t.Fatal("nil runtime accepted")
			}
			if result, err := test.call(&Activities{V1Runtime: &v1RuntimeStub{}}, context.Background()); result != nil || err == nil {
				t.Fatal("one-shot runtime used as bounded runtime")
			}
		})
	}
}

func TestExecutionActivityRejectsMismatchedOrInvalidResult(t *testing.T) {
	for _, mutate := range []func(*llm.ExecutionResultV1){
		func(r *llm.ExecutionResultV1) { r.RequestID = "llmtw_req_758729e1-13b4-4784-886b-1dc96fed2710" },
		func(r *llm.ExecutionResultV1) { r.State = "secret-provider-diagnostic" },
		func(r *llm.ExecutionResultV1) { r.RetryAfterSeconds = 0 },
	} {
		result := pendingExecution()
		mutate(&result)
		runtime := &executionRuntimeStub{result: result}
		a := &Activities{V1Runtime: runtime}
		got, err := a.PollExecutionV1(context.Background(), llm.ExecutionReferenceV1{RequestID: executionTestID, Context: validGenerateV1Request().Context})
		if got != nil || err == nil || strings.Contains(err.Error(), "secret-provider-diagnostic") {
			t.Fatalf("result=%#v error=%v", got, err)
		}
	}
	runtime := &executionRuntimeStub{result: pendingExecution()}
	if got, err := (&Activities{V1Runtime: runtime}).CompactStepV1(context.Background(), validCompactV1Request()); got != nil || err == nil {
		t.Fatal("wrong request kind accepted")
	}
}

func TestExecutionActivitySanitizesRetryableError(t *testing.T) {
	runtime := &executionRuntimeStub{err: provider.NewError(provider.CodeStateUnavailable, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetrySameOperation, "secret-database-diagnostic")}
	got, err := (&Activities{V1Runtime: runtime}).GenerateStepV1(context.Background(), validGenerateV1Request())
	var appErr *temporal.ApplicationError
	if got != nil || !errors.As(err, &appErr) || appErr.NonRetryable() || strings.Contains(err.Error(), "secret-database-diagnostic") {
		t.Fatalf("result=%#v error=%v", got, err)
	}
}

func TestExecutionCompletedResponseCannotEscapeItsRequest(t *testing.T) {
	response := validMaterializedGenerateResponse("unrelated-operation")
	result := llm.ExecutionResultV1{RequestID: executionTestID, Kind: "generate", State: llm.ExecutionCompleted, Generate: &response}
	runtime := &executionRuntimeStub{result: result}
	a := &Activities{V1Runtime: runtime}
	if got, err := a.GenerateStepV1(context.Background(), validGenerateV1Request()); got != nil || err == nil {
		t.Fatal("another operation's completed response was returned")
	}
	response.OperationKey = validGenerateV1Request().OperationKey
	got, err := a.GenerateStepV1(context.Background(), validGenerateV1Request())
	if err != nil || got == nil || got.Generate == nil {
		t.Fatalf("valid completion failed: %v", err)
	}
	response.Output = []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: strings.Repeat("secret-provider-output", 1000)}}}}
	a.PayloadLimits.MaxInlineBytes = 2048
	if got, err := a.GenerateStepV1(context.Background(), validGenerateV1Request()); got != nil || err == nil || strings.Contains(err.Error(), "secret-provider-output") {
		t.Fatalf("oversized result escaped: %v", err)
	}
}
