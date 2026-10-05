package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const testRequestID = "llmtw_req_49c0cb63-15ed-4da7-aa74-17a7d711c9a5"

type workflowStep struct {
	name  string
	state llm.ExecutionStateV1
	err   error
	alter func(*llm.ExecutionResultV1)
}
type workflowFixture struct {
	env       *testsuite.TestWorkflowEnvironment
	input     llm.PrepareExecutionV1
	completed llm.ExecutionResultV1
	mu        sync.Mutex
	steps     []workflowStep
	calls     []string
}

func workflowJSON(t *testing.T, name string, target any) {
	t.Helper()
	data, err := os.ReadFile("../llm/testdata/v1/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatal(err)
	}
}
func workflowTest(t *testing.T, kind string, steps ...workflowStep) *workflowFixture {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	f := &workflowFixture{env: suite.NewTestWorkflowEnvironment(), steps: steps}
	f.completed = llm.ExecutionResultV1{RequestID: testRequestID, Kind: kind, State: llm.ExecutionCompleted}
	if kind == "generate" {
		f.input.Generate = new(llm.GenerateRequestV1)
		workflowJSON(t, "generate-root", f.input.Generate)
		f.completed.Generate = new(llm.GenerateResponseV1)
		workflowJSON(t, "generate-response", f.completed.Generate)
		f.completed.Generate.OperationID, f.completed.Generate.OperationKey = testRequestID, f.input.Generate.OperationKey
	} else {
		f.input.Compact = new(llm.CompactRequestV1)
		workflowJSON(t, "compact-request", f.input.Compact)
		f.completed.Compact = new(llm.CompactResponseV1)
		workflowJSON(t, "compact-response", f.completed.Compact)
		f.completed.Compact.OperationID, f.completed.Compact.OperationKey = testRequestID, f.input.Compact.OperationKey
	}
	RegisterInternal(f.env)
	register := func(name string, fn any) {
		f.env.RegisterActivityWithOptions(fn, sdkactivity.RegisterOptions{Name: name})
	}
	register(activity.PrepareActivityName, func(_ context.Context, input llm.PrepareExecutionV1) (*llm.ExecutionResultV1, error) {
		if !reflect.DeepEqual(input, f.input) {
			return nil, invalidState()
		}
		return f.next(activity.PrepareActivityName)
	})
	register(activity.GenerateActivityName, func(_ context.Context, input llm.GenerateRequestV1) (*llm.ExecutionResultV1, error) {
		if !reflect.DeepEqual(input, *f.input.Generate) {
			return nil, invalidState()
		}
		return f.next(activity.GenerateActivityName)
	})
	register(activity.CompactActivityName, func(_ context.Context, input llm.CompactRequestV1) (*llm.ExecutionResultV1, error) {
		if !reflect.DeepEqual(input, *f.input.Compact) {
			return nil, invalidState()
		}
		return f.next(activity.CompactActivityName)
	})
	for _, name := range []string{activity.AcquireBudgetActivityName, activity.PollActivityName, activity.CompleteActivityName} {
		register(name, func(_ context.Context, ref llm.ExecutionReferenceV1) (*llm.ExecutionResultV1, error) {
			if ref.RequestID != testRequestID || !reflect.DeepEqual(ref.Context, f.caller()) {
				return nil, invalidState()
			}
			return f.next(name)
		})
	}
	return f
}
func (f *workflowFixture) caller() llm.RequestContext {
	if f.input.Generate != nil {
		return f.input.Generate.Context
	}
	return f.input.Compact.Context
}
func (f *workflowFixture) next(name string) (*llm.ExecutionResultV1, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
	if len(f.steps) == 0 || f.steps[0].name != name {
		return nil, invalidState()
	}
	step := f.steps[0]
	f.steps = f.steps[1:]
	if step.err != nil {
		return nil, step.err
	}
	result := llm.ExecutionResultV1{RequestID: testRequestID, Kind: f.completed.Kind, State: step.state}
	switch step.state {
	case llm.ExecutionCompleted:
		result = f.completed
	case llm.ExecutionBudgetWait, llm.ExecutionCacheWait, llm.ExecutionPending:
		result.RetryAfterSeconds = 3
	case llm.ExecutionFailed:
		result.FailureCode = "provider_error"
	}
	if step.alter != nil {
		step.alter(&result)
	}
	return &result, nil
}
func (f *workflowFixture) run(t *testing.T) {
	t.Helper()
	f.env.ExecuteWorkflow(RequestWorkflowName, f.input)
	if err := f.env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result llm.ExecutionResultV1
	if err := f.env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result, f.completed) || len(f.steps) != 0 {
		t.Fatalf("unexpected result or remaining steps: %#v, %d", result, len(f.steps))
	}
}
func step(name string, state llm.ExecutionStateV1) workflowStep {
	return workflowStep{name: name, state: state}
}

func TestExecuteRequestRoutesBoundedSteps(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		for _, mode := range []string{"cached", "sync", "async", "budget-wait", "cache-wait", "unknown", "retryable"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				submit := activity.GenerateActivityName
				if kind == "compact" {
					submit = activity.CompactActivityName
				}
				steps := []workflowStep{step(activity.PrepareActivityName, llm.ExecutionBudgetRequired)}
				switch mode {
				case "cached":
					steps = []workflowStep{step(activity.PrepareActivityName, llm.ExecutionCompleted)}
				case "cache-wait":
					steps = []workflowStep{step(activity.PrepareActivityName, llm.ExecutionCacheWait), step(activity.AcquireBudgetActivityName, llm.ExecutionCacheWait), step(activity.AcquireBudgetActivityName, llm.ExecutionCompleted)}
				default:
					if mode == "budget-wait" {
						steps = append(steps, step(activity.AcquireBudgetActivityName, llm.ExecutionBudgetWait), step(activity.AcquireBudgetActivityName, llm.ExecutionBudgetWait))
					}
					steps = append(steps, step(activity.AcquireBudgetActivityName, llm.ExecutionAcquired))
					if mode == "unknown" {
						steps = append(steps, step(submit, llm.ExecutionPending), step(activity.PollActivityName, llm.ExecutionOutcomeUnknown), step(activity.AcquireBudgetActivityName, llm.ExecutionAcquired))
					}
					if mode == "retryable" {
						retry := step(submit, llm.ExecutionFailed)
						retry.alter = func(v *llm.ExecutionResultV1) { v.Retryable = true }
						steps = append(steps, retry, step(activity.AcquireBudgetActivityName, llm.ExecutionAcquired))
					}
					if mode == "async" {
						steps = append(steps, step(submit, llm.ExecutionPending), step(activity.PollActivityName, llm.ExecutionPending), step(activity.PollActivityName, llm.ExecutionProviderCompleted))
					} else {
						steps = append(steps, step(submit, llm.ExecutionProviderCompleted))
					}
					steps = append(steps, step(activity.CompleteActivityName, llm.ExecutionCompleted))
				}
				f := workflowTest(t, kind, steps...)
				start := f.env.Now()
				f.run(t)
				if (mode == "budget-wait" || mode == "async" || mode == "cache-wait") && f.env.Now().Sub(start) < 6*time.Second {
					t.Fatal("waits did not use timers")
				}
			})
		}
	}
}
func TestExecuteRequestRecoversAlreadyDispatchedWork(t *testing.T) {
	f := workflowTest(t, "generate", step(activity.PrepareActivityName, llm.ExecutionPending), step(activity.PollActivityName, llm.ExecutionProviderCompleted), step(activity.CompleteActivityName, llm.ExecutionCompleted))
	f.run(t)
	f = workflowTest(t, "generate", step(activity.PrepareActivityName, llm.ExecutionProviderCompleted), step(activity.CompleteActivityName, llm.ExecutionCompleted))
	f.run(t)
}
func TestExecuteRequestRetriesOnlySameActivityOnStorageFailure(t *testing.T) {
	f := workflowTest(t, "generate", workflowStep{name: activity.PrepareActivityName, err: temporal.NewApplicationError("storage temporarily unavailable", activity.ErrorTypeProviderTransient)}, step(activity.PrepareActivityName, llm.ExecutionCompleted))
	f.run(t)
}
func TestExecuteRequestRejectsFailureAndIdentityMismatch(t *testing.T) {
	for _, name := range []string{"failure", "id", "kind", "operation-key", "activity-error"} {
		t.Run(name, func(t *testing.T) {
			bad := step(activity.PollActivityName, llm.ExecutionFailed)
			switch name {
			case "id":
				bad.state = llm.ExecutionProviderCompleted
				bad.alter = func(v *llm.ExecutionResultV1) { v.RequestID = "llmtw_req_49c0cb63-15ed-4da7-aa74-17a7d711c9a6" }
			case "kind":
				bad.state = llm.ExecutionProviderCompleted
				bad.alter = func(v *llm.ExecutionResultV1) { v.Kind = "compact" }
			case "operation-key":
				bad.state = llm.ExecutionCompleted
				bad.alter = func(v *llm.ExecutionResultV1) { copy := *v.Generate; copy.OperationKey = "wrong"; v.Generate = &copy }
			case "activity-error":
				bad.err = temporal.NewNonRetryableApplicationError("denied", activity.ErrorTypeAuthentication, nil)
			}
			f := workflowTest(t, "generate", step(activity.PrepareActivityName, llm.ExecutionPending), bad)
			f.env.ExecuteWorkflow(RequestWorkflowName, f.input)
			if f.env.GetWorkflowError() == nil || len(f.calls) != 2 {
				t.Fatal("failure was retried or accepted")
			}
		})
	}
}
func TestExecuteRequestCancellationDoesNotAbandonPaidWork(t *testing.T) {
	f := workflowTest(t, "generate", step(activity.PrepareActivityName, llm.ExecutionPending), step(activity.PollActivityName, llm.ExecutionProviderCompleted), step(activity.CompleteActivityName, llm.ExecutionCompleted))
	f.env.RegisterDelayedCallback(func() { f.env.CancelWorkflow() }, time.Second)
	f.run(t)
}
func TestInternalWorkflowsContinueAsNew(t *testing.T) {
	for _, budget := range []bool{false, true} {
		f := workflowTest(t, "generate", step(activity.PrepareActivityName, llm.ExecutionPending), step(activity.PollActivityName, llm.ExecutionPending))
		f.env.SetContinueAsNewSuggested(true)
		name, input := RequestWorkflowName, any(f.input)
		if budget {
			f.steps = []workflowStep{step(activity.AcquireBudgetActivityName, llm.ExecutionBudgetWait)}
			name = BudgetWorkflowName
			input = BudgetRequest{Reference: llm.ExecutionReferenceV1{RequestID: testRequestID, Context: f.caller()}, Kind: "generate"}
		}
		f.env.ExecuteWorkflow(name, input)
		var continued *workflow.ContinueAsNewError
		if !errors.As(f.env.GetWorkflowError(), &continued) || continued.WorkflowType.Name != name {
			t.Fatalf("not continued: %v", f.env.GetWorkflowError())
		}
	}
}
func TestBudgetWorkflowReturnsExistingWork(t *testing.T) {
	for _, state := range []llm.ExecutionStateV1{llm.ExecutionAcquired, llm.ExecutionPending, llm.ExecutionProviderCompleted, llm.ExecutionCompleted, llm.ExecutionOutcomeUnknown, llm.ExecutionFailed} {
		f := workflowTest(t, "generate", step(activity.AcquireBudgetActivityName, state))
		f.env.ExecuteWorkflow(BudgetWorkflowName, BudgetRequest{Reference: llm.ExecutionReferenceV1{RequestID: testRequestID, Context: f.caller()}, Kind: "generate"})
		if err := f.env.GetWorkflowError(); err != nil {
			t.Fatal(err)
		}
		var result llm.ExecutionResultV1
		if err := f.env.GetWorkflowResult(&result); err != nil || result.State != state {
			t.Fatalf("wrong budget result: %v", err)
		}
	}
}
func TestBudgetWorkflowRejectsNonProgressingAcquisition(t *testing.T) {
	f := workflowTest(t, "generate", step(activity.AcquireBudgetActivityName, llm.ExecutionBudgetRequired))
	f.env.ExecuteWorkflow(BudgetWorkflowName, BudgetRequest{Reference: llm.ExecutionReferenceV1{RequestID: testRequestID, Context: f.caller()}, Kind: "generate"})
	if f.env.GetWorkflowError() == nil {
		t.Fatal("nonprogressing acquisition accepted")
	}
}

func TestBudgetWorkflowBoundsHistoryWithoutServerHint(t *testing.T) {
	steps := make([]workflowStep, maxStepsPerRun)
	for i := range steps {
		steps[i] = step(activity.AcquireBudgetActivityName, llm.ExecutionBudgetWait)
	}
	f := workflowTest(t, "generate", steps...)
	f.env.ExecuteWorkflow(BudgetWorkflowName, BudgetRequest{Reference: llm.ExecutionReferenceV1{RequestID: testRequestID, Context: f.caller()}, Kind: "generate"})
	var continued *workflow.ContinueAsNewError
	if !errors.As(f.env.GetWorkflowError(), &continued) || len(f.calls) != maxStepsPerRun {
		t.Fatal("unbounded budget history")
	}
}

func TestRequestWorkflowContinuesOnlyBetweenCompletedSteps(t *testing.T) {
	f := workflowTest(t, "generate", step(activity.PrepareActivityName, llm.ExecutionBudgetRequired), step(activity.AcquireBudgetActivityName, llm.ExecutionAcquired))
	f.env.SetContinueAsNewSuggested(true)
	f.env.ExecuteWorkflow(RequestWorkflowName, f.input)
	var continued *workflow.ContinueAsNewError
	if !errors.As(f.env.GetWorkflowError(), &continued) || len(f.steps) != 0 {
		t.Fatal("did not finish budget child before continuing")
	}
	// The next run prepares the same public request and discovers its acquired
	// attempt. It must submit once, not create another budget reservation.
	f = workflowTest(t, "generate", step(activity.PrepareActivityName, llm.ExecutionAcquired), step(activity.GenerateActivityName, llm.ExecutionProviderCompleted), step(activity.CompleteActivityName, llm.ExecutionCompleted))
	f.run(t)
}

func TestBudgetWorkflowBacksOffConsecutiveDenials(t *testing.T) {
	steps := make([]workflowStep, 0, 12)
	for i := 0; i < 11; i++ {
		steps = append(steps, step(activity.AcquireBudgetActivityName, llm.ExecutionBudgetWait))
	}
	steps = append(steps, step(activity.AcquireBudgetActivityName, llm.ExecutionAcquired))
	f := workflowTest(t, "generate", steps...)
	var timers []time.Duration
	f.env.SetOnTimerScheduledListener(func(_ string, duration time.Duration) {
		timers = append(timers, duration)
	})
	f.env.ExecuteWorkflow(BudgetWorkflowName, BudgetRequest{Reference: llm.ExecutionReferenceV1{RequestID: testRequestID, Context: f.caller()}, Kind: "generate"})
	if err := f.env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	// The fixture hints three seconds, which wins over the first two backoffs.
	want := []time.Duration{3 * time.Second, 3 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, 64 * time.Second, 128 * time.Second, 256 * time.Second, maxBudgetBackoff, maxBudgetBackoff}
	if !reflect.DeepEqual(timers, want) {
		t.Fatalf("budget wait timers = %v, want %v", timers, want)
	}
}

func TestBudgetBackoffHonoursLongerHintAndCarriedWaits(t *testing.T) {
	if got := budgetBackoff(llm.ExecutionResultV1{RetryAfterSeconds: 900}, 0); got != 15*time.Minute {
		t.Fatalf("hinted backoff = %v", got)
	}
	if got := budgetBackoff(llm.ExecutionResultV1{RetryAfterSeconds: 1}, 3); got != 8*time.Second {
		t.Fatalf("carried backoff = %v", got)
	}
	if got := budgetBackoff(llm.ExecutionResultV1{}, 1000); got != maxBudgetBackoff {
		t.Fatalf("capped backoff = %v", got)
	}
}
