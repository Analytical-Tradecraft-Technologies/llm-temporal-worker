// Package workflows orchestrates bounded activities. Provider I/O, authorization,
// storage, and budget accounting belong to activities and never run here.
package workflows

import (
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	RequestWorkflowName = "llm.request.execute.v1"
	BudgetWorkflowName  = "llm.budget.wait.v1"
	maxStepsPerRun      = 128
	maxBudgetBackoff    = 5 * time.Minute
)

// RegisterInternal registers implementation workflows, not client entry points.
// The worker registers these together with the public generation/compaction
// workflows. Runtime startup verifies the required execution capabilities.
func RegisterInternal(registry worker.WorkflowRegistry) {
	registry.RegisterWorkflowWithOptions(ExecuteRequest, workflow.RegisterOptions{Name: RequestWorkflowName})
	registry.RegisterWorkflowWithOptions(WaitForBudget, workflow.RegisterOptions{Name: BudgetWorkflowName})
}

// BudgetRequest includes the expected operation kind so a corrupt/misrouted
// activity response cannot be passed back as another operation's reservation.
type BudgetRequest struct {
	Reference llm.ExecutionReferenceV1 `json:"reference"`
	Kind      string                   `json:"kind"`
	// Waits counts budget denials already waited out, carried across
	// continue-as-new so the backoff does not reset to one second.
	Waits int `json:"waits,omitempty"`
}

// WaitForBudget acquires once per activity, then waits using a Temporal timer.
// Acquisition may find a cached response or an already dispatched attempt; such
// results go straight back to the request workflow without a second submission.
func WaitForBudget(ctx workflow.Context, input BudgetRequest) (llm.ExecutionResultV1, error) {
	if _, err := input.Reference.MarshalJSON(); err != nil || (input.Kind != "generate" && input.Kind != "compact") || input.Waits < 0 {
		return llm.ExecutionResultV1{}, invalidState()
	}
	ctx = executionContext(ctx)
	for steps := 0; ; steps++ {
		var result llm.ExecutionResultV1
		if err := workflow.ExecuteActivity(ctx, activity.AcquireBudgetActivityName, input.Reference).Get(ctx, &result); err != nil {
			return result, err
		}
		if err := checkResult(result, input.Kind, input.Reference.RequestID); err != nil {
			return llm.ExecutionResultV1{}, err
		}
		switch result.State {
		case llm.ExecutionBudgetRequired:
			// A valid acquire result must either acquire, wait, or recover saved work.
			return llm.ExecutionResultV1{}, invalidState()
		case llm.ExecutionBudgetWait:
			if err := workflow.Sleep(ctx, budgetBackoff(result, input.Waits)); err != nil {
				return llm.ExecutionResultV1{}, err
			}
			input.Waits++
		case llm.ExecutionCacheWait:
			if err := wait(ctx, result); err != nil {
				return llm.ExecutionResultV1{}, err
			}
		default:
			return result, nil
		}
		if continueRun(ctx, steps+1) {
			return llm.ExecutionResultV1{}, workflow.NewContinueAsNewError(ctx, BudgetWorkflowName, input)
		}
	}
}

// ExecuteRequest is shared by Generate and Compact. Repeated activity attempts
// operate on one durable request; outcome_unknown explicitly returns to budget
// acquisition, which creates an independent paid attempt. No provider ID or
// reservation is carried in workflow history.
func ExecuteRequest(ctx workflow.Context, input llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
	if _, err := input.MarshalJSON(); err != nil {
		return llm.ExecutionResultV1{}, invalidState()
	}
	ctx = executionContext(ctx)
	kind, caller := "compact", llm.RequestContext{}
	if input.Generate != nil {
		kind, caller = "generate", input.Generate.Context
	} else {
		caller = input.Compact.Context
	}
	var result llm.ExecutionResultV1
	if err := workflow.ExecuteActivity(ctx, activity.PrepareActivityName, input).Get(ctx, &result); err != nil {
		return result, err
	}
	if err := checkResult(result, kind, ""); err != nil {
		return llm.ExecutionResultV1{}, err
	}
	ref := llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: caller}
	for steps := 0; ; steps++ {
		switch result.State {
		case llm.ExecutionCompleted:
			if !responseMatches(input, result) {
				return llm.ExecutionResultV1{}, invalidState()
			}
			return result, nil
		case llm.ExecutionFailed:
			if !result.Retryable {
				return llm.ExecutionResultV1{}, temporal.NewNonRetryableApplicationError("model request failed", result.FailureCode, nil)
			}
			// Known retryable failures, like uncertain paid outcomes, require explicit
			// acquisition. Never let an activity retry silently buy a replacement.
			if err := workflow.Sleep(ctx, time.Second); err != nil {
				return llm.ExecutionResultV1{}, err
			}
			result.State = llm.ExecutionOutcomeUnknown
			fallthrough
		case llm.ExecutionBudgetRequired, llm.ExecutionBudgetWait, llm.ExecutionCacheWait, llm.ExecutionOutcomeUnknown:
			if result.State == llm.ExecutionBudgetWait || result.State == llm.ExecutionCacheWait {
				if err := wait(ctx, result); err != nil {
					return llm.ExecutionResultV1{}, err
				}
			}
			child := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON, WaitForCancellation: false})
			if err := workflow.ExecuteChildWorkflow(child, BudgetWorkflowName, BudgetRequest{Reference: ref, Kind: kind}).Get(ctx, &result); err != nil {
				return llm.ExecutionResultV1{}, err
			}
		case llm.ExecutionAcquired:
			name, request := activity.CompactActivityName, any(input.Compact)
			if input.Generate != nil {
				name, request = activity.GenerateActivityName, input.Generate
			}
			if err := workflow.ExecuteActivity(ctx, name, request).Get(ctx, &result); err != nil {
				return llm.ExecutionResultV1{}, err
			}
		case llm.ExecutionPending:
			if err := wait(ctx, result); err != nil {
				return llm.ExecutionResultV1{}, err
			}
			if err := workflow.ExecuteActivity(ctx, activity.PollActivityName, ref).Get(ctx, &result); err != nil {
				return llm.ExecutionResultV1{}, err
			}
		case llm.ExecutionProviderCompleted:
			if err := workflow.ExecuteActivity(ctx, activity.CompleteActivityName, ref).Get(ctx, &result); err != nil {
				return llm.ExecutionResultV1{}, err
			}
		default:
			return llm.ExecutionResultV1{}, invalidState()
		}
		if err := checkResult(result, kind, ref.RequestID); err != nil {
			return llm.ExecutionResultV1{}, err
		}
		if result.State != llm.ExecutionCompleted && result.State != llm.ExecutionFailed && continueRun(ctx, steps+1) {
			// Preparation reloads saved cloud progress. All child work and activities
			// are finished here, so Continue-As-New cannot abandon an in-flight call.
			return llm.ExecutionResultV1{}, workflow.NewContinueAsNewError(ctx, RequestWorkflowName, input)
		}
	}
}

func executionContext(ctx workflow.Context) workflow.Context {
	// Cancellation is not a service operation yet. Do not let a canceled caller
	// abandon a paid provider request or a cache fill another caller is awaiting.
	detached, _ := workflow.NewDisconnectedContext(ctx)
	return workflow.WithActivityOptions(detached, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute, ScheduleToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout: 30 * time.Second,
		RetryPolicy:      &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: time.Minute},
	})
}

// budgetBackoff waits at least the acquisition hint, doubling from one second
// per consecutive denial up to maxBudgetBackoff. A saturated long window would
// otherwise poll the acquire Activity once per second per waiting request.
func budgetBackoff(result llm.ExecutionResultV1, waits int) time.Duration {
	backoff := maxBudgetBackoff
	if waits >= 0 && waits < 16 {
		backoff = min(time.Second<<waits, maxBudgetBackoff)
	}
	return max(time.Duration(result.RetryAfterSeconds)*time.Second, backoff)
}

func wait(ctx workflow.Context, result llm.ExecutionResultV1) error {
	return workflow.Sleep(ctx, time.Duration(result.RetryAfterSeconds)*time.Second)
}
func continueRun(ctx workflow.Context, steps int) bool {
	return steps >= maxStepsPerRun || workflow.GetInfo(ctx).GetContinueAsNewSuggested()
}
func checkResult(result llm.ExecutionResultV1, kind, id string) error {
	if result.Validate() != nil || result.Kind != kind || (id != "" && result.RequestID != id) {
		return invalidState()
	}
	if result.Generate != nil && result.Generate.OperationID != result.RequestID {
		return invalidState()
	}
	if result.Compact != nil && result.Compact.OperationID != result.RequestID {
		return invalidState()
	}
	return nil
}
func responseMatches(input llm.PrepareExecutionV1, result llm.ExecutionResultV1) bool {
	if input.Generate != nil {
		return result.Generate != nil && result.Generate.OperationKey == input.Generate.OperationKey
	}
	return result.Compact != nil && result.Compact.OperationKey == input.Compact.OperationKey
}
func invalidState() error {
	return temporal.NewNonRetryableApplicationError("invalid request execution state", activity.ErrorTypeStateCorrupt, nil)
}
