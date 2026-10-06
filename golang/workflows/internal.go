// Package workflows orchestrates bounded activities. Provider I/O, authorization,
// storage, and budget accounting belong to activities and never run here.
package workflows

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
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
//
// Every workflow is registered with a raw payload argument and decoded inside
// workflow code (see rawWorkflow); limits is the worker's inline payload limit.
func RegisterInternal(registry worker.WorkflowRegistry, limits activity.PayloadLimits) {
	registry.RegisterWorkflowWithOptions(rawWorkflow(limits, ExecuteRequest), workflow.RegisterOptions{Name: RequestWorkflowName})
	registry.RegisterWorkflowWithOptions(rawWorkflow(limits, WaitForBudget), workflow.RegisterOptions{Name: BudgetWorkflowName})
}

// rawWorkflow adapts a typed workflow to the registered raw-payload signature.
// Letting the SDK decode a typed argument before workflow code runs would fail
// an invalid or oversize input as an untyped, retryable wrapper error whose
// message echoes decoder text (and therefore caller values) into history.
// Decoding here is a pure function of the recorded input, so it is
// deterministic on replay, and the wire format is unchanged: callers still
// send the typed v1 JSON record.
func rawWorkflow[T, R any](limits activity.PayloadLimits, run func(workflow.Context, T) (R, error)) func(workflow.Context, converter.RawValue) (R, error) {
	return func(ctx workflow.Context, raw converter.RawValue) (R, error) {
		input, ok := activity.DecodeBoundedPayload[T](limits, raw)
		if !ok {
			var zero R
			return zero, invalidInput()
		}
		return run(ctx, input)
	}
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

// UnmarshalJSON makes the budget workflow input a closed record like the v1
// requests: JSON null, unknown fields, an unknown kind, a
// negative wait count or an invalid reference are rejected at decode, before
// the workflow can schedule an acquisition.
func (request *BudgetRequest) UnmarshalJSON(data []byte) error {
	type record BudgetRequest
	var decoded *record
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil || decoded == nil || !BudgetRequest(*decoded).valid() {
		return errors.New("budget request is invalid")
	}
	*request = BudgetRequest(*decoded)
	return nil
}

func (request BudgetRequest) valid() bool {
	_, err := request.Reference.MarshalJSON()
	return err == nil && (request.Kind == "generate" || request.Kind == "compact") && request.Waits >= 0
}

// WaitForBudget acquires once per activity, then waits using a Temporal timer.
// Acquisition may find a cached response or an already dispatched attempt; such
// results go straight back to the request workflow without a second submission.
func WaitForBudget(ctx workflow.Context, input BudgetRequest) (llm.ExecutionResultV1, error) {
	if !input.valid() {
		return llm.ExecutionResultV1{}, invalidInput()
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
func ExecuteRequest(ctx workflow.Context, input llm.PrepareExecutionV1) (returned llm.ExecutionResultV1, returnedErr error) {
	if _, err := input.MarshalJSON(); err != nil {
		return llm.ExecutionResultV1{}, invalidInput()
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
	defer func() {
		if !workflow.IsContinueAsNewError(returnedErr) && workflow.GetVersion(ctx, "langfuse-export-v1", workflow.DefaultVersion, 1) != workflow.DefaultVersion {
			exportCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second, ScheduleToCloseTimeout: 30 * time.Second, RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumAttempts: 3}})
			if err := workflow.ExecuteActivity(exportCtx, activity.ExportLangfuseActivityName, ref).Get(exportCtx, nil); err != nil {
				workflow.GetLogger(ctx).Warn("Langfuse export failed", "request_id", ref.RequestID)
				workflow.GetMetricsHandler(ctx).Counter("langfuse_export_failures").Inc(1)
			}
		}
	}()
	for steps := 0; ; steps++ {
		switch result.State {
		case llm.ExecutionCompleted:
			if !responseMatches(input, result) {
				return llm.ExecutionResultV1{}, invalidState()
			}
			return result, nil
		case llm.ExecutionFailed:
			if !result.Retryable {
				return llm.ExecutionResultV1{}, failedExecutionError(result)
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
		// Configuration validation rejects provider timeouts and keepalive
		// intervals these bounds cannot honour.
		StartToCloseTimeout: config.ActivityStartToClose, ScheduleToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout: config.ActivityHeartbeatTimeout,
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

// invalidInput is the stable, content-free failure for a workflow input that
// cannot be decoded, exceeds the inline limit, or is not a valid v1 request.
func invalidInput() error {
	return temporal.NewNonRetryableApplicationError("workflow input is invalid or exceeds its limit", activity.ErrorTypeInvalidArgument, nil,
		activity.SafeErrorDetails{Code: "invalid_argument", Phase: "decode", Dispatch: "not_dispatched"})
}
func invalidState() error {
	return temporal.NewNonRetryableApplicationError("invalid request execution state", activity.ErrorTypeStateCorrupt, nil)
}

// ExecutionFailureDetails is the detail payload of a failed public workflow:
// the provider failure's stable error code and dispatch certainty, never the
// provider's message. Results saved before these fields existed carry none.
type ExecutionFailureDetails struct {
	ErrorCode string `json:"error_code,omitempty"`
	Dispatch  string `json:"dispatch,omitempty"`
}

func failedExecutionError(result llm.ExecutionResultV1) error {
	if result.ErrorCode == "" && result.Dispatch == "" {
		return temporal.NewNonRetryableApplicationError("model request failed", result.FailureCode, nil)
	}
	return temporal.NewNonRetryableApplicationError("model request failed", result.FailureCode, nil, ExecutionFailureDetails{ErrorCode: result.ErrorCode, Dispatch: result.Dispatch})
}
