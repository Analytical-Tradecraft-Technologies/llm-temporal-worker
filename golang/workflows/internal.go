// Package workflows orchestrates bounded activities. Provider I/O, authorization,
// storage, and budget accounting belong to activities and never run here.
package workflows

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
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
	// LangfuseExportWorkflowName runs the best-effort export after the request
	// workflow has returned its result (#1351).
	LangfuseExportWorkflowName = "llm.langfuse.export.v1"
	maxStepsPerRun             = 128
	maxBudgetBackoff           = 5 * time.Minute
	// Change ID prefixes for runs whose Preparation recorded no reference
	// capability (#1362). Each check appends its own sequence number; see
	// ExecuteRequest.
	requestCapabilityChange = "reference-capability-v1-"
	budgetCapabilityChange  = "budget-reference-capability-v1-"
	// asyncExportChange moves the Langfuse export into a child workflow.
	asyncExportChange = "langfuse-export-async-v1"
	// exportRetryChange gives the export workflow's Activity room to retry a
	// saturated in-flight limit (#1459).
	exportRetryChange = "langfuse-export-retry-v1"
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
	registry.RegisterWorkflowWithOptions(rawWorkflow(limits, ExportLangfuse), workflow.RegisterOptions{Name: LangfuseExportWorkflowName})
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
		// A request workflow started this child before reference steps carried a
		// capability (#1362). Acquisition would fail as for an unknown request, so
		// hand the request back; the parent obtains a capability and starts a new
		// child. A directly started workflow has no parent and keeps failing like
		// an unknown request.
		if input.Reference.Capability == "" && workflow.GetInfo(ctx).ParentWorkflowExecution != nil &&
			workflow.GetVersion(ctx, budgetCapabilityChange+strconv.Itoa(steps), workflow.DefaultVersion, 1) != workflow.DefaultVersion {
			// A parent from before this change starts another child at once; the
			// timer keeps that loop from spinning until it continues as new.
			if err := workflow.Sleep(ctx, time.Second); err != nil {
				return llm.ExecutionResultV1{}, err
			}
			return llm.ExecutionResultV1{RequestID: input.Reference.RequestID, Kind: input.Kind, State: llm.ExecutionBudgetRequired}, nil
		}
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
			result.Capability = ""
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
	// Preparation returns the internal capability reference steps present with
	// the request ID. Keep it in the reference only; no returned result carries it.
	ref := llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: caller, Capability: result.Capability}
	// A Preparation recorded before that capability existed returned none, and
	// reference steps now fail without one (#1362). Before each reference step,
	// prepare the same request again: that re-authorizes the caller and issues
	// the capability, and this run keeps the progress it already has. Old
	// histories replay reference steps that ran without a capability, so every
	// check gets its own change ID and only the first one past the old history
	// takes the new path. ctx carries the activity options for the preparation.
	checks := 0
	authorize := func(ctx workflow.Context) error {
		if ref.Capability != "" {
			return nil
		}
		checks++
		if workflow.GetVersion(ctx, requestCapabilityChange+strconv.Itoa(checks), workflow.DefaultVersion, 1) == workflow.DefaultVersion {
			return nil
		}
		var prepared llm.ExecutionResultV1
		if err := workflow.ExecuteActivity(ctx, activity.PrepareActivityName, input).Get(ctx, &prepared); err != nil {
			return err
		}
		if err := checkResult(prepared, kind, ref.RequestID); err != nil {
			return err
		}
		ref.Capability = prepared.Capability
		if ref.Capability == "" {
			// An Activity worker from before the capability prepared it again
			// during a rolling deploy. Its reference steps need none; back off so
			// a retry cannot spin.
			return workflow.Sleep(ctx, time.Second)
		}
		return nil
	}
	defer func() {
		returned.Capability = ""
	}()
	defer func() {
		if !workflow.IsContinueAsNewError(returnedErr) && workflow.GetVersion(ctx, "langfuse-export-v1", workflow.DefaultVersion, 1) != workflow.DefaultVersion {
			exportCtx := workflow.WithActivityOptions(ctx, exportActivityOptions)
			// The export is best effort, so its capability recovery is bounded
			// like the export rather than retried for a day.
			if err := authorize(exportCtx); err != nil {
				workflow.GetLogger(ctx).Warn("Langfuse export skipped", "request_id", ref.RequestID)
				return
			}
			// Hand the export to an abandoned child so a slow or unreachable
			// Langfuse no longer delays the caller's result (#1351). Waiting only
			// for the child to start costs one workflow task. Histories that
			// already scheduled the export Activity keep running it inline.
			if workflow.GetVersion(ctx, asyncExportChange, workflow.DefaultVersion, 1) != workflow.DefaultVersion {
				child := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
					ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON, WaitForCancellation: false,
					WorkflowExecutionTimeout: exportWorkflowTimeout,
				})
				if err := workflow.ExecuteChildWorkflow(child, LangfuseExportWorkflowName, ref).GetChildWorkflowExecution().Get(ctx, nil); err != nil {
					workflow.GetLogger(ctx).Warn("Langfuse export not started", "request_id", ref.RequestID)
				}
				return
			}
			if err := workflow.ExecuteActivity(exportCtx, activity.ExportLangfuseActivityName, ref).Get(exportCtx, nil); err != nil {
				// The export Activity counts each failed attempt in llmtw_langfuse_export_failures_total.
				workflow.GetLogger(ctx).Warn("Langfuse export failed", "request_id", ref.RequestID)
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
			if err := authorize(ctx); err != nil {
				return llm.ExecutionResultV1{}, err
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
			if err := authorize(ctx); err != nil {
				return llm.ExecutionResultV1{}, err
			}
			if err := workflow.ExecuteActivity(ctx, activity.PollActivityName, ref).Get(ctx, &result); err != nil {
				return llm.ExecutionResultV1{}, err
			}
		case llm.ExecutionProviderCompleted:
			if err := authorize(ctx); err != nil {
				return llm.ExecutionResultV1{}, err
			}
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

// exportActivityOptions bound the best-effort Langfuse export and the
// capability recovery before it. They never inherit the request workflow's
// unbounded retries.
var exportActivityOptions = workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second, ScheduleToCloseTimeout: 30 * time.Second, RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumAttempts: 3}}

// exportWorkflowActivityOptions let the export workflow, which nobody waits
// for, ride out a burst that saturates the in-flight limit: the Activity backs
// off between attempts without holding a slot. An open breaker and rejections
// still end the export at once.
var exportWorkflowActivityOptions = workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second, ScheduleToCloseTimeout: 2 * time.Minute, RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: 8 * time.Second, MaximumAttempts: 10}}

// exportWorkflowTimeout bounds an export workflow that no worker picks up, for
// example while a rolling deploy still runs workers that do not register it.
const exportWorkflowTimeout = 5 * time.Minute

// ExportLangfuse delivers one finished request's trace. The request workflow
// starts it as an abandoned child and does not wait for it, so an outage costs
// the caller nothing. A failed export is logged and the workflow still
// completes, reporting whether the export succeeded.
func ExportLangfuse(ctx workflow.Context, ref llm.ExecutionReferenceV1) (bool, error) {
	if _, err := ref.MarshalJSON(); err != nil {
		return false, invalidInput()
	}
	options := exportActivityOptions
	if workflow.GetVersion(ctx, exportRetryChange, workflow.DefaultVersion, 1) != workflow.DefaultVersion {
		options = exportWorkflowActivityOptions
	}
	ctx = workflow.WithActivityOptions(ctx, options)
	if err := workflow.ExecuteActivity(ctx, activity.ExportLangfuseActivityName, ref).Get(ctx, nil); err != nil {
		// The export Activity counts each failed attempt in llmtw_langfuse_export_failures_total.
		workflow.GetLogger(ctx).Warn("Langfuse export failed", "request_id", ref.RequestID)
		return false, nil
	}
	return true, nil
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
	// ProviderStatus is the upstream HTTP status, when the result carries one.
	ProviderStatus int `json:"provider_status,omitempty"`
}

func failedExecutionError(result llm.ExecutionResultV1) error {
	if result.ErrorCode == "" && result.Dispatch == "" && result.ProviderStatus == 0 {
		return temporal.NewNonRetryableApplicationError("model request failed", result.FailureCode, nil)
	}
	return temporal.NewNonRetryableApplicationError("model request failed", result.FailureCode, nil,
		ExecutionFailureDetails{ErrorCode: result.ErrorCode, Dispatch: result.Dispatch, ProviderStatus: result.ProviderStatus})
}
