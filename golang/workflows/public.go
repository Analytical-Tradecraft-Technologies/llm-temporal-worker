package workflows

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	GenerateWorkflowName = "llm.generate.workflow.v1"
	CompactWorkflowName  = "llm.compact.workflow.v1"
	QueryWorkflowName    = "llm.query.workflow.v1"
)

// Register installs the public and internal workflows. Each accepts the raw
// Temporal payload and applies limits and the strict v1 decode in workflow
// code, so an invalid request fails as a typed llm_invalid_argument.
func Register(registry worker.WorkflowRegistry, limits activity.PayloadLimits) {
	RegisterInternal(registry, limits)
	registry.RegisterWorkflowWithOptions(rawWorkflow(limits, Generate), workflow.RegisterOptions{Name: GenerateWorkflowName})
	registry.RegisterWorkflowWithOptions(rawWorkflow(limits, Compact), workflow.RegisterOptions{Name: CompactWorkflowName})
	registry.RegisterWorkflowWithOptions(rawWorkflow(limits, Query), workflow.RegisterOptions{Name: QueryWorkflowName})
}

// Query exposes all five control-plane queries through a workflow boundary.
// One activity attempt preserves the existing query retry contract; unlike paid
// Generate/Compact work, the query remains cancellable with its caller.
func Query(ctx workflow.Context, input llm.QueryRequestV1) (*llm.QueryResponseV1, error) {
	if _, err := input.MarshalJSON(); err != nil {
		return nil, invalidInput()
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    config.ActivityStartToClose,
		ScheduleToCloseTimeout: config.ActivityStartToClose,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	var result llm.QueryResponseV1
	if err := workflow.ExecuteActivity(ctx, activity.QueryActivityName, input).Get(ctx, &result); err != nil {
		return nil, err
	}
	if result.OperationKey != input.OperationKey || result.Kind != input.Kind {
		return nil, invalidState()
	}
	if _, err := result.MarshalJSON(); err != nil {
		return nil, invalidState()
	}
	return &result, nil
}
func childContext(ctx workflow.Context) workflow.Context {
	return workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON, WaitForCancellation: false})
}

// Generate compacts the inherited parent when preparation requires it, then
// delegates the LLM request to the internal workflow. Append and SettingsPatch
// remain untouched; application tool calls are returned to the caller.
func Generate(ctx workflow.Context, input llm.GenerateRequestV1) (*llm.GenerateResponseV1, error) {
	if _, err := input.MarshalJSON(); err != nil {
		return nil, invalidInput()
	}
	ctx = executionContext(ctx)
	original := input
	version := workflow.GetVersion(ctx, "public-generation-binding-v1", workflow.DefaultVersion, 1)
	var plan llm.GenerationPlanV1
	if err := workflow.ExecuteActivity(ctx, activity.PlanGenerationActivityName, input).Get(ctx, &plan); err != nil {
		return nil, err
	}
	if version >= 1 && plan.EffectiveParent != nil {
		input.Parent = plan.EffectiveParent
	} else if plan.CompactBeforeGenerate {
		if input.Parent == nil {
			return nil, invalidState()
		}
		compact := llm.CompactRequestV1{OperationKey: compactionKey(input), Context: input.Context, Parent: *input.Parent, Cache: &llm.CachePolicyV1{}}
		var result llm.CompactResponseV1
		if err := workflow.ExecuteChildWorkflow(childContext(ctx), CompactWorkflowName, compact).Get(ctx, &result); err != nil {
			return nil, err
		}
		if result.OperationKey != compact.OperationKey || result.Checkpoint.Handle == "" {
			return nil, invalidState()
		}
		parent := result.Checkpoint.Handle
		input.Parent = &parent
	}
	prepared := llm.PrepareExecutionV1{Generate: &input}
	if version >= 1 {
		prepared.OriginalGenerate = &llm.GenerationOriginV1{Parent: original.Parent}
	}
	result, err := executeChild(ctx, prepared)
	if err != nil {
		return nil, err
	}
	return result.Generate, nil
}

// Compact is independently callable and is also the main workflow's child.
// It shares caching, budget acquisition, polling and accounting with Generate.
func Compact(ctx workflow.Context, input llm.CompactRequestV1) (*llm.CompactResponseV1, error) {
	if _, err := input.MarshalJSON(); err != nil {
		return nil, invalidInput()
	}
	result, err := executeChild(executionContext(ctx), llm.PrepareExecutionV1{Compact: &input})
	if err != nil {
		return nil, err
	}
	return result.Compact, nil
}
func executeChild(ctx workflow.Context, input llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
	var result llm.ExecutionResultV1
	if err := workflow.ExecuteChildWorkflow(childContext(ctx), RequestWorkflowName, input).Get(ctx, &result); err != nil {
		return result, err
	}
	kind := "compact"
	if input.Generate != nil {
		kind = "generate"
	}
	if result.State != llm.ExecutionCompleted || checkResult(result, kind, "") != nil || !responseMatches(input, result) {
		return llm.ExecutionResultV1{}, invalidState()
	}
	return result, nil
}
func compactionKey(input llm.GenerateRequestV1) string {
	data, _ := json.Marshal(input)
	digest := sha256.Sum256(data)
	return "llmtw_compact_" + hex.EncodeToString(digest[:])
}
