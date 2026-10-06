package workflows

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestPublicGenerateOptionalCompaction(t *testing.T) {
	for _, compact := range []bool{false, true} {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		var request llm.GenerateRequestV1
		workflowJSON(t, "generate-root", &request)
		parent := llm.CheckpointHandle("ckp_v1.original")
		request.Parent = &parent
		var generated llm.GenerateResponseV1
		workflowJSON(t, "generate-response", &generated)
		generated.OperationKey = request.OperationKey
		generated.OperationID = testRequestID
		var summary llm.CompactResponseV1
		workflowJSON(t, "compact-response", &summary)
		summary.OperationKey = compactionKey(request)
		summary.OperationID = testRequestID
		planned, compactions, generations := 0, 0, 0
		env.RegisterWorkflowWithOptions(Generate, workflow.RegisterOptions{Name: GenerateWorkflowName})
		env.RegisterWorkflowWithOptions(Compact, workflow.RegisterOptions{Name: CompactWorkflowName})
		env.RegisterActivityWithOptions(func(_ context.Context, input llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
			planned++
			if !reflect.DeepEqual(input, request) {
				t.Error("changed planning input")
			}
			return llm.GenerationPlanV1{CompactBeforeGenerate: compact}, nil
		}, sdkactivity.RegisterOptions{Name: activity.PlanGenerationActivityName})
		env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input llm.PrepareExecutionV1) (*llm.ExecutionResultV1, error) {
			if input.Compact != nil {
				compactions++
				if input.Compact.Parent != parent || input.Compact.OperationKey != summary.OperationKey || input.Compact.Cache == nil || input.Compact.Cache.MaxAgeSeconds != 0 {
					t.Error("incorrect compact request")
				}
				return &llm.ExecutionResultV1{RequestID: testRequestID, Kind: "compact", State: llm.ExecutionCompleted, Compact: &summary}, nil
			}
			generations++
			expected := request
			if compact {
				next := summary.Checkpoint.Handle
				expected.Parent = &next
			}
			if !reflect.DeepEqual(*input.Generate, expected) {
				t.Error("main changed append/settings or did not use compacted parent")
			}
			// No application tool execution happens at this boundary; return the model
			// response verbatim. The internal workflow validates checkpoint/output.
			return &llm.ExecutionResultV1{RequestID: testRequestID, Kind: "generate", State: llm.ExecutionCompleted, Generate: &generated}, nil
		}, workflow.RegisterOptions{Name: RequestWorkflowName})
		env.ExecuteWorkflow(GenerateWorkflowName, request)
		if err := env.GetWorkflowError(); err != nil {
			t.Fatal(err)
		}
		var got llm.GenerateResponseV1
		if err := env.GetWorkflowResult(&got); err != nil {
			t.Fatal(err)
		}
		expectedCompactions := 0
		if compact {
			expectedCompactions = 1
		}
		if !reflect.DeepEqual(got, generated) || planned != 1 || generations != 1 || compactions != expectedCompactions {
			t.Fatal("unexpected workflow orchestration")
		}
	}
}

func TestPublicWorkflowsUseActualInternalStateMachine(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		submit := activity.GenerateActivityName
		if kind == "compact" {
			submit = activity.CompactActivityName
		}
		f := workflowTest(t, kind, step(activity.PrepareActivityName, llm.ExecutionBudgetRequired), step(activity.AcquireBudgetActivityName, llm.ExecutionAcquired), step(submit, llm.ExecutionPending), step(activity.PollActivityName, llm.ExecutionProviderCompleted), step(activity.CompleteActivityName, llm.ExecutionCompleted))
		// Registered as the worker registers them: all four workflows receive the
		// raw payload, and the caller sends the unchanged v1 wire record.
		f.env.RegisterWorkflowWithOptions(rawWorkflow(activity.PayloadLimits{}, Generate), workflow.RegisterOptions{Name: GenerateWorkflowName})
		f.env.RegisterWorkflowWithOptions(rawWorkflow(activity.PayloadLimits{}, Compact), workflow.RegisterOptions{Name: CompactWorkflowName})
		f.env.RegisterActivityWithOptions(func(context.Context, llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
			return llm.GenerationPlanV1{}, nil
		}, sdkactivity.RegisterOptions{Name: activity.PlanGenerationActivityName})
		f.env.RegisterDelayedCallback(func() { f.env.CancelWorkflow() }, time.Second)
		name, input := GenerateWorkflowName, wireFixture(t, "generate-root")
		if kind == "compact" {
			name, input = CompactWorkflowName, wireFixture(t, "compact-request")
		}
		f.env.ExecuteWorkflow(name, input)
		if err := f.env.GetWorkflowError(); err != nil {
			t.Fatal(err)
		}
		if len(f.steps) != 0 {
			t.Fatal("public caller abandoned unfinished work")
		}
	}
}

func TestPublicGenerateDoesNotGenerateAfterCompactionFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var request llm.GenerateRequestV1
	workflowJSON(t, "generate-root", &request)
	parent := llm.CheckpointHandle("ckp_v1.original")
	request.Parent = &parent
	env.RegisterWorkflowWithOptions(Generate, workflow.RegisterOptions{Name: GenerateWorkflowName})
	env.RegisterActivityWithOptions(func(context.Context, llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
		return llm.GenerationPlanV1{CompactBeforeGenerate: true}, nil
	}, sdkactivity.RegisterOptions{Name: activity.PlanGenerationActivityName})
	env.RegisterWorkflowWithOptions(func(workflow.Context, llm.CompactRequestV1) (*llm.CompactResponseV1, error) {
		return nil, temporal.NewNonRetryableApplicationError("summary failed", "incomplete_response", nil)
	}, workflow.RegisterOptions{Name: CompactWorkflowName})
	env.ExecuteWorkflow(GenerateWorkflowName, request)
	if env.GetWorkflowError() == nil {
		t.Fatal("compaction failure accepted")
	}
}
