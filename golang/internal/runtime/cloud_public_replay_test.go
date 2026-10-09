package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type publicReplayRuntime struct {
	activity.UnconfiguredV1Runtime
	*CloudExecutionRuntime
}

func publicReplayEnvironment(f *boundedCloudFixture) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	activities := &activity.Activities{V1Runtime: publicReplayRuntime{CloudExecutionRuntime: f.runtime}}
	env.RegisterWorkflowWithOptions(workflows.Generate, workflow.RegisterOptions{Name: workflows.GenerateWorkflowName})
	env.RegisterWorkflowWithOptions(workflows.Compact, workflow.RegisterOptions{Name: workflows.CompactWorkflowName})
	env.RegisterWorkflowWithOptions(workflows.ExecuteRequest, workflow.RegisterOptions{Name: workflows.RequestWorkflowName})
	env.RegisterWorkflowWithOptions(workflows.WaitForBudget, workflow.RegisterOptions{Name: workflows.BudgetWorkflowName})
	env.RegisterActivityWithOptions(activities.PlanGenerationV1, sdkactivity.RegisterOptions{Name: activity.PlanGenerationActivityName})
	env.RegisterActivityWithOptions(activities.PrepareExecutionV1, sdkactivity.RegisterOptions{Name: activity.PrepareActivityName})
	env.RegisterActivityWithOptions(activities.AcquireBudgetV1, sdkactivity.RegisterOptions{Name: activity.AcquireBudgetActivityName})
	env.RegisterActivityWithOptions(activities.GenerateStepV1, sdkactivity.RegisterOptions{Name: activity.GenerateActivityName})
	env.RegisterActivityWithOptions(activities.CompactStepV1, sdkactivity.RegisterOptions{Name: activity.CompactActivityName})
	env.RegisterActivityWithOptions(activities.PollExecutionV1, sdkactivity.RegisterOptions{Name: activity.PollActivityName})
	env.RegisterActivityWithOptions(activities.CompleteExecutionV1, sdkactivity.RegisterOptions{Name: activity.CompleteActivityName})
	env.RegisterActivityWithOptions(func(context.Context, llm.ExecutionReferenceV1) error { return nil }, sdkactivity.RegisterOptions{Name: activity.ExportLangfuseActivityName})
	return env
}

func TestPublicGenerateReplaysCompletedOperationWithoutCurrentParentOrRoutes(t *testing.T) {
	for _, changed := range []string{"expired-parent", "removed-routes"} {
		t.Run(changed, func(t *testing.T) {
			f := boundedCloud(t, false)
			policy := json.RawMessage(`{"recent_turns":0}`)
			f.request.SettingsPatch.CompactionPolicy.Set = &policy
			root := f.finish(t)
			f.now = f.now.Add(time.Minute)
			f.request = llm.GenerateRequestV1{OperationKey: "public-replay-child", Context: f.request.Context, Parent: &root.Generate.Checkpoint.Handle, Append: []llm.Item{preparationMessage("follow-up")}}
			original := f.finish(t)
			before := f.submits.Load()
			if changed == "expired-parent" {
				f.now = f.now.Add(24*time.Hour - 30*time.Second)
			} else {
				f.runtime.execution.admission.planning.providers.catalog.Models = map[string]routing.Model{}
			}
			// The retained result is valid independently of the public preflight.
			direct, err := f.runtime.PrepareExecutionV1(context.Background(), llm.PrepareExecutionV1{Generate: &f.request})
			boundedState(t, direct, err, llm.ExecutionCompleted)
			env := publicReplayEnvironment(f)
			env.ExecuteWorkflow(workflows.GenerateWorkflowName, f.request)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("public replay failed despite a retained completed operation: %v", err)
			}
			var replay llm.GenerateResponseV1
			if err := env.GetWorkflowResult(&replay); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(replay, *original.Generate) || f.submits.Load() != before {
				t.Fatal("public replay changed the saved result or submitted new provider work")
			}
		})
	}
}

func TestPublicGenerateResumesPreparedOperationAfterParentExpiry(t *testing.T) {
	f := boundedCloud(t, false)
	root := f.finish(t)
	f.now = f.now.Add(time.Minute)
	f.request = llm.GenerateRequestV1{OperationKey: "prepared-public-child", Context: f.request.Context, Parent: &root.Generate.Checkpoint.Handle, Append: []llm.Item{preparationMessage("prepared follow-up")}}
	prepared, err := f.runtime.PrepareExecutionV1(context.Background(), llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, prepared, err, llm.ExecutionBudgetRequired)
	before := f.submits.Load()
	f.now = f.now.Add(24 * time.Hour)
	f.restart(t)
	env := publicReplayEnvironment(f)
	env.ExecuteWorkflow(workflows.GenerateWorkflowName, f.request)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("prepared public operation could not resume after parent expiry: %v", err)
	}
	var result llm.GenerateResponseV1
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.OperationKey != f.request.OperationKey || f.submits.Load() != before+1 {
		t.Fatal("prepared public operation did not complete exactly one provider submission")
	}
}

func TestPublicGenerateReplayPreservesInputBindingAndAuthorization(t *testing.T) {
	for _, changed := range []string{"append", "scope"} {
		t.Run(changed, func(t *testing.T) {
			f := boundedCloud(t, false)
			f.finish(t)
			before := f.submits.Load()
			if changed == "append" {
				f.request.Append = []llm.Item{preparationMessage("different input")}
			} else {
				f.request.Context.Project = "unauthorized"
			}
			env := publicReplayEnvironment(f)
			env.ExecuteWorkflow(workflows.GenerateWorkflowName, f.request)
			if env.GetWorkflowError() == nil || f.submits.Load() != before {
				t.Fatal("changed public replay was accepted or submitted provider work")
			}
		})
	}
}

func TestPublicGenerateReplaysAutomaticCompactionAfterCheckpointExpiry(t *testing.T) {
	f := compactionTriggerFixture(t, `{"recent_turns":1}`)
	for i := 1; i <= 3; i++ {
		f.turn(t, "history-"+string(rune('0'+i)), strings.Repeat("history ", 3<<10))
	}
	f.request.OperationKey = "compacted-public-generate"
	f.request.Append = []llm.Item{preparationMessage("fourth")}
	original := f.request
	before := f.submits.Load()
	invoke := f.adapter.invoke
	calls := 0
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		calls++
		// The Generate result is retained slightly longer than its Compact
		// checkpoint, as it would be after a separate provider operation.
		if calls == 2 {
			f.now = f.now.Add(time.Second)
		}
		return invoke(ctx, call, o)
	}
	env := publicReplayEnvironment(f)
	env.ExecuteWorkflow(workflows.GenerateWorkflowName, original)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("original compacted public Generate: %v", err)
	}
	var want llm.GenerateResponseV1
	if err := env.GetWorkflowResult(&want); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || f.submits.Load() != before+2 {
		t.Fatal("fixture did not execute one Compact and one Generate")
	}
	plan, err := f.runtime.PlanGenerationV1(context.Background(), original)
	if err != nil || !plan.CompactBeforeGenerate || plan.EffectiveParent == nil {
		t.Fatalf("missing durable effective-parent binding: %+v %v", plan, err)
	}
	f.now = f.now.Add(24*time.Hour - 500*time.Millisecond)
	f.restart(t)
	if _, err := f.runtime.preparation.replay.Compact(context.Background(), llm.CompactRequestV1{OperationKey: "expired-check", Context: original.Context, Parent: *plan.EffectiveParent}); err == nil {
		t.Fatal("fixture Compact checkpoint is still live")
	}
	f.runtime.execution.admission.planning.providers.catalog.Models = map[string]routing.Model{}
	env = publicReplayEnvironment(f)
	env.ExecuteWorkflow(workflows.GenerateWorkflowName, original)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("compacted public replay after checkpoint expiry and route removal: %v", err)
	}
	var got llm.GenerateResponseV1
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) || f.submits.Load() != before+2 {
		t.Fatal("compacted replay changed result or submitted work")
	}
	changed := original
	changed.Parent = original.Parent
	if _, err := f.runtime.PrepareExecutionV1(context.Background(), llm.PrepareExecutionV1{Generate: &changed, OriginalGenerate: &llm.GenerationOriginV1{Parent: original.Parent}}); err == nil {
		t.Fatal("effective parent substitution accepted")
	}
}
