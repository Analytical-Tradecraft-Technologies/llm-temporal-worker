package workflows

import (
	"context"
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"testing"
)

func TestLangfuseFailureIsBoundedAndPreservesOperation(t *testing.T) {
	f := workflowTest(t, "generate", workflowStep{name: activity.PrepareActivityName, state: llm.ExecutionCompleted})
	calls := 0
	f.env.RegisterActivityWithOptions(func(_ context.Context, ref llm.ExecutionReferenceV1) error {
		calls++
		if ref.Context.Actor != f.caller().Actor {
			t.Error("lost caller identity")
		}
		return errors.New("sink unavailable")
	}, sdkactivity.RegisterOptions{Name: "llm.ExportLangfuse.v1"})
	f.run(t)
	if calls != 3 {
		t.Fatalf("export attempts = %d, want bounded 3", calls)
	}
}

func TestLangfuseOldHistorySkipsExport(t *testing.T) {
	f := workflowTest(t, "generate", workflowStep{name: activity.PrepareActivityName, state: llm.ExecutionCompleted})
	f.env.OnGetVersion("langfuse-export-v1", workflow.DefaultVersion, workflow.Version(1)).Return(workflow.DefaultVersion)
	calls := 0
	f.env.RegisterActivityWithOptions(func(context.Context, llm.ExecutionReferenceV1) error { calls++; return nil }, sdkactivity.RegisterOptions{Name: activity.ExportLangfuseActivityName})
	f.run(t)
	if calls != 0 {
		t.Fatal("export added to old history")
	}
}

func TestLangfuseRejectedExportPreservesOriginalFailure(t *testing.T) {
	f := workflowTest(t, "generate", workflowStep{name: activity.PrepareActivityName, state: llm.ExecutionFailed})
	calls := 0
	f.env.RegisterActivityWithOptions(func(context.Context, llm.ExecutionReferenceV1) error {
		calls++
		return temporal.NewNonRetryableApplicationError("Langfuse export rejected", "langfuse_export_rejected", nil)
	}, sdkactivity.RegisterOptions{Name: activity.ExportLangfuseActivityName})
	f.env.ExecuteWorkflow(RequestWorkflowName, f.input)
	var original *temporal.ApplicationError
	if !errors.As(f.env.GetWorkflowError(), &original) || original.Type() != "provider_error" {
		t.Fatalf("export masked original error: %v", f.env.GetWorkflowError())
	}
	if calls != 1 {
		t.Fatalf("rejected export retried %d times", calls)
	}
}
