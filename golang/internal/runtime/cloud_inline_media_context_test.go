package runtime

import (
	"context"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

// A 600 KiB inline image is a few thousand billed tokens, not the ~205k its
// base64 would be as text. The fallback estimator must admit it on a
// 128k-context route, reserve the per-image allowance, and not ask for
// compaction on the next turn while the image stays in history.
func TestCloudExecutionAdmitsInlineImageOnContextLimitedRoute(t *testing.T) {
	const contextTokens = 128_000
	f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
		b.estimator.Tokenizer, b.estimator.MaxOutput = nil, 1024
		model := b.source.value.Routes.Models["alias"]
		model.Routes[0].ContextTokens = contextTokens
		b.source.value.Routes.Models["alias"] = model
		b.gen.Append = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.TextPart{Text: "describe this image"},
			llm.ImagePart{Bytes: make([]byte, 600<<10), MediaType: "image/png"},
		}}}
	})
	ctx := context.Background()
	prepared, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, prepared, err, llm.ExecutionBudgetRequired)
	scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
	attempt, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(prepared.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := f.repository.LoadBudgetPlan(ctx, scope, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Text estimate plus one image allowance: well inside the window.
	if got := plan.Estimate.InputTokens; got < budget.MediaImageInputTokenFloor || got > budget.MediaImageInputTokenFloor+1000 {
		t.Fatalf("reserved input tokens = %d, want the image allowance %d plus a small text estimate", got, budget.MediaImageInputTokenFloor)
	}
	parent := f.finish(t)
	if f.submits.Load() != 1 {
		t.Fatalf("provider submits = %d, want 1", f.submits.Load())
	}
	handle := parent.Generate.Checkpoint.Handle
	f.request.Parent, f.request.OperationKey, f.request.SettingsPatch = &handle, "next-turn", llm.SettingsPatchV1{}
	f.request.Append = []llm.Item{preparationMessage("and now?")}
	decision, err := f.runtime.PlanGenerationV1(ctx, f.request)
	if err != nil || decision.CompactBeforeGenerate {
		t.Fatalf("retained inline image forced compaction: decision=%+v err=%v", decision, err)
	}
}
