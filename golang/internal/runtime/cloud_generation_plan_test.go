package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

func TestCloudGenerationPlan(t *testing.T) {
	for _, mode := range []string{"root", "short", "no-prefix", "tokens", "bytes", "provider-limit"} {
		t.Run(mode, func(t *testing.T) {
			f := boundedCloud(t, false)
			policy := json.RawMessage(`{"recent_turns":0}`)
			switch mode {
			case "no-prefix":
				policy = json.RawMessage(`{"recent_turns":100}`)
			case "tokens":
				policy = json.RawMessage(`{"recent_turns":0,"trigger_tokens":10,"target_tokens":5}`)
			case "bytes":
				policy = json.RawMessage(`{"recent_turns":0,"materialization_threshold_bytes":1}`)
			}
			f.request.SettingsPatch.CompactionPolicy.Set = &policy
			if mode != "root" {
				parent := f.finish(t)
				handle := parent.Generate.Checkpoint.Handle
				f.request.Parent = &handle
				f.request.OperationKey = "next-turn"
				f.request.SettingsPatch = llm.SettingsPatchV1{}
				f.now = f.now.Add(time.Second)
			}
			if mode == "tokens" || mode == "no-prefix" {
				f.cap.BudgetEstimator.Tokenizer = func(llm.Request, routing.Candidate) (int64, error) { return 50000, nil }
			}
			f.restart(t)
			if mode == "provider-limit" {
				providers := f.runtime.execution.admission.planning.providers
				for name, model := range providers.catalog.Models {
					for i := range model.Routes {
						model.Routes[i].ContextBytes = 1
					}
					providers.catalog.Models[name] = model
				}
			}
			before := f.submits.Load()
			decision, err := f.runtime.PlanGenerationV1(context.Background(), f.request)
			want := mode == "tokens" || mode == "bytes" || mode == "provider-limit"
			if err != nil || decision.CompactBeforeGenerate != want {
				t.Fatalf("decision=%+v want=%t err=%v", decision, want, err)
			}
			if f.submits.Load() != before {
				t.Fatal("planning performed paid work")
			}
		})
	}
}

func TestCloudGenerationPlanRejectsUnauthorizedBeforeRead(t *testing.T) {
	f := boundedCloud(t, false)
	f.options.ResolveScope = func(context.Context, llm.RequestContext) (string, error) { return "", errors.New("denied") }
	f.restart(t)
	parent := llm.CheckpointHandle("ckp_v1.never-read")
	f.request.Parent = &parent
	_, err := f.runtime.PlanGenerationV1(context.Background(), f.request)
	if err == nil || f.submits.Load() != 0 {
		t.Fatal("unauthorized planning accepted")
	}
}

func TestCloudGenerationPlanLargeInputAndTokenizerFailure(t *testing.T) {
	f := boundedCloud(t, false)
	policy := json.RawMessage(`{"recent_turns":0}`)
	f.request.SettingsPatch.CompactionPolicy.Set = &policy
	parent := f.finish(t)
	handle := parent.Generate.Checkpoint.Handle
	f.request.Parent = &handle
	f.request.OperationKey = "large-next"
	f.request.Append = []llm.Item{preparationMessage(strings.Repeat("x", 300<<10))}
	f.cap.BudgetEstimator.Tokenizer = nil
	f.restart(t)
	decision, err := f.runtime.PlanGenerationV1(context.Background(), f.request)
	if err != nil || !decision.CompactBeforeGenerate {
		t.Fatalf("large plan: %+v %v", decision, err)
	}
	f.cap.BudgetEstimator.Tokenizer = func(llm.Request, routing.Candidate) (int64, error) { return 0, errors.New("cannot count") }
	f.restart(t)
	if _, err := f.runtime.PlanGenerationV1(context.Background(), f.request); err == nil {
		t.Fatal("tokenizer failure ignored")
	}
}

func TestCloudGenerationPlanMaterializesOutputCap(t *testing.T) {
	f := boundedCloud(t, false)
	policy := json.RawMessage(`{"recent_turns":0}`)
	f.request.SettingsPatch.CompactionPolicy.Set = &policy
	parent := f.finish(t)
	handle := parent.Generate.Checkpoint.Handle
	f.request.Parent = &handle
	f.request.OperationKey = "next-output-cap"
	f.request.SettingsPatch = llm.SettingsPatchV1{}
	f.restart(t)
	providers := f.runtime.execution.admission.planning.providers
	providers.outputLimit = 1000
	seen := false
	providers.planner = planningPlannerFunc(func(ctx context.Context, input routing.Input) (routing.Plan, error) {
		seen = true
		if input.Request.Output == nil || input.Request.Output.MaxTokens == nil {
			t.Fatal("preflight omitted effective output cap")
		}
		for name, model := range input.Catalog.Models {
			for i := range model.Routes {
				model.Routes[i].OutputTokens = int64(*input.Request.Output.MaxTokens)
			}
			input.Catalog.Models[name] = model
		}
		return (routing.DeterministicPlanner{}).Plan(ctx, input)
	})
	before := f.submits.Load()
	if _, err := f.runtime.PlanGenerationV1(context.Background(), f.request); err != nil {
		t.Fatal(err)
	}
	if !seen || f.submits.Load() != before {
		t.Fatal("preflight skipped planning or performed paid work")
	}
}
