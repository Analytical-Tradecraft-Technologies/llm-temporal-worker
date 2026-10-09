package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

func TestCloudGenerationPlan(t *testing.T) {
	for _, mode := range []string{"root", "short", "no-prefix", "tokens", "bytes", "provider-limit", "provider-token-limit"} {
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
				// The parent's history is what crosses the trigger; the request
				// that compaction leaves behind stays below it.
				f.cap.BudgetEstimator.Tokenizer = func(request llm.Request, _ routing.Candidate) (int64, error) {
					if len(request.Input) > 1 {
						return 50000, nil
					}
					return 5, nil
				}
			}
			f.restart(t)
			if mode == "provider-limit" || mode == "provider-token-limit" {
				providers := f.runtime.execution.admission.planning.providers
				for name, model := range providers.catalog.Models {
					for i := range model.Routes {
						if mode == "provider-limit" {
							model.Routes[i].ContextBytes = 1
						} else {
							model.Routes[i].ContextTokens = 1
						}
					}
					providers.catalog.Models[name] = model
				}
			}
			before := f.submits.Load()
			decision, err := f.runtime.PlanGenerationV1(context.Background(), f.request)
			want := mode == "tokens" || mode == "bytes" || mode == "provider-limit" || mode == "provider-token-limit"
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
	// The large turn is history: the next turn can compact it away.
	f.request.Append = []llm.Item{preparationMessage(strings.Repeat("x", 300<<10))}
	parent := f.finish(t)
	handle := parent.Generate.Checkpoint.Handle
	f.request.Parent = &handle
	f.request.OperationKey = "large-next"
	f.request.Append = []llm.Item{preparationMessage("next")}
	f.cap.BudgetEstimator.Tokenizer = nil
	f.restart(t)
	decision, err := f.runtime.PlanGenerationV1(context.Background(), f.request)
	if err != nil || !decision.CompactBeforeGenerate {
		t.Fatalf("large plan: %+v %v", decision, err)
	}
	f.cap.BudgetEstimator.Tokenizer = func(llm.Request, routing.Candidate) (int64, error) { return 0, errors.New("cannot count") }
	f.restart(t)
	if replay, err := f.runtime.PlanGenerationV1(context.Background(), f.request); err != nil || replay != decision {
		t.Fatalf("saved decision was replanned: %+v %v", replay, err)
	}
	f.request.OperationKey = "large-next-new-plan"
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

// Selection falls through to a later route with a larger context window, so
// the plan must not request compaction while such a route can take the
// transcript. It must once that route is blocked or too small as well.
func TestCloudGenerationPlanConsidersEveryCandidateContextWindow(t *testing.T) {
	// The fixture counts 10 input tokens and reserves 16 output tokens.
	f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
		model := b.source.value.Routes.Models["alias"]
		small := model.Routes[0]
		small.ID, small.Model, small.ContextTokens = "small-context", "small-model", 25
		model.Routes[0].ContextTokens = 26
		model.Routes = append([]routing.Route{small}, model.Routes...)
		b.source.value.Routes.Models["alias"] = model
		b.estimator.Tokenizer = func(llm.Request, routing.Candidate) (int64, error) { return 10, nil }
	})
	shared := &sharedHealthFixture{}
	f.cap.ProviderRouteStatus = shared
	var models []string
	invoke := f.adapter.invoke
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		models = append(models, call.Model)
		return invoke(ctx, call, o)
	}
	policy := json.RawMessage(`{"recent_turns":0}`)
	f.request.SettingsPatch.CompactionPolicy.Set = &policy
	f.restart(t)
	parent := f.finish(t)
	handle := parent.Generate.Checkpoint.Handle
	f.request.Parent = &handle
	f.request.OperationKey = "next-turn"
	f.request.SettingsPatch = llm.SettingsPatchV1{}
	f.now = f.now.Add(time.Second)
	f.restart(t)
	decision, err := f.runtime.PlanGenerationV1(context.Background(), f.request)
	if err != nil || decision.CompactBeforeGenerate {
		t.Fatalf("compaction requested although the second route fits: %+v %v", decision, err)
	}
	f.finish(t)
	if len(models) != 2 || models[0] != "provider-model" || models[1] != "provider-model" {
		t.Fatalf("generation did not use the larger route: %v", models)
	}

	// A blocked route cannot take the transcript, so only the small one is left.
	snapshot, err := f.cap.Snapshot.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	large := snapshot.Routes.Models["alias"].Routes[1]
	f.request.OperationKey = "blocked-turn"
	shared.status = control.RouteStatus{ConfigDigest: snapshot.ConfigDigest, ConfigEpoch: snapshot.ConfigEpoch, RouteID: large.ID, EndpointID: large.EndpointID, EndpointAccountHMAC: large.EndpointAccountHMAC, Provider: large.Provider, EndpointFamily: large.Family, Circuit: control.CircuitOpen, ObservedAt: f.now, StaleAfter: f.now.Add(time.Minute), Credit: control.CreditOK, Billing: control.BillingOK}
	if decision, err := f.runtime.PlanGenerationV1(context.Background(), f.request); err != nil || !decision.CompactBeforeGenerate {
		t.Fatalf("blocked larger route still counted as fitting: %+v %v", decision, err)
	}
	shared.status = control.RouteStatus{}

	// A route that fits but that selection would skip at compilation must not
	// suppress compaction either: generation would end without a route.
	f.adapter.compile = func(provider.CompileInput) (provider.Call, error) {
		return provider.Call{}, provider.NewError(provider.CodeUnsupportedCapability, provider.PhaseCompile, provider.DispatchNotDispatched, provider.RetryNextRoute, "unsupported")
	}
	f.request.OperationKey = "uncompilable-turn"
	if decision, err := f.runtime.PlanGenerationV1(context.Background(), f.request); err != nil || !decision.CompactBeforeGenerate {
		t.Fatalf("uncompilable larger route still counted as usable: %+v %v", decision, err)
	}
	f.adapter.compile = nil
	f.request.OperationKey = "usable-turn"
	if decision, err := f.runtime.PlanGenerationV1(context.Background(), f.request); err != nil || decision.CompactBeforeGenerate {
		t.Fatalf("usable larger route no longer suppresses compaction: %+v %v", decision, err)
	}

	providers := f.runtime.execution.admission.planning.providers
	model := providers.catalog.Models["alias"]
	model.Routes[1].ContextTokens = 25
	providers.catalog.Models["alias"] = model
	f.request.OperationKey = "no-route-fits-turn"
	if decision, err := f.runtime.PlanGenerationV1(context.Background(), f.request); err != nil || !decision.CompactBeforeGenerate {
		t.Fatalf("no route fits but compaction was not requested: %+v %v", decision, err)
	}
}
