package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/openairesponses"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestBudgetDefaultOutputLimitSurvivesRecovery(t *testing.T) {
	for _, compact := range []bool{false, true} {
		name := "generate"
		if compact {
			name = "compact"
		}
		t.Run(name, func(t *testing.T) {
			f := newBudgetPlanningFixture(t)
			request := &f.generate.Request
			if compact {
				request = f.summary.Request
			}
			request.Output.MaxTokens = nil
			planning := f.planning(t)
			var planned PlannedBudgetCall
			var err error
			if compact {
				planned, err = planning.Compact(context.Background(), f.summary, f.attempt)
			} else {
				planned, err = planning.Generate(context.Background(), f.generate, f.attempt)
			}
			if err != nil {
				t.Fatal(err)
			}
			check := func(request llm.Request) {
				t.Helper()
				if request.Output == nil || request.Output.MaxTokens == nil || int64(*request.Output.MaxTokens) != planned.Estimate.OutputTokens {
					t.Fatalf("compiled output limit does not match reserved %d tokens: %#v", planned.Estimate.OutputTokens, request.Output)
				}
			}
			check(f.adapter.inputs[len(f.adapter.inputs)-1].Request)
			binding, err := planned.Provider.RecoveryBinding(planned.Route)
			if err != nil {
				t.Fatal(err)
			}
			recovery, err := f.cap.NewProviderRecovery(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if compact {
				_, err = recovery.Compact(context.Background(), f.summary, binding)
			} else {
				_, err = recovery.Generate(context.Background(), f.generate, binding)
			}
			if err != nil {
				t.Fatal(err)
			}
			check(f.adapter.inputs[len(f.adapter.inputs)-1].Request)
			if request.Output.MaxTokens != nil {
				t.Fatal("caller request mutated")
			}
		})
	}
}

func TestBudgetLimitReachesResponsesWireAndRecovery(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	f.generate.Request.Output.MaxTokens = nil
	client, err := openairesponses.NewClient(openairesponses.ClientConfig{BaseURL: "https://api.openai.com/v1/", APIKey: "test-key", HTTPClient: &http.Client{Transport: planningTransportFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("unexpected network call") })}})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := openairesponses.New(client, "endpoint", "profile/v1")
	if err != nil {
		t.Fatal(err)
	}
	f.cap.Adapters = engine.AdapterMap{"endpoint": adapter}
	planning := f.planning(t)
	planned, err := planning.Generate(context.Background(), f.generate, f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	check := func(call PlannedProviderCall) {
		t.Helper()
		data, err := json.Marshal(call.Call.SDKParams)
		if err != nil {
			t.Fatal(err)
		}
		var wire struct {
			MaxOutputTokens int64 `json:"max_output_tokens"`
		}
		if err := json.Unmarshal(data, &wire); err != nil {
			t.Fatal(err)
		}
		if wire.MaxOutputTokens != 16 || wire.MaxOutputTokens != planned.Estimate.OutputTokens {
			t.Fatalf("wire limit %d differs from reservation %d", wire.MaxOutputTokens, planned.Estimate.OutputTokens)
		}
	}
	check(planned.Provider)
	binding, err := planned.Provider.RecoveryBinding(planned.Route)
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := f.cap.NewProviderRecovery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := recovery.Generate(context.Background(), f.generate, binding)
	if err != nil {
		t.Fatal(err)
	}
	check(restored)
	f.cap.BudgetEstimator.MaxOutput = 32
	recovery, err = f.cap.NewProviderRecovery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.Generate(context.Background(), f.generate, binding); err == nil {
		t.Fatal("changed output limit reused an old reservation")
	}
}

// A default above the route's output ceiling is capped at the ceiling for
// the compiled call, the reservation and exact-route recovery alike (#1523).
func TestBudgetDefaultOutputLimitIsCappedAtRouteCeilingThroughRecovery(t *testing.T) {
	for _, compact := range []bool{false, true} {
		name := "generate"
		if compact {
			name = "compact"
		}
		t.Run(name, func(t *testing.T) {
			f := newBudgetPlanningFixture(t)
			f.source.value.Routes.Models["alias"].Routes[0].OutputTokens = 8
			request := &f.generate.Request
			if compact {
				request = f.summary.Request
			}
			request.Output.MaxTokens = nil
			planning := f.planning(t)
			var planned PlannedBudgetCall
			var err error
			if compact {
				planned, err = planning.Compact(context.Background(), f.summary, f.attempt)
			} else {
				planned, err = planning.Generate(context.Background(), f.generate, f.attempt)
			}
			if err != nil {
				t.Fatal(err)
			}
			if planned.Estimate.OutputTokens != 8 {
				t.Fatalf("reserved %d output tokens, want the route ceiling 8", planned.Estimate.OutputTokens)
			}
			check := func(request llm.Request) {
				t.Helper()
				if request.Output == nil || request.Output.MaxTokens == nil || *request.Output.MaxTokens != 8 {
					t.Fatalf("compiled output limit = %#v, want 8", request.Output)
				}
			}
			check(f.adapter.inputs[len(f.adapter.inputs)-1].Request)
			binding, err := planned.Provider.RecoveryBinding(planned.Route)
			if err != nil {
				t.Fatal(err)
			}
			recovery, err := f.cap.NewProviderRecovery(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if compact {
				_, err = recovery.Compact(context.Background(), f.summary, binding)
			} else {
				_, err = recovery.Generate(context.Background(), f.generate, binding)
			}
			if err != nil {
				t.Fatal(err)
			}
			check(f.adapter.inputs[len(f.adapter.inputs)-1].Request)
			if request.Output.MaxTokens != nil {
				t.Fatal("caller request mutated")
			}
		})
	}
}
