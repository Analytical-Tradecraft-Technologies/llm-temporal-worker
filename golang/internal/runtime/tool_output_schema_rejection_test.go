package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

func toolOutputContract() llm.Tool {
	return llm.Tool{Name: "lookup", InputSchema: json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
		OutputSchema: json.RawMessage(`{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}`)}
}

// Compile the real adapters independently of routing. Neither portability mode
// may silently discard a function tool's output contract.
func TestToolOutputSchemaCompilerRejection(t *testing.T) {
	for _, route := range hierarchyRoutes()[:3] {
		t.Run(route.name, func(t *testing.T) {
			adapter := route.adapter(t)
			for _, strict := range []bool{true, false} {
				request := llm.Request{OperationKey: "tool-contract", Model: "provider-model", ServiceClass: llm.ServiceClassStandard,
					Input: []llm.Item{llm.HumanText("lookup")},
					Tools: []llm.Tool{toolOutputContract()}, ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceAuto, Parallel: true}}
				input := provider.CompileInput{Request: request, Strict: strict,
					Query: provider.CapabilityQuery{EndpointID: "endpoint", Family: route.family, Model: request.Model}}
				_, err := adapter.Compile(context.Background(), input)
				testkit.ProviderError(t, err, testkit.ProviderClass{Code: provider.CodeUnsupportedCapability, Phase: provider.PhaseCompile, Dispatch: provider.DispatchNotDispatched}, fmt.Sprintf("strict=%v: compile error", strict))
				input.Request.Tools[0].OutputSchema = nil
				if _, err := adapter.Compile(context.Background(), input); err != nil {
					t.Fatalf("strict=%v: input-only tool rejected: %v", strict, err)
				}
				// Final model output keeps the adapter's existing contract:
				// Messages supports it, while Converse explicitly rejects it.
				input.Request.Output = &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Strict: true, Schema: toolOutputContract().OutputSchema}}
				call, err := adapter.Compile(context.Background(), input)
				if route.family == provider.FamilyBedrockConverse {
					testkit.ProviderError(t, err, testkit.ProviderClass{Code: provider.CodeUnsupportedCapability}, "Converse final-response contract changed")
				} else if err != nil || len(call.OutputSchema) == 0 {
					t.Fatalf("strict=%v: final response schema rejected: %v", strict, err)
				}
			}
		})
	}
}

func TestToolOutputSchemaRoutingRejectsBeforeBudgetOrDispatch(t *testing.T) {
	for _, route := range hierarchyRoutes()[:3] {
		t.Run(route.name, func(t *testing.T) {
			f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
				route.configure(b)
				b.source.value.Routes.Models["alias"].Routes[0].Capabilities.Features[routing.FeatureToolCall] = routing.Capability{State: routing.CapabilityNative}
				b.prices(t, []pricing.Entry{b.entry})
			})
			accepts := 0
			f.cap.Budgets = &admissionLeaser{BudgetLeaser: f.cap.Budgets, accept: func(context.Context, durable.ReserveRequest) (durable.ReserveResult, error) {
				accepts++
				return durable.ReserveResult{}, errors.New("unexpected budget admission")
			}}
			f.cap.Adapters = engine.AdapterMap{"endpoint": hierarchyInvokeAdapter{Adapter: route.adapter(t), invoke: f.adapter.invoke}}
			f.restart(t)
			f.request.SettingsPatch.Tools.Set = preparationPointer([]llm.Tool{toolOutputContract()})
			f.request.SettingsPatch.ToolPolicy.Set = preparationPointer(llm.ToolPolicy{Mode: llm.ToolChoiceAuto, Parallel: true})
			ctx := context.Background()
			v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
			if err == nil {
				boundedState(t, v, err, llm.ExecutionBudgetRequired)
				_, err = f.runtime.AcquireBudgetV1(ctx, f.ref(v))
			}
			details := rejectedSelection(t, err, provider.CodeUnsupportedCapability, provider.PhaseCompile)
			if details["route_1"] != "route" || details["reason_1"] != "route_compile_rejected" {
				t.Fatalf("wrong rejection: %v", details)
			}
			if accepts != 0 || f.submits.Load() != 0 {
				t.Fatalf("unsupported contract admitted %d budgets and dispatched %d requests", accepts, f.submits.Load())
			}
		})
	}
}
