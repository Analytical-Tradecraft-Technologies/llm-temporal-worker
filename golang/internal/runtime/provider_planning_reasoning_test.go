package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/anthropicmessages"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// A v1 request can set reasoning effort and summary but never a reasoning
// mode. Such a request must stay routable on an Anthropic Messages route and
// must not turn thinking on.
func TestProviderPlanningRoutesV1ReasoningEffortOnAnthropicMessages(t *testing.T) {
	for _, test := range []struct {
		name        string
		portability llm.PortabilityMode
		summary     llm.ReasoningSummary
	}{
		{name: "strict", portability: llm.PortabilityStrict},
		{name: "strict summary none", portability: llm.PortabilityStrict, summary: llm.ReasoningSummaryNone},
		{name: "best effort summary concise", portability: llm.PortabilityBestEffort, summary: llm.ReasoningSummaryConcise},
	} {
		t.Run(test.name, func(t *testing.T) {
			capabilities, source, _, request, _, _ := planningFixture()
			profile := anthropicmessages.DefaultProfile("anthropic")
			client, err := anthropicmessages.NewClient(anthropicmessages.ClientConfig{BaseURL: "https://api.anthropic.com", APIKey: "test-key", HTTPClient: &http.Client{Transport: planningTransportFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("unexpected HTTP request")
			})}})
			if err != nil {
				t.Fatal(err)
			}
			adapter, err := anthropicmessages.NewAdapter(client, "endpoint", profile)
			if err != nil {
				t.Fatal(err)
			}
			query := provider.CapabilityQuery{EndpointID: "endpoint", Family: provider.FamilyAnthropicMessages, Model: "provider-model", ServiceClass: llm.ServiceClassStandard}
			set, err := adapter.Capabilities(context.Background(), query)
			if err != nil {
				t.Fatal(err)
			}
			model := source.value.Routes.Models["alias"]
			route := &model.Routes[0]
			route.Provider, route.Family = "anthropic", string(provider.FamilyAnthropicMessages)
			route.ProviderTiers = map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard_only"}
			route.Capabilities = routing.CapabilitySet{Version: set.Version, Features: map[routing.Feature]routing.Capability{
				routing.FeatureText:      {State: routing.CapabilityNative},
				routing.FeatureReasoning: {State: routing.CapabilityNative},
			}}
			capabilities.Adapters = engine.AdapterMap{"endpoint": adapter}

			request.SettingsPatch.Portability.Set = preparationPointer(test.portability)
			request.SettingsPatch.ReasoningEffort.Set = preparationPointer(llm.ReasoningEffortHigh)
			if test.summary != "" {
				request.SettingsPatch.ReasoningSummary.Set = preparationPointer(test.summary)
			}
			prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
			if err != nil {
				t.Fatal(err)
			}
			if prepared.Request.Reasoning == nil || prepared.Request.Reasoning.Mode != "" || prepared.Request.Reasoning.Effort != llm.ReasoningEffortHigh {
				t.Fatalf("prepared reasoning = %+v", prepared.Request.Reasoning)
			}
			planning, err := capabilities.NewProviderPlanning(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			planned, err := planning.Generate(context.Background(), prepared)
			if err != nil {
				t.Fatalf("v1 reasoning effort is not routable on Anthropic Messages: %v", err)
			}
			data, err := json.Marshal(planned.Call.SDKParams)
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]any
			if err := json.Unmarshal(data, &wire); err != nil {
				t.Fatal(err)
			}
			config, _ := wire["output_config"].(map[string]any)
			if planned.Call.Family != provider.FamilyAnthropicMessages || config["effort"] != "high" {
				t.Fatalf("effort was not lowered to output_config: %s", data)
			}
			if thinking, exists := wire["thinking"]; exists {
				t.Fatalf("v1 effort or summary enabled thinking = %#v", thinking)
			}
		})
	}
}
