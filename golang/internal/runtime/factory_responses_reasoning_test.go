package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

// The default endpoint configuration denies provider storage, so the adapters
// the production factory builds must keep reasoning replay stateless.
func TestProductionFactoryResponsesAdaptersReplayReasoningStatelessly(t *testing.T) {
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
			return []byte("test-key"), nil
		}),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) {
			return engine.Snapshot{}, nil
		}),
		HTTPClient: &http.Client{},
	})
	if err != nil {
		t.Fatalf("NewProductionEngineFactory() error = %v", err)
	}
	value := config.Config{Endpoints: map[string]config.EndpointConfig{
		"openai": {Family: "openai_responses", BaseURL: "https://api.openai.com/v1", OutboundHosts: []string{"api.openai.com"}, Auth: config.AuthConfig{Kind: "bearer_env", Name: "OPENAI_KEY"}},
		"azure": {Family: "azure_openai_responses", BaseURL: "https://example.openai.azure.com/openai/v1", OutboundHosts: []string{"example.openai.azure.com"},
			Auth: config.AuthConfig{Kind: "header_env", Name: "AZURE_KEY"}, Extensions: map[string]map[string]any{"azure": {"api_version": "v1"}}},
	}}
	snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
		"model": {Routes: []routing.Route{
			{EndpointID: "openai", Capabilities: routing.CapabilitySet{Version: "cap-v1"}},
			{EndpointID: "azure", Capabilities: routing.CapabilitySet{Version: "cap-v1"}},
		}},
	}}}
	human := func(text string) llm.Message {
		return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
	}
	answer := llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "answer"}}}
	reasoning := func(opaque string) llm.ProviderState {
		return llm.ProviderState{Provider: "openai", EndpointFamily: "responses", MediaType: "application/vnd.openai.reasoning+json", Opaque: []byte(opaque)}
	}
	for _, endpointID := range []string{"openai", "azure"} {
		t.Run(endpointID, func(t *testing.T) {
			adapter, err := factory.buildAdapter(context.Background(), value, snapshot, endpointID)
			if err != nil {
				t.Fatalf("buildAdapter() error = %v", err)
			}
			compile := func(request llm.Request) map[string]any {
				t.Helper()
				request.Model = "model"
				call, err := adapter.Compile(context.Background(), provider.CompileInput{
					Request: request,
					Query:   provider.CapabilityQuery{EndpointID: endpointID, Family: provider.FamilyOpenAIResponses, Model: "model"},
					Strict:  true,
				})
				if err != nil {
					t.Fatalf("Compile() error = %v", err)
				}
				encoded, err := json.Marshal(call.SDKParams)
				if err != nil {
					t.Fatal(err)
				}
				var wire map[string]any
				if err := json.Unmarshal(encoded, &wire); err != nil {
					t.Fatal(err)
				}
				return wire
			}
			replayed := func(wire map[string]any) []map[string]any {
				var items []map[string]any
				for _, raw := range wire["input"].([]any) {
					if item, ok := raw.(map[string]any); ok && item["type"] == "reasoning" {
						items = append(items, item)
					}
				}
				return items
			}

			first := compile(llm.Request{OperationKey: "first", Reasoning: &llm.ReasoningSpec{Effort: llm.ReasoningEffortLow}, Input: []llm.Item{human("question")}})
			if include, _ := first["include"].([]any); len(include) != 1 || include[0] != "reasoning.encrypted_content" {
				t.Fatalf("first-turn include = %v, want reasoning.encrypted_content", first["include"])
			}
			if first["store"] != false {
				t.Fatalf("first-turn store = %v, want false", first["store"])
			}

			sealed := compile(llm.Request{OperationKey: "second", Input: []llm.Item{
				human("question"), reasoning(`{"id":"rs_1","type":"reasoning","summary":[],"encrypted_content":"sealed"}`), answer, human("follow-up"),
			}})
			if items := replayed(sealed); len(items) != 1 || items[0]["encrypted_content"] != "sealed" {
				t.Fatalf("replayed reasoning = %v, want the item with its encrypted content", items)
			}

			bare := compile(llm.Request{OperationKey: "second", Input: []llm.Item{
				human("question"), reasoning(`{"id":"rs_1","type":"reasoning","summary":[],"encrypted_content":null}`), answer, human("follow-up"),
			}})
			if items := replayed(bare); len(items) != 0 {
				t.Fatalf("reasoning item without encrypted content was sent as an ID reference: %v", items)
			}

			dangling := compile(llm.Request{OperationKey: "second", Input: []llm.Item{
				human("question"), reasoning(`{"id":"rs_1","type":"reasoning","summary":[],"encrypted_content":"sealed"}`), human("follow-up"),
			}})
			if items := replayed(dangling); len(items) != 0 {
				t.Fatalf("dangling reasoning item was sent: %v", items)
			}
		})
	}
}
