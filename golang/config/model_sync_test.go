package config_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/schema"
)

// modelSyncConfig is the example configuration with a sync-only OpenRouter
// endpoint and an optional direct OpenAI endpoint.
func modelSyncConfig(t *testing.T) config.Config {
	t.Helper()
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	loaded.Endpoints["openrouter-sync"] = config.EndpointConfig{
		Family: "openai_chat", BaseURL: "https://openrouter.ai/api/v1", OutboundHosts: []string{"openrouter.ai"},
		Auth: config.AuthConfig{Kind: "bearer_env", Name: "OPENROUTER_API_KEY"}, AccountRegion: "global", Timeout: config.Duration(115 * time.Second),
		ServiceClasses:    map[llm.ServiceClass]config.TierConfig{llm.ServiceClassStandard: {ProviderValue: "default"}},
		CapabilityProfile: "openrouter-chat-v1", Optional: true,
		Extensions: map[string]map[string]any{"openrouter": {}},
	}
	loaded.Endpoints["openai-sync"] = config.EndpointConfig{
		Family: "openai_responses", BaseURL: "https://api.openai.com/v1", OutboundHosts: []string{"api.openai.com"},
		Auth: config.AuthConfig{Kind: "bearer_env", Name: "OPENAI_SYNC_API_KEY"}, AccountRegion: "global", Timeout: config.Duration(115 * time.Second),
		ServiceClasses:    map[llm.ServiceClass]config.TierConfig{llm.ServiceClassStandard: {ProviderValue: "default"}},
		CapabilityProfile: "openai-responses-prod-v3", Optional: true,
	}
	loaded.ModelSync = &config.ModelSyncConfig{
		OpenRouter:         config.ModelSyncOpenRouterConfig{Endpoint: "openrouter-sync"},
		Direct:             []config.ModelSyncDirectConfig{{Endpoint: "openai-sync", Provider: "openai"}},
		RefreshIntervalMin: config.Duration(55 * time.Minute), RefreshIntervalMax: config.Duration(65 * time.Minute),
	}
	return loaded
}

func TestModelSyncConfigValidates(t *testing.T) {
	value := modelSyncConfig(t)
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	// A sync-only deployment needs no configured models or price catalogs.
	value.Models = map[string]config.ModelConfig{}
	value.Pricing.Catalogs = nil
	for id, endpoint := range value.Endpoints {
		if !value.ModelSyncEndpoint(id) {
			delete(value.Endpoints, id)
			continue
		}
		value.Endpoints[id] = endpoint
	}
	if err := value.Validate(); err != nil {
		t.Fatalf("sync-only configuration rejected: %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(encoded); err != nil {
		t.Fatalf("sync-only configuration does not match the schema: %v", err)
	}
}

func TestModelSyncConfigRejectsUnsafeShapes(t *testing.T) {
	cases := map[string]func(*config.Config){
		"missing openrouter endpoint": func(value *config.Config) { value.ModelSync.OpenRouter.Endpoint = "absent" },
		"openrouter without marker": func(value *config.Config) {
			endpoint := value.Endpoints["openrouter-sync"]
			endpoint.Extensions = nil
			value.Endpoints["openrouter-sync"] = endpoint
		},
		"pinned provider order": func(value *config.Config) {
			endpoint := value.Endpoints["openrouter-sync"]
			endpoint.Extensions = map[string]map[string]any{"openrouter": {"provider_order": []any{"OpenAI"}}}
			value.Endpoints["openrouter-sync"] = endpoint
		},
		"duplicate direct endpoint": func(value *config.Config) {
			value.ModelSync.Direct = append(value.ModelSync.Direct, value.ModelSync.Direct[0])
		},
		"refresh bounds": func(value *config.Config) {
			value.ModelSync.RefreshIntervalMin = config.Duration(2 * time.Hour)
		},
		"optional endpoint outside model sync": func(value *config.Config) {
			endpoint := value.Endpoints["openai-prod"]
			endpoint.Optional = true
			value.Endpoints["openai-prod"] = endpoint
		},
		"optional endpoint referenced by a route": func(value *config.Config) {
			model := value.Models["invoice-summarizer"]
			model.Routes = append(model.Routes, config.RouteConfig{ID: "sync", Endpoint: "openai-sync", Model: "gpt-x", Classes: []llm.ServiceClass{llm.ServiceClassStandard}})
			value.Models["invoice-summarizer"] = model
		},
		"routed sync endpoint without price catalog": func(value *config.Config) {
			endpoint := value.Endpoints["openai-sync"]
			endpoint.Optional = false
			value.Endpoints["openai-sync"] = endpoint
			model := value.Models["invoice-summarizer"]
			model.Routes = append(model.Routes, config.RouteConfig{ID: "sync", Endpoint: "openai-sync", Model: "gpt-x", Classes: []llm.ServiceClass{llm.ServiceClassStandard}})
			value.Models["invoice-summarizer"] = model
		},
	}
	for name, mutate := range cases {
		value := modelSyncConfig(t)
		mutate(&value)
		if err := value.Validate(); err == nil {
			t.Fatalf("%s: configuration unexpectedly valid", name)
		}
	}
}

func TestModelSyncDefaultsRefreshIntervals(t *testing.T) {
	data := string(exampleYAML(t)) + `
model_sync:
  openrouter:
    endpoint: openrouter-pinned
`
	value, err := config.Load([]byte(data))
	if err == nil {
		t.Fatal("a pinned OpenRouter endpoint was accepted for model_sync")
	}
	if !strings.Contains(err.Error(), "provider_order") {
		t.Fatalf("error = %v, want the provider_order rejection", err)
	}
	_ = value
	unpinned := strings.Replace(data, "        provider_order: [ProviderA]\n", "", 1)
	value, err = config.Load([]byte(unpinned))
	if err != nil {
		t.Fatal(err)
	}
	if value.ModelSync.RefreshIntervalMin != config.Duration(55*time.Minute) || value.ModelSync.RefreshIntervalMax != config.Duration(65*time.Minute) {
		t.Fatalf("refresh intervals = %v..%v, want 55m..65m", value.ModelSync.RefreshIntervalMin, value.ModelSync.RefreshIntervalMax)
	}
}

func TestConfigWithoutModelSyncKeepsItsCanonicalDigest(t *testing.T) {
	value, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "model_sync") || strings.Contains(string(encoded), `"optional"`) {
		t.Fatal("unused model-sync fields entered canonical configuration JSON")
	}
}
