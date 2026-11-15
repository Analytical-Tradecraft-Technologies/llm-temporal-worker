package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// modelSyncConfig is the example configuration with a sync-only OpenRouter
// endpoint and an optional direct OpenAI endpoint.
func modelSyncConfig(t *testing.T) config.Config {
	t.Helper()
	loaded := mustLoadConfig(t, exampleYAML(t))
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
	testassert.NoError(t, value.Validate())
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
	testassert.NoError(t, value.Validate(), "sync-only configuration rejected")
	testassert.NoError(t, validateConfigSchema(t, value), "sync-only configuration does not match the schema")
}

func TestModelSyncConfigRejectsUnsafeShapes(t *testing.T) {
	cases := map[string]func(*config.Config){
		"missing openrouter endpoint": func(value *config.Config) { value.ModelSync.OpenRouter.Endpoint = "absent" },
		"openrouter without marker": func(value *config.Config) {
			editEndpoint(value, "openrouter-sync", func(endpoint *config.EndpointConfig) { endpoint.Extensions = nil })
		},
		"pinned provider order": func(value *config.Config) {
			editEndpoint(value, "openrouter-sync", func(endpoint *config.EndpointConfig) {
				endpoint.Extensions = map[string]map[string]any{"openrouter": {"provider_order": []any{"OpenAI"}}}
			})
		},
		"duplicate direct endpoint": func(value *config.Config) {
			value.ModelSync.Direct = append(value.ModelSync.Direct, value.ModelSync.Direct[0])
		},
		"refresh bounds": func(value *config.Config) {
			value.ModelSync.RefreshIntervalMin = config.Duration(2 * time.Hour)
		},
		"optional endpoint outside model sync": func(value *config.Config) {
			editEndpoint(value, "openai-prod", func(endpoint *config.EndpointConfig) { endpoint.Optional = true })
		},
		"optional endpoint referenced by a route": func(value *config.Config) {
			model := value.Models["invoice-summarizer"]
			model.Routes = append(model.Routes, config.RouteConfig{ID: "sync", Endpoint: "openai-sync", Model: "gpt-x", Classes: []llm.ServiceClass{llm.ServiceClassStandard}})
			value.Models["invoice-summarizer"] = model
		},
		"routed sync endpoint without price catalog": func(value *config.Config) {
			editEndpoint(value, "openai-sync", func(endpoint *config.EndpointConfig) { endpoint.Optional = false })
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
	value = mustLoadConfig(t, []byte(unpinned))
	if value.ModelSync.RefreshIntervalMin != config.Duration(55*time.Minute) || value.ModelSync.RefreshIntervalMax != config.Duration(65*time.Minute) {
		t.Fatalf("refresh intervals = %v..%v, want 55m..65m", value.ModelSync.RefreshIntervalMin, value.ModelSync.RefreshIntervalMax)
	}
}

func TestConfigWithoutModelSyncKeepsItsCanonicalDigest(t *testing.T) {
	value := mustLoadConfig(t, exampleYAML(t))
	encoded := mustMarshalJSON(t, value)
	if strings.Contains(string(encoded), "model_sync") || strings.Contains(string(encoded), `"optional"`) {
		t.Fatal("unused model-sync fields entered canonical configuration JSON")
	}
}
