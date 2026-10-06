package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/catalog"
	"github.com/mfow/llm-temporal-worker/golang/internal/modelsync"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

func modelSyncTestConfig() config.Config {
	return config.Config{
		Version: "llm-temporal-worker/v1",
		Endpoints: map[string]config.EndpointConfig{
			"openrouter": {Family: "openai_chat", BaseURL: "https://openrouter.ai/api/v1", AccountRegion: "global", CapabilityProfile: "chat",
				ServiceClasses: map[llm.ServiceClass]config.TierConfig{llm.ServiceClassStandard: {ProviderValue: "default"}},
				Extensions:     map[string]map[string]any{"openrouter": {}}},
			"openai-direct": {Family: "openai_responses", BaseURL: "https://api.openai.com/v1", AccountRegion: "global", CapabilityProfile: "responses", Optional: true,
				ServiceClasses: map[llm.ServiceClass]config.TierConfig{llm.ServiceClassStandard: {ProviderValue: "default"}}},
			"configured": {Family: "openai_responses", BaseURL: "https://api.openai.com/v1", AccountRegion: "global", CapabilityProfile: "responses", PriceCatalog: "prices",
				ServiceClasses: map[llm.ServiceClass]config.TierConfig{llm.ServiceClassStandard: {ProviderValue: "default"}}},
		},
		Models: map[string]config.ModelConfig{"openai/gpt-5.4": {Routes: []config.RouteConfig{{ID: "configured", Endpoint: "configured", Model: "gpt-5.4", Classes: []llm.ServiceClass{llm.ServiceClassStandard}}}}},
		ModelSync: &config.ModelSyncConfig{
			OpenRouter:         config.ModelSyncOpenRouterConfig{Endpoint: "openrouter"},
			Direct:             []config.ModelSyncDirectConfig{{Endpoint: "openai-direct", Provider: "openai"}},
			RefreshIntervalMin: config.Duration(55 * time.Minute), RefreshIntervalMax: config.Duration(65 * time.Minute),
		},
	}
}

func modelSyncTestCatalogs(t *testing.T) loadedCatalogs {
	t.Helper()
	features := provider.CapabilitySet{Version: "cap-v1", Features: map[provider.Feature]provider.Capability{
		provider.FeatureText: {State: provider.CapabilityNative}, provider.FeatureToolCall: {State: provider.CapabilityNative},
	}}
	configuredPrice := pricing.Entry{Provider: "openai", Family: "openai_responses", EndpointID: "configured", Region: "global", Model: "gpt-5.4", ProviderTier: "default",
		Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("9"), OutputPerMillion: pricing.MustDecimalUSD("9")}}
	prices, err := pricing.CompileUSD("runtime-prices/config-v1", []pricing.Entry{configuredPrice})
	if err != nil {
		t.Fatal(err)
	}
	base := engine.Snapshot{Version: "config-v1", ConfigDigest: [32]byte{1}, ConfigEpoch: "config-v1",
		Routes: routing.Catalog{Version: "llm-temporal-worker/v1", Models: map[string]routing.Model{"openai/gpt-5.4": {Name: "openai/gpt-5.4", Routes: []routing.Route{{
			ID: "configured", EndpointID: "configured", Provider: "openai", Family: "openai_responses", Region: "global", Model: "gpt-5.4",
			Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"},
			Capabilities: routing.CapabilitySet{Version: "cap-v1"}, PriceAvailable: true,
		}}}}},
		Prices: pricing.NewResolver(prices)}
	return loadedCatalogs{snapshot: base, prices: prices, bundle: catalog.Bundle{Capabilities: map[string]catalog.CapabilityProfile{
		"chat":      {ID: "chat", Family: provider.FamilyOpenAIChat, Set: features},
		"responses": {ID: "responses", Family: provider.FamilyOpenAIResponses, Set: features},
	}}}
}

func modelSyncTestDocument() modelsync.Document {
	return modelsync.Document{Schema: modelsync.DocumentSchema, FetchedAt: time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC), Models: []modelsync.Model{
		{ID: "openai/gpt-5.4", ContextLength: 1050000, Endpoints: []modelsync.Endpoint{{Tag: "openai", Pricing: modelsync.Pricing{Prompt: "0.0000025", Completion: "0.000015"}}}},
		{ID: "openai/gpt-5.4-mini", ContextLength: 400000, Endpoints: []modelsync.Endpoint{
			{Tag: "azure", Pricing: modelsync.Pricing{Prompt: "0.0000008", Completion: "0.0000048"}},
			{Tag: "openai", Pricing: modelsync.Pricing{Prompt: "0.00000075", Completion: "0.0000045"}},
		}},
	}}
}

func TestModelSyncLayersSyncedRoutesWithoutChangingTheConfigIdentity(t *testing.T) {
	value := modelSyncTestConfig()
	loaded := modelSyncTestCatalogs(t)
	syncRuntime, err := newModelSyncRuntime(value, loaded, catalog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	// The optional direct endpoint has no credential, so only OpenRouter and
	// the configured endpoint have adapters.
	if err := syncRuntime.enable(map[string]provider.Adapter{"openrouter": nil, "configured": nil}); err != nil {
		t.Fatal(err)
	}
	document := modelSyncTestDocument()
	if err := syncRuntime.install(&document); err != nil {
		t.Fatal(err)
	}
	snapshot, err := syncRuntime.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ConfigDigest != loaded.snapshot.ConfigDigest || snapshot.ConfigEpoch != loaded.snapshot.ConfigEpoch || snapshot.Version != loaded.snapshot.Version {
		t.Fatal("installing a synced catalog changed the configuration identity")
	}
	// The configured model keeps its configured route.
	if routes := snapshot.Routes.Models["openai/gpt-5.4"].Routes; len(routes) != 1 || routes[0].ID != "configured" {
		t.Fatalf("configured model routes = %+v", routes)
	}
	mini := snapshot.Routes.Models["openai/gpt-5.4-mini"].Routes
	if len(mini) != 1 || mini[0].EndpointID != "openrouter" || mini[0].Model != "openai/gpt-5.4-mini" {
		t.Fatalf("synced model routes = %+v, want only the enabled OpenRouter route", mini)
	}
	quote, err := snapshot.Prices.Resolve(pricing.Query{Provider: "openrouter", Family: "openai_chat", EndpointID: "openrouter", Region: "global",
		Model: "openai/gpt-5.4-mini", ProviderTier: "default", At: time.Now()})
	if err != nil {
		t.Fatalf("synced route has no price: %v", err)
	}
	if got := quote.Entry.Prices.InputPerMillion.CanonicalString(); got != "0.8" {
		t.Fatalf("OpenRouter input price = %s, want the 0.8 maximum over upstreams", got)
	}
	plan, err := routing.DeterministicPlanner{}.Plan(context.Background(), routing.Input{Catalog: snapshot.Routes, Now: time.Now(),
		Request: llm.Request{OperationKey: "sync-test", Model: "openai/gpt-5.4-mini", ServiceClass: llm.ServiceClassStandard, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hi"}}}}}})
	if err != nil || len(plan.Candidates) != 1 || plan.Candidates[0].PriceVersion != "" {
		t.Fatalf("plan = %+v, err = %v; want one unpinned candidate", plan.Candidates, err)
	}
	// A later refresh with new prices replaces the snapshot; an earlier
	// captured snapshot is unchanged.
	document.FetchedAt = document.FetchedAt.Add(time.Hour)
	document.Models[1].Endpoints[0].Pricing.Prompt = "0.000001"
	if err := syncRuntime.install(&document); err != nil {
		t.Fatal(err)
	}
	refreshed, _ := syncRuntime.Current(context.Background())
	newQuote, err := refreshed.Prices.Resolve(pricing.Query{Provider: "openrouter", Family: "openai_chat", EndpointID: "openrouter", Region: "global",
		Model: "openai/gpt-5.4-mini", ProviderTier: "default", At: time.Now()})
	if err != nil || newQuote.Entry.Prices.InputPerMillion.CanonicalString() != "1" || newQuote.Entry.Version == quote.Entry.Version {
		t.Fatalf("refreshed quote = %+v, err = %v", newQuote, err)
	}
	if again, _ := snapshot.Prices.Resolve(pricing.Query{Provider: "openrouter", Family: "openai_chat", EndpointID: "openrouter", Region: "global",
		Model: "openai/gpt-5.4-mini", ProviderTier: "default", At: time.Now()}); again.Entry.Prices.InputPerMillion.CanonicalString() != "0.8" {
		t.Fatal("a refresh mutated an already captured snapshot")
	}
}

func TestModelSyncRoutesDirectEndpointFirstWhenEnabled(t *testing.T) {
	value := modelSyncTestConfig()
	syncRuntime, err := newModelSyncRuntime(value, modelSyncTestCatalogs(t), catalog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := syncRuntime.enable(map[string]provider.Adapter{"openrouter": nil, "openai-direct": nil, "configured": nil}); err != nil {
		t.Fatal(err)
	}
	document := modelSyncTestDocument()
	if err := syncRuntime.install(&document); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := syncRuntime.Current(context.Background())
	routes := snapshot.Routes.Models["openai/gpt-5.4-mini"].Routes
	if len(routes) != 2 || routes[0].EndpointID != "openai-direct" || routes[0].Model != "gpt-5.4-mini" || routes[1].EndpointID != "openrouter" {
		t.Fatalf("routes = %+v, want direct then OpenRouter", routes)
	}
	capabilities, ok := syncRuntime.capabilities("openai-direct")
	if !ok || capabilities.Version != "cap-v1" || capabilities.Features[provider.FeatureUsage].State != provider.CapabilityNative {
		t.Fatalf("sync endpoint capabilities = %+v", capabilities)
	}
}

func TestModelSyncRejectsADirectProviderWithoutRules(t *testing.T) {
	value := modelSyncTestConfig()
	value.ModelSync.Direct[0].Provider = "unknown"
	if _, err := newModelSyncRuntime(value, modelSyncTestCatalogs(t), catalog.Options{}); err == nil {
		t.Fatal("a direct provider without rules was accepted")
	}
}
