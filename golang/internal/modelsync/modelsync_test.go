package modelsync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

var fixtureTime = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

// fixtureServer serves the captured OpenRouter responses under testdata.
func fixtureServer(t *testing.T, requests *atomic.Int64) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if requests != nil {
			requests.Add(1)
		}
		name := "models.json"
		if path := strings.TrimPrefix(request.URL.Path, "/models/"); path != request.URL.Path {
			name = "endpoints-" + strings.ReplaceAll(strings.TrimSuffix(path, "/endpoints"), "/", "__") + ".json"
		}
		data, err := os.ReadFile(filepath.Join("testdata", "openrouter", name))
		if err != nil {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write(data)
	}))
	t.Cleanup(server.Close)
	return server
}

func fetchFixture(t *testing.T) Document {
	t.Helper()
	server := fixtureServer(t, nil)
	document, err := Fetcher{Client: server.Client(), BaseURL: server.URL, Clock: func() time.Time { return fixtureTime }}.Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	return document
}

func mergedRules(t *testing.T, layers ...string) Rules {
	t.Helper()
	builtIn, err := ParseRules(DefaultRulesDocument())
	if err != nil {
		t.Fatal(err)
	}
	documents := []RulesDocument{builtIn}
	for _, layer := range layers {
		document, err := ParseRules([]byte(layer))
		if err != nil {
			t.Fatal(err)
		}
		documents = append(documents, document)
	}
	rules, err := MergeRules(documents...)
	if err != nil {
		t.Fatal(err)
	}
	return rules
}

func TestFetchSkipsMediaUnpricedAliasAndVariantModels(t *testing.T) {
	document := fetchFixture(t)
	var ids []string
	for _, model := range document.Models {
		ids = append(ids, model.ID)
	}
	// The fixture also lists openai/gpt-5-image and openai/gpt-audio, whose
	// image and audio output is never routable.
	want := "anthropic/claude-sonnet-4.5,inference-net/schematron-v2-turbo,openai/gpt-5.4"
	if got := strings.Join(ids, ","); got != want {
		t.Fatalf("models = %s, want %s", got, want)
	}
	if !document.FetchedAt.Equal(fixtureTime) {
		t.Fatalf("FetchedAt = %v", document.FetchedAt)
	}
}

func TestFetchRejectsWhenTooManyEndpointLookupsFail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/models" {
			data, _ := os.ReadFile(filepath.Join("testdata", "openrouter", "models.json"))
			_, _ = writer.Write(data)
			return
		}
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	if _, err := (Fetcher{Client: server.Client(), BaseURL: server.URL}).Fetch(context.Background()); err == nil {
		t.Fatal("Fetch() succeeded although every endpoint lookup failed")
	}
}

func TestDocumentRoundTripsCanonically(t *testing.T) {
	document := fetchFixture(t)
	encoded, err := document.Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeDocument(encoded)
	if err != nil {
		t.Fatal(err)
	}
	again, err := decoded.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if Digest(encoded) != Digest(again) {
		t.Fatal("re-encoded Document digest changed")
	}
	if _, err := DecodeDocument(append(encoded[:len(encoded)-1], []byte(`,"extra":1}`)...)); err == nil {
		t.Fatal("DecodeDocument accepted an unknown field")
	}
}

func TestPerMillionMovesTheDecimalPointExactly(t *testing.T) {
	for input, want := range map[string]string{
		"0.0000025": "2.5", "0.000015": "15", "0": "0", "0.0000000325": "0.0325", "1": "1000000", "0.01": "10000",
	} {
		got, err := perMillion(input)
		if err != nil {
			t.Fatalf("perMillion(%q) error = %v", input, err)
		}
		if got.CanonicalString() != want {
			t.Fatalf("perMillion(%q) = %s, want %s", input, got.CanonicalString(), want)
		}
	}
	if _, err := perMillion("-1"); err == nil {
		t.Fatal("perMillion accepted a negative price")
	}
}

const testPrices = `{input_per_million: "1", output_per_million: "2", cache_read_per_million: "0.1", cache_write_per_million: "0", per_request: "0"}`

func TestRulesResolveDirectModelsAndMergeOverrides(t *testing.T) {
	rules := mergedRules(t, `
version: model-sync-rules/v1
exclude: ["openai/gpt-audio*"]
providers:
  anthropic:
    models:
      anthropic/claude-sonnet-4.5:
        prices:
          standard_only: {input_per_million: "2.7", output_per_million: "13.5", cache_read_per_million: "0.27", cache_write_per_million: "3.375", per_request: "0"}
      anthropic/claude-opus-4.5: {exclude: true}
      anthropic/claude-custom:
        model: claude-custom-20261001
        prices:
          standard_only: `+testPrices+`
`)
	sonnet, ok := rules.Direct("anthropic", "anthropic/claude-sonnet-4.5")
	if !ok || sonnet.Model != "claude-sonnet-4-5" || sonnet.ContextTokens != 200000 {
		t.Fatalf("sonnet = %+v, %v; want the built-in mapping and limits kept", sonnet, ok)
	}
	if sonnet.Prices["standard_only"].Input != "2.7" || sonnet.Prices["auto"].Input != "3" {
		t.Fatalf("sonnet prices = %+v; want one tier overridden and the other kept", sonnet.Prices)
	}
	if _, ok := rules.Direct("anthropic", "anthropic/claude-opus-4.5"); ok {
		t.Fatal("an excluded built-in model still has a direct route")
	}
	if custom, ok := rules.Direct("anthropic", "anthropic/claude-custom"); !ok || custom.Model != "claude-custom-20261001" {
		t.Fatalf("custom model = %+v, %v", custom, ok)
	}
	if gpt, ok := rules.Direct("openai", "openai/gpt-5.4"); !ok || gpt.Model != "gpt-5.4" || gpt.ContextTokens != 272000 {
		t.Fatalf("gpt-5.4 = %+v, %v; want the context capped below the long-prompt price", gpt, ok)
	}
	if _, ok := rules.Direct("anthropic", "openai/gpt-5.4"); ok {
		t.Fatal("a provider served a model outside its prefix")
	}
	if !rules.Excluded("openai/gpt-audio-mini") || rules.Excluded("openai/gpt-5.4") {
		t.Fatal("exclude pattern did not apply")
	}
}

func TestDefaultRulesPriceEveryDirectModelCompletely(t *testing.T) {
	rules := mergedRules(t)
	for name, provider := range rules.Providers {
		if len(provider.Models) == 0 {
			t.Fatalf("provider %q has no models", name)
		}
		for id, model := range provider.Models {
			if len(model.Prices) == 0 {
				t.Fatalf("%s has no prices", id)
			}
		}
	}
	exa, ok := rules.Direct("exa", "exa/exa")
	if !ok || exa.Model != "exa" || exa.Prices["standard"].PerRequest != "0.005" {
		t.Fatalf("exa = %+v, %v", exa, ok)
	}
	opus, _ := rules.Direct("anthropic", "anthropic/claude-opus-5.5")
	if got := opus.Prices["standard_only"]; got.Input != "4" || got.Output != "20" || got.CacheRead != "0.2" || got.CacheWrite != "5" {
		t.Fatalf("claude-opus-5.5 prices = %+v; want Anthropic's published $4/$20, $0.20 cache read, $5 cache write", got)
	}
}

func TestGenerateDefaultRulesFromFirstPartyEndpoints(t *testing.T) {
	document := fetchFixture(t)
	rendered, err := GenerateDefaultRules(document)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseRules(rendered)
	if err != nil {
		t.Fatal(err)
	}
	rules, err := MergeRules(parsed)
	if err != nil {
		t.Fatal(err)
	}
	gpt, ok := rules.Direct("openai", "openai/gpt-5.4")
	if !ok || gpt.ContextTokens != 272000 || gpt.Prices["flex"].Input != "1.25" || gpt.Prices["default"].Input != "2.5" || gpt.Prices["priority"].Input != "5" {
		t.Fatalf("generated gpt-5.4 = %+v", gpt)
	}
	sonnet, ok := rules.Direct("anthropic", "anthropic/claude-sonnet-4.5")
	if !ok || sonnet.ContextTokens != 200000 || sonnet.Prices["standard_only"].CacheWrite != "3.75" || sonnet.Prices["auto"].Output != "15" {
		t.Fatalf("generated sonnet = %+v", sonnet)
	}
	if _, ok := rules.Providers["anthropic"].Models["inference-net/schematron-v2-turbo"]; ok {
		t.Fatal("a third-party model was generated as a direct model")
	}
}

func TestRulesRejectInvalidDocuments(t *testing.T) {
	for name, document := range map[string]string{
		"version":        "version: v0\n",
		"unknown field":  "version: model-sync-rules/v1\nsurprise: true\n",
		"transform":      "version: model-sync-rules/v1\nproviders:\n  x:\n    prefix: x/\n    model_id: lowercase\n",
		"prefix":         "version: model-sync-rules/v1\nproviders:\n  x:\n    prefix: x\n    model_id: verbatim\n",
		"no prices":      "version: model-sync-rules/v1\nproviders:\n  x:\n    prefix: x/\n    model_id: verbatim\n    models:\n      x/m: {model: m}\n",
		"partial price":  "version: model-sync-rules/v1\nproviders:\n  x:\n    prefix: x/\n    model_id: verbatim\n    models:\n      x/m: {prices: {standard: {input_per_million: \"1\"}}}\n",
		"outside prefix": "version: model-sync-rules/v1\nproviders:\n  x:\n    prefix: x/\n    model_id: verbatim\n    models:\n      y/m: {prices: {standard: " + testPrices + "}}\n",
	} {
		document, err := ParseRules([]byte(document))
		if err == nil {
			_, err = MergeRules(document)
		}
		if err == nil {
			t.Fatalf("%s: rules unexpectedly accepted", name)
		}
	}
}

func testSource(endpointID, rulesProvider, family string, tiers map[llm.ServiceClass]string) Source {
	return Source{
		EndpointID: endpointID, RulesProvider: rulesProvider, Family: family, Region: "global", AccountRegion: "global", Tiers: tiers,
		Capabilities: routing.CapabilitySet{Version: endpointID + "-cap", Features: map[routing.Feature]routing.Capability{
			routing.FeatureText: {State: routing.CapabilityNative}, routing.FeatureToolCall: {State: routing.CapabilityNative},
			routing.FeatureStructuredOutput: {State: routing.CapabilityNative}, routing.FeatureReasoning: {State: routing.CapabilityNative},
		}},
	}
}

func priceFor(t *testing.T, entries []pricing.Entry, endpointID, model, tier string) pricing.Entry {
	t.Helper()
	for _, entry := range entries {
		if entry.EndpointID == endpointID && entry.Model == model && entry.ProviderTier == tier {
			return entry
		}
	}
	t.Fatalf("no price for %s %s %s", endpointID, model, tier)
	return pricing.Entry{}
}

func assertPrices(t *testing.T, entry pricing.Entry, input, output, cacheRead, cacheWrite string) {
	t.Helper()
	got := []string{entry.Prices.InputPerMillion.CanonicalString(), entry.Prices.OutputPerMillion.CanonicalString(), entry.Prices.CacheReadPerMillion.CanonicalString(), entry.Prices.CacheWritePerMillion.CanonicalString(), entry.Prices.ReasoningPerMillion.CanonicalString()}
	want := []string{input, output, cacheRead, cacheWrite, "0"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("%s %s prices = %v, want %v", entry.Model, entry.ProviderTier, got, want)
	}
}

func TestCompileRoutesDirectFirstAndPricesEveryRoute(t *testing.T) {
	document := fetchFixture(t)
	openrouter := testSource("openrouter", "", "openai_chat", map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"})
	openai := testSource("openai-direct", "openai", "openai_responses", map[llm.ServiceClass]string{llm.ServiceClassEconomy: "flex", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: "priority"})
	anthropic := testSource("anthropic-direct", "anthropic", "anthropic_messages", map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard_only", llm.ServiceClassPriority: "auto"})
	exa := testSource("exa-direct", "exa", "openai_chat", map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"})
	compiled, err := Compile(Input{Document: &document, Rules: mergedRules(t), OpenRouter: &openrouter, Direct: []Source{openai, anthropic, exa},
		Reserved: map[string]struct{}{"inference-net/schematron-v2-turbo": {}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, present := compiled.Models["inference-net/schematron-v2-turbo"]; present {
		t.Fatal("a configured model name was overridden by a synced model")
	}

	sonnet := compiled.Models["anthropic/claude-sonnet-4.5"]
	if len(sonnet.Routes) != 2 || sonnet.Routes[0].EndpointID != "anthropic-direct" || sonnet.Routes[1].EndpointID != "openrouter" {
		t.Fatalf("sonnet routes = %+v, want direct then OpenRouter", sonnet.Routes)
	}
	direct := sonnet.Routes[0]
	if direct.Model != "claude-sonnet-4-5" || direct.ContextTokens != 200000 || direct.OutputTokens != 64000 || direct.PriceVersion != "" {
		t.Fatalf("direct sonnet route = model %q context %d output %d price version %q", direct.Model, direct.ContextTokens, direct.OutputTokens, direct.PriceVersion)
	}
	assertPrices(t, priceFor(t, compiled.Prices, "anthropic-direct", "claude-sonnet-4-5", "standard_only"), "3", "15", "0.3", "3.75")
	// OpenRouter may pick any upstream and any long-prompt tier, so it
	// reserves at the componentwise maximum.
	assertPrices(t, priceFor(t, compiled.Prices, "openrouter", "anthropic/claude-sonnet-4.5", "default"), "6.6", "24.75", "0.66", "13.2")
	if sonnet.Routes[1].Model != "anthropic/claude-sonnet-4.5" || sonnet.Routes[1].ContextTokens != 1000000 {
		t.Fatalf("OpenRouter sonnet route = %+v", sonnet.Routes[1])
	}

	gpt := compiled.Models["openai/gpt-5.4"]
	if len(gpt.Routes) != 2 || gpt.Routes[0].Model != "gpt-5.4" || len(gpt.Routes[0].Classes) != 3 || gpt.Routes[0].ContextTokens != 272000 {
		t.Fatalf("gpt direct route = %+v", gpt.Routes[0])
	}
	// A model the rules price but OpenRouter did not list routes directly.
	if routes := compiled.Models["openai/gpt-5.5"].Routes; len(routes) != 1 || routes[0].EndpointID != "openai-direct" {
		t.Fatalf("rules-only model routes = %+v", routes)
	}
	assertPrices(t, priceFor(t, compiled.Prices, "openai-direct", "gpt-5.4", "flex"), "1.25", "7.5", "0.125", "0")
	assertPrices(t, priceFor(t, compiled.Prices, "openai-direct", "gpt-5.4", "default"), "2.5", "15", "0.25", "0")
	assertPrices(t, priceFor(t, compiled.Prices, "openai-direct", "gpt-5.4", "priority"), "5", "30", "0.5", "0")

	exaModel := compiled.Models["exa/exa"]
	if len(exaModel.Routes) != 1 || exaModel.Routes[0].Model != "exa" {
		t.Fatalf("exa routes = %+v", exaModel.Routes)
	}
	if got := priceFor(t, compiled.Prices, "exa-direct", "exa", "standard").Prices.PerRequest.CanonicalString(); got != "0.005" {
		t.Fatalf("exa per-request price = %s", got)
	}
	if _, err := pricing.CompileUSD("test", compiled.Prices); err != nil {
		t.Fatalf("compiled prices do not form a valid catalog: %v", err)
	}
	if _, err := routing.CompileCatalog("test", compiled.Models); err != nil {
		t.Fatalf("compiled routes do not form a valid catalog: %v", err)
	}
}

func TestCompileNarrowsFeaturesTheModelCannotAccept(t *testing.T) {
	document := fetchFixture(t)
	openrouter := testSource("openrouter", "", "openai_chat", map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"})
	compiled, err := Compile(Input{Document: &document, Rules: mergedRules(t), OpenRouter: &openrouter})
	if err != nil {
		t.Fatal(err)
	}
	route := compiled.Models["inference-net/schematron-v2-turbo"].Routes[0]
	features := route.Capabilities.Features
	if features[routing.FeatureToolCall].State != routing.CapabilityUnsupported || features[routing.FeatureReasoning].State != routing.CapabilityUnsupported {
		t.Fatalf("features = %+v, want tools and reasoning unsupported", features)
	}
	if features[routing.FeatureStructuredOutput].State != routing.CapabilityNative || route.Capabilities.Version != "openrouter-cap" {
		t.Fatalf("narrowing changed a supported feature or the capability version: %+v", route.Capabilities)
	}
	if openrouter.Capabilities.Features[routing.FeatureToolCall].State != routing.CapabilityNative {
		t.Fatal("narrowing mutated the shared source capabilities")
	}
}

func TestCompileWithoutDocumentRoutesOnlyDirectModels(t *testing.T) {
	exa := testSource("exa-direct", "exa", "openai_chat", map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"})
	anthropic := testSource("anthropic-direct", "anthropic", "anthropic_messages", map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard_only"})
	openrouter := testSource("openrouter", "", "openai_chat", map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"})
	compiled, err := Compile(Input{Rules: mergedRules(t), OpenRouter: &openrouter, Direct: []Source{exa, anthropic}})
	if err != nil {
		t.Fatal(err)
	}
	for id, model := range compiled.Models {
		for _, route := range model.Routes {
			if route.EndpointID == "openrouter" {
				t.Fatalf("model %q has an OpenRouter route before any Document", id)
			}
		}
	}
	if len(compiled.Models["exa/exa"].Routes) != 1 || len(compiled.Models["anthropic/claude-opus-5.5"].Routes) != 1 {
		t.Fatalf("direct models missing before the first Document: %d models", len(compiled.Models))
	}
}

func TestRefresherFetchesOnceAcrossWorkersAndInstallsEverywhere(t *testing.T) {
	var requests atomic.Int64
	server := fixtureServer(t, &requests)
	store := NewMemoryStore(func() time.Time { return fixtureTime })
	now := fixtureTime
	clock := func() time.Time { return now }
	fetches := 0
	installed := map[int]int{}
	newWorker := func(id int) *Refresher {
		return &Refresher{
			Store: store,
			Fetch: func(ctx context.Context) (Document, error) {
				fetches++
				return Fetcher{Client: server.Client(), BaseURL: server.URL, Clock: clock}.Fetch(ctx)
			},
			Install:     func(Document) error { installed[id]++; return nil },
			MinInterval: 55 * time.Minute, MaxInterval: 65 * time.Minute, Clock: clock,
		}
	}
	first, second := newWorker(1), newWorker(2)
	if err := first.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The second worker's timer fires shortly after: the Document is fresh,
	// so it installs instead of fetching again.
	now = now.Add(10 * time.Minute)
	if err := second.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fetches != 1 || installed[1] != 1 || installed[2] != 1 {
		t.Fatalf("fetches = %d installed = %v, want one fetch installed by both", fetches, installed)
	}
	// Once the interval has passed, the next timer refetches.
	now = now.Add(50 * time.Minute)
	if err := second.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fetches != 2 || installed[2] != 2 {
		t.Fatalf("fetches = %d installed = %v after the interval, want a second fetch installed", fetches, installed)
	}
	// Polling an unchanged publication does not reinstall it.
	if _, err := second.Sync(context.Background()); err != nil || installed[2] != 2 {
		t.Fatalf("Sync() reinstalled an unchanged Document: %v %v", installed, err)
	}
}

func TestRefresherKeepsThePublishedDocumentWhenAFetchFails(t *testing.T) {
	store := NewMemoryStore(nil)
	good := fetchFixture(t)
	encoded, _ := good.Encode()
	if _, err := store.Publish(context.Background(), Digest(encoded), good.FetchedAt, encoded); err != nil {
		t.Fatal(err)
	}
	refresher := &Refresher{Store: store, MinInterval: time.Hour, MaxInterval: time.Hour,
		Clock:   func() time.Time { return fixtureTime.Add(2 * time.Hour) },
		Fetch:   func(context.Context) (Document, error) { return Document{}, context.DeadlineExceeded },
		Install: func(Document) error { return nil }}
	if err := refresher.Refresh(context.Background()); err == nil {
		t.Fatal("Refresh() hid a fetch failure")
	}
	if digest, _ := store.LatestDigest(context.Background()); digest != Digest(encoded) {
		t.Fatal("a failed fetch replaced the published Document")
	}
	// The lease was released, so the next attempt is not blocked.
	if acquired, _ := store.AcquireRefresh(context.Background(), "next", time.Minute); !acquired {
		t.Fatal("refresh lease was not released after a failed fetch")
	}
}

func TestMemoryStoreNeverPublishesAnOlderDocument(t *testing.T) {
	store := NewMemoryStore(nil)
	ctx := context.Background()
	if published, _ := store.Publish(ctx, "new", fixtureTime, []byte("new")); !published {
		t.Fatal("first publish rejected")
	}
	if published, _ := store.Publish(ctx, "old", fixtureTime.Add(-time.Minute), []byte("old")); published {
		t.Fatal("an older fetch replaced a newer Document")
	}
	if published, _ := store.Publish(ctx, "same", fixtureTime, []byte("same")); published {
		t.Fatal("a fetch with the same time replaced the Document")
	}
	if acquired, _ := store.AcquireRefresh(ctx, "a", time.Minute); !acquired {
		t.Fatal("lease not acquired")
	}
	if acquired, _ := store.AcquireRefresh(ctx, "b", time.Minute); acquired {
		t.Fatal("lease acquired twice")
	}
	_ = store.ReleaseRefresh(ctx, "b")
	if acquired, _ := store.AcquireRefresh(ctx, "c", time.Minute); acquired {
		t.Fatal("a non-owner released the lease")
	}
}

func TestRefreshIntervalStaysWithinBounds(t *testing.T) {
	refresher := &Refresher{MinInterval: 55 * time.Minute, MaxInterval: 65 * time.Minute}
	for range 200 {
		interval := refresher.interval()
		if interval < 55*time.Minute || interval > 65*time.Minute {
			t.Fatalf("interval %v outside [55m, 65m]", interval)
		}
	}
}
