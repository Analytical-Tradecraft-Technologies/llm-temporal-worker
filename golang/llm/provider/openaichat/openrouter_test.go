package openaichat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestOpenRouterPinsProviderRoutingAndPricing(t *testing.T) {
	var got *http.Request
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		got = request
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"or-req-1"}},
			Body:       io.NopCloser(strings.NewReader(`{"id":"gen-1","model":"router-model","service_tier":"default","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6,"cost":0.00000123,"cost_details":{"upstream_inference_cost":0.00000123}}}`)),
			Request:    request,
		}, nil
	})
	client, err := NewOpenRouterClient(OpenRouterClientConfig{BaseURL: openRouterBaseURL, APIKey: "or-key", HTTPReferer: "https://client.example", Title: "contract", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := NewOpenRouterProfile(OpenRouterProfileConfig{
		ID:                        "openrouter-pinned",
		CapabilityVersion:         "openrouter/v1",
		BaseURL:                   openRouterBaseURL,
		Model:                     "router-model",
		Capabilities:              profileTestCapabilities("openrouter/v1"),
		ServiceTiers:              map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""},
		ActualServiceClasses:      map[string]llm.ServiceClass{"default": llm.ServiceClassStandard, "standard": llm.ServiceClassStandard, "priority": llm.ServiceClassPriority},
		MissingActualServiceClass: llm.ServiceClassStandard,
		ProviderOrder:             []string{"ProviderA", "ProviderB"},
		RequireParameters:         true,
		Upstreams:                 allModelsUpstreams{"ProviderA", "ProviderB/fp8", "ProviderC"},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openrouter-a", profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "or-op", Model: "router-model"}, Query: provider.CapabilityQuery{EndpointID: "openrouter-a", Family: provider.FamilyOpenAIChat, Model: "router-model"}, Strict: true})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	providerBody, ok := wire["provider"].(map[string]any)
	if !ok || providerBody["allow_fallbacks"] != false || providerBody["require_parameters"] != true {
		t.Fatalf("provider routing body = %#v", wire["provider"])
	}
	if got.Header.Get("Authorization") != "Bearer or-key" || got.Header.Get("HTTP-Referer") != "https://client.example" || got.Header.Get("X-OpenRouter-Title") != "contract" {
		t.Fatalf("openrouter headers = %#v", got.Header)
	}
	if result.Response.Provider.GenerationID != "gen-1" || result.Response.Cost.ActualCostUSD == nil || result.Response.Cost.ActualCostUSD.String() != "0.000001230000000000" {
		t.Fatalf("openrouter metadata = %#v", result.Response)
	}
}

func TestParseDecimalUSDAcceptsExponentForm(t *testing.T) {
	amount, err := parseDecimalUSD(json.RawMessage(`1.23e-7`), "openrouter_cost")
	if err != nil {
		t.Fatal(err)
	}
	if got := amount.String(); got != "0.000000123000000000" {
		t.Fatalf("exponent cost = %s, want 0.000000123000000000", got)
	}
}

func TestOpenRouterRejectsCallerProviderOverride(t *testing.T) {
	profile, err := NewOpenRouterProfile(OpenRouterProfileConfig{ID: "or", CapabilityVersion: "or/v1", BaseURL: openRouterBaseURL, Capabilities: profileTestCapabilities("or/v1"), ServiceTiers: map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""}, ProviderOrder: []string{"ProviderA"}, RequireParameters: true, Upstreams: allModelsUpstreams{"ProviderA"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = lowerRequest(llm.Request{Model: "model", Extensions: map[string]json.RawMessage{"openrouter": json.RawMessage(`{"provider_order":["Caller"]}`)}}, profile, "standard")
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("provider override error = %v", err)
	}
}

func openRouterProviderBody(t *testing.T, profile Profile, model string, class llm.ServiceClass) map[string]any {
	t.Helper()
	lowering, err := profile.forRequest(model, class, nil)
	if err != nil {
		t.Fatal(err)
	}
	params, err := lowerRequest(llm.Request{Model: model}, lowering, "default")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	body, ok := wire["provider"].(map[string]any)
	if !ok {
		t.Fatalf("provider routing body = %#v", wire["provider"])
	}
	return body
}

func TestOpenRouterUpstreamSelectionNamesAllowedEndpointsAndSortsByClass(t *testing.T) {
	tiers := map[llm.ServiceClass]string{llm.ServiceClassEconomy: "default", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: "default"}
	upstreams := StaticOpenRouterUpstreams{"deepseek/deepseek-v3.2": {Tags: []string{"deepinfra/fp8", "novita/fp8"}, Quantizations: []string{"bf16", "fp8"}}}
	profile, err := NewOpenRouterProfile(OpenRouterProfileConfig{ID: "or", CapabilityVersion: "or/v1", BaseURL: openRouterBaseURL, Capabilities: profileTestCapabilities("or/v1"), ServiceTiers: tiers, SelectUpstream: true, RequireParameters: true,
		Upstreams: upstreams, ZeroRetention: true, DenyPromptTraining: true})
	if err != nil {
		t.Fatal(err)
	}
	for class, sort := range map[llm.ServiceClass]string{llm.ServiceClassEconomy: "price", llm.ServiceClassStandard: "price", llm.ServiceClassPriority: "throughput"} {
		body := openRouterProviderBody(t, profile, "deepseek/deepseek-v3.2", class)
		want := map[string]any{"only": []any{"deepinfra/fp8", "novita/fp8"}, "quantizations": []any{"bf16", "fp8"}, "sort": sort,
			"zdr": true, "data_collection": "deny", "require_parameters": true}
		if !reflect.DeepEqual(body, want) {
			t.Fatalf("%s provider body = %#v, want %#v", class, body, want)
		}
	}
	// A model without allowed endpoints is never sent.
	if _, err := profile.forRequest("vendor/unchecked", llm.ServiceClassStandard, nil); err == nil {
		t.Fatal("a model without allowed upstream endpoints was lowered")
	}
	// Callers cannot steer the upstream: an explicit order would bypass the
	// allowed endpoints and the price reservation.
	lowering, err := profile.forRequest("deepseek/deepseek-v3.2", llm.ServiceClassStandard, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lowerRequest(llm.Request{Model: "deepseek/deepseek-v3.2", Extensions: map[string]json.RawMessage{"openrouter": json.RawMessage(`{"provider_order":["Caller"]}`)}}, lowering, "default"); err == nil {
		t.Fatal("upstream selection accepted a caller provider order")
	}
	if _, err := NewOpenRouterProfile(OpenRouterProfileConfig{ID: "or", CapabilityVersion: "or/v1", BaseURL: openRouterBaseURL, Capabilities: profileTestCapabilities("or/v1"), ServiceTiers: tiers, SelectUpstream: true, ProviderOrder: []string{"ProviderA"}, RequireParameters: true, Upstreams: upstreams}); err == nil {
		t.Fatal("upstream selection accepted a pinned provider order")
	}
	if _, err := NewOpenRouterProfile(OpenRouterProfileConfig{ID: "or", CapabilityVersion: "or/v1", BaseURL: openRouterBaseURL, Capabilities: profileTestCapabilities("or/v1"), ServiceTiers: tiers, SelectUpstream: true, RequireParameters: true}); err == nil {
		t.Fatal("a profile without upstreams was accepted")
	}
}

func TestOpenRouterPinnedOrderIsLimitedToAllowedEndpoints(t *testing.T) {
	tiers := map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""}
	upstreams := StaticOpenRouterUpstreams{
		"vendor/model":    {Tags: []string{"provider-a/fp8", "provider-b", "provider-c"}},
		"vendor/excluded": {Tags: []string{"provider-c"}},
	}
	profile, err := NewOpenRouterProfile(OpenRouterProfileConfig{ID: "or", CapabilityVersion: "or/v1", BaseURL: openRouterBaseURL, Capabilities: profileTestCapabilities("or/v1"), ServiceTiers: tiers,
		ProviderOrder: []string{"provider-a", "provider-b"}, RequireParameters: true, Upstreams: upstreams})
	if err != nil {
		t.Fatal(err)
	}
	body := openRouterProviderBody(t, profile, "vendor/model", llm.ServiceClassStandard)
	want := map[string]any{"order": []any{"provider-a", "provider-b"}, "only": []any{"provider-a/fp8", "provider-b"}, "allow_fallbacks": false, "require_parameters": true}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("provider body = %#v, want %#v", body, want)
	}
	if _, err := profile.forRequest("vendor/excluded", llm.ServiceClassStandard, nil); err == nil {
		t.Fatal("a pinned order with no allowed endpoint was lowered")
	}
}

// allModelsUpstreams allows the same endpoint tags for every model.
type allModelsUpstreams []string

func (tags allModelsUpstreams) OpenRouterUpstream(string) (OpenRouterUpstream, bool) {
	return OpenRouterUpstream{Tags: append([]string(nil), tags...)}, true
}

func TestOpenRouterSendsTheReservedPriceCeiling(t *testing.T) {
	tiers := map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: ""}
	profile, err := NewOpenRouterProfile(OpenRouterProfileConfig{ID: "or", CapabilityVersion: "or/v1", BaseURL: openRouterBaseURL, Capabilities: profileTestCapabilities("or/v1"), ServiceTiers: tiers, SelectUpstream: true, RequireParameters: true,
		Upstreams: StaticOpenRouterUpstreams{"vendor/model": {Tags: []string{"provider-a"}}}})
	if err != nil {
		t.Fatal(err)
	}
	lowering, err := profile.forRequest("vendor/model", llm.ServiceClassStandard, &provider.PriceCeiling{PromptPerMillion: "0.26", CompletionPerMillion: "1.5", PerRequest: "0"})
	if err != nil {
		t.Fatal(err)
	}
	params, err := lowerRequest(llm.Request{Model: "vendor/model"}, lowering, "default")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	// Exact decimals travel as JSON numbers, never through a float.
	if !strings.Contains(string(encoded), `"max_price":{"completion":1.5,"prompt":0.26,"request":0}`) {
		t.Fatalf("wire body %s lacks the price ceiling", encoded)
	}
	if _, err := profile.forRequest("vendor/model", llm.ServiceClassStandard, &provider.PriceCeiling{PromptPerMillion: "x", CompletionPerMillion: "1", PerRequest: "0"}); err == nil {
		t.Fatal("a malformed price ceiling was sent")
	}
}
