package openaichat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
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
	profile, err := NewOpenRouterProfile(OpenRouterProfileConfig{ID: "or", CapabilityVersion: "or/v1", BaseURL: openRouterBaseURL, Capabilities: profileTestCapabilities("or/v1"), ServiceTiers: map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""}, ProviderOrder: []string{"ProviderA"}, RequireParameters: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = lowerRequest(llm.Request{Model: "model", Extensions: map[string]json.RawMessage{"openrouter": json.RawMessage(`{"provider_order":["Caller"]}`)}}, profile, "standard")
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("provider override error = %v", err)
	}
}

func TestOpenRouterUpstreamSelectionSendsOnlyRequireParameters(t *testing.T) {
	tiers := map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: ""}
	profile, err := NewOpenRouterProfile(OpenRouterProfileConfig{ID: "or", CapabilityVersion: "or/v1", BaseURL: openRouterBaseURL, Capabilities: profileTestCapabilities("or/v1"), ServiceTiers: tiers, SelectUpstream: true, RequireParameters: true})
	if err != nil {
		t.Fatal(err)
	}
	params, err := lowerRequest(llm.Request{Model: "openai/gpt-5.4"}, profile, "default")
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
	providerBody, ok := wire["provider"].(map[string]any)
	if !ok || len(providerBody) != 1 || providerBody["require_parameters"] != true {
		t.Fatalf("provider routing body = %#v, want only require_parameters", wire["provider"])
	}
	// Callers cannot steer the upstream: model sync reserves at the most
	// expensive one, but an explicit order would bypass require_parameters.
	if _, err := lowerRequest(llm.Request{Model: "openai/gpt-5.4", Extensions: map[string]json.RawMessage{"openrouter": json.RawMessage(`{"provider_order":["Caller"]}`)}}, profile, "default"); err == nil {
		t.Fatal("upstream selection accepted a caller provider order")
	}
	if _, err := NewOpenRouterProfile(OpenRouterProfileConfig{ID: "or", CapabilityVersion: "or/v1", BaseURL: openRouterBaseURL, Capabilities: profileTestCapabilities("or/v1"), ServiceTiers: tiers, SelectUpstream: true, ProviderOrder: []string{"ProviderA"}, RequireParameters: true}); err == nil {
		t.Fatal("upstream selection accepted a pinned provider order")
	}
}
