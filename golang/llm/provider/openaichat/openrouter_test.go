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
	if !ok || providerBody["allow_fallbacks"] != false || providerBody["require_parameters"] != true || providerBody["data_collection"] != "deny" {
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

func TestOpenRouterProfileLowersExplicitCompatibleWireShape(t *testing.T) {
	profile, err := NewOpenRouterProfile(OpenRouterProfileConfig{
		ID:                        "or-compatible",
		CapabilityVersion:         "or-compatible/v1",
		BaseURL:                   openRouterBaseURL,
		Model:                     "router-model",
		Capabilities:              profileTestCapabilities("or-compatible/v1"),
		ServiceTiers:              map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""},
		MissingActualServiceClass: llm.ServiceClassStandard,
		ProviderOrder:             []string{"ProviderA"},
		RequireParameters:         true,
		WireShape: WireShape{
			OutputTokenLimitField: OutputTokenLimitFieldMaxTokens,
			Store:                 DefaultFalseFieldUnsupported,
			ParallelToolCalls:     DefaultFalseFieldUnsupported,
			OmitServiceTier:       true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	maxTokens := 256
	params, err := lowerRequest(llm.Request{
		Model: "router-model",
		Output: &llm.OutputSpec{
			MaxTokens: &maxTokens,
			Format: llm.OutputFormat{
				Kind:   llm.OutputKindJSONSchema,
				Name:   "answer",
				Strict: true,
				Schema: json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}`),
			},
		},
		ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNone},
	}, profile, "standard")
	if err != nil {
		t.Fatal(err)
	}
	wire := marshalWire(t, params)
	if wire["max_tokens"] != float64(maxTokens) {
		t.Fatalf("max_tokens = %#v, wire = %#v", wire["max_tokens"], wire)
	}
	if responseFormat, ok := wire["response_format"].(map[string]any); !ok || responseFormat["type"] != "json_schema" {
		t.Fatalf("response_format = %#v, wire = %#v", wire["response_format"], wire)
	}
	for _, field := range []string{"max_completion_tokens", "store", "parallel_tool_calls", "service_tier"} {
		if _, exists := wire[field]; exists {
			t.Fatalf("unsupported field %q was emitted: %#v", field, wire)
		}
	}
	providerBody := wire["provider"].(map[string]any)
	if providerBody["allow_fallbacks"] != false || providerBody["require_parameters"] != true || providerBody["data_collection"] != "deny" {
		t.Fatalf("provider routing body = %#v", providerBody)
	}
}

func TestOpenRouterProfileRejectsUnsupportedWireShape(t *testing.T) {
	_, err := NewOpenRouterProfile(OpenRouterProfileConfig{
		ID:                        "or-invalid-shape",
		CapabilityVersion:         "or-invalid-shape/v1",
		BaseURL:                   openRouterBaseURL,
		Capabilities:              profileTestCapabilities("or-invalid-shape/v1"),
		ServiceTiers:              map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""},
		MissingActualServiceClass: llm.ServiceClassStandard,
		ProviderOrder:             []string{"ProviderA"},
		RequireParameters:         true,
		WireShape:                 WireShape{OutputTokenLimitField: OutputTokenLimitField("future_tokens")},
	})
	if err == nil || !strings.Contains(err.Error(), "output token limit field") {
		t.Fatalf("unsupported wire shape error = %v", err)
	}
}

func TestOpenRouterProfileRejectsRequiredUnsupportedTrueField(t *testing.T) {
	profile, err := NewOpenRouterProfile(OpenRouterProfileConfig{
		ID:                        "or-no-parallel",
		CapabilityVersion:         "or-no-parallel/v1",
		BaseURL:                   openRouterBaseURL,
		Capabilities:              profileTestCapabilities("or-no-parallel/v1"),
		ServiceTiers:              map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""},
		MissingActualServiceClass: llm.ServiceClassStandard,
		ProviderOrder:             []string{"ProviderA"},
		RequireParameters:         true,
		WireShape:                 WireShape{ParallelToolCalls: DefaultFalseFieldUnsupported},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = lowerRequest(llm.Request{
		Model: "router-model",
		Tools: []llm.Tool{{
			Name:        "lookup",
			InputSchema: json.RawMessage(`{"type":"object"}`),
		}},
		ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceRequired, Parallel: true},
	}, profile, "standard")
	if err == nil || !strings.Contains(err.Error(), "does not support parallel_tool_calls") {
		t.Fatalf("parallel_tool_calls error = %v", err)
	}
}
