package anthropicmessages

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// A priority request is Anthropic fast mode: standard capacity at fast speed,
// sent under the fast-mode beta and classified and priced by usage.speed.
func TestPriorityInvokesFastMode(t *testing.T) {
	for _, tc := range []struct {
		class     llm.ServiceClass
		usage     string
		wantBeta  bool
		wantSpeed any
		wantTier  string
		actual    llm.ServiceClass
	}{
		{class: llm.ServiceClassPriority, usage: `"service_tier":"standard","speed":"fast"`, wantBeta: true, wantSpeed: "fast", wantTier: FastTier, actual: llm.ServiceClassPriority},
		{class: llm.ServiceClassStandard, usage: `"service_tier":"standard","speed":"standard"`, wantTier: "standard", actual: llm.ServiceClassStandard},
	} {
		t.Run(string(tc.class), func(t *testing.T) {
			var beta string
			var body map[string]any
			client, err := NewClient(ClientConfig{BaseURL: "http://127.0.0.1", APIKey: "test-key", HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				beta = request.Header.Get("anthropic-beta")
				encoded, _ := io.ReadAll(request.Body)
				if err := json.Unmarshal(encoded, &body); err != nil {
					t.Error(err)
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"msg-fast","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"model":"claude-contract","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1,` + tc.usage + `}}`)), Request: request}, nil
			})}})
			if err != nil {
				t.Fatal(err)
			}
			profile := testProfile()
			profile.ExpectedBaseURL = ""
			adapter, err := New(client, "anthropic-prod", profile)
			if err != nil {
				t.Fatal(err)
			}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{
				Request: llm.Request{OperationKey: "fast-" + string(tc.class), Model: "claude-contract", ServiceClass: tc.class, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hi"}}}}},
				Query:   provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := adapter.Invoke(context.Background(), call, nil)
			if err != nil {
				t.Fatal(err)
			}
			if (beta == fastModeBeta) != tc.wantBeta || body["speed"] != tc.wantSpeed || body["service_tier"] != "standard_only" {
				t.Fatalf("beta = %q, body speed = %v, service_tier = %v", beta, body["speed"], body["service_tier"])
			}
			service := result.Response.Service
			if service.Actual == nil || *service.Actual != tc.actual || service.ProviderValue != tc.wantTier {
				t.Fatalf("service = %#v", service)
			}
			if result.Response.Cost.Status == llm.CostStatusUnknown {
				t.Fatalf("fast tier is priced by its own catalog entry, cost = %#v", result.Response.Cost)
			}
		})
	}
}

// Fast-mode usage on a call that did not attempt the fast tier still carries
// a multiplier its catalog entry does not model.
func TestFastUsageOutsideFastTierStaysUnknown(t *testing.T) {
	var message anthropic.Message
	if err := json.Unmarshal([]byte(`{"id":"msg-synthetic","type":"message","role":"assistant","model":"claude-contract","stop_reason":"end_turn","content":[{"type":"text","text":"answer"}],"usage":{"input_tokens":1,"output_tokens":1,"service_tier":"standard","speed":"fast","inference_geo":"us"}}`), &message); err != nil {
		t.Fatal(err)
	}
	for _, tier := range []string{"standard_only", FastTier} {
		got, err := mustProfile(t, testProfile()).liftResponse(provider.Call{
			OperationKey: "fast-usage", Model: "claude-contract", ServiceClass: llm.ServiceClassStandard, Metadata: provider.CallMetadata{ProviderTier: tier},
		}, &message, "req-synthetic")
		if err != nil {
			t.Fatal(err)
		}
		// US-only inference is a multiplier on every tier.
		if got.Cost.Status != llm.CostStatusUnknown {
			t.Fatalf("tier %s: cost = %#v, want unknown", tier, got.Cost)
		}
	}
	if !priceModifiedUsage(map[string]json.RawMessage{"speed": json.RawMessage(`"fast"`)}, "standard_only") {
		t.Fatal("fast usage on a standard call is price-modified")
	}
	if priceModifiedUsage(map[string]json.RawMessage{"speed": json.RawMessage(`"fast"`)}, FastTier) {
		t.Fatal("fast usage on a fast call is priced by the fast entry")
	}
}
