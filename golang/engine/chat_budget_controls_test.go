package engine

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/openaichat"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

func TestChatChoiceCountRejectsBeforeAdmissionAndDispatch(t *testing.T) {
	for _, count := range []string{`2`, `-1`, `null`, `true`, `"1"`, `1.5`, `1`} {
		t.Run(count, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				var wire map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&wire); err != nil {
					t.Error(err)
				}
				if string(wire["n"]) != "1" || string(wire["max_completion_tokens"]) != "16" {
					t.Errorf("wire limits: n=%s cap=%s", wire["n"], wire["max_completion_tokens"])
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"chatcmpl-budget","object":"chat.completion","created":1700000000,"model":"logical-model","service_tier":"default","choices":[{"index":0,"message":{"role":"assistant","content":"world"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
			}))
			defer server.Close()
			client, err := openaichat.NewClient(openaichat.ClientConfig{BaseURL: server.URL + "/v1", APIKey: "test", HTTPClient: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			features := map[provider.Feature]provider.Capability{}
			for _, feature := range []provider.Feature{provider.FeatureText, provider.FeatureImage, provider.FeatureDocument, provider.FeatureToolCall, provider.FeatureStructuredOutput, provider.FeatureReasoning, provider.FeatureContinuation, provider.FeatureStreaming, provider.FeatureUsage} {
				features[feature] = provider.Capability{State: provider.CapabilityUnsupported, Reason: "test profile"}
			}
			features[provider.FeatureText] = provider.Capability{State: provider.CapabilityNative}
			features[provider.FeatureToolCall] = provider.Capability{State: provider.CapabilityNative}
			features[provider.FeatureUsage] = provider.Capability{State: provider.CapabilityNative}
			adapter, err := openaichat.New(client, "endpoint-1", openaichat.Profile{ID: "chat-budget", CapabilityVersion: "chat-budget/v1", Capabilities: provider.CapabilitySet{Version: "chat-budget/v1", Features: features}, ServiceTiers: map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: ""}, ActualServiceClasses: map[string]llm.ServiceClass{"default": llm.ServiceClassStandard}, AllowedExtensions: map[string]openaichat.ExtensionSpec{"choices": {Fields: map[string]string{"count": "n"}}}})
			if err != nil {
				t.Fatal(err)
			}
			harness := newHarness(t, adapter)
			recording := &recordingAdmission{AdmissionStore: harness.admission}
			harness.engine.dependencies.Admission = recording
			harness.engine.dependencies.Estimator.MaxOutput = 16
			routes, err := routing.CompileCatalog("chat-routes", map[string]routing.Model{"logical-model": {Routes: []routing.Route{{ID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1", Family: string(provider.FamilyOpenAIChat), Region: "us-east-1", AccountRegion: "us-east-1", Model: "logical-model", ModelLineage: "provider-lineage", Classes: []llm.ServiceClass{llm.ServiceClassStandard}, ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"}, PriceVersion: "chat-prices", PriceAvailable: true, ExtensionNames: []string{"choices"}, Capabilities: routing.CapabilitySet{Version: "chat-route/v1", Features: map[routing.Feature]routing.Capability{routing.FeatureText: {State: routing.CapabilityNative}}}}}}})
			if err != nil {
				t.Fatal(err)
			}
			prices, err := pricing.CompileUSD("chat-prices", []pricing.Entry{{Provider: "provider-1", Family: string(provider.FamilyOpenAIChat), EndpointID: "endpoint-1", Region: "us-east-1", Model: "logical-model", ProviderTier: "default", Version: "chat-prices", Prices: pricing.UnitPrices{PerRequest: pricing.MustDecimalUSD("0.000001"), OutputPerMillion: pricing.MustDecimalUSD("1")}}})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := harness.engine.dependencies.Snapshots.(StaticSnapshot)
			snapshot.Value.Routes = routes
			snapshot.Value.Prices = pricing.NewResolver(prices)
			harness.engine.dependencies.Snapshots = snapshot
			request := baseRequest("chat-choice-" + count)
			request.ServiceClass = llm.ServiceClassStandard
			request.Extensions = map[string]json.RawMessage{"choices": json.RawMessage(`{"count":` + count + `}`)}
			_, err = harness.engine.Generate(context.Background(), request)
			if count == "1" {
				if err != nil {
					t.Fatal(err)
				}
				if recording.begin.ReservationUSD.Cmp(pricing.MustUSD("0.000017")) != 0 {
					t.Fatalf("reservation = %s, want single request plus 16 output tokens", recording.begin.ReservationUSD)
				}
				if requests.Load() != 1 || recording.begin.ID == "" {
					t.Fatal("valid single-choice request did not reserve and dispatch once")
				}
			} else {
				if err == nil {
					t.Fatal("unsupported choice count succeeded")
				}
				if requests.Load() != 0 || recording.begin.ID != "" {
					t.Fatal("rejected choice count reserved or dispatched")
				}
			}
		})
	}
}
