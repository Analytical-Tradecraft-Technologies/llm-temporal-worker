//go:build cloudworkflowintegration && openaismoke

package runtime

import (
	"context"
	"errors"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/openairesponses"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// This paid gate uses real Temporal, Redis and OpenAI. The generic KV/blob
// substrate remains in memory; this is not an AWS qualification.
func TestLocalOpenAISmoke(t *testing.T) {
	if os.Getenv("LLMTW_OPENAI_SMOKE") != "1" || os.Getenv("CI") != "" {
		t.Fatal("operator-only smoke requires explicit authorization")
	}
	key := os.Getenv("OPENAI_API_KEY")
	if key == "" {
		t.Fatal("OPENAI_API_KEY is required")
	}
	h := newLiveCloudWorkflow(t, false, false)
	request, submissions := configureOpenAISmoke(t, h.fixture, key)
	h.startWorker(t)
	result := h.generate(t, request)
	if result.Status != llm.ResponseStatusCompleted || result.Cache.Disposition != "miss_populated" {
		t.Fatal("generation did not complete and populate cache")
	}
	var answer strings.Builder
	for _, item := range result.Output {
		if message, ok := item.(llm.Message); ok {
			for _, part := range message.Content {
				if text, ok := part.(llm.TextPart); ok {
					answer.WriteString(text.Text)
				}
			}
		}
	}
	if strings.TrimSpace(answer.String()) != "OK" {
		t.Fatal("provider answer did not match smoke prompt")
	}
	request.OperationKey = "smoke-cache-replay"
	replay := h.generate(t, request)
	if replay.Cache.Disposition != "hit" || submissions.Load() != 1 {
		t.Fatal("cache replay made a second provider submission")
	}
	t.Log("real OpenAI generation, checkpoint publication and cache replay passed")
}

func configureOpenAISmoke(t *testing.T, f *boundedCloudFixture, key string) (llm.GenerateRequestV1, *atomic.Int32) {
	t.Helper()
	source := f.cap.Snapshot.(*planningSource)
	model := source.value.Routes.Models["alias"]
	model.Routes[0].Model = "gpt-6-luna"
	model.Routes[0].Capabilities.Features[routing.FeatureReasoning] = routing.Capability{State: routing.CapabilityNative}
	source.value.Routes.Models["alias"] = model
	prices, err := pricing.CompileUSD("smoke-luna/v1", []pricing.Entry{{Provider: "openai", Family: string(provider.FamilyOpenAIResponses), EndpointID: "endpoint", Region: "region", Model: "gpt-6-luna", ProviderTier: "default", Version: "price/v1", Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("0.10"), OutputPerMillion: pricing.MustDecimalUSD("0.50")}}})
	if err != nil {
		t.Fatal(err)
	}
	source.value.Prices = pricing.NewResolver(prices)
	for i := range source.value.BudgetPolicies[0].Windows {
		source.value.BudgetPolicies[0].Windows[i].LimitUSD = pricing.MustUSD("0.01")
	}
	f.cap.BudgetEstimator.Tokenizer = nil
	f.cap.BudgetEstimator.MaxOutput = 256
	var submissions atomic.Int32
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	t.Cleanup(transport.CloseIdleConnections)
	sdk, err := openairesponses.NewClient(openairesponses.ClientConfig{BaseURL: "https://api.openai.com/v1/", APIKey: key, HTTPClient: &http.Client{Timeout: 90 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("smoke redirects disabled") }, Transport: planningTransportFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "api.openai.com" || r.URL.Path != "/v1/responses" || r.Method != http.MethodPost {
			return nil, errors.New("smoke only permits OpenAI response creation")
		}
		if submissions.Add(1) != 1 {
			return nil, errors.New("smoke submission allowance exhausted")
		}
		return transport.RoundTrip(r)
	})}})
	if err != nil {
		t.Fatal("cannot construct OpenAI client")
	}
	adapter, err := openairesponses.New(sdk, "endpoint", "profile/v1")
	if err != nil {
		t.Fatal(err)
	}
	f.cap.Adapters = engine.AdapterMap{"endpoint": adapter}
	request := f.request
	request.Append = []llm.Item{preparationMessage("Reply with exactly OK.")}
	request.SettingsPatch.ServiceClass.Set = preparationPointer(llm.ServiceClassStandard)
	request.SettingsPatch.ServiceClassFallbacks.Set = nil
	request.SettingsPatch.Output.Set = &llm.OutputSpec{MaxTokens: preparationPointer(256), Format: llm.OutputFormat{Kind: llm.OutputKindText}}
	request.SettingsPatch.ReasoningEffort.Set = preparationPointer(llm.ReasoningEffortLow)
	request.Cache = &llm.CachePolicyV1{}

	return request, &submissions
}

func TestOpenAISmokePlanning(t *testing.T) {
	f := boundedCloud(t, false)
	request, submissions := configureOpenAISmoke(t, f, "offline-fixture-key")
	f.restart(t)
	result, err := f.runtime.PrepareExecutionV1(context.Background(), llm.PrepareExecutionV1{Generate: &request})
	if err != nil || result.State != llm.ExecutionBudgetRequired {
		t.Fatalf("smoke planning failed: state=%s error=%v", result.State, err)
	}
	if submissions.Load() != 0 {
		t.Fatal("planning unexpectedly dispatched provider request")
	}
}
