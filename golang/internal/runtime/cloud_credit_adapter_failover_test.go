package runtime

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/anthropicmessages"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/bedrockmessages"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/openairesponses"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

// creditDeniedHTTPClient answers every request with the given provider
// status and body and counts the requests that reached it.
func creditDeniedHTTPClient(requests *atomic.Int32, status int, body string) *http.Client {
	return &http.Client{Transport: planningTransportFunc(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		if r.Body != nil {
			_, _ = io.Copy(io.Discard, r.Body)
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Request: r, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
}

func anthropicCreditAdapter(t *testing.T, httpClient *http.Client) provider.Adapter {
	client, err := anthropicmessages.NewClient(anthropicmessages.ClientConfig{BaseURL: "https://127.0.0.1", APIKey: "test-key", HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	profile := anthropicmessages.DefaultProfile("endpoint")
	profile.CapabilityVersion, profile.Capabilities.Version = "profile/v1", "profile/v1"
	adapter, err := anthropicmessages.New(client, "endpoint", profile)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func responsesCreditAdapter(t *testing.T, httpClient *http.Client) provider.Adapter {
	client, err := openairesponses.NewClient(openairesponses.ClientConfig{BaseURL: "https://api.openai.com/v1/", APIKey: "test-key", HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := openairesponses.New(client, "endpoint", "profile/v1")
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

// A 402 from the real Anthropic, Bedrock Messages and Responses adapters is a
// route-level credit denial, the same as on the Chat Completions path: the
// route is held for the credit hold and the request moves to another route
// instead of failing as the caller's invalid_argument (#1441). A billing 429,
// OpenAI credit_balance_exhausted or the Anthropic tier spend cap, is the same
// denial rather than a rate limit retried on the dead account (#1442).
func TestCloudAdapterCreditDenialHoldsRouteAndFailsOver(t *testing.T) {
	anthropicBody := `{"type":"error","error":{"type":"billing_error","message":"credit balance is too low"}}`
	for _, test := range []struct {
		name    string
		family  provider.Family
		tier    string
		adapter func(*testing.T, *http.Client) provider.Adapter
		status  int
		body    string
	}{
		{name: "anthropic_messages", family: provider.FamilyAnthropicMessages, tier: "standard_only", status: http.StatusPaymentRequired, body: anthropicBody, adapter: anthropicCreditAdapter},
		{name: "bedrock_anthropic_messages", family: provider.FamilyBedrockMessages, tier: "default", status: http.StatusPaymentRequired, body: anthropicBody, adapter: func(t *testing.T, httpClient *http.Client) provider.Adapter {
			client, err := bedrockmessages.NewClient(context.Background(), bedrockmessages.ClientConfig{BaseURL: "https://127.0.0.1", HTTPClient: httpClient, AWSConfig: hierarchyAWSConfig()})
			if err != nil {
				t.Fatal(err)
			}
			profile := bedrockmessages.DefaultProfile("endpoint")
			profile.CapabilityVersion, profile.Capabilities.Version = "profile/v1", "profile/v1"
			adapter, err := bedrockmessages.New(client, "endpoint", profile)
			if err != nil {
				t.Fatal(err)
			}
			return adapter
		}},
		{name: "openai_responses", family: provider.FamilyOpenAIResponses, tier: "default", status: http.StatusPaymentRequired, body: `{"error":{"message":"credits used up"}}`, adapter: responsesCreditAdapter},
		{name: "anthropic_messages_spend_cap", family: provider.FamilyAnthropicMessages, tier: "standard_only", status: http.StatusTooManyRequests, adapter: anthropicCreditAdapter,
			body: `{"type":"error","error":{"type":"rate_limit_error","message":"You have reached your API usage limits","details":{"error_code":"enforced_spend_limit_reached"}}}`},
		{name: "openai_responses_credit_balance_exhausted", family: provider.FamilyOpenAIResponses, tier: "default", status: http.StatusTooManyRequests, adapter: responsesCreditAdapter,
			body: `{"error":{"message":"credit balance exhausted","type":"insufficient_quota","code":"credit_balance_exhausted","param":null}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			route := hierarchyRoute{name: test.name, family: test.family, tier: test.tier}
			f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
				route.configure(b)
				model := b.source.value.Routes.Models["alias"]
				second := model.Routes[0]
				second.ID, second.EndpointID = "alternate", "alternate-endpoint"
				model.Routes = append(model.Routes, second)
				b.source.value.Routes.Models["alias"] = model
				b.source.value.BudgetPolicies[0].Match.EndpointID = ""
				entry := b.entry
				entry.EndpointID = "alternate-endpoint"
				b.prices(t, []pricing.Entry{b.entry, entry})
			})
			shared := &keyedHealthFixture{}
			f.cap.ProviderRouteStatus, f.cap.ProviderStatusRecorder = shared, shared
			var requests atomic.Int32
			f.cap.Adapters = engine.AdapterMap{"endpoint": test.adapter(t, creditDeniedHTTPClient(&requests, test.status, test.body)), "alternate-endpoint": f.adapter.executionSyncAdapter}
			f.restart(t)

			ctx := context.Background()
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			if !v.Retryable || v.ErrorCode != string(provider.CodePermissionDenied) {
				t.Fatalf("%d = %+v, want a retryable permission_denied", test.status, v)
			}
			if len(shared.records) != 1 {
				t.Fatalf("records = %+v, want one", shared.records)
			}
			for _, status := range shared.records {
				if status.EndpointID != "endpoint" || status.Circuit != control.CircuitOpen || !status.StaleAfter.Equal(f.now.Add(creditFailureHold)) {
					t.Fatalf("status = %+v, want the credit hold on the rejected route", status)
				}
			}
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionAcquired)
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			// Past the ordinary cooldown, a new request still skips the held
			// route instead of failing on it again.
			f.now = f.now.Add(time.Minute)
			f.restart(t)
			f.request.OperationKey = "after-credit-denial"
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			if requests.Load() != 1 || f.submits.Load() != 2 {
				t.Fatalf("provider requests = %d, alternate submits = %d, want one rejected request and the rest on the alternate", requests.Load(), f.submits.Load())
			}
		})
	}
}
