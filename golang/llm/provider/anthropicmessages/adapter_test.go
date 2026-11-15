package anthropicmessages

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestStreamingCapabilityCannotOutrunAdapterPort(t *testing.T) {
	adaptertest.StreamingCapabilityCannotOutrunAdapterPort(t, (*Adapter)(nil), DefaultProfile("anthropic-no-stream").Capabilities, func(streaming provider.Capability) (provider.CapabilitySet, provider.CapabilitySet, error) {
		profile := DefaultProfile("anthropic-no-stream")
		profile.Capabilities.Features[provider.FeatureStreaming] = streaming
		validated, err := NewProfile(profile)
		if err != nil {
			return provider.CapabilitySet{}, provider.CapabilitySet{}, err
		}
		adapter := &Adapter{endpointID: validated.ID, profile: validated}
		reported, err := adapter.Capabilities(context.Background(), provider.CapabilityQuery{EndpointID: validated.ID, Family: provider.FamilyAnthropicMessages})
		return validated.Capabilities, reported, err
	})
}

func TestInvokeMakesExactlyOneSDKCallAndLiftsResponse(t *testing.T) {
	responseBody := `{"id":"msg_invoke","type":"message","role":"assistant","model":"claude-contract","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":1,"service_tier":"standard"}}`
	calls := 0
	client, err := NewClient(ClientConfig{
		BaseURL: "http://127.0.0.1/contract",
		APIKey:  "test-key",
		HTTPClient: &http.Client{Transport: testkit.RoundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls++
			if request.URL.Path != "/contract/v1/messages" {
				t.Errorf("request path = %q", request.URL.Path)
			}
			return adaptertest.JSONResponse(request, http.StatusOK, responseBody, "Request-Id", "req-invoke"), nil
		})},
	})
	testassert.NoError(t, err)
	adapter, call := compileProdCall(t, client, "http://127.0.0.1/contract", "invoke")
	observer := &adaptertest.Observer{}
	result, err := adapter.Invoke(context.Background(), call, observer)
	testassert.NoError(t, err)
	if calls != 1 || observer.Before != 1 || observer.Headers != 1 || observer.Progress != 1 {
		t.Fatalf("transport/observer counts = %d %#v", calls, observer)
	}
	if result.Response.OperationKey != "invoke" || result.Response.Provider.RequestID != "req-invoke" || result.Response.Status != llm.ResponseStatusCompleted || result.Response.Service.Actual == nil || *result.Response.Service.Actual != llm.ServiceClassStandard {
		t.Fatalf("response = %#v", result.Response)
	}
}

func TestInvokeMapsRateLimitAndAuthenticationErrorsWithoutRetry(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		fixture   string
		wantCode  provider.Code
		requestID string
	}{
		{name: "rate limit", status: http.StatusTooManyRequests, fixture: "error.rate-limit.json", wantCode: provider.CodeProviderRateLimited, requestID: "req-rate"},
		{name: "auth", status: http.StatusUnauthorized, fixture: "error.authentication.json", wantCode: provider.CodeAuthentication, requestID: "req-auth"},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			body := string(mustReadFixture(t, test.fixture))
			client, err := NewClient(ClientConfig{
				BaseURL: "http://127.0.0.1/contract",
				APIKey:  "test-key",
				HTTPClient: &http.Client{Transport: testkit.RoundTripFunc(func(request *http.Request) (*http.Response, error) {
					calls++
					return adaptertest.JSONResponse(request, test.status, body, "Request-Id", test.requestID, "Retry-After", "2"), nil
				})},
			})
			testassert.NoError(t, err)
			adapter, call := compileProdCall(t, client, "http://127.0.0.1/contract", "error-"+test.name)
			_, err = adapter.Invoke(context.Background(), call, nil)
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) || providerErr.Code != test.wantCode || providerErr.Provider.RequestID != test.requestID {
				t.Fatalf("mapped error = %#v", err)
			}
			if calls != 1 {
				t.Fatalf("SDK call count = %d, want 1", calls)
			}
		})
	}
}

func TestInvokeRedirectResponseIsAmbiguousAndNotFollowed(t *testing.T) {
	var calls int
	client, err := NewClient(ClientConfig{
		BaseURL: "https://provider.example/v1",
		APIKey:  "test-key",
		HTTPClient: &http.Client{
			Transport:     adaptertest.RedirectTransport(&calls, `{"error":{"type":"api_error","message":"redirect"}}`),
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	})
	testassert.NoError(t, err)
	adapter, call := compileProdCall(t, client, "https://provider.example/v1", "anthropic-redirect")
	observer := &adaptertest.Observer{}
	_, err = adapter.Invoke(context.Background(), call, observer)
	mapped := adaptertest.AmbiguousRedirect(t, err)
	if calls != 1 || observer.Before != 1 || observer.Headers != 0 {
		t.Fatalf("transport/observer = calls=%d observer=%#v, want one request, one pre-write mark, and no headers", calls, observer)
	}
	if mapped.SafeDetails["status"] != "307" || mapped.SafeDetails["endpoint"] != "anthropic-prod" {
		t.Fatalf("safe details = %#v, want redirect status and endpoint only", mapped.SafeDetails)
	}
}

func TestInvokePreDispatchCallerDeadlineStaysRetryNever(t *testing.T) {
	transport := adaptertest.NewPreDialDeadlineTransport()
	client, err := NewClient(ClientConfig{
		BaseURL:    "https://api.anthropic.com/v1",
		APIKey:     "test-key",
		HTTPClient: &http.Client{Transport: transport},
	})
	testassert.NoError(t, err)
	adapter, call := compileProdCall(t, client, "", "anthropic-pre-dial-deadline")
	observer := &adaptertest.Observer{}
	adaptertest.CallerDeadlineBeforeDial(t, transport, func(ctx context.Context) error {
		_, err := adapter.Invoke(ctx, call, observer)
		return err
	})
	if observer.Before != 1 || observer.Headers != 0 {
		t.Fatalf("observer = %#v, want one pre-write mark and no headers", observer)
	}
}

// compileProdCall builds the "anthropic-prod" adapter over client with the
// test profile, expecting baseURL unless it is empty, and strictly compiles
// an empty claude-contract request under operationKey.
func compileProdCall(t *testing.T, client *Client, baseURL, operationKey string) (*Adapter, provider.Call) {
	t.Helper()
	profile := testProfile()
	if baseURL != "" {
		profile.ExpectedBaseURL = baseURL
	}
	adapter, err := New(client, "anthropic-prod", profile)
	testassert.NoError(t, err)
	return adapter, adaptertest.CompileStrict(t, adapter, "anthropic-prod", provider.FamilyAnthropicMessages, llm.Request{OperationKey: operationKey, Model: "claude-contract"})
}
