package openaichat

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestProfileRequiresExplicitFeaturesAndClasses(t *testing.T) {
	profile := testProfile()
	delete(profile.Capabilities.Features, provider.FeatureDocument)
	testassert.ErrorContains(t, testassert.Err(NewProfile(profile)), "document")
	profile = testProfile()
	delete(profile.ServiceTiers, llm.ServiceClassPriority)
	testassert.ErrorContains(t, testassert.Err(NewProfile(profile)), "priority")
}

func TestStreamingCapabilityCannotOutrunAdapterPort(t *testing.T) {
	if capability := profileTestCapabilities("chat-profile-test/v1").Features[provider.FeatureStreaming]; capability.State != provider.CapabilityUnsupported {
		t.Fatalf("profile test streaming capability = %#v, want unsupported", capability)
	}
	adaptertest.StreamingCapabilityCannotOutrunAdapterPort(t, (*Adapter)(nil), testProfile().Capabilities, declareChatCapability(provider.FeatureStreaming))
}

func TestContinuationCapabilityCannotOutrunAdapterPort(t *testing.T) {
	adaptertest.CapabilityCannotOutrunAdapterPort(t, provider.FeatureContinuation, "continuation", declareChatCapability(provider.FeatureContinuation))
}

// declareChatCapability declares feature on the test profile and reports
// the capabilities of the validated profile and of a "chat-prod" adapter.
func declareChatCapability(feature provider.Feature) adaptertest.Declare {
	return func(capability provider.Capability) (provider.CapabilitySet, provider.CapabilitySet, error) {
		profile := testProfile()
		profile.Capabilities.Features[feature] = capability
		validated, err := NewProfile(profile)
		if err != nil {
			return provider.CapabilitySet{}, provider.CapabilitySet{}, err
		}
		adapter := &Adapter{endpointID: "chat-prod", profile: validated}
		reported, err := adapter.Capabilities(context.Background(), provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat})
		return validated.Capabilities, reported, err
	}
}

func TestCompileRejectsUnknownCapabilityInStrictMode(t *testing.T) {
	adapter := testAdapter(t)
	request := llm.Request{
		OperationKey: "op-unknown",
		Model:        "chat-model",
		Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png"},
		}}},
	}
	_, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: "chat-prod",
			Family:     provider.FamilyOpenAIChat,
			Model:      "chat-model",
		},
		Capability: provider.CapabilitySet{Version: "partial", Features: map[provider.Feature]provider.Capability{
			provider.FeatureText:  {State: provider.CapabilityNative},
			provider.FeatureUsage: {State: provider.CapabilityNative},
		}},
		Strict: true,
	})
	testassert.ErrorContains(t, err, "image")
}

func TestCompileRejectsContinuationBeforeInvocation(t *testing.T) {
	adapter := testAdapter(t)
	_, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{
			OperationKey: "op-continuation",
			Model:        "chat-model",
			Continuation: &llm.Continuation{Handle: "chat:completion-1"},
		},
		Query:  provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
		Strict: true,
	})
	testassert.ErrorContains(t, err, "continuation")
}

func TestInvokeSubmitsExactlyOnceAndLiftsResponse(t *testing.T) {
	responseBody := `{"id":"chatcmpl-1","object":"chat.completion","created":1700000000,"model":"chat-model","service_tier":"default","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"done","refusal":""}}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`
	calls := 0
	client, err := NewClient(ClientConfig{
		BaseURL: "https://127.0.0.1/contract",
		APIKey:  "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls++
			return adaptertest.JSONResponse(request, http.StatusOK, responseBody, "X-Request-Id", "req-1"), nil
		})},
	})
	testassert.NoError(t, err)
	adapter, call := compileChatProdCall(t, client, "op-invoke")
	observer := &adaptertest.Observer{}
	result, err := adapter.Invoke(context.Background(), call, observer)
	testassert.NoError(t, err)
	if calls != 1 || observer.Before != 1 || observer.Headers != 1 || observer.Progress != 1 {
		t.Fatalf("transport/observer counts = %d %#v", calls, observer)
	}
	if result.Response.OperationKey != "op-invoke" || result.Response.Provider.RequestID != "req-1" || result.Response.Status != llm.ResponseStatusCompleted {
		t.Fatalf("response = %#v", result.Response)
	}
}

func TestInvokeRedirectResponseIsAmbiguousAndNotFollowed(t *testing.T) {
	var calls int
	client, err := NewClient(ClientConfig{
		BaseURL: "https://provider.example/v1",
		APIKey:  "test-key",
		HTTPClient: &http.Client{
			Transport:     adaptertest.RedirectTransport(&calls, `{"error":{"message":"redirect"}}`),
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	})
	testassert.NoError(t, err)
	adapter, call := compileChatProdCall(t, client, "chat-redirect")
	observer := &adaptertest.Observer{}
	_, err = adapter.Invoke(context.Background(), call, observer)
	mapped := adaptertest.AmbiguousRedirect(t, err)
	if calls != 1 || observer.Before != 1 || observer.Headers != 0 {
		t.Fatalf("transport/observer = calls=%d observer=%#v, want one request, one pre-write mark, and no headers", calls, observer)
	}
	encoded, marshalErr := json.Marshal(mapped)
	testassert.NoError(t, marshalErr)
	if strings.Contains(string(encoded), "redirect.example") || strings.Contains(mapped.Error(), "continuation-secret") {
		t.Fatalf("safe adapter error leaked redirect target: %s", encoded)
	}
}

func TestInvokePreDispatchCallerDeadlineStaysRetryNever(t *testing.T) {
	transport := adaptertest.NewPreDialDeadlineTransport()
	client, err := NewClient(ClientConfig{
		BaseURL:    "https://provider.example/v1",
		APIKey:     "test-key",
		HTTPClient: &http.Client{Transport: transport},
	})
	testassert.NoError(t, err)
	adapter, call := compileChatProdCall(t, client, "chat-pre-dial-deadline")
	observer := &adaptertest.Observer{}
	adaptertest.CallerDeadlineBeforeDial(t, transport, func(ctx context.Context) error {
		_, err := adapter.Invoke(ctx, call, observer)
		return err
	})
	if observer.Before != 1 || observer.Headers != 0 {
		t.Fatalf("observer = %#v, want one pre-write mark and no headers", observer)
	}
}

type roundTripFunc = adaptertest.RoundTripFunc

func testProfile() Profile {
	features := map[provider.Feature]provider.Capability{}
	for _, feature := range provider.AllFeatures() {
		state := provider.CapabilityNative
		if feature == provider.FeatureDocument || feature == provider.FeatureContinuation || feature == provider.FeatureStreaming {
			state = provider.CapabilityUnsupported
		}
		features[feature] = provider.Capability{State: state, Reason: "contract test"}
	}
	return Profile{
		ID:                "chat-contract",
		CapabilityVersion: "chat-contract/v1",
		Capabilities:      provider.CapabilitySet{Version: "chat-contract/v1", Features: features},
		ServiceTiers: map[llm.ServiceClass]string{
			llm.ServiceClassEconomy:  "flex",
			llm.ServiceClassStandard: "default",
			llm.ServiceClassPriority: "priority",
		},
		ActualServiceClasses: map[string]llm.ServiceClass{
			"flex":     llm.ServiceClassEconomy,
			"default":  llm.ServiceClassStandard,
			"priority": llm.ServiceClassPriority,
		},
		AllowedExtensions: map[string]ExtensionSpec{
			"chat.contract": {Fields: map[string]string{"provider_hint": "user"}},
		},
	}
}

func testAdapter(t *testing.T) *Adapter {
	t.Helper()
	validated, err := NewProfile(testProfile())
	testassert.NoError(t, err)
	return &Adapter{endpointID: "chat-prod", profile: validated}
}

// compileChatProdCall builds the "chat-prod" adapter over client with the
// test profile and strictly compiles an empty chat-model request under
// operationKey.
func compileChatProdCall(t *testing.T, client *Client, operationKey string) (*Adapter, provider.Call) {
	t.Helper()
	adapter, err := New(client, "chat-prod", testProfile())
	testassert.NoError(t, err)
	return adapter, adaptertest.CompileStrict(t, adapter, "chat-prod", provider.FamilyOpenAIChat, llm.Request{OperationKey: operationKey, Model: "chat-model"})
}
