package openaichat

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestProfileRequiresExplicitFeaturesAndClasses(t *testing.T) {
	profile := testProfile()
	delete(profile.Capabilities.Features, provider.FeatureDocument)
	if _, err := NewProfile(profile); err == nil || !strings.Contains(err.Error(), "document") {
		t.Fatalf("missing feature error = %v", err)
	}
	profile = testProfile()
	delete(profile.ServiceTiers, llm.ServiceClassPriority)
	if _, err := NewProfile(profile); err == nil || !strings.Contains(err.Error(), "priority") {
		t.Fatalf("missing tier error = %v", err)
	}
}

func TestStreamingCapabilityCannotOutrunAdapterPort(t *testing.T) {
	profile := testProfile()
	if capability := profile.Capabilities.Features[provider.FeatureStreaming]; capability.State != provider.CapabilityUnsupported || capability.Reason == "" {
		t.Fatalf("default streaming capability = %#v, want unsupported with a reason", capability)
	}
	if capability := profileTestCapabilities("chat-profile-test/v1").Features[provider.FeatureStreaming]; capability.State != provider.CapabilityUnsupported {
		t.Fatalf("profile test streaming capability = %#v, want unsupported", capability)
	}

	for _, state := range []provider.CapabilityState{provider.CapabilityNative, provider.CapabilityEmulated} {
		t.Run(string(state), func(t *testing.T) {
			profile := testProfile()
			profile.Capabilities.Features[provider.FeatureStreaming] = provider.Capability{State: state}
			if _, err := NewProfile(profile); err == nil || !strings.Contains(err.Error(), "OpenStream") {
				t.Fatalf("NewProfile() error = %v, want an OpenStream capability error", err)
			}
		})
	}

	profile = testProfile()
	profile.Capabilities.Features[provider.FeatureStreaming] = provider.Capability{State: provider.CapabilityUnknown, Transform: "unverified-stream"}
	validated, err := NewProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if capability := validated.Capabilities.Features[provider.FeatureStreaming]; capability.State != provider.CapabilityUnsupported || capability.Transform != "" {
		t.Fatalf("validated streaming capability = %#v, want normalized unsupported capability", capability)
	}
	adapter := &Adapter{endpointID: "chat-prod", profile: validated}
	set, err := adapter.Capabilities(context.Background(), provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat})
	if err != nil {
		t.Fatal(err)
	}
	if capability := set.Features[provider.FeatureStreaming]; capability.State != provider.CapabilityUnsupported || capability.Transform != "" {
		t.Fatalf("reported streaming capability = %#v, want normalized unsupported capability", capability)
	}
	if _, ok := any((*Adapter)(nil)).(provider.StreamingAdapter); ok {
		t.Fatal("adapter advertises streaming capability without an OpenStream implementation")
	}
}

func TestContinuationCapabilityCannotOutrunAdapterPort(t *testing.T) {
	for _, state := range []provider.CapabilityState{provider.CapabilityNative, provider.CapabilityEmulated} {
		t.Run(string(state), func(t *testing.T) {
			profile := testProfile()
			profile.Capabilities.Features[provider.FeatureContinuation] = provider.Capability{State: state}
			if _, err := NewProfile(profile); err == nil || !strings.Contains(err.Error(), "continuation") {
				t.Fatalf("NewProfile() error = %v, want a continuation capability error", err)
			}
		})
	}

	profile := testProfile()
	profile.Capabilities.Features[provider.FeatureContinuation] = provider.Capability{State: provider.CapabilityUnknown, Transform: "unverified-continuation"}
	validated, err := NewProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if capability := validated.Capabilities.Features[provider.FeatureContinuation]; capability.State != provider.CapabilityUnsupported || capability.Transform != "" {
		t.Fatalf("validated continuation capability = %#v, want normalized unsupported capability", capability)
	}
	adapter := &Adapter{endpointID: "chat-prod", profile: validated}
	set, err := adapter.Capabilities(context.Background(), provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat})
	if err != nil {
		t.Fatal(err)
	}
	if capability := set.Features[provider.FeatureContinuation]; capability.State != provider.CapabilityUnsupported || capability.Transform != "" {
		t.Fatalf("reported continuation capability = %#v, want normalized unsupported capability", capability)
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
	if err == nil || !strings.Contains(err.Error(), "image") {
		t.Fatalf("strict unknown capability error = %v", err)
	}
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
	if err == nil || !strings.Contains(err.Error(), "continuation") {
		t.Fatalf("continuation error = %v", err)
	}
}

func TestInvokeSubmitsExactlyOnceAndLiftsResponse(t *testing.T) {
	responseBody := `{"id":"chatcmpl-1","object":"chat.completion","created":1700000000,"model":"chat-model","service_tier":"default","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"done","refusal":""}}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`
	calls := 0
	client, err := NewClient(ClientConfig{
		BaseURL: "http://127.0.0.1/contract",
		APIKey:  "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls++
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"req-1"}},
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "chat-prod", testProfile())
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-invoke", Model: "chat-model"},
		Query:   provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &chatObserver{}
	result, err := adapter.Invoke(context.Background(), call, observer)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || observer.before != 1 || observer.headers != 1 || observer.progress != 1 {
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
			Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{
					StatusCode: http.StatusTemporaryRedirect,
					Header: http.Header{
						"Content-Type": []string{"application/json"},
						"Location":     []string{"https://redirect.example/continuation-secret"},
					},
					Body:    io.NopCloser(strings.NewReader(`{"error":{"message":"redirect"}}`)),
					Request: request,
				}, nil
			}),
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "chat-prod", testProfile())
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "chat-redirect", Model: "chat-model"},
		Query:   provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &chatObserver{}
	_, err = adapter.Invoke(context.Background(), call, observer)
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", err, err)
	}
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped redirect = %#v, want ambiguous non-retriable provider-unavailable", mapped)
	}
	if calls != 1 || observer.before != 1 || observer.headers != 0 {
		t.Fatalf("transport/observer = calls=%d observer=%#v, want one request, one pre-write mark, and no headers", calls, observer)
	}
	encoded, marshalErr := json.Marshal(mapped)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(encoded), "redirect.example") || strings.Contains(mapped.Error(), "continuation-secret") {
		t.Fatalf("safe adapter error leaked redirect target: %s", encoded)
	}
}

func TestInvokePreDispatchCallerDeadlineStaysRetryNever(t *testing.T) {
	deadline := newPreDialDeadlineContext()
	transport := &preDialDeadlineTransport{started: make(chan struct{})}
	client, err := NewClient(ClientConfig{
		BaseURL:    "https://provider.example/v1",
		APIKey:     "test-key",
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "chat-prod", testProfile())
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "chat-pre-dial-deadline", Model: "chat-model"},
		Query:   provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &chatObserver{}
	result := make(chan error, 1)
	go func() {
		_, invokeErr := adapter.Invoke(deadline, call, observer)
		result <- invokeErr
	}()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("provider transport was not reached")
	}
	deadline.expire()
	var invokeErr error
	select {
	case invokeErr = <-result:
	case <-time.After(time.Second):
		t.Fatal("Invoke() did not return after the pre-dial deadline")
	}
	var mapped *provider.Error
	if !errors.As(invokeErr, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", invokeErr, invokeErr)
	}
	if mapped.Code != provider.CodeDeadlineExceeded || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped error = %#v, want non-retryable pre-dispatch caller deadline", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderPreDispatch) || !errors.Is(mapped, context.DeadlineExceeded) {
		t.Fatalf("mapped error = %v, want certified pre-dispatch caller deadline", mapped)
	}
	if transport.calls != 1 || transport.bodyReads != 0 || observer.before != 1 || observer.headers != 0 {
		t.Fatalf("transport/observer = calls=%d bodyReads=%d observer=%#v", transport.calls, transport.bodyReads, observer)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type preDialDeadlineContext struct {
	done chan struct{}
	once sync.Once
}

func newPreDialDeadlineContext() *preDialDeadlineContext {
	return &preDialDeadlineContext{done: make(chan struct{})}
}

func (ctx *preDialDeadlineContext) Deadline() (time.Time, bool) {
	return time.Now().Add(time.Hour), true
}

func (ctx *preDialDeadlineContext) Done() <-chan struct{} { return ctx.done }

func (ctx *preDialDeadlineContext) Err() error {
	select {
	case <-ctx.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (*preDialDeadlineContext) Value(any) any { return nil }

func (ctx *preDialDeadlineContext) expire() {
	ctx.once.Do(func() { close(ctx.done) })
}

type preDialDeadlineTransport struct {
	started   chan struct{}
	once      sync.Once
	calls     int
	bodyReads int
}

func (transport *preDialDeadlineTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.calls++
	if request.Body != nil {
		request.Body = &countingReadCloser{ReadCloser: request.Body, reads: &transport.bodyReads}
	}
	transport.once.Do(func() { close(transport.started) })
	<-request.Context().Done()
	provider.RecordPreDispatchContext(request.Context(), request.Context().Err())
	return nil, request.Context().Err()
}

type countingReadCloser struct {
	io.ReadCloser
	reads *int
}

func (reader *countingReadCloser) Read(data []byte) (int, error) {
	(*reader.reads)++
	return reader.ReadCloser.Read(data)
}

type chatObserver struct {
	before   int
	headers  int
	progress int
}

func (observer *chatObserver) BeforePossibleWrite(context.Context) error {
	observer.before++
	return nil
}

func (observer *chatObserver) AfterResponseHeaders(context.Context, provider.ResponseMetadata) error {
	observer.headers++
	return nil
}

func (observer *chatObserver) OnProgress(context.Context, provider.Progress) {
	observer.progress++
}

func testProfile() Profile {
	features := map[provider.Feature]provider.Capability{}
	for _, feature := range allFeatures() {
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
	if err != nil {
		t.Fatal(err)
	}
	return &Adapter{endpointID: "chat-prod", profile: validated}
}

func marshalWire(t *testing.T, value any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}
