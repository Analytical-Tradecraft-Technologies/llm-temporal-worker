package anthropicmessages

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestStreamingCapabilityCannotOutrunAdapterPort(t *testing.T) {
	profile := DefaultProfile("anthropic-no-stream")
	if capability := profile.Capabilities.Features[provider.FeatureStreaming]; capability.State != provider.CapabilityUnsupported || capability.Reason == "" {
		t.Fatalf("default streaming capability = %#v, want unsupported with a reason", capability)
	}

	for _, state := range []provider.CapabilityState{provider.CapabilityNative, provider.CapabilityEmulated} {
		t.Run(string(state), func(t *testing.T) {
			profile := DefaultProfile("anthropic-no-stream")
			profile.Capabilities.Features[provider.FeatureStreaming] = provider.Capability{State: state}
			if _, err := NewProfile(profile); err == nil || !strings.Contains(err.Error(), "OpenStream") {
				t.Fatalf("NewProfile() error = %v, want an OpenStream capability error", err)
			}
		})
	}

	profile = DefaultProfile("anthropic-no-stream")
	profile.Capabilities.Features[provider.FeatureStreaming] = provider.Capability{State: provider.CapabilityUnknown, Transform: "unverified-stream"}
	validated, err := NewProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if capability := validated.Capabilities.Features[provider.FeatureStreaming]; capability.State != provider.CapabilityUnsupported || capability.Transform != "" {
		t.Fatalf("validated streaming capability = %#v, want normalized unsupported capability", capability)
	}
	adapter := &Adapter{endpointID: validated.ID, profile: validated}
	set, err := adapter.Capabilities(context.Background(), provider.CapabilityQuery{EndpointID: validated.ID, Family: provider.FamilyAnthropicMessages})
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

func TestInvokeMakesExactlyOneSDKCallAndLiftsResponse(t *testing.T) {
	responseBody := `{"id":"msg_invoke","type":"message","role":"assistant","model":"claude-contract","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":1,"service_tier":"standard"}}`
	calls := 0
	client, err := NewClient(ClientConfig{
		BaseURL: "http://127.0.0.1/contract",
		APIKey:  "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls++
			if request.URL.Path != "/contract/v1/messages" {
				t.Errorf("request path = %q", request.URL.Path)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}, "Request-Id": []string{"req-invoke"}},
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := testProfile()
	profile.ExpectedBaseURL = "http://127.0.0.1/contract"
	adapter, err := New(client, "anthropic-prod", profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "invoke", Model: "claude-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &anthropicObserver{}
	result, err := adapter.Invoke(context.Background(), call, observer)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || observer.before != 1 || observer.headers != 1 || observer.progress != 1 {
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
				HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					calls++
					return &http.Response{StatusCode: test.status, Header: http.Header{"Content-Type": []string{"application/json"}, "Request-Id": []string{test.requestID}, "Retry-After": []string{"2"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
				})},
			})
			if err != nil {
				t.Fatal(err)
			}
			profile := testProfile()
			profile.ExpectedBaseURL = "http://127.0.0.1/contract"
			adapter, err := New(client, "anthropic-prod", profile)
			if err != nil {
				t.Fatal(err)
			}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "error-" + test.name, Model: "claude-contract"}, Query: provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"}, Strict: true})
			if err != nil {
				t.Fatal(err)
			}
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
			Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{
					StatusCode: http.StatusTemporaryRedirect,
					Header: http.Header{
						"Content-Type": []string{"application/json"},
						"Location":     []string{"https://redirect.example/continuation-secret"},
					},
					Body:    io.NopCloser(strings.NewReader(`{"error":{"type":"api_error","message":"redirect"}}`)),
					Request: request,
				}, nil
			}),
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := testProfile()
	profile.ExpectedBaseURL = "https://provider.example/v1"
	adapter, err := New(client, "anthropic-prod", profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "anthropic-redirect", Model: "claude-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &anthropicObserver{}
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
	if mapped.SafeDetails["status"] != "307" || mapped.SafeDetails["endpoint"] != "anthropic-prod" {
		t.Fatalf("safe details = %#v, want redirect status and endpoint only", mapped.SafeDetails)
	}
}

func TestInvokePreDispatchCallerDeadlineStaysRetryNever(t *testing.T) {
	deadline := newPreDialDeadlineContext()
	transport := &preDialDeadlineTransport{started: make(chan struct{})}
	client, err := NewClient(ClientConfig{
		BaseURL:    "https://api.anthropic.com/v1",
		APIKey:     "test-key",
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "anthropic-prod", testProfile())
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "anthropic-pre-dial-deadline", Model: "claude-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &anthropicObserver{}
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

type anthropicObserver struct {
	before   int
	headers  int
	progress int
}

func (observer *anthropicObserver) BeforePossibleWrite(context.Context) error {
	observer.before++
	return nil
}

func (observer *anthropicObserver) AfterResponseHeaders(context.Context, provider.ResponseMetadata) error {
	observer.headers++
	return nil
}

func (observer *anthropicObserver) OnProgress(context.Context, provider.Progress) {
	observer.progress++
}
