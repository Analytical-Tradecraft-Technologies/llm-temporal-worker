package bedrockmessages

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestStreamingCapabilityCannotOutrunAdapterPort(t *testing.T) {
	profile := DefaultProfile("bedrock-no-stream")
	if capability := profile.Capabilities.Features[provider.FeatureStreaming]; capability.State != provider.CapabilityUnsupported || capability.Reason == "" {
		t.Fatalf("default streaming capability = %#v, want unsupported with a reason", capability)
	}

	for _, state := range []provider.CapabilityState{provider.CapabilityNative, provider.CapabilityEmulated} {
		t.Run(string(state), func(t *testing.T) {
			profile := DefaultProfile("bedrock-no-stream")
			profile.Capabilities.Features[provider.FeatureStreaming] = provider.Capability{State: state}
			if _, err := NewProfile(profile); err == nil || !strings.Contains(err.Error(), "OpenStream") {
				t.Fatalf("NewProfile() error = %v, want an OpenStream capability error", err)
			}
		})
	}

	profile = DefaultProfile("bedrock-no-stream")
	profile.Capabilities.Features[provider.FeatureStreaming] = provider.Capability{State: provider.CapabilityUnknown, Transform: "unverified-stream"}
	validated, err := NewProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if capability := validated.Capabilities.Features[provider.FeatureStreaming]; capability.State != provider.CapabilityUnsupported || capability.Transform != "" {
		t.Fatalf("validated streaming capability = %#v, want normalized unsupported capability", capability)
	}
	adapter := &Adapter{endpointID: validated.ID, profile: validated}
	set, err := adapter.Capabilities(context.Background(), provider.CapabilityQuery{EndpointID: validated.ID, Family: provider.FamilyBedrockMessages})
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

func TestCompileMapsOnlyTheThreePublicBedrockServiceClasses(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
	for _, test := range []struct {
		class llm.ServiceClass
		tier  string
	}{
		{class: llm.ServiceClassEconomy, tier: "flex"},
		{class: llm.ServiceClassStandard, tier: "default"},
		{class: llm.ServiceClassPriority, tier: "priority"},
	} {
		call, err := adapter.Compile(context.Background(), provider.CompileInput{
			Request: llm.Request{OperationKey: "tier-" + string(test.class), Model: "claude-contract", ServiceClass: test.class},
			Query:   provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"}, Strict: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if call.Metadata.ProviderTier != test.tier {
			t.Fatalf("class %q provider tier = %q, want %q", test.class, call.Metadata.ProviderTier, test.tier)
		}
		wire := marshalBedrockWire(t, call.SDKParams)
		if wire["service_tier"] != test.tier {
			t.Fatalf("class %q wire tier = %#v, want %q", test.class, wire["service_tier"], test.tier)
		}
	}
}

func TestCompileStrictRejectsMixedInstructionLevels(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
	_, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{
			OperationKey: "bedrock-strict-instructions",
			Model:        "claude-contract",
			Portability:  llm.PortabilityStrict,
			Instructions: []llm.Instruction{
				{Level: llm.InstructionLevelPolicy, Text: "policy"},
				{Level: llm.InstructionLevelApplication, Text: "application"},
			},
		},
		Query:  provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"},
		Strict: true,
	})
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("compile error = %T %v, want provider error", err, err)
	}
	if mapped.Phase != provider.PhaseCompile || mapped.Dispatch != provider.DispatchNotDispatched || !strings.Contains(mapped.SafeMessage, "instruction hierarchy") {
		t.Fatalf("compile error = %#v, want strict instruction hierarchy rejection", mapped)
	}
}

func TestCompileRejectsPinnedAnthropicAWSContinuation(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
	_, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{
			OperationKey: "bedrock-reject-aws-continuation",
			Model:        "claude-contract",
			Continuation: &llm.Continuation{
				Handle:     "anthropic-messages:msg-aws-fixture",
				EndpointID: "anthropic-aws",
				Model:      "claude-contract",
				Pinned:     true,
			},
		},
		Query:  provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"},
		Strict: true,
	})
	if err == nil || !strings.Contains(err.Error(), "continuation endpoint") {
		t.Fatalf("cross-family continuation error = %v", err)
	}
}

func TestProfileRejectsReservedCapacityAsPublicTier(t *testing.T) {
	profile := DefaultProfile("bedrock-contract")
	profile.ServiceTiers[llm.ServiceClassEconomy] = "reserved"
	if _, err := NewProfile(profile); err == nil || !strings.Contains(err.Error(), "invalid provider tier") {
		t.Fatalf("reserved tier validation error = %v", err)
	}
}

func TestInvokeUsesBedrockMiddlewareOnceAndLiftsAWSRequestID(t *testing.T) {
	responseBody := string(mustReadBedrockFixture(t, "invoke.response.json"))
	calls := 0
	var requestBody []byte
	client, err := NewClient(context.Background(), ClientConfig{
		BaseURL: "http://127.0.0.1",
		HTTPClient: &http.Client{Transport: bedrockRoundTrip(func(request *http.Request) (*http.Response, error) {
			calls++
			requestBody, _ = io.ReadAll(request.Body)
			if !strings.HasSuffix(request.URL.Path, "/model/claude-contract/invoke") {
				t.Errorf("Bedrock request path = %q", request.URL.Path)
			}
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}, "X-Amzn-Requestid": []string{"bedrock-req-1"}}, Body: io.NopCloser(strings.NewReader(responseBody)), Request: request}, nil
		})},
		AWSConfig: aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("contract-access", "contract-secret", "")},
	})
	if err != nil {
		t.Fatalf("invoke error: %#v", err)
	}
	profile := mustBedrockProfile(t, "http://127.0.0.1")
	adapter, err := New(client, "bedrock-prod", profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "bedrock-once", Model: "claude-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"}, Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, nil)
	if err != nil {
		var providerErr *provider.Error
		if errors.As(err, &providerErr) {
			t.Fatalf("invoke error: %#v cause=%v", providerErr, providerErr.Cause)
		}
		t.Fatalf("invoke error: %#v", err)
	}
	if calls != 1 || result.Response.Provider.RequestID != "bedrock-req-1" || result.Response.Service.Actual == nil || *result.Response.Service.Actual != llm.ServiceClassStandard {
		t.Fatalf("calls/response = %d %#v", calls, result.Response)
	}
	if !strings.Contains(string(requestBody), `"service_tier":"default"`) {
		t.Fatalf("Bedrock request body omitted default tier: %s", requestBody)
	}
	if strings.Contains(string(requestBody), "contract-secret") {
		t.Fatal("AWS secret appeared in Bedrock request body")
	}
}

func TestBedrockCallSerializationDoesNotContainResolvedCredentials(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "bedrock-credentials", Model: "claude-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"}, Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(call)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "contract-secret") || strings.Contains(string(encoded), "contract-access") {
		t.Fatalf("credential value appeared in call serialization: %s", encoded)
	}
}

func TestInvokeRedirectResponseIsAmbiguousAndNotFollowed(t *testing.T) {
	const baseURL = "https://bedrock-runtime.us-east-1.amazonaws.com"
	var calls int
	client, err := NewClient(context.Background(), ClientConfig{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Transport: bedrockRoundTrip(func(request *http.Request) (*http.Response, error) {
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
		AWSConfig: aws.Config{
			Region:      "us-east-1",
			Credentials: credentials.NewStaticCredentialsProvider("contract-access", "contract-secret", ""),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "bedrock-prod", mustBedrockProfile(t, baseURL))
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "bedrock-redirect", Model: "claude-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(context.Background(), call, provider.NopObserver{})
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", err, err)
	}
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped redirect = %#v, want ambiguous non-retriable provider-unavailable", mapped)
	}
	if calls != 1 {
		t.Fatalf("provider calls = %d, want one without redirect follow", calls)
	}
	if mapped.SafeDetails["status"] != "307" || mapped.SafeDetails["endpoint"] != "bedrock-prod" {
		t.Fatalf("safe details = %#v, want redirect status and endpoint only", mapped.SafeDetails)
	}
}

func TestInvokePreDispatchCallerDeadlineStaysRetryNever(t *testing.T) {
	deadline := newPreDialDeadlineContext()
	transport := &preDialDeadlineTransport{started: make(chan struct{})}
	const baseURL = "https://bedrock-runtime.us-east-1.amazonaws.com"
	client, err := NewClient(context.Background(), ClientConfig{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Transport: transport},
		AWSConfig: aws.Config{
			Region:      "us-east-1",
			Credentials: credentials.NewStaticCredentialsProvider("contract-access", "contract-secret", ""),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "bedrock-prod", mustBedrockProfile(t, baseURL))
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "bedrock-pre-dial-deadline", Model: "claude-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := provider.NopObserver{}
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
	if transport.calls != 1 || transport.bodyReads != 0 {
		t.Fatalf("pre-dial transport calls/body reads = %d/%d, want 1/0", transport.calls, transport.bodyReads)
	}
}

type bedrockRoundTrip func(*http.Request) (*http.Response, error)

func (function bedrockRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
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

func mustBedrockProfile(t *testing.T, baseURL string) Profile {
	t.Helper()
	profile := DefaultProfile("bedrock-contract")
	profile.CapabilityVersion = "bedrock-contract/v1"
	profile.Capabilities.Version = profile.CapabilityVersion
	profile.ExpectedBaseURL = baseURL
	profile.ExpectedModel = "claude-contract"
	validated, err := NewProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	return validated
}

func marshalBedrockWire(t *testing.T, value any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func mustReadBedrockFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/contracts/bedrock-anthropic/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
