package openairesponses

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

	"github.com/openai/openai-go/v3/responses"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestCompileUsesAdapterCapabilitiesAndMetadata(t *testing.T) {
	adapter := newFixtureAdapter(t, []byte(`{"id":"unused"}`))
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-compile", Model: "gpt-contract", ServiceClass: llm.ServiceClassPriority},
		Query:   provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if call.OperationKey != "op-compile" || call.ServiceClass != llm.ServiceClassPriority || call.Metadata.ProviderTier != "priority" {
		t.Fatalf("call = %#v", call)
	}
	if call.Metadata.CapabilityVersion == "" || call.Metadata.EstimatedBytes <= 0 || call.Metadata.SchemaDigest == ([32]byte{}) {
		t.Fatalf("metadata = %#v", call.Metadata)
	}
	params, ok := call.SDKParams.(responses.ResponseNewParams)
	if !ok {
		t.Fatalf("SDK params type = %T", call.SDKParams)
	}
	if got := marshalParams(t, params)["service_tier"]; got != "priority" {
		t.Fatalf("compiled service tier = %#v", got)
	}
}

func TestInvokeRunsOneRequestAndLiftsResponse(t *testing.T) {
	responseBody, err := os.ReadFile("testdata/contracts/openai-responses/response.completed.json")
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	adapter := newFixtureAdapterWithTransport(t, responseBody, func() { calls++ })
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "op-invoke", Model: "gpt-contract"}})
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	result, err := adapter.Invoke(context.Background(), call, observer)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("HTTP calls = %d, want one", calls)
	}
	if result.Response.OperationKey != "op-invoke" || result.Response.Provider.ResponseID != "resp-1" {
		t.Fatalf("response = %#v", result.Response)
	}
	if observer.before != 1 || observer.headers != 1 || observer.progress != 1 || observer.metadata.RequestID != "req-invoke" {
		t.Fatalf("observer = %#v", observer)
	}
}

func TestInvokeRejectsNullResponseWithoutPanicking(t *testing.T) {
	var calls int
	adapter := newFixtureAdapterWithTransport(t, []byte("null"), func() { calls++ })
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-empty", Model: "gpt-contract"},
	})
	if err != nil {
		t.Fatal(err)
	}

	observer := &recordingObserver{}
	_, err = adapter.Invoke(context.Background(), call, observer)
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", err, err)
	}
	if mapped.Code != provider.CodeProviderInvalidResponse || mapped.Phase != provider.PhaseLift || mapped.Dispatch != provider.DispatchAccepted {
		t.Fatalf("nil response error = %#v, want accepted lift invalid response", mapped)
	}
	if calls != 1 {
		t.Fatalf("HTTP calls = %d, want one", calls)
	}
	if observer.before != 1 || observer.headers != 0 || observer.progress != 0 {
		t.Fatalf("observer = %#v, want pre-write only", observer)
	}
}

func TestStrictStoredResponseContinuationCompilesFollowUp(t *testing.T) {
	responseBody, err := os.ReadFile("testdata/contracts/openai-responses/response.completed.json")
	if err != nil {
		t.Fatal(err)
	}
	adapter := newFixtureAdapter(t, responseBody)
	first, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{
			OperationKey: "op-first",
			Model:        "gpt-contract",
			Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "first turn"}}}},
			Extensions:   map[string]json.RawMessage{"openai.responses": json.RawMessage(`{"store":true}`)},
		},
		Query:  provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
		Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstResult, err := adapter.Invoke(context.Background(), first, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
	if firstResult.Response.Continuation == nil {
		t.Fatal("stored response did not yield a continuation")
	}
	followUp, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{
			OperationKey: "op-follow-up",
			Model:        "gpt-contract",
			Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "second turn"}}}},
			Continuation: firstResult.Response.Continuation,
		},
		Query:  provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
		Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	params, ok := followUp.SDKParams.(responses.ResponseNewParams)
	if !ok {
		t.Fatalf("follow-up SDK params type = %T", followUp.SDKParams)
	}
	if got := marshalParams(t, params)["previous_response_id"]; got != "resp-1" {
		t.Fatalf("previous response ID = %#v, want resp-1", got)
	}
}

func TestInvokeOmitsContinuationForUnstoredResponse(t *testing.T) {
	responseBody, err := os.ReadFile("testdata/contracts/openai-responses/response.completed.json")
	if err != nil {
		t.Fatal(err)
	}
	adapter := newFixtureAdapter(t, responseBody)
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{
			OperationKey: "op-unstored",
			Model:        "gpt-contract",
			Extensions:   map[string]json.RawMessage{"openai.responses": json.RawMessage(`{"store":false}`)},
		},
		Query:  provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
		Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Response.Continuation != nil {
		t.Fatalf("unstored response continuation = %#v, want nil", result.Response.Continuation)
	}
}

func TestCapabilitiesRejectMismatchedFamily(t *testing.T) {
	adapter := newFixtureAdapter(t, []byte(`{}`))
	_, err := adapter.Capabilities(context.Background(), provider.CapabilityQuery{Family: provider.FamilyOpenAIChat})
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatched family error = %v", err)
	}
}

func TestInvokeTransportErrorIncludesEndpointIDAndRedactsCause(t *testing.T) {
	cause := errors.New("Authorization: Bearer provider-secret; prompt=private-content; continuation=opaque-handle; body=provider-raw")
	client, err := NewClient(ClientConfig{
		BaseURL: "https://provider.example/v1",
		APIKey:  "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, cause
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openai-prod", "cap-test")
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "op-transport", Model: "gpt-contract"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(context.Background(), call, nil)
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", err, err)
	}
	if got, want := mapped.SafeDetails["endpoint"], "openai-prod"; got != want {
		t.Fatalf("endpoint detail = %q, want %q", got, want)
	}
	encoded, marshalErr := json.Marshal(mapped)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	for _, raw := range []string{"provider-secret", "private-content", "opaque-handle", "provider-raw"} {
		if strings.Contains(string(encoded), raw) || strings.Contains(mapped.Error(), raw) {
			t.Fatalf("safe adapter error leaked %q: %s", raw, encoded)
		}
	}
	if !errors.Is(mapped, cause) {
		t.Fatal("adapter error did not retain the local diagnostic cause")
	}
}

func TestInvokeRedirectResponseIsAmbiguousAndNotFollowed(t *testing.T) {
	var calls, redirects int
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
			CheckRedirect: func(*http.Request, []*http.Request) error {
				redirects++
				return http.ErrUseLastResponse
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openai-prod", "cap-test")
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-redirect", Model: "gpt-contract"},
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	_, err = adapter.Invoke(context.Background(), call, observer)
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", err, err)
	}
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped redirect = %#v, want ambiguous non-retriable provider-unavailable", mapped)
	}
	if calls != 1 || redirects != 1 {
		t.Fatalf("transport/redirect calls = %d/%d, want 1/1", calls, redirects)
	}
	if observer.before != 1 || observer.headers != 0 {
		t.Fatalf("observer = %#v, want one pre-write mark and no headers", observer)
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
		BaseURL: "https://provider.example/v1",
		APIKey:  "test-key",
		HTTPClient: &http.Client{
			Transport: transport,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openai-prod", "cap-test")
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-pre-dial-deadline", Model: "gpt-contract"},
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
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
	if got, want := mapped.Code, provider.CodeDeadlineExceeded; got != want {
		t.Fatalf("error code = %q, want %q", got, want)
	}
	if got, want := mapped.Phase, provider.PhaseDispatch; got != want {
		t.Fatalf("error phase = %q, want %q", got, want)
	}
	if got, want := mapped.Dispatch, provider.DispatchNotDispatched; got != want {
		t.Fatalf("dispatch certainty = %q, want %q", got, want)
	}
	if got, want := mapped.Retry, provider.RetryNever; got != want {
		t.Fatalf("retry disposition = %q, want %q", got, want)
	}
	if !errors.Is(mapped, provider.ErrProviderPreDispatch) || !errors.Is(mapped, context.DeadlineExceeded) {
		t.Fatalf("mapped error = %v, want certified pre-dispatch caller deadline", mapped)
	}
	if transport.calls != 1 || transport.bodyReads != 0 {
		t.Fatalf("pre-dial transport calls/body reads = %d/%d, want 1/0", transport.calls, transport.bodyReads)
	}
	if observer.before != 1 || observer.headers != 0 {
		t.Fatalf("observer = %#v, want exactly one pre-write mark and no headers", observer)
	}
}

type recordingObserver struct {
	before   int
	headers  int
	progress int
	metadata provider.ResponseMetadata
}

func (observer *recordingObserver) BeforePossibleWrite(context.Context) error {
	observer.before++
	return nil
}

func (observer *recordingObserver) AfterResponseHeaders(_ context.Context, metadata provider.ResponseMetadata) error {
	observer.headers++
	observer.metadata = metadata
	return nil
}

func (observer *recordingObserver) OnProgress(context.Context, provider.Progress) {
	observer.progress++
}

func newFixtureAdapter(t *testing.T, body []byte) *Adapter {
	return newFixtureAdapterWithTransport(t, body, nil)
}

func newFixtureAdapterWithTransport(t *testing.T, body []byte, onCall func()) *Adapter {
	t.Helper()
	client, err := NewClient(ClientConfig{
		BaseURL: "http://127.0.0.1/contract",
		APIKey:  "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if onCall != nil {
				onCall()
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"req-invoke"}},
				Body:       io.NopCloser(strings.NewReader(string(body))),
				Request:    request,
			}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openai-prod", "cap-test")
	if err != nil {
		t.Fatal(err)
	}
	return adapter
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
	*reader.reads++
	return reader.ReadCloser.Read(data)
}
