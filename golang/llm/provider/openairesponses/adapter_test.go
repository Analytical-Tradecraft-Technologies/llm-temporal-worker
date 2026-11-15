package openairesponses

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
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

func TestCompileRejectsUnusableCapabilitiesBeforeDispatch(t *testing.T) {
	for _, test := range []struct {
		name   string
		state  provider.CapabilityState
		strict bool
	}{
		{name: "unsupported strict", state: provider.CapabilityUnsupported, strict: true},
		{name: "unsupported best effort", state: provider.CapabilityUnsupported, strict: false},
		{name: "unknown strict", state: provider.CapabilityUnknown, strict: true},
		{name: "unknown best effort", state: provider.CapabilityUnknown, strict: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls int
			adapter := newFixtureAdapterWithTransport(t, []byte(`{"id":"unused"}`), func() { calls++ })
			capabilitySet := capabilities("cap-test")
			capabilitySet.Features[provider.FeatureText] = provider.Capability{State: test.state, Reason: "test capability state"}
			_, err := adapter.Compile(context.Background(), provider.CompileInput{
				Request: llm.Request{OperationKey: "op-capability", Model: "gpt-contract", Input: []llm.Item{
					llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}},
				}},
				Query: provider.CapabilityQuery{
					EndpointID: "openai-prod",
					Family:     provider.FamilyOpenAIResponses,
					Model:      "gpt-contract",
				},
				Capability: capabilitySet,
				Strict:     test.strict,
			})
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) {
				t.Fatalf("Compile() error = %T %v, want *provider.Error", err, err)
			}
			if providerErr.Code != provider.CodeUnsupportedCapability || providerErr.Phase != provider.PhaseCompile || providerErr.Dispatch != provider.DispatchNotDispatched {
				t.Fatalf("Compile() error = %#v, want unsupported compile rejection", providerErr)
			}
			if !strings.Contains(providerErr.SafeMessage, "text") || !strings.Contains(providerErr.SafeMessage, string(test.state)) {
				t.Fatalf("Compile() safe message = %q, want feature and state", providerErr.SafeMessage)
			}
			if calls != 0 {
				t.Fatalf("HTTP calls = %d, want zero after capability rejection", calls)
			}
		})
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
	observer := &adaptertest.Observer{}
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
	if observer.Before != 1 || observer.Headers != 1 || observer.Progress != 1 || observer.Metadata.RequestID != "req-invoke" {
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

	observer := &adaptertest.Observer{}
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
	if observer.Before != 1 || observer.Headers != 0 || observer.Progress != 0 {
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
			Transport: adaptertest.RedirectTransport(&calls, `{"error":{"message":"redirect"}}`),
			CheckRedirect: func(*http.Request, []*http.Request) error {
				redirects++
				return http.ErrUseLastResponse
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, call := compileOpenAIProdCall(t, client, "op-redirect")
	observer := &adaptertest.Observer{}
	_, err = adapter.Invoke(context.Background(), call, observer)
	mapped := adaptertest.AmbiguousRedirect(t, err)
	if calls != 1 || redirects != 1 {
		t.Fatalf("transport/redirect calls = %d/%d, want 1/1", calls, redirects)
	}
	if observer.Before != 1 || observer.Headers != 0 {
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
	transport := adaptertest.NewPreDialDeadlineTransport()
	client, err := NewClient(ClientConfig{
		BaseURL:    "https://provider.example/v1",
		APIKey:     "test-key",
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, call := compileOpenAIProdCall(t, client, "op-pre-dial-deadline")
	assertCallerDeadlineBeforeDial(t, transport, adapter, call)
}

// compileOpenAIProdCall builds the "openai-prod" adapter over client and
// compiles an empty gpt-contract request under operationKey.
func compileOpenAIProdCall(t *testing.T, client *Client, operationKey string) (*Adapter, provider.Call) {
	t.Helper()
	adapter, err := New(client, "openai-prod", "cap-test")
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: operationKey, Model: "gpt-contract"}})
	if err != nil {
		t.Fatal(err)
	}
	return adapter, call
}

// assertCallerDeadlineBeforeDial checks that a caller deadline expiring
// before transport dials stays a never-retried pre-dispatch failure after
// exactly one pre-write mark and no headers.
func assertCallerDeadlineBeforeDial(t *testing.T, transport *adaptertest.PreDialDeadlineTransport, adapter *Adapter, call provider.Call) {
	t.Helper()
	observer := &adaptertest.Observer{}
	adaptertest.CallerDeadlineBeforeDial(t, transport, func(ctx context.Context) error {
		_, err := adapter.Invoke(ctx, call, observer)
		return err
	})
	if observer.Before != 1 || observer.Headers != 0 {
		t.Fatalf("observer = %#v, want exactly one pre-write mark and no headers", observer)
	}
}

func newFixtureAdapter(t *testing.T, body []byte) *Adapter {
	return newFixtureAdapterWithTransport(t, body, nil)
}

func newFixtureAdapterWithTransport(t *testing.T, body []byte, onCall func()) *Adapter {
	t.Helper()
	client, err := NewClient(ClientConfig{
		BaseURL: "https://127.0.0.1/contract",
		APIKey:  "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if onCall != nil {
				onCall()
			}
			return adaptertest.JSONResponse(request, http.StatusOK, string(body), "X-Request-Id", "req-invoke"), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openai-prod", "cap-test", allowSupportedExtensions)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

// allowSupportedExtensions allows every supported openai.responses extension
// field, as an endpoint that lists all of them would.
var allowSupportedExtensions = WithAllowedExtensionFields("include", "store", "truncation")

type roundTripFunc = adaptertest.RoundTripFunc

// A continuation pinned to another endpoint, or whose first Responses state
// is not a stored-response ID, fails at compile instead of after admission
// (#1667).
func TestCompileRejectsForeignOrMistypedContinuations(t *testing.T) {
	adapter := newFixtureAdapter(t, nil)
	reasoning := llm.ProviderState{Provider: "openai", EndpointFamily: "responses", MediaType: reasoningStateMediaType, Opaque: []byte(`{"type":"reasoning"}`)}
	response := llm.ProviderState{Provider: "openai", EndpointFamily: "responses", MediaType: responseStateMediaType, Opaque: []byte("resp-1")}
	for want, continuation := range map[string]*llm.Continuation{
		"is not pinned endpoint":                 {Handle: "openai-responses:resp-1", EndpointID: "azure-prod", Pinned: true, ProviderStates: []llm.ProviderState{response}},
		"is not an OpenAI Responses response ID": {Handle: "openai-responses:resp-1", EndpointID: "openai-prod", Pinned: true, ProviderStates: []llm.ProviderState{reasoning, response}},
	} {
		name := want
		t.Run(name, func(t *testing.T) {
			_, err := adapter.Compile(context.Background(), provider.CompileInput{
				Request: llm.Request{
					OperationKey: "op-follow-up",
					Model:        "gpt-contract",
					Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "second turn"}}}},
					Continuation: continuation,
				},
				Query:  provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
				Strict: true,
			})
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeInvalidArgument || mapped.Dispatch != provider.DispatchNotDispatched || !strings.Contains(err.Error(), want) {
				t.Fatalf("Compile = %v, want invalid_argument before dispatch naming %q", err, want)
			}
		})
	}
}
