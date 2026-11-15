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

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func TestCompileUsesAdapterCapabilitiesAndMetadata(t *testing.T) {
	adapter := newFixtureAdapter(t, []byte(`{"id":"unused"}`))
	call := adaptertest.CompileStrict(t, adapter, "openai-prod", provider.FamilyOpenAIResponses, llm.Request{OperationKey: "op-compile", Model: "gpt-contract", ServiceClass: llm.ServiceClassPriority})
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
	if got := contract.JSONObject(t, params)["service_tier"]; got != "priority" {
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
					llm.HumanText("hello"),
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
			testkit.ProviderError(t, providerErr, testkit.ProviderClass{Code: provider.CodeUnsupportedCapability, Phase: provider.PhaseCompile, Dispatch: provider.DispatchNotDispatched}, "Compile() error")
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
	testassert.NoError(t, err)
	var calls int
	adapter := newFixtureAdapterWithTransport(t, responseBody, func() { calls++ })
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "op-invoke", Model: "gpt-contract"}})
	testassert.NoError(t, err)
	observer := &adaptertest.Observer{}
	result, err := adapter.Invoke(context.Background(), call, observer)
	testassert.NoError(t, err)
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
	testassert.NoError(t, err)

	observer := &adaptertest.Observer{}
	_, err = adapter.Invoke(context.Background(), call, observer)
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", err, err)
	}
	testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: provider.CodeProviderInvalidResponse, Phase: provider.PhaseLift, Dispatch: provider.DispatchAccepted}, "nil response error")
	if calls != 1 {
		t.Fatalf("HTTP calls = %d, want one", calls)
	}
	if observer.Before != 1 || observer.Headers != 0 || observer.Progress != 0 {
		t.Fatalf("observer = %#v, want pre-write only", observer)
	}
}

func TestStrictStoredResponseContinuationCompilesFollowUp(t *testing.T) {
	responseBody, err := os.ReadFile("testdata/contracts/openai-responses/response.completed.json")
	testassert.NoError(t, err)
	adapter := newFixtureAdapter(t, responseBody)
	first := adaptertest.CompileStrict(t, adapter, "openai-prod", provider.FamilyOpenAIResponses, llm.Request{
		OperationKey: "op-first",
		Model:        "gpt-contract",
		Input:        []llm.Item{llm.HumanText("first turn")},
		Extensions:   map[string]json.RawMessage{"openai.responses": json.RawMessage(`{"store":true}`)},
	})
	firstResult := adaptertest.Invoke(t, adapter, first)
	if firstResult.Response.Continuation == nil {
		t.Fatal("stored response did not yield a continuation")
	}
	followUp := adaptertest.CompileStrict(t, adapter, "openai-prod", provider.FamilyOpenAIResponses, llm.Request{
		OperationKey: "op-follow-up",
		Model:        "gpt-contract",
		Input:        []llm.Item{llm.HumanText("second turn")},
		Continuation: firstResult.Response.Continuation,
	})
	params, ok := followUp.SDKParams.(responses.ResponseNewParams)
	if !ok {
		t.Fatalf("follow-up SDK params type = %T", followUp.SDKParams)
	}
	if got := contract.JSONObject(t, params)["previous_response_id"]; got != "resp-1" {
		t.Fatalf("previous response ID = %#v, want resp-1", got)
	}
}

func TestInvokeOmitsContinuationForUnstoredResponse(t *testing.T) {
	responseBody, err := os.ReadFile("testdata/contracts/openai-responses/response.completed.json")
	testassert.NoError(t, err)
	adapter := newFixtureAdapter(t, responseBody)
	call := adaptertest.CompileStrict(t, adapter, "openai-prod", provider.FamilyOpenAIResponses, llm.Request{
		OperationKey: "op-unstored",
		Model:        "gpt-contract",
		Extensions:   map[string]json.RawMessage{"openai.responses": json.RawMessage(`{"store":false}`)},
	})
	result := adaptertest.Invoke(t, adapter, call)
	if result.Response.Continuation != nil {
		t.Fatalf("unstored response continuation = %#v, want nil", result.Response.Continuation)
	}
}

func TestCapabilitiesRejectMismatchedFamily(t *testing.T) {
	adapter := newFixtureAdapter(t, []byte(`{}`))
	_, err := adapter.Capabilities(context.Background(), provider.CapabilityQuery{Family: provider.FamilyOpenAIChat})
	testassert.ErrorContains(t, err, "does not match")
}

func TestInvokeTransportErrorIncludesEndpointIDAndRedactsCause(t *testing.T) {
	cause := errors.New("Authorization: Bearer provider-secret; prompt=private-content; continuation=opaque-handle; body=provider-raw")
	adapter, call := compileOpenAIProdCall(t, &http.Client{Transport: testkit.RoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, cause
	})}, "op-transport")
	_, err := adapter.Invoke(context.Background(), call, nil)
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", err, err)
	}
	if got, want := mapped.SafeDetails["endpoint"], "openai-prod"; got != want {
		t.Fatalf("endpoint detail = %q, want %q", got, want)
	}
	encoded, marshalErr := json.Marshal(mapped)
	testassert.NoError(t, marshalErr)
	for _, raw := range []string{"provider-secret", "private-content", "opaque-handle", "provider-raw"} {
		if strings.Contains(string(encoded), raw) || strings.Contains(mapped.Error(), raw) {
			t.Fatalf("safe adapter error leaked %q: %s", raw, encoded)
		}
	}
	testassert.ErrorIs(t, mapped, cause, "adapter error did not retain the local diagnostic cause")
}

func TestInvokeRedirectResponseIsAmbiguousAndNotFollowed(t *testing.T) {
	var calls, redirects int
	adapter, call := compileOpenAIProdCall(t, &http.Client{
		Transport: adaptertest.RedirectTransport(&calls, `{"error":{"message":"redirect"}}`),
		CheckRedirect: func(*http.Request, []*http.Request) error {
			redirects++
			return http.ErrUseLastResponse
		},
	}, "op-redirect")
	observer := &adaptertest.Observer{}
	_, err := adapter.Invoke(context.Background(), call, observer)
	mapped := adaptertest.AmbiguousRedirect(t, err)
	if calls != 1 || redirects != 1 {
		t.Fatalf("transport/redirect calls = %d/%d, want 1/1", calls, redirects)
	}
	if observer.Before != 1 || observer.Headers != 0 {
		t.Fatalf("observer = %#v, want one pre-write mark and no headers", observer)
	}
	encoded, marshalErr := json.Marshal(mapped)
	testassert.NoError(t, marshalErr)
	if strings.Contains(string(encoded), "redirect.example") || strings.Contains(mapped.Error(), "continuation-secret") {
		t.Fatalf("safe adapter error leaked redirect target: %s", encoded)
	}
}

func TestInvokePreDispatchCallerDeadlineStaysRetryNever(t *testing.T) {
	transport := adaptertest.NewPreDialDeadlineTransport()
	adapter, call := compileOpenAIProdCall(t, &http.Client{Transport: transport}, "op-pre-dial-deadline")
	assertCallerDeadlineBeforeDial(t, transport, adapter, call)
}

// newOpenAIProdAdapter builds the "openai-prod" adapter over a client for
// baseURL that sends requests through httpClient.
func newOpenAIProdAdapter(t *testing.T, baseURL string, httpClient *http.Client, options ...AdapterOption) *Adapter {
	t.Helper()
	client, err := NewClient(ClientConfig{BaseURL: baseURL, APIKey: "test-key", HTTPClient: httpClient})
	testassert.NoError(t, err)
	adapter, err := New(client, "openai-prod", "cap-test", options...)
	testassert.NoError(t, err)
	return adapter
}

// compileOpenAIProdCall builds the "openai-prod" adapter over httpClient and
// compiles an empty gpt-contract request under operationKey.
func compileOpenAIProdCall(t *testing.T, httpClient *http.Client, operationKey string) (*Adapter, provider.Call) {
	t.Helper()
	adapter := newOpenAIProdAdapter(t, "https://provider.example/v1", httpClient)
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: operationKey, Model: "gpt-contract"}})
	testassert.NoError(t, err)
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
	return newOpenAIProdAdapter(t, "https://127.0.0.1/contract", &http.Client{Transport: testkit.RoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if onCall != nil {
			onCall()
		}
		return adaptertest.JSONResponse(request, http.StatusOK, string(body), "X-Request-Id", "req-invoke"), nil
	})}, allowSupportedExtensions)
}

// allowSupportedExtensions allows every supported openai.responses extension
// field, as an endpoint that lists all of them would.
var allowSupportedExtensions = WithAllowedExtensionFields("include", "store", "truncation")

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
					Input:        []llm.Item{llm.HumanText("second turn")},
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
