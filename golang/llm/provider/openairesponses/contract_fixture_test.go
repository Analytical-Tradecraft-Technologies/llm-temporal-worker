package openairesponses

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/responses"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/contracttest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

type responsesFixtureProfile struct {
	id        string
	endpoint  string
	requestID string
}

var responsesFixtureProfiles = []responsesFixtureProfile{
	{id: "openai-responses", endpoint: "openai-fixture", requestID: "req-openai-fixture"},
	{id: "azure-responses", endpoint: "azure-fixture", requestID: "req-azure-fixture"},
}

func TestResponsesContractFixturesMatchCurrentLoweringAndLifting(t *testing.T) {
	for _, profile := range responsesFixtureProfiles {
		t.Run(profile.id, func(t *testing.T) {
			request := loadContractRequestFixture(t, profile.id, "request.semantic.json")
			normalized, serviceClass := contract.NormalizeRequest(t, request)
			params, err := fixtureAdapterForProfile(t, profile).lowerRequest(normalized, serviceClass, "")
			if err != nil {
				t.Fatal(err)
			}
			gotWire := contract.MarshalJSON(t, params)
			assertCanonicalFixtureJSON(t, gotWire, profile.id, "request.wire.json")

			response := loadContractResponseFixture(t, profile.id, "response.completed.json")
			call := profile.call(response, normalized.OperationKey, serviceClass, params)
			lifted := mustLift(t, call, &response, profile.requestID)
			gotSemantic := contract.MarshalJSON(t, lifted)
			assertCanonicalFixtureJSON(t, gotSemantic, profile.id, "response.semantic.json")

			want := loadContractSemanticResponseFixture(t, profile.id, "response.semantic.json")
			assertOpaqueReasoningState(t, lifted, want)
		})
	}
}

func TestResponsesContractFixturesVerifySemanticRoundTrip(t *testing.T) {
	for _, profile := range responsesFixtureProfiles {
		t.Run(profile.id, func(t *testing.T) {
			requestSemantic := readContractFixture(t, profile.id, "request.semantic.json")
			responseSemantic := readContractFixture(t, profile.id, "response.semantic.json")
			semantic, err := json.Marshal(struct {
				Request  json.RawMessage `json:"request"`
				Response json.RawMessage `json:"response"`
			}{Request: requestSemantic, Response: responseSemantic})
			if err != nil {
				t.Fatal(err)
			}
			metadata := loadContractFixtureMetadata(t, profile.id)
			responseWire := readContractFixture(t, profile.id, "response.completed.json")
			if err := contracttest.VerifySemanticRoundTrip(semantic, func(semantic []byte) ([]byte, error) {
				var fixture struct {
					Request  json.RawMessage `json:"request"`
					Response json.RawMessage `json:"response"`
				}
				if err := json.Unmarshal(semantic, &fixture); err != nil {
					return nil, err
				}
				var request llm.Request
				if err := json.Unmarshal(fixture.Request, &request); err != nil {
					return nil, err
				}
				normalized, err := llm.NormalizeRequest(request)
				if err != nil {
					return nil, err
				}
				serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
				if err != nil {
					return nil, err
				}
				params, err := lowerRequest(normalized, serviceClass)
				if err != nil {
					return nil, err
				}
				var response responses.Response
				if err := json.Unmarshal(responseWire, &response); err != nil {
					return nil, err
				}
				call := profile.call(response, normalized.OperationKey, serviceClass, params)
				lifted, err := liftResponse(call, &response, profile.requestID)
				if err != nil {
					return nil, err
				}
				fixture.Response, err = json.Marshal(lifted)
				if err != nil {
					return nil, err
				}
				return json.Marshal(fixture)
			}, metadata.GeneratedFieldExemptions); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResponsesContractFixturesCoverUsageClassAndStrictLoss(t *testing.T) {
	for _, profile := range responsesFixtureProfiles {
		t.Run(profile.id, func(t *testing.T) {
			assertProfileClassFacts(t, profile)

			response := loadContractResponseFixture(t, profile.id, "usage-cost.response.json")
			call := profile.call(response, "fixture-usage-cost", llm.ServiceClassStandard, nil)
			lifted := mustLift(t, call, &response, profile.requestID)
			got := contract.MarshalJSON(t, lifted)
			assertCanonicalFixtureJSON(t, got, profile.id, "usage-cost.semantic.json")
			if lifted.Cost != (llm.Cost{}) {
				t.Fatalf("provider cost should remain unreported for fixture response: %#v", lifted.Cost)
			}

			strictLoss := loadContractRequestFixture(t, profile.id, "strict-loss.semantic.json")
			adapter := fixtureAdapterForProfile(t, profile)
			assertLossyRequestIsRejectedBeforeDispatch(t, adapter, profile, strictLoss)
			strictLoss.Portability = llm.PortabilityBestEffort
			assertLossyRequestIsRejectedBeforeDispatch(t, adapter, profile, strictLoss)

			assertClassifiedErrorFixture(t, profile)
		})
	}
}

func TestResponsesContractFixturesCoverContinuationCompatibility(t *testing.T) {
	for _, profile := range responsesFixtureProfiles {
		t.Run(profile.id, func(t *testing.T) {
			request := loadContractRequestFixture(t, profile.id, "continuation-compatibility.semantic.json")
			adapter := fixtureAdapterForProfile(t, profile)
			call := adaptertest.CompileStrict(t, adapter, profile.endpoint, provider.FamilyOpenAIResponses, request)
			if !call.Metadata.OpaqueStateRequired {
				t.Fatal("continuation call did not retain opaque-state requirement")
			}
			gotWire := contract.MarshalJSON(t, call.SDKParams)
			assertCanonicalFixtureJSON(t, gotWire, profile.id, "continuation-compatibility.wire.json")
		})
	}
}

func TestResponsesContractFixturesStayRedactedAndRespectCoverageBoundary(t *testing.T) {
	report := contract.ValidateRepository(t)
	wantProfiles := map[string]contracttest.Profile{
		"openai-responses": {
			ID:       "openai-responses",
			Coverage: contracttest.CoverageEnforced,
			Path:     "llm/provider/openairesponses/testdata/contracts/openai-responses",
		},
		"azure-responses": {
			ID:       "azure-responses",
			Coverage: contracttest.CoverageEnforced,
			Path:     "llm/provider/openairesponses/testdata/contracts/azure-responses",
		},
	}
	found := make(map[string]contracttest.Profile)
	for _, profile := range append(report.Bootstrap, report.Enforced...) {
		found[profile.ID] = profile
	}
	for id, want := range wantProfiles {
		profile, ok := found[id]
		if !ok {
			t.Fatalf("coverage report is missing %s: %#v", id, report)
		}
		if profile != want {
			t.Fatalf("%s fixture profile = %#v, want %#v", id, profile, want)
		}
	}
	for _, profile := range responsesFixtureProfiles {
		redaction := readContractFixture(t, profile.id, "security-redaction.wire.json")
		if !bytes.Contains(redaction, []byte("[REDACTED]")) {
			t.Fatalf("%s redaction fixture has no explicit marker", profile.id)
		}
		for _, unsafe := range []string{"fixture-openai-key", "azure-key", "Bearer sk-", "api-key-real"} {
			if bytes.Contains(redaction, []byte(unsafe)) {
				t.Fatalf("%s redaction fixture contains %q", profile.id, unsafe)
			}
		}
	}
}

func TestResponsesDecoderFixturesRemainFragmentationInvariant(t *testing.T) {
	for _, profile := range responsesFixtureProfiles {
		t.Run(profile.id, func(t *testing.T) {
			wire := readContractFixture(t, profile.id, "stream.decoder.events")
			metadata := loadContractFixtureMetadata(t, profile.id)
			want := streamtest.FragmentationInvariant(t, wire, 17, 11, DecodeStream)
			assembled, err := streamtest.Assemble("fixture-stream", want)
			if err != nil {
				t.Fatal(err)
			}
			gotSemantic := contract.MarshalJSON(t, assembled)
			assertCanonicalFixtureJSON(t, gotSemantic, profile.id, "stream.decoder.semantic.json")
			if err := contracttest.VerifyStreamAssemblyEquivalent(wire, readContractFixture(t, profile.id, "stream.decoder.semantic.json"), func(events []byte) ([]byte, error) {
				return streamtest.AssembleJSON(events, "fixture-stream", DecodeStream)
			}, metadata.GeneratedFieldExemptions); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenAIResponsesContractFixtureUsesDirectTransport(t *testing.T) {
	profile := responsesFixtureProfiles[0]
	request := loadContractRequestFixture(t, profile.id, "request.semantic.json")
	responseBody := readContractFixture(t, profile.id, "response.completed.json")
	var got *http.Request
	var calls int
	client, err := NewClient(ClientConfig{
		BaseURL: "https://127.0.0.1/v1",
		APIKey:  "fixture-openai-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls++
			got = request
			return adaptertest.JSONResponse(request, http.StatusOK, string(responseBody), "X-Request-Id", profile.requestID), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAdapter(client, profile.endpoint, "openai-responses/fixture", allowSupportedExtensions)
	if err != nil {
		t.Fatal(err)
	}
	call := adaptertest.CompileStrict(t, adapter, profile.endpoint, provider.FamilyOpenAIResponses, request)
	result := adaptertest.Invoke(t, adapter, call)
	if calls != 1 {
		t.Fatalf("OpenAI Responses HTTP calls = %d, want one", calls)
	}
	if got == nil || got.Method != http.MethodPost || got.URL.Path != "/v1/responses" || got.URL.RawQuery != "" {
		t.Fatalf("OpenAI Responses request = %v", gotURL(got))
	}
	if got.Header.Get("Authorization") != "Bearer fixture-openai-key" || got.Header.Get("Api-Key") != "" {
		t.Fatalf("OpenAI Responses auth headers = %#v", got.Header)
	}
	body, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalFixtureJSON(t, body, profile.id, "request.wire.json")
	gotSemantic := contract.MarshalJSON(t, result.Response)
	assertCanonicalFixtureJSON(t, gotSemantic, profile.id, "response.semantic.json")
}

func TestAzureResponsesContractFixtureUsesAzureTransport(t *testing.T) {
	profile := responsesFixtureProfiles[1]
	request := loadContractRequestFixture(t, profile.id, "request.semantic.json")
	responseBody := readContractFixture(t, profile.id, "response.completed.json")
	var got *http.Request
	client, err := NewAzureClient(AzureClientConfig{
		Endpoint:   "https://127.0.0.1",
		APIVersion: "v1",
		APIKey:     "test-azure-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			got = request
			return adaptertest.JSONResponse(request, http.StatusOK, string(responseBody), "X-Request-Id", profile.requestID), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAzureAdapter(client, profile.endpoint, "azure-responses/fixture", allowSupportedExtensions)
	if err != nil {
		t.Fatal(err)
	}
	call := adaptertest.CompileStrict(t, adapter, profile.endpoint, provider.FamilyOpenAIResponses, request)
	result := adaptertest.Invoke(t, adapter, call)
	if got == nil || got.URL.Path != "/openai/v1/responses" || got.URL.Query().Get("api-version") != "v1" {
		t.Fatalf("Azure Responses request URL = %v", gotURL(got))
	}
	if got.Header.Get("Api-Key") != "test-azure-key" || got.Header.Get("Authorization") != "" {
		t.Fatalf("Azure Responses auth headers = %#v", got.Header)
	}
	body, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalFixtureJSON(t, body, profile.id, "request.wire.json")
	gotSemantic := contract.MarshalJSON(t, result.Response)
	assertCanonicalFixtureJSON(t, gotSemantic, profile.id, "response.semantic.json")
}

func TestCapabilitiesKeepStreamingUnsupportedAndContinuationNative(t *testing.T) {
	adapter := newFixtureAdapter(t, []byte(`{"id":"unused"}`))
	set, err := adapter.Capabilities(context.Background(), provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses})
	if err != nil {
		t.Fatal(err)
	}
	if capability := set.Features[provider.FeatureStreaming]; capability.State != provider.CapabilityUnsupported {
		t.Fatalf("streaming capability = %#v, want unsupported until a streaming adapter dispatches SDK streams", capability)
	}
	if capability := set.Features[provider.FeatureContinuation]; capability.State != provider.CapabilityNative {
		t.Fatalf("continuation capability = %#v, want native for stateful same-endpoint Responses chains", capability)
	}
}

func assertProfileClassFacts(t *testing.T, profile responsesFixtureProfile) {
	t.Helper()
	wire, actualByClass := contract.ClassFacts(t, readContractFixture(t, profile.id, "class-facts.semantic.json"), readContractFixture(t, profile.id, "class-facts.wire.json"))
	for _, fact := range wire {
		if !fact.Supported {
			continue
		}
		params, err := lowerRequest(llm.Request{OperationKey: "class-fact", Model: "fixture-model"}, fact.Class)
		if err != nil {
			t.Fatalf("%s %s: %v", profile.id, fact.Class, err)
		}
		if got := contract.JSONObject(t, params)["service_tier"]; got != fact.RequestedTier {
			t.Fatalf("%s %s requested tier = %#v, want %q", profile.id, fact.Class, got, fact.RequestedTier)
		}
		actual := serviceClassForTier(responses.ResponseServiceTier(fact.ActualTier))
		if actual == nil || *actual != actualByClass[fact.Class] {
			t.Fatalf("%s %s actual class = %#v, want %q", profile.id, fact.Class, actual, actualByClass[fact.Class])
		}
	}
}

func assertLossyRequestIsRejectedBeforeDispatch(t *testing.T, adapter *Adapter, profile responsesFixtureProfile, request llm.Request) {
	t.Helper()
	_, err := adaptertest.Compile(adapter, profile.endpoint, provider.FamilyOpenAIResponses, request, true)
	providerErr, ok := err.(*provider.Error)
	if !ok {
		t.Fatalf("%s lossy compile error = %T %v, want provider error", profile.id, err, err)
	}
	if providerErr.Dispatch != provider.DispatchNotDispatched || providerErr.Phase != provider.PhaseCompile || !strings.Contains(providerErr.SafeMessage, "sampling") {
		t.Fatalf("%s lossy compile error = %#v", profile.id, providerErr)
	}
	var expected struct {
		Code     provider.Code              `json:"code"`
		Phase    provider.Phase             `json:"phase"`
		Dispatch provider.DispatchCertainty `json:"dispatch"`
	}
	if err := json.Unmarshal(readContractFixture(t, profile.id, "strict-loss.wire.json"), &expected); err != nil {
		t.Fatal(err)
	}
	if providerErr.Code != expected.Code || providerErr.Phase != expected.Phase || providerErr.Dispatch != expected.Dispatch {
		t.Fatalf("%s strict-loss facts = %#v, want %#v", profile.id, providerErr, expected)
	}
}

func assertClassifiedErrorFixture(t *testing.T, profile responsesFixtureProfile) {
	t.Helper()
	fixture := contract.DecodeJSON[contract.ClassifiedErrorFixture](t, readContractFixture(t, profile.id, "classified-error.wire.json"))
	mapped := mapError(&openai.Error{
		Code:       fixture.Code,
		Message:    fixture.Message,
		StatusCode: fixture.Status,
		Response:   &http.Response{Header: http.Header{"X-Request-Id": []string{fixture.RequestID}, "Retry-After": []string{fixture.RetryAfter}}},
	})
	if mapped.Code != provider.CodeProviderRateLimited || mapped.Dispatch != provider.DispatchRejected || mapped.Provider.RequestID != fixture.RequestID {
		t.Fatalf("%s mapped error = %#v", profile.id, mapped)
	}
	encoded := contract.MarshalJSON(t, mapped)
	if strings.Contains(string(encoded), fixture.Message) {
		t.Fatalf("%s classified error leaked fixture body", profile.id)
	}
}

func assertOpaqueReasoningState(t *testing.T, got, want llm.Response) {
	t.Helper()
	var gotState, wantState *llm.ProviderState
	for _, item := range got.Output {
		if state, ok := item.(llm.ProviderState); ok {
			copy := state
			gotState = &copy
		}
	}
	for _, item := range want.Output {
		if state, ok := item.(llm.ProviderState); ok {
			copy := state
			wantState = &copy
		}
	}
	if gotState == nil || wantState == nil || gotState.MediaType != wantState.MediaType || !bytes.Equal(gotState.Opaque, wantState.Opaque) {
		t.Fatalf("opaque reasoning state = %#v, want %#v", gotState, wantState)
	}
}

func fixtureAdapterForProfile(t *testing.T, profile responsesFixtureProfile) *Adapter {
	t.Helper()
	if profile.id == "azure-responses" {
		client, err := NewAzureClient(AzureClientConfig{
			Endpoint:   "http://127.0.0.1",
			APIVersion: "v1",
			APIKey:     "test-azure-key",
			HTTPClient: http.DefaultClient,
		})
		if err != nil {
			t.Fatal(err)
		}
		adapter, err := NewAzureAdapter(client, profile.endpoint, "azure-responses/fixture", allowSupportedExtensions)
		if err != nil {
			t.Fatal(err)
		}
		return adapter
	}
	adapter := newFixtureAdapter(t, []byte(`{"id":"unused"}`))
	adapter.endpointID = profile.endpoint
	return adapter
}

func loadContractRequestFixture(t *testing.T, profile, name string) llm.Request {
	t.Helper()
	return contract.DecodeJSON[llm.Request](t, readContractFixture(t, profile, name))
}

func loadContractResponseFixture(t *testing.T, profile, name string) responses.Response {
	t.Helper()
	return contract.DecodeJSON[responses.Response](t, readContractFixture(t, profile, name))
}

func loadContractFixtureMetadata(t *testing.T, profile string) contracttest.Metadata {
	t.Helper()
	return contract.DecodeMetadata(t, readContractFixture(t, profile, "metadata.yaml"))
}

func loadContractSemanticResponseFixture(t *testing.T, profile, name string) llm.Response {
	t.Helper()
	return contract.DecodeJSON[llm.Response](t, readContractFixture(t, profile, name))
}

func readContractFixture(t *testing.T, profile, name string) []byte {
	t.Helper()
	return contract.ReadFixture(t, filepath.Join("testdata", "contracts", profile, name))
}

func assertCanonicalFixtureJSON(t *testing.T, got []byte, profile, name string) {
	t.Helper()
	contract.AssertCanonicalJSON(t, got, readContractFixture(t, profile, name), profile+"/"+name)
}

// call is the call profile's endpoint makes for response's model under
// operationKey and serviceClass, carrying params.
func (profile responsesFixtureProfile) call(response responses.Response, operationKey string, serviceClass llm.ServiceClass, params any) provider.Call {
	return provider.Call{
		EndpointID:   profile.endpoint,
		Family:       provider.FamilyOpenAIResponses,
		Model:        string(response.Model),
		OperationKey: operationKey,
		ServiceClass: serviceClass,
		SDKParams:    params,
	}
}
