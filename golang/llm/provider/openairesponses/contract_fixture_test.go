package openairesponses

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/responses"
	yaml "go.yaml.in/yaml/v4"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/contracttest"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/streamtest"
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
			normalized, err := llm.NormalizeRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
			if err != nil {
				t.Fatal(err)
			}
			params, err := lowerRequest(normalized, serviceClass)
			if err != nil {
				t.Fatal(err)
			}
			gotWire, err := json.Marshal(params)
			if err != nil {
				t.Fatal(err)
			}
			assertCanonicalFixtureJSON(t, gotWire, profile.id, "request.wire.json")

			response := loadContractResponseFixture(t, profile.id, "response.completed.json")
			call := provider.Call{
				EndpointID:   profile.endpoint,
				Family:       provider.FamilyOpenAIResponses,
				Model:        string(response.Model),
				OperationKey: normalized.OperationKey,
				ServiceClass: serviceClass,
				SDKParams:    params,
			}
			lifted, err := liftResponse(call, &response, profile.requestID)
			if err != nil {
				t.Fatal(err)
			}
			gotSemantic, err := json.Marshal(lifted)
			if err != nil {
				t.Fatal(err)
			}
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
				call := provider.Call{
					EndpointID:   profile.endpoint,
					Family:       provider.FamilyOpenAIResponses,
					Model:        string(response.Model),
					OperationKey: normalized.OperationKey,
					ServiceClass: serviceClass,
					SDKParams:    params,
				}
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
			call := provider.Call{
				EndpointID:   profile.endpoint,
				Family:       provider.FamilyOpenAIResponses,
				Model:        string(response.Model),
				OperationKey: "fixture-usage-cost",
				ServiceClass: llm.ServiceClassStandard,
			}
			lifted, err := liftResponse(call, &response, profile.requestID)
			if err != nil {
				t.Fatal(err)
			}
			got, err := json.Marshal(lifted)
			if err != nil {
				t.Fatal(err)
			}
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
			call, err := adapter.Compile(context.Background(), provider.CompileInput{
				Request: request,
				Query: provider.CapabilityQuery{
					EndpointID: profile.endpoint,
					Family:     provider.FamilyOpenAIResponses,
					Model:      request.Model,
				},
				Strict: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !call.Metadata.OpaqueStateRequired {
				t.Fatal("continuation call did not retain opaque-state requirement")
			}
			gotWire, err := json.Marshal(call.SDKParams)
			if err != nil {
				t.Fatal(err)
			}
			assertCanonicalFixtureJSON(t, gotWire, profile.id, "continuation-compatibility.wire.json")
		})
	}
}

func TestResponsesContractFixturesStayRedactedAndRespectCoverageBoundary(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	report, err := contracttest.ValidateRepository(root)
	if err != nil {
		t.Fatal(err)
	}
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
			want, err := DecodeStream(bytes.NewReader(wire))
			if err != nil {
				t.Fatal(err)
			}
			for split := 1; split < len(wire); split++ {
				got, err := DecodeStream(&responseChunkReader{chunks: [][]byte{wire[:split], {}, wire[split:]}})
				if err != nil {
					t.Fatalf("split %d: %v", split, err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("split %d changed decoded events\n got: %#v\nwant: %#v", split, got, want)
				}
			}
			for _, chunks := range [][][]byte{streamtest.Fragment(wire, 1), streamtest.RandomFragment(wire, 17, 11)} {
				got, err := DecodeStream(&responseChunkReader{chunks: chunks})
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("fragmented events differ\n got: %#v\nwant: %#v", got, want)
				}
			}
			assembler := provider.NewAssembler("fixture-stream")
			for _, event := range want {
				if err := assembler.Add(event); err != nil {
					t.Fatal(err)
				}
			}
			assembled, err := assembler.Result()
			if err != nil {
				t.Fatal(err)
			}
			gotSemantic, err := json.Marshal(assembled)
			if err != nil {
				t.Fatal(err)
			}
			assertCanonicalFixtureJSON(t, gotSemantic, profile.id, "stream.decoder.semantic.json")
			if err := contracttest.VerifyStreamAssemblyEquivalent(wire, readContractFixture(t, profile.id, "stream.decoder.semantic.json"), func(events []byte) ([]byte, error) {
				return assembleResponsesFixtureStream(events, "fixture-stream")
			}, metadata.GeneratedFieldExemptions); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func assembleResponsesFixtureStream(wire []byte, operationKey string) ([]byte, error) {
	events, err := DecodeStream(bytes.NewReader(wire))
	if err != nil {
		return nil, err
	}
	assembler := provider.NewAssembler(operationKey)
	for _, event := range events {
		if err := assembler.Add(event); err != nil {
			return nil, err
		}
	}
	response, err := assembler.Result()
	if err != nil {
		return nil, err
	}
	return json.Marshal(response)
}

func TestOpenAIResponsesContractFixtureUsesDirectTransport(t *testing.T) {
	profile := responsesFixtureProfiles[0]
	request := loadContractRequestFixture(t, profile.id, "request.semantic.json")
	responseBody := readContractFixture(t, profile.id, "response.completed.json")
	var got *http.Request
	var calls int
	client, err := NewClient(ClientConfig{
		BaseURL: "http://127.0.0.1/v1",
		APIKey:  "fixture-openai-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls++
			got = request
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{profile.requestID}},
				Body:       io.NopCloser(bytes.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAdapter(client, profile.endpoint, "openai-responses/fixture")
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: profile.endpoint,
			Family:     provider.FamilyOpenAIResponses,
			Model:      request.Model,
		},
		Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
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
	gotSemantic, err := json.Marshal(result.Response)
	if err != nil {
		t.Fatal(err)
	}
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
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{profile.requestID}},
				Body:       io.NopCloser(bytes.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAzureAdapter(client, profile.endpoint, "azure-responses/fixture")
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: profile.endpoint,
			Family:     provider.FamilyOpenAIResponses,
			Model:      request.Model,
		},
		Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
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
	gotSemantic, err := json.Marshal(result.Response)
	if err != nil {
		t.Fatal(err)
	}
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
	var semantic []struct {
		Class  llm.ServiceClass `json:"class"`
		Actual llm.ServiceClass `json:"actual"`
	}
	if err := json.Unmarshal(readContractFixture(t, profile.id, "class-facts.semantic.json"), &semantic); err != nil {
		t.Fatal(err)
	}
	var wire []struct {
		Class         llm.ServiceClass `json:"class"`
		Supported     bool             `json:"supported"`
		RequestedTier string           `json:"requested_tier"`
		ActualTier    string           `json:"actual_tier"`
	}
	if err := json.Unmarshal(readContractFixture(t, profile.id, "class-facts.wire.json"), &wire); err != nil {
		t.Fatal(err)
	}
	actualByClass := make(map[llm.ServiceClass]llm.ServiceClass, len(semantic))
	for _, fact := range semantic {
		actualByClass[fact.Class] = fact.Actual
	}
	for _, fact := range wire {
		if !fact.Supported {
			continue
		}
		params, err := lowerRequest(llm.Request{OperationKey: "class-fact", Model: "fixture-model"}, fact.Class)
		if err != nil {
			t.Fatalf("%s %s: %v", profile.id, fact.Class, err)
		}
		if got := marshalParams(t, params)["service_tier"]; got != fact.RequestedTier {
			t.Fatalf("%s %s requested tier = %#v, want %q", profile.id, fact.Class, got, fact.RequestedTier)
		}
		actual, err := serviceClassForTier(responses.ResponseServiceTier(fact.ActualTier))
		if err != nil {
			t.Fatalf("%s %s actual tier %q: %v", profile.id, fact.Class, fact.ActualTier, err)
		}
		if actual == nil || *actual != actualByClass[fact.Class] {
			t.Fatalf("%s %s actual class = %#v, want %q", profile.id, fact.Class, actual, actualByClass[fact.Class])
		}
	}
}

func assertLossyRequestIsRejectedBeforeDispatch(t *testing.T, adapter *Adapter, profile responsesFixtureProfile, request llm.Request) {
	t.Helper()
	_, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: profile.endpoint, Family: provider.FamilyOpenAIResponses, Model: request.Model},
		Strict:  true,
	})
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
	var fixture struct {
		Status     int    `json:"status"`
		Code       string `json:"code"`
		RequestID  string `json:"request_id"`
		RetryAfter string `json:"retry_after"`
		Message    string `json:"message"`
	}
	if err := json.Unmarshal(readContractFixture(t, profile.id, "classified-error.wire.json"), &fixture); err != nil {
		t.Fatal(err)
	}
	mapped := mapError(&openai.Error{
		Code:       fixture.Code,
		Message:    fixture.Message,
		StatusCode: fixture.Status,
		Response:   &http.Response{Header: http.Header{"X-Request-Id": []string{fixture.RequestID}, "Retry-After": []string{fixture.RetryAfter}}},
	})
	if mapped.Code != provider.CodeProviderRateLimited || mapped.Dispatch != provider.DispatchRejected || mapped.Provider.RequestID != fixture.RequestID {
		t.Fatalf("%s mapped error = %#v", profile.id, mapped)
	}
	encoded, err := json.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
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
		adapter, err := NewAzureAdapter(client, profile.endpoint, "azure-responses/fixture")
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
	var request llm.Request
	if err := json.Unmarshal(readContractFixture(t, profile, name), &request); err != nil {
		t.Fatal(err)
	}
	return request
}

func loadContractResponseFixture(t *testing.T, profile, name string) responses.Response {
	t.Helper()
	var response responses.Response
	if err := json.Unmarshal(readContractFixture(t, profile, name), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func loadContractFixtureMetadata(t *testing.T, profile string) contracttest.Metadata {
	t.Helper()
	var metadata contracttest.Metadata
	if err := yaml.Unmarshal(readContractFixture(t, profile, "metadata.yaml"), &metadata); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func loadContractSemanticResponseFixture(t *testing.T, profile, name string) llm.Response {
	t.Helper()
	var response llm.Response
	if err := json.Unmarshal(readContractFixture(t, profile, name), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func readContractFixture(t *testing.T, profile, name string) []byte {
	t.Helper()
	path := filepath.Join("testdata", "contracts", profile, name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func assertCanonicalFixtureJSON(t *testing.T, got []byte, profile, name string) {
	t.Helper()
	gotCanonical, err := llm.CanonicalJSON(got)
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical, err := llm.CanonicalJSON(readContractFixture(t, profile, name))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotCanonical, wantCanonical) {
		t.Fatalf("%s/%s mismatch\n got: %s\nwant: %s", profile, name, gotCanonical, wantCanonical)
	}
}
