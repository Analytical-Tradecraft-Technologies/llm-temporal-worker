package openaichat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	openai "github.com/openai/openai-go/v3"
	yaml "go.yaml.in/yaml/v4"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/contracttest"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

type chatFixtureProfile struct {
	id        string
	path      string
	endpoint  string
	requestID string
	profile   Profile
}

func TestChatContractProfilesAreEnforcedIndependently(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	report, err := contracttest.ValidateRepository(root)
	if err != nil {
		t.Fatal(err)
	}

	wantPaths := map[string]string{
		"openai-chat":     "llm/provider/openaichat/testdata/contracts/common/chat",
		"openrouter-chat": "llm/provider/openaichat/testdata/contracts/openrouter-chat",
		"exa-chat":        "llm/provider/openaichat/testdata/contracts/exa-chat",
	}
	found := make(map[string]contracttest.Profile)
	for _, profile := range report.Enforced {
		found[profile.ID] = profile
	}
	for id, wantPath := range wantPaths {
		profile, ok := found[id]
		if !ok {
			t.Fatalf("enforced report is missing %s: %#v", id, report)
		}
		if profile.Path != wantPath {
			t.Fatalf("%s fixture path = %q, want %q", id, profile.Path, wantPath)
		}
	}
}

func TestChatContractFixturesMatchCurrentLoweringAndLifting(t *testing.T) {
	for _, profile := range chatFixtureProfiles(t) {
		t.Run(profile.id, func(t *testing.T) {
			request := loadChatFixtureRequest(t, profile, "request.semantic.json")
			normalized, err := llm.NormalizeRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
			if err != nil {
				t.Fatal(err)
			}
			tier, err := profile.profile.providerTier(serviceClass)
			if err != nil {
				t.Fatal(err)
			}
			params, err := lowerRequest(normalized, profile.profile, tier)
			if err != nil {
				t.Fatal(err)
			}
			gotWire, err := json.Marshal(params)
			if err != nil {
				t.Fatal(err)
			}
			assertCanonicalChatFixture(t, gotWire, profile, "request.wire.json")

			response := loadChatFixtureResponse(t, profile, "response.completed.json")
			lifted, err := profile.profile.liftResponse(chatFixtureCall(profile, normalized, serviceClass), &response, profile.requestID)
			if err != nil {
				t.Fatal(err)
			}
			gotSemantic, err := json.Marshal(lifted)
			if err != nil {
				t.Fatal(err)
			}
			assertCanonicalChatFixture(t, gotSemantic, profile, "response.semantic.json")
		})
	}
}

func TestChatContractFixturesVerifySemanticRoundTrip(t *testing.T) {
	for _, profile := range chatFixtureProfiles(t) {
		t.Run(profile.id, func(t *testing.T) {
			requestSemantic := readChatFixture(t, profile, "request.semantic.json")
			responseSemantic := readChatFixture(t, profile, "response.semantic.json")
			semantic, err := json.Marshal(struct {
				Request  json.RawMessage `json:"request"`
				Response json.RawMessage `json:"response"`
			}{Request: requestSemantic, Response: responseSemantic})
			if err != nil {
				t.Fatal(err)
			}
			metadata := loadChatFixtureMetadata(t, profile)
			responseWire := readChatFixture(t, profile, "response.completed.json")
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
				tier, err := profile.profile.providerTier(serviceClass)
				if err != nil {
					return nil, err
				}
				if _, err := lowerRequest(normalized, profile.profile, tier); err != nil {
					return nil, err
				}
				var response openai.ChatCompletion
				if err := json.Unmarshal(responseWire, &response); err != nil {
					return nil, err
				}
				lifted, err := profile.profile.liftResponse(chatFixtureCall(profile, normalized, serviceClass), &response, profile.requestID)
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

func TestChatContractFixturesCoverUsageClassesLossDiagnosticsAndContinuation(t *testing.T) {
	for _, profile := range chatFixtureProfiles(t) {
		t.Run(profile.id, func(t *testing.T) {
			assertChatFixtureClassFacts(t, profile)

			response := loadChatFixtureResponse(t, profile, "usage-cost.response.json")
			lifted, err := profile.profile.liftResponse(provider.Call{
				EndpointID:   profile.endpoint,
				Family:       provider.FamilyOpenAIChat,
				Model:        string(response.Model),
				OperationKey: "fixture-usage-cost",
				ServiceClass: llm.ServiceClassStandard,
			}, &response, profile.requestID)
			if err != nil {
				t.Fatal(err)
			}
			gotUsage, err := json.Marshal(lifted)
			if err != nil {
				t.Fatal(err)
			}
			assertCanonicalChatFixture(t, gotUsage, profile, "usage-cost.semantic.json")

			assertChatFixtureCompileError(t, profile, "strict-loss.semantic.json", "strict-loss.wire.json")
			assertChatBestEffortDiagnostic(t, profile)
			assertChatFixtureCompileError(t, profile, "continuation-compatibility.semantic.json", "continuation-compatibility.wire.json")
			assertChatFixtureClassifiedError(t, profile)
			assertChatFixtureRedaction(t, profile)
		})
	}
}

func TestChatContractFixturesKeepDecoderOutputFragmentationInvariant(t *testing.T) {
	for _, profile := range chatFixtureProfiles(t) {
		t.Run(profile.id, func(t *testing.T) {
			wire := readChatFixture(t, profile, "stream.decoder.events")
			metadata := loadChatFixtureMetadata(t, profile)
			want, err := DecodeStream(bytes.NewReader(wire))
			if err != nil {
				t.Fatal(err)
			}
			if len(want) == 0 {
				t.Fatal("stream fixture decoded no events")
			}
			for split := 1; split < len(wire); split++ {
				got, err := DecodeStream(&chunkReader{chunks: [][]byte{wire[:split], {}, wire[split:]}})
				if err != nil {
					t.Fatalf("split %d: %v", split, err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("split %d changed decoded events\n got: %#v\nwant: %#v", split, got, want)
				}
			}
			for _, chunks := range [][][]byte{streamtest.Fragment(wire, 1), streamtest.RandomFragment(wire, 17, 11)} {
				got, err := DecodeStream(&chunkReader{chunks: chunks})
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
			if _, err := assembler.Result(); err != nil {
				t.Fatal(err)
			}
			if err := contracttest.VerifyStreamAssemblyEquivalent(wire, readChatFixture(t, profile, "stream.decoder.semantic.json"), func(events []byte) ([]byte, error) {
				return assembleChatFixtureStream(events, "fixture-"+profile.id+"-stream")
			}, metadata.GeneratedFieldExemptions); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func assembleChatFixtureStream(wire []byte, operationKey string) ([]byte, error) {
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

func TestChatContractFixturesUseIndependentProfileTransports(t *testing.T) {
	for _, profile := range chatFixtureProfiles(t) {
		t.Run(profile.id, func(t *testing.T) {
			request := loadChatFixtureRequest(t, profile, "request.semantic.json")
			responseBody := readChatFixture(t, profile, "response.completed.json")
			var got *http.Request
			client := newChatFixtureClient(t, profile, roundTripFunc(func(request *http.Request) (*http.Response, error) {
				got = request
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{profile.requestID}},
					Body:       io.NopCloser(bytes.NewReader(responseBody)),
					Request:    request,
				}, nil
			}))
			adapter, err := New(client, profile.endpoint, profile.profile)
			if err != nil {
				t.Fatal(err)
			}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{
				Request: request,
				Query: provider.CapabilityQuery{
					EndpointID: profile.endpoint,
					Family:     provider.FamilyOpenAIChat,
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
			if got == nil {
				t.Fatal("fixture transport was not called")
			}
			body, err := io.ReadAll(got.Body)
			if err != nil {
				t.Fatal(err)
			}
			assertCanonicalChatFixture(t, body, profile, "request.wire.json")
			gotSemantic, err := json.Marshal(result.Response)
			if err != nil {
				t.Fatal(err)
			}
			assertCanonicalChatFixture(t, gotSemantic, profile, "response.semantic.json")
			assertChatFixtureAuth(t, profile, got)
		})
	}
}

func chatFixtureProfiles(t *testing.T) []chatFixtureProfile {
	t.Helper()

	openAIProfile, err := NewProfile(testProfile())
	if err != nil {
		t.Fatal(err)
	}
	openRouterProfile, err := NewOpenRouterProfile(OpenRouterProfileConfig{
		ID:                "openrouter-contract",
		CapabilityVersion: "openrouter-contract/v1",
		BaseURL:           openRouterBaseURL,
		Model:             "router-model",
		Capabilities:      chatFixtureCapabilities("openrouter-contract/v1"),
		ServiceTiers: map[llm.ServiceClass]string{
			llm.ServiceClassEconomy:  "",
			llm.ServiceClassStandard: "standard",
			llm.ServiceClassPriority: "",
		},
		ActualServiceClasses: map[string]llm.ServiceClass{
			"default":  llm.ServiceClassStandard,
			"standard": llm.ServiceClassStandard,
			"priority": llm.ServiceClassPriority,
		},
		MissingActualServiceClass: llm.ServiceClassStandard,
		ProviderOrder:             []string{"fixture-provider"},
		RequireParameters:         true,
	})
	if err != nil {
		t.Fatal(err)
	}
	exaProfile, err := NewExaProfile(ExaProfileConfig{
		ID:                "exa-contract",
		CapabilityVersion: "exa-contract/v1",
		BaseURL:           exaBaseURL,
		Model:             "exa",
		Capabilities:      chatFixtureCapabilities("exa-contract/v1"),
		ServiceTiers: map[llm.ServiceClass]string{
			llm.ServiceClassEconomy:  "",
			llm.ServiceClassStandard: "standard",
			llm.ServiceClassPriority: "",
		},
		ActualServiceClasses:      map[string]llm.ServiceClass{"standard": llm.ServiceClassStandard},
		MissingActualServiceClass: llm.ServiceClassStandard,
	})
	if err != nil {
		t.Fatal(err)
	}

	return []chatFixtureProfile{
		{id: "openai-chat", path: filepath.Join("common", "chat"), endpoint: "openai-chat-fixture", requestID: "req-openai-chat-fixture", profile: openAIProfile},
		{id: "openrouter-chat", path: "openrouter-chat", endpoint: "openrouter-chat-fixture", requestID: "req-openrouter-chat-fixture", profile: openRouterProfile},
		{id: "exa-chat", path: "exa-chat", endpoint: "exa-chat-fixture", requestID: "req-exa-chat-fixture", profile: exaProfile},
	}
}

func chatFixtureCapabilities(version string) provider.CapabilitySet {
	features := make(map[provider.Feature]provider.Capability, len(allFeatures()))
	for _, feature := range allFeatures() {
		state := provider.CapabilityNative
		if feature == provider.FeatureDocument || feature == provider.FeatureContinuation || feature == provider.FeatureStreaming {
			state = provider.CapabilityUnsupported
		}
		features[feature] = provider.Capability{State: state, Reason: "fixture contract"}
	}
	return provider.CapabilitySet{Version: version, Features: features}
}

func newChatFixtureClient(t *testing.T, profile chatFixtureProfile, transport http.RoundTripper) *Client {
	t.Helper()
	if transport == nil {
		transport = http.DefaultTransport
	}
	httpClient := &http.Client{Transport: transport}
	var (
		client *Client
		err    error
	)
	switch profile.id {
	case "openai-chat":
		client, err = NewClient(ClientConfig{BaseURL: "http://127.0.0.1/v1", APIKey: "fixture-chat-key", HTTPClient: httpClient})
	case "openrouter-chat":
		client, err = NewOpenRouterClient(OpenRouterClientConfig{BaseURL: openRouterBaseURL, APIKey: "fixture-openrouter-key", HTTPClient: httpClient})
	case "exa-chat":
		client, err = NewExaClient(ExaClientConfig{BaseURL: exaBaseURL, APIKey: "fixture-exa-key", HTTPClient: httpClient})
	default:
		t.Fatalf("unknown chat fixture profile %q", profile.id)
	}
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func chatFixtureCall(profile chatFixtureProfile, request llm.Request, serviceClass llm.ServiceClass) provider.Call {
	return provider.Call{
		EndpointID:   profile.endpoint,
		Family:       provider.FamilyOpenAIChat,
		Model:        request.Model,
		OperationKey: request.OperationKey,
		ServiceClass: serviceClass,
	}
}

func loadChatFixtureRequest(t *testing.T, profile chatFixtureProfile, name string) llm.Request {
	t.Helper()
	var request llm.Request
	if err := json.Unmarshal(readChatFixture(t, profile, name), &request); err != nil {
		t.Fatal(err)
	}
	return request
}

func loadChatFixtureResponse(t *testing.T, profile chatFixtureProfile, name string) openai.ChatCompletion {
	t.Helper()
	var response openai.ChatCompletion
	if err := json.Unmarshal(readChatFixture(t, profile, name), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func loadChatFixtureMetadata(t *testing.T, profile chatFixtureProfile) contracttest.Metadata {
	t.Helper()
	var metadata contracttest.Metadata
	if err := yaml.Unmarshal(readChatFixture(t, profile, "metadata.yaml"), &metadata); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func readChatFixture(t *testing.T, profile chatFixtureProfile, name string) []byte {
	t.Helper()
	path := filepath.Join("testdata", "contracts", profile.path, name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func assertCanonicalChatFixture(t *testing.T, got []byte, profile chatFixtureProfile, name string) {
	t.Helper()
	gotCanonical, err := llm.CanonicalJSON(got)
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical, err := llm.CanonicalJSON(readChatFixture(t, profile, name))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotCanonical, wantCanonical) {
		t.Fatalf("%s/%s mismatch\n got: %s\nwant: %s", profile.id, name, gotCanonical, wantCanonical)
	}
}

func assertChatFixtureClassFacts(t *testing.T, profile chatFixtureProfile) {
	t.Helper()
	var semantic []struct {
		Class  llm.ServiceClass `json:"class"`
		Actual llm.ServiceClass `json:"actual"`
	}
	if err := json.Unmarshal(readChatFixture(t, profile, "class-facts.semantic.json"), &semantic); err != nil {
		t.Fatal(err)
	}
	actualByClass := make(map[llm.ServiceClass]llm.ServiceClass, len(semantic))
	for _, fact := range semantic {
		actualByClass[fact.Class] = fact.Actual
	}

	var wire []struct {
		Class         llm.ServiceClass `json:"class"`
		Supported     bool             `json:"supported"`
		RequestedTier string           `json:"requested_tier"`
		ActualTier    string           `json:"actual_tier"`
	}
	if err := json.Unmarshal(readChatFixture(t, profile, "class-facts.wire.json"), &wire); err != nil {
		t.Fatal(err)
	}
	for _, fact := range wire {
		tier, err := profile.profile.providerTier(fact.Class)
		if !fact.Supported {
			if err == nil {
				t.Fatalf("%s %s unexpectedly has provider tier %q", profile.id, fact.Class, tier)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s %s: %v", profile.id, fact.Class, err)
		}
		if tier != fact.RequestedTier {
			t.Fatalf("%s %s requested tier = %q, want %q", profile.id, fact.Class, tier, fact.RequestedTier)
		}
		actual, err := profile.profile.actualClass(fact.ActualTier)
		if err != nil {
			t.Fatalf("%s %s actual tier %q: %v", profile.id, fact.Class, fact.ActualTier, err)
		}
		if actual == nil || *actual != actualByClass[fact.Class] {
			t.Fatalf("%s %s actual class = %#v, want %q", profile.id, fact.Class, actual, actualByClass[fact.Class])
		}
	}
}

func assertChatFixtureCompileError(t *testing.T, profile chatFixtureProfile, requestName, expectedName string) {
	t.Helper()
	request := loadChatFixtureRequest(t, profile, requestName)
	adapter, err := New(newChatFixtureClient(t, profile, nil), profile.endpoint, profile.profile)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: profile.endpoint,
			Family:     provider.FamilyOpenAIChat,
			Model:      request.Model,
		},
		Strict: true,
	})
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("%s %s compile error = %T %v, want provider error", profile.id, requestName, err, err)
	}
	var expected struct {
		Code            provider.Code              `json:"code"`
		Phase           provider.Phase             `json:"phase"`
		Dispatch        provider.DispatchCertainty `json:"dispatch"`
		MessageContains string                     `json:"message_contains"`
	}
	if err := json.Unmarshal(readChatFixture(t, profile, expectedName), &expected); err != nil {
		t.Fatal(err)
	}
	if mapped.Code != expected.Code || mapped.Phase != expected.Phase || mapped.Dispatch != expected.Dispatch {
		t.Fatalf("%s %s compile error = %#v, want %#v", profile.id, requestName, mapped, expected)
	}
	if expected.MessageContains != "" && !strings.Contains(mapped.SafeMessage, expected.MessageContains) {
		t.Fatalf("%s %s safe message = %q, want substring %q", profile.id, requestName, mapped.SafeMessage, expected.MessageContains)
	}
}

func assertChatBestEffortDiagnostic(t *testing.T, profile chatFixtureProfile) {
	t.Helper()
	request := loadChatFixtureRequest(t, profile, "best-effort-diagnostic.semantic.json")
	if request.Portability != llm.PortabilityBestEffort {
		t.Fatalf("%s best-effort fixture portability = %q", profile.id, request.Portability)
	}
	adapter, err := New(newChatFixtureClient(t, profile, nil), profile.endpoint, profile.profile)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: profile.endpoint,
			Family:     provider.FamilyOpenAIChat,
			Model:      request.Model,
		},
		Strict: true,
	})
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("%s best-effort compile error = %T %v, want provider error", profile.id, err, err)
	}
	if mapped.Code != provider.CodeInvalidArgument || mapped.Phase != provider.PhaseCompile || mapped.Dispatch != provider.DispatchNotDispatched || !strings.Contains(mapped.SafeMessage, "top_k") {
		t.Fatalf("%s best-effort diagnostic = %#v", profile.id, mapped)
	}
}

func assertChatFixtureClassifiedError(t *testing.T, profile chatFixtureProfile) {
	t.Helper()
	var fixture struct {
		Status     int    `json:"status"`
		Code       string `json:"code"`
		RequestID  string `json:"request_id"`
		RetryAfter string `json:"retry_after"`
		Message    string `json:"message"`
	}
	if err := json.Unmarshal(readChatFixture(t, profile, "classified-error.wire.json"), &fixture); err != nil {
		t.Fatal(err)
	}
	mapped := mapError(&openai.Error{
		Code:       fixture.Code,
		Message:    fixture.Message,
		StatusCode: fixture.Status,
		Response:   &http.Response{Header: http.Header{"X-Request-Id": []string{fixture.RequestID}, "Retry-After": []string{fixture.RetryAfter}}},
	}, profile.id)
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

func assertChatFixtureRedaction(t *testing.T, profile chatFixtureProfile) {
	t.Helper()
	redaction := readChatFixture(t, profile, "security-redaction.wire.json")
	if !bytes.Contains(redaction, []byte("[REDACTED]")) {
		t.Fatalf("%s redaction fixture has no explicit marker", profile.id)
	}
	for _, unsafe := range []string{"fixture-chat-key", "fixture-openrouter-key", "fixture-exa-key", "Bearer sk-", "api-key-real"} {
		if bytes.Contains(redaction, []byte(unsafe)) {
			t.Fatalf("%s redaction fixture contains %q", profile.id, unsafe)
		}
	}
}

func assertChatFixtureAuth(t *testing.T, profile chatFixtureProfile, request *http.Request) {
	t.Helper()
	switch profile.id {
	case "openai-chat":
		if request.URL.Path != "/v1/chat/completions" || request.Header.Get("Authorization") != "Bearer fixture-chat-key" {
			t.Fatalf("openai chat transport = %s %#v", request.URL, request.Header)
		}
	case "openrouter-chat":
		if request.URL.Path != "/api/v1/chat/completions" || request.Header.Get("Authorization") != "Bearer fixture-openrouter-key" {
			t.Fatalf("openrouter chat transport = %s %#v", request.URL, request.Header)
		}
	case "exa-chat":
		if request.URL.Path != "/chat/completions" || request.Header.Get("X-Api-Key") != "fixture-exa-key" || request.Header.Get("Authorization") != "" {
			t.Fatalf("exa chat transport = %s %#v", request.URL, request.Header)
		}
	}
}
