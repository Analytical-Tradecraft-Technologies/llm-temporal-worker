package anthropicmessages

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

	"github.com/anthropics/anthropic-sdk-go"
	yaml "go.yaml.in/yaml/v4"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/contracttest"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

const anthropicDirectFixtureEndpoint = "anthropic-prod"

func TestAnthropicDirectContractProfileIsEnforced(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	report, err := contracttest.ValidateRepository(root)
	if err != nil {
		t.Fatal(err)
	}

	const wantPath = "llm/provider/anthropicmessages/testdata/contracts/anthropic-direct"
	for _, profile := range report.Enforced {
		if profile.ID == "anthropic-direct" {
			if profile.Path != wantPath {
				t.Fatalf("anthropic-direct fixture path = %q, want %q", profile.Path, wantPath)
			}
			return
		}
	}
	t.Fatalf("enforced report is missing anthropic-direct: %#v", report)
}

func TestAnthropicDirectContractFixturesMatchCurrentLoweringAndLifting(t *testing.T) {
	profile := anthropicDirectFixtureProfile(t)
	request := loadAnthropicDirectFixtureRequest(t, "request.semantic.json")
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
	if err != nil {
		t.Fatal(err)
	}
	tier, err := profile.providerTier(serviceClass)
	if err != nil {
		t.Fatal(err)
	}
	params, err := lowerRequest(normalized, profile, tier)
	if err != nil {
		t.Fatal(err)
	}
	gotWire, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalAnthropicDirectFixture(t, gotWire, "request.wire.json")

	response := loadAnthropicDirectFixtureResponse(t, "response.completed.json")
	lifted, err := profile.liftResponse(anthropicDirectFixtureCall(normalized, serviceClass), &response, "req-fixture")
	if err != nil {
		t.Fatal(err)
	}
	gotSemantic, err := json.Marshal(lifted)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalAnthropicDirectFixture(t, gotSemantic, "response.semantic.json")
}

func TestAnthropicDirectContractFixturesVerifySemanticRoundTrip(t *testing.T) {
	profile := anthropicDirectFixtureProfile(t)
	requestSemantic := readAnthropicDirectFixture(t, "request.semantic.json")
	responseSemantic := readAnthropicDirectFixture(t, "response.semantic.json")
	semantic, err := json.Marshal(struct {
		Request  json.RawMessage `json:"request"`
		Response json.RawMessage `json:"response"`
	}{Request: requestSemantic, Response: responseSemantic})
	if err != nil {
		t.Fatal(err)
	}
	metadata := loadAnthropicDirectFixtureMetadata(t)
	responseWire := readAnthropicDirectFixture(t, "response.completed.json")
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
		tier, err := profile.providerTier(serviceClass)
		if err != nil {
			return nil, err
		}
		if _, err := lowerRequest(normalized, profile, tier); err != nil {
			return nil, err
		}
		var response anthropic.Message
		if err := json.Unmarshal(responseWire, &response); err != nil {
			return nil, err
		}
		lifted, err := profile.liftResponse(anthropicDirectFixtureCall(normalized, serviceClass), &response, "req-fixture")
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
}

func TestAnthropicDirectContractFixturesCoverUsageClassesLossErrorsAndContinuation(t *testing.T) {
	profile := anthropicDirectFixtureProfile(t)
	assertAnthropicDirectFixtureClassFacts(t, profile)

	response := loadAnthropicDirectFixtureResponse(t, "usage-cost.response.json")
	lifted, err := profile.liftResponse(provider.Call{
		EndpointID:   anthropicDirectFixtureEndpoint,
		Family:       provider.FamilyAnthropicMessages,
		Model:        "claude-contract",
		OperationKey: "fixture-usage-cost",
		ServiceClass: llm.ServiceClassStandard,
	}, &response, "req-anthropic-usage")
	if err != nil {
		t.Fatal(err)
	}
	gotUsage, err := json.Marshal(lifted)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalAnthropicDirectFixture(t, gotUsage, "usage-cost.semantic.json")

	assertAnthropicDirectFixtureCompileError(t, profile, loadAnthropicDirectFixtureRequest(t, "strict-loss.semantic.json"), "strict-loss.wire.json")
	assertAnthropicDirectFixtureCompileError(t, profile, loadAnthropicDirectFixtureRequest(t, "best-effort-diagnostic.semantic.json"), "best-effort-diagnostic.wire.json")
	assertAnthropicDirectFixtureContinuation(t, profile)
	assertAnthropicDirectFixtureClassifiedError(t)
	assertAnthropicDirectFixtureRedaction(t)
}

func TestAnthropicDirectContractFixtureDecoderIsFragmentationInvariant(t *testing.T) {
	wire := readAnthropicDirectFixture(t, "stream.decoder.events")
	metadata := loadAnthropicDirectFixtureMetadata(t)
	want, err := DecodeStream(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	if len(want) == 0 {
		t.Fatal("stream fixture decoded no events")
	}
	for split := 1; split < len(wire); split++ {
		got, err := DecodeStream(&anthropicChunkReader{chunks: [][]byte{wire[:split], {}, wire[split:]}})
		if err != nil {
			t.Fatalf("split %d: %v", split, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("split %d changed decoded events\n got: %#v\nwant: %#v", split, got, want)
		}
	}
	for _, chunks := range [][][]byte{streamtest.Fragment(wire, 1), streamtest.RandomFragment(wire, 29, 11)} {
		got, err := DecodeStream(&anthropicChunkReader{chunks: chunks})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("fragmented events differ\n got: %#v\nwant: %#v", got, want)
		}
	}
	assembler := provider.NewAssembler("fixture-anthropic-direct-stream")
	for _, event := range want {
		if err := assembler.Add(event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := assembler.Result(); err != nil {
		t.Fatal(err)
	}
	if err := contracttest.VerifyStreamAssemblyEquivalent(wire, readAnthropicDirectFixture(t, "stream.decoder.semantic.json"), func(events []byte) ([]byte, error) {
		return assembleAnthropicFixtureStream(events, "fixture-anthropic-direct-stream")
	}, metadata.GeneratedFieldExemptions); err != nil {
		t.Fatal(err)
	}
}

func assembleAnthropicFixtureStream(wire []byte, operationKey string) ([]byte, error) {
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

func TestAnthropicDirectContractFixturesUseIndependentProfileTransport(t *testing.T) {
	request := loadAnthropicDirectFixtureRequest(t, "request.semantic.json")
	responseBody := readAnthropicDirectFixture(t, "response.completed.json")
	var got *http.Request
	client := newAnthropicDirectFixtureClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		got = request
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "Request-Id": []string{"req-fixture"}},
			Body:       io.NopCloser(bytes.NewReader(responseBody)),
			Request:    request,
		}, nil
	}))
	adapter, err := New(client, anthropicDirectFixtureEndpoint, anthropicDirectFixtureProfile(t))
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: anthropicDirectFixtureEndpoint,
			Family:     provider.FamilyAnthropicMessages,
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
	assertCanonicalAnthropicDirectFixture(t, body, "request.wire.json")
	if got.URL.Path != "/v1/messages" || got.Header.Get("X-Api-Key") != "anthropic-fixture-key" || got.Header.Get("Authorization") != "" {
		t.Fatalf("Anthropic direct transport = %s %#v", got.URL, got.Header)
	}
	gotSemantic, err := json.Marshal(result.Response)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalAnthropicDirectFixture(t, gotSemantic, "response.semantic.json")
}

func newAnthropicDirectFixtureClient(t *testing.T, transport http.RoundTripper) *Client {
	t.Helper()
	if transport == nil {
		transport = http.DefaultTransport
	}
	client, err := NewClient(ClientConfig{
		BaseURL:    "https://api.anthropic.com",
		APIKey:     "anthropic-fixture-key",
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func anthropicDirectFixtureProfile(t *testing.T) Profile {
	t.Helper()
	profile := testProfile()
	// The official SDK owns the /v1/messages path. Profiles pin the endpoint
	// origin from the production configuration rather than duplicating that path.
	profile.ExpectedBaseURL = "https://api.anthropic.com"
	return mustProfile(t, profile)
}

func anthropicDirectFixtureCall(request llm.Request, serviceClass llm.ServiceClass) provider.Call {
	return provider.Call{
		EndpointID:   anthropicDirectFixtureEndpoint,
		Family:       provider.FamilyAnthropicMessages,
		Model:        request.Model,
		OperationKey: request.OperationKey,
		ServiceClass: serviceClass,
	}
}

func loadAnthropicDirectFixtureRequest(t *testing.T, name string) llm.Request {
	t.Helper()
	var request llm.Request
	if err := json.Unmarshal(readAnthropicDirectFixture(t, name), &request); err != nil {
		t.Fatal(err)
	}
	return request
}

func loadAnthropicDirectFixtureResponse(t *testing.T, name string) anthropic.Message {
	t.Helper()
	var response anthropic.Message
	if err := json.Unmarshal(readAnthropicDirectFixture(t, name), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func loadAnthropicDirectFixtureMetadata(t *testing.T) contracttest.Metadata {
	t.Helper()
	var metadata contracttest.Metadata
	if err := yaml.Unmarshal(readAnthropicDirectFixture(t, "metadata.yaml"), &metadata); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func readAnthropicDirectFixture(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("testdata", "contracts", "anthropic-direct", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func assertCanonicalAnthropicDirectFixture(t *testing.T, got []byte, name string) {
	t.Helper()
	gotCanonical, err := llm.CanonicalJSON(got)
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical, err := llm.CanonicalJSON(readAnthropicDirectFixture(t, name))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotCanonical, wantCanonical) {
		t.Fatalf("anthropic-direct/%s mismatch\n got: %s\nwant: %s", name, gotCanonical, wantCanonical)
	}
}

func assertAnthropicDirectFixtureClassFacts(t *testing.T, profile Profile) {
	t.Helper()
	var semantic []struct {
		Class  llm.ServiceClass `json:"class"`
		Actual llm.ServiceClass `json:"actual"`
	}
	if err := json.Unmarshal(readAnthropicDirectFixture(t, "class-facts.semantic.json"), &semantic); err != nil {
		t.Fatal(err)
	}
	actualByClass := make(map[llm.ServiceClass]llm.ServiceClass, len(semantic))
	for _, fact := range semantic {
		actualByClass[fact.Class] = fact.Actual
	}

	var wire []struct {
		Class            llm.ServiceClass `json:"class"`
		Supported        bool             `json:"supported"`
		RequestedTier    string           `json:"requested_tier"`
		ActualTier       string           `json:"actual_tier"`
		PriorityCapacity *bool            `json:"priority_capacity"`
	}
	if err := json.Unmarshal(readAnthropicDirectFixture(t, "class-facts.wire.json"), &wire); err != nil {
		t.Fatal(err)
	}
	for _, fact := range wire {
		fixtureProfile := profile
		if fact.PriorityCapacity != nil {
			fixtureProfile.PriorityCapacity = *fact.PriorityCapacity
		}
		tier, err := fixtureProfile.providerTier(fact.Class)
		if !fact.Supported {
			if err == nil {
				t.Fatalf("%s unexpectedly has provider tier %q", fact.Class, tier)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", fact.Class, err)
		}
		if tier != fact.RequestedTier {
			t.Fatalf("%s requested tier = %q, want %q", fact.Class, tier, fact.RequestedTier)
		}
		actual, err := fixtureProfile.actualClass(fact.ActualTier)
		if err != nil {
			t.Fatalf("%s actual tier %q: %v", fact.Class, fact.ActualTier, err)
		}
		if actual == nil || *actual != actualByClass[fact.Class] {
			t.Fatalf("%s actual class = %#v, want %q", fact.Class, actual, actualByClass[fact.Class])
		}
	}
}

func assertAnthropicDirectFixtureCompileError(t *testing.T, profile Profile, request llm.Request, expectedName string) {
	t.Helper()
	adapter, err := New(newAnthropicDirectFixtureClient(t, nil), anthropicDirectFixtureEndpoint, profile)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: anthropicDirectFixtureEndpoint,
			Family:     provider.FamilyAnthropicMessages,
			Model:      request.Model,
		},
		Strict: request.Portability == llm.PortabilityStrict,
	})
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("%s compile error = %T %v, want provider error", request.OperationKey, err, err)
	}
	var expected struct {
		Code            provider.Code              `json:"code"`
		Phase           provider.Phase             `json:"phase"`
		Dispatch        provider.DispatchCertainty `json:"dispatch"`
		MessageContains string                     `json:"message_contains"`
	}
	if err := json.Unmarshal(readAnthropicDirectFixture(t, expectedName), &expected); err != nil {
		t.Fatal(err)
	}
	if mapped.Code != expected.Code || mapped.Phase != expected.Phase || mapped.Dispatch != expected.Dispatch {
		t.Fatalf("%s compile error = %#v, want %#v", request.OperationKey, mapped, expected)
	}
	if expected.MessageContains != "" && !strings.Contains(mapped.SafeMessage, expected.MessageContains) {
		t.Fatalf("%s safe message = %q, want substring %q", request.OperationKey, mapped.SafeMessage, expected.MessageContains)
	}
}

func assertAnthropicDirectFixtureContinuation(t *testing.T, profile Profile) {
	t.Helper()
	request := loadAnthropicDirectFixtureRequest(t, "continuation-compatibility.semantic.json")
	adapter, err := New(newAnthropicDirectFixtureClient(t, nil), anthropicDirectFixtureEndpoint, profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: anthropicDirectFixtureEndpoint,
			Family:     provider.FamilyAnthropicMessages,
			Model:      request.Model,
		},
		Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(call.SDKParams)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalAnthropicDirectFixture(t, wire, "continuation-compatibility.wire.json")

	incompatible := request
	continuation := *request.Continuation
	continuation.EndpointID = "other-anthropic-endpoint"
	incompatible.Continuation = &continuation
	assertAnthropicDirectFixtureCompileError(t, profile, incompatible, "continuation-incompatible.wire.json")
}

func assertAnthropicDirectFixtureClassifiedError(t *testing.T) {
	t.Helper()
	var fixture struct {
		Status     int    `json:"status"`
		RequestID  string `json:"request_id"`
		RetryAfter string `json:"retry_after"`
		Message    string `json:"message"`
	}
	if err := json.Unmarshal(readAnthropicDirectFixture(t, "classified-error.wire.json"), &fixture); err != nil {
		t.Fatal(err)
	}
	mapped := mapAPIError(&anthropic.Error{
		StatusCode: fixture.Status,
		RequestID:  fixture.RequestID,
		Response:   &http.Response{Header: http.Header{"Retry-After": []string{fixture.RetryAfter}}},
	}, "anthropic.messages/anthropic-contract")
	if mapped.Code != provider.CodeProviderRateLimited || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetryAfter || mapped.Provider.RequestID != fixture.RequestID || mapped.SafeDetails["retry_after"] != fixture.RetryAfter {
		t.Fatalf("mapped error = %#v", mapped)
	}
	encoded, err := json.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), fixture.Message) {
		t.Fatal("classified error leaked fixture body")
	}
}

func assertAnthropicDirectFixtureRedaction(t *testing.T) {
	t.Helper()
	redaction := readAnthropicDirectFixture(t, "security-redaction.wire.json")
	if !bytes.Contains(redaction, []byte("[REDACTED]")) {
		t.Fatal("redaction fixture has no explicit marker")
	}
	for _, unsafe := range []string{"anthropic-fixture-key", "Bearer sk-", "api-key-real"} {
		if bytes.Contains(redaction, []byte(unsafe)) {
			t.Fatalf("redaction fixture contains %q", unsafe)
		}
	}
}
