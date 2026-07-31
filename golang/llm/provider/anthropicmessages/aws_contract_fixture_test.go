package anthropicmessages

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
	anthropicaws "github.com/anthropics/anthropic-sdk-go/aws"
	yaml "go.yaml.in/yaml/v4"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/contracttest"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

const anthropicAWSFixtureEndpoint = "anthropic-aws"

func TestAnthropicAWSContractProfileIsEnforced(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	report, err := contracttest.ValidateRepository(root)
	if err != nil {
		t.Fatal(err)
	}

	const wantPath = "llm/provider/anthropicmessages/testdata/contracts/anthropic-aws"
	for _, profile := range report.Enforced {
		if profile.ID == "anthropic-aws" {
			if profile.Path != wantPath {
				t.Fatalf("anthropic-aws fixture path = %q, want %q", profile.Path, wantPath)
			}
			return
		}
	}
	t.Fatalf("enforced report is missing anthropic-aws: %#v", report)
}

func TestAnthropicAWSContractFixturesMatchCurrentLoweringAndLifting(t *testing.T) {
	profile := anthropicAWSFixtureProfile(t)
	request := loadAnthropicAWSFixtureRequest(t, "request.semantic.json")
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
	assertCanonicalAnthropicAWSFixture(t, gotWire, "request.wire.json")

	response := loadAnthropicAWSFixtureResponse(t, "response.completed.json")
	lifted, err := profile.liftResponse(anthropicAWSFixtureCall(normalized, serviceClass), &response, "req-anthropic-aws-fixture")
	if err != nil {
		t.Fatal(err)
	}
	gotSemantic, err := json.Marshal(lifted)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalAnthropicAWSFixture(t, gotSemantic, "response.semantic.json")
}

func TestAnthropicAWSContractFixturesVerifySemanticRoundTrip(t *testing.T) {
	profile := anthropicAWSFixtureProfile(t)
	requestSemantic := readAnthropicAWSFixture(t, "request.semantic.json")
	responseSemantic := readAnthropicAWSFixture(t, "response.semantic.json")
	semantic, err := json.Marshal(struct {
		Request  json.RawMessage `json:"request"`
		Response json.RawMessage `json:"response"`
	}{Request: requestSemantic, Response: responseSemantic})
	if err != nil {
		t.Fatal(err)
	}
	metadata := loadAnthropicAWSFixtureMetadata(t)
	responseWire := readAnthropicAWSFixture(t, "response.completed.json")
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
		lifted, err := profile.liftResponse(anthropicAWSFixtureCall(normalized, serviceClass), &response, "req-anthropic-aws-fixture")
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

func TestAnthropicAWSContractFixtureLowersToolInputAndOutput(t *testing.T) {
	profile := anthropicAWSFixtureProfile(t)
	request := loadAnthropicAWSFixtureRequest(t, "request.tool-input-output.semantic.json")
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	tier, err := profile.providerTier(normalized.ServiceClass)
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
	assertCanonicalAnthropicAWSFixture(t, gotWire, "request.tool-input-output.wire.json")
}

func TestAnthropicAWSContractFixturesCoverUsageClassesLossErrorsAndContinuation(t *testing.T) {
	profile := anthropicAWSFixtureProfile(t)
	assertAnthropicAWSFixtureClassFacts(t, profile)

	response := loadAnthropicAWSFixtureResponse(t, "usage-cost.response.json")
	lifted, err := profile.liftResponse(provider.Call{
		EndpointID:   anthropicAWSFixtureEndpoint,
		Family:       provider.FamilyAnthropicMessages,
		Model:        "claude-contract",
		OperationKey: "fixture-anthropic-aws-usage",
		ServiceClass: llm.ServiceClassStandard,
	}, &response, "req-anthropic-aws-usage")
	if err != nil {
		t.Fatal(err)
	}
	gotUsage, err := json.Marshal(lifted)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalAnthropicAWSFixture(t, gotUsage, "usage-cost.semantic.json")

	assertAnthropicAWSFixtureCompileError(t, profile, loadAnthropicAWSFixtureRequest(t, "strict-loss.semantic.json"), "strict-loss.wire.json")
	assertAnthropicAWSFixtureCompileError(t, profile, loadAnthropicAWSFixtureRequest(t, "best-effort-diagnostic.semantic.json"), "best-effort-diagnostic.wire.json")
	assertAnthropicAWSFixtureContinuation(t, profile)
	assertAnthropicAWSFixtureClassifiedError(t)
	assertAnthropicAWSFixtureRedaction(t)
	assertAnthropicAWSFixtureOpaqueThinkingAndToolOutput(t, profile)
}

func TestAnthropicAWSContractFixtureDecoderIsFragmentationInvariant(t *testing.T) {
	wire := readAnthropicAWSFixture(t, "stream.decoder.events")
	metadata := loadAnthropicAWSFixtureMetadata(t)
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
	for _, chunks := range [][][]byte{streamtest.Fragment(wire, 1), streamtest.RandomFragment(wire, 31, 17)} {
		got, err := DecodeStream(&anthropicChunkReader{chunks: chunks})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("fragmented events differ\n got: %#v\nwant: %#v", got, want)
		}
	}
	var fragments []string
	for _, event := range want {
		if delta, ok := event.(provider.ToolArgumentsDelta); ok {
			fragments = append(fragments, delta.Fragment)
		}
	}
	if strings.Join(fragments, "") != `{"q":"sydney"}` {
		t.Fatalf("tool argument fragments = %#v", fragments)
	}
	assembler := provider.NewAssembler("fixture-anthropic-aws-stream")
	for _, event := range want {
		if err := assembler.Add(event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := assembler.Result(); err != nil {
		t.Fatal(err)
	}
	if err := contracttest.VerifyStreamAssemblyEquivalent(wire, readAnthropicAWSFixture(t, "stream.decoder.semantic.json"), func(events []byte) ([]byte, error) {
		return assembleAnthropicFixtureStream(events, "fixture-anthropic-aws-stream")
	}, metadata.GeneratedFieldExemptions); err != nil {
		t.Fatal(err)
	}
}

func TestAnthropicAWSContractFixturesUseIndependentProfileTransport(t *testing.T) {
	request := loadAnthropicAWSFixtureRequest(t, "request.semantic.json")
	responseBody := readAnthropicAWSFixture(t, "response.completed.json")
	var got *http.Request
	client := newAnthropicAWSFixtureClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		got = request
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "Request-Id": []string{"req-anthropic-aws-fixture"}},
			Body:       io.NopCloser(bytes.NewReader(responseBody)),
			Request:    request,
		}, nil
	}))
	adapter, err := New(client, anthropicAWSFixtureEndpoint, anthropicAWSFixtureProfile(t))
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: anthropicAWSFixtureEndpoint,
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
	assertCanonicalAnthropicAWSFixture(t, body, "request.wire.json")
	if got.URL.Path != "/aws-contract/v1/messages" {
		t.Fatalf("Anthropic AWS transport path = %q", got.URL.Path)
	}
	gotSemantic, err := json.Marshal(result.Response)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalAnthropicAWSFixture(t, gotSemantic, "response.semantic.json")
}

func newAnthropicAWSFixtureClient(t *testing.T, transport http.RoundTripper) *Client {
	t.Helper()
	if transport == nil {
		transport = http.DefaultTransport
	}
	client, err := NewAWSClient(context.Background(), AWSClientConfig{
		BaseURL:    "http://127.0.0.1/aws-contract",
		HTTPClient: &http.Client{Transport: transport},
		AWSConfig: anthropicaws.ClientConfig{
			AWSRegion:   "us-east-1",
			WorkspaceID: "ws-contract",
			SkipAuth:    true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func anthropicAWSFixtureProfile(t *testing.T) Profile {
	t.Helper()
	profile := testProfile()
	profile.ID = anthropicAWSFixtureEndpoint
	profile.CapabilityVersion = "anthropic-aws-contract/v1"
	profile.Capabilities.Version = profile.CapabilityVersion
	profile.ExpectedBaseURL = "http://127.0.0.1/aws-contract"
	return mustProfile(t, profile)
}

func anthropicAWSFixtureCall(request llm.Request, serviceClass llm.ServiceClass) provider.Call {
	return provider.Call{
		EndpointID:   anthropicAWSFixtureEndpoint,
		Family:       provider.FamilyAnthropicMessages,
		Model:        request.Model,
		OperationKey: request.OperationKey,
		ServiceClass: serviceClass,
	}
}

func loadAnthropicAWSFixtureRequest(t *testing.T, name string) llm.Request {
	t.Helper()
	var request llm.Request
	if err := json.Unmarshal(readAnthropicAWSFixture(t, name), &request); err != nil {
		t.Fatal(err)
	}
	return request
}

func loadAnthropicAWSFixtureResponse(t *testing.T, name string) anthropic.Message {
	t.Helper()
	var response anthropic.Message
	if err := json.Unmarshal(readAnthropicAWSFixture(t, name), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func loadAnthropicAWSFixtureMetadata(t *testing.T) contracttest.Metadata {
	t.Helper()
	var metadata contracttest.Metadata
	if err := yaml.Unmarshal(readAnthropicAWSFixture(t, "metadata.yaml"), &metadata); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func readAnthropicAWSFixture(t *testing.T, name string) []byte {
	t.Helper()
	return mustReadAWSFixture(t, name)
}

func assertCanonicalAnthropicAWSFixture(t *testing.T, got []byte, name string) {
	t.Helper()
	gotCanonical, err := llm.CanonicalJSON(got)
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical, err := llm.CanonicalJSON(readAnthropicAWSFixture(t, name))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotCanonical, wantCanonical) {
		t.Fatalf("anthropic-aws/%s mismatch\n got: %s\nwant: %s", name, gotCanonical, wantCanonical)
	}
}

func assertAnthropicAWSFixtureClassFacts(t *testing.T, profile Profile) {
	t.Helper()
	var semantic []struct {
		Class  llm.ServiceClass `json:"class"`
		Actual llm.ServiceClass `json:"actual"`
	}
	if err := json.Unmarshal(readAnthropicAWSFixture(t, "class-facts.semantic.json"), &semantic); err != nil {
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
	if err := json.Unmarshal(readAnthropicAWSFixture(t, "class-facts.wire.json"), &wire); err != nil {
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

func assertAnthropicAWSFixtureCompileError(t *testing.T, profile Profile, request llm.Request, expectedName string) {
	t.Helper()
	adapter, err := New(newAnthropicAWSFixtureClient(t, nil), anthropicAWSFixtureEndpoint, profile)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: anthropicAWSFixtureEndpoint,
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
	if err := json.Unmarshal(readAnthropicAWSFixture(t, expectedName), &expected); err != nil {
		t.Fatal(err)
	}
	if mapped.Code != expected.Code || mapped.Phase != expected.Phase || mapped.Dispatch != expected.Dispatch {
		t.Fatalf("%s compile error = %#v, want %#v", request.OperationKey, mapped, expected)
	}
	if expected.MessageContains != "" && !strings.Contains(mapped.SafeMessage, expected.MessageContains) {
		t.Fatalf("%s safe message = %q, want substring %q", request.OperationKey, mapped.SafeMessage, expected.MessageContains)
	}
}

func assertAnthropicAWSFixtureContinuation(t *testing.T, profile Profile) {
	t.Helper()
	request := loadAnthropicAWSFixtureRequest(t, "continuation-compatibility.semantic.json")
	adapter, err := New(newAnthropicAWSFixtureClient(t, nil), anthropicAWSFixtureEndpoint, profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: anthropicAWSFixtureEndpoint,
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
	assertCanonicalAnthropicAWSFixture(t, wire, "continuation-compatibility.wire.json")

	incompatible := request
	continuation := *request.Continuation
	continuation.EndpointID = "anthropic-prod"
	incompatible.Continuation = &continuation
	assertAnthropicAWSFixtureCompileError(t, profile, incompatible, "continuation-incompatible.wire.json")
}

func assertAnthropicAWSFixtureClassifiedError(t *testing.T) {
	t.Helper()
	calls := 0
	client := newAnthropicAWSFixtureClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{
			StatusCode: http.StatusUnauthorized,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "Request-Id": []string{"aws-auth-fixture"}},
			Body:       io.NopCloser(bytes.NewReader(readAnthropicAWSFixture(t, "error.authentication.json"))),
			Request:    request,
		}, nil
	}))
	adapter, err := New(client, anthropicAWSFixtureEndpoint, anthropicAWSFixtureProfile(t))
	if err != nil {
		t.Fatal(err)
	}
	request := loadAnthropicAWSFixtureRequest(t, "request.semantic.json")
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: anthropicAWSFixtureEndpoint,
			Family:     provider.FamilyAnthropicMessages,
			Model:      request.Model,
		},
		Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(context.Background(), call, provider.NopObserver{})
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeAuthentication || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchRejected || mapped.Provider.RequestID != "aws-auth-fixture" {
		t.Fatalf("AWS authentication error = %#v", err)
	}
	if calls != 1 {
		t.Fatalf("AWS gateway request count = %d, want 1", calls)
	}
	encoded, marshalErr := json.Marshal(mapped)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if bytes.Contains(encoded, []byte("credentials rejected")) {
		t.Fatal("normalized error leaked provider response body")
	}
}

func assertAnthropicAWSFixtureRedaction(t *testing.T) {
	t.Helper()
	redaction := readAnthropicAWSFixture(t, "security-redaction.wire.json")
	if !bytes.Contains(redaction, []byte("[REDACTED]")) {
		t.Fatal("redaction fixture has no explicit marker")
	}
	for _, unsafe := range []string{"aws-secret", "AKIA", "Bearer ", "x-amz-security-token-real"} {
		if bytes.Contains(redaction, []byte(unsafe)) {
			t.Fatalf("redaction fixture contains %q", unsafe)
		}
	}
}

func assertAnthropicAWSFixtureOpaqueThinkingAndToolOutput(t *testing.T, profile Profile) {
	t.Helper()
	response := loadAnthropicAWSFixtureResponse(t, "response.thinking-tool.json")
	lifted, err := profile.liftResponse(provider.Call{
		EndpointID:   anthropicAWSFixtureEndpoint,
		Family:       provider.FamilyAnthropicMessages,
		Model:        "claude-contract",
		OperationKey: "fixture-anthropic-aws-thinking",
		ServiceClass: llm.ServiceClassPriority,
	}, &response, "req-anthropic-aws-thinking")
	if err != nil {
		t.Fatal(err)
	}
	if lifted.Status != llm.ResponseStatusToolCalls || lifted.Service.Actual == nil || *lifted.Service.Actual != llm.ServiceClassPriority || lifted.Continuation == nil || len(lifted.Continuation.ProviderStates) != 2 {
		t.Fatalf("thinking/tool response = %#v", lifted)
	}
	var thinkingState struct {
		Signature string `json:"signature"`
	}
	if err := json.Unmarshal(lifted.Continuation.ProviderStates[0].Opaque, &thinkingState); err != nil || thinkingState.Signature != "sig-aws-thinking" {
		t.Fatalf("thinking state did not retain the opaque signature: %s", lifted.Continuation.ProviderStates[0].Opaque)
	}
	if len(lifted.Output) != 4 {
		t.Fatalf("thinking/tool output count = %d, want 4", len(lifted.Output))
	}
	call, ok := lifted.Output[3].(llm.ToolCall)
	if !ok || call.ID != "toolu-aws-1" || call.Name != "lookup" {
		t.Fatalf("tool output = %#v", lifted.Output[3])
	}
	arguments, err := llm.CanonicalJSON(call.Arguments)
	if err != nil || string(arguments) != `{"q":"sydney"}` {
		t.Fatalf("tool output = %#v", lifted.Output[3])
	}
}
