package anthropicmessages

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	anthropicaws "github.com/anthropics/anthropic-sdk-go/aws"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/contracttest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

const anthropicAWSFixtureEndpoint = "anthropic-aws"

func TestAnthropicAWSContractProfileIsEnforced(t *testing.T) {
	anthropicAWSFixture.testProfileIsEnforced(t)
}

func TestAnthropicAWSContractFixturesMatchCurrentLoweringAndLifting(t *testing.T) {
	anthropicAWSFixture.testLoweringAndLifting(t)
}

func TestAnthropicAWSContractFixturesVerifySemanticRoundTrip(t *testing.T) {
	anthropicAWSFixture.testSemanticRoundTrip(t)
}

func TestAnthropicAWSContractFixtureLowersToolInputAndOutput(t *testing.T) {
	profile := anthropicAWSFixtureProfile(t)
	request := anthropicAWSFixture.loadRequest(t, "request.tool-input-output.semantic.json")
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
	anthropicAWSFixture.assertCanonical(t, gotWire, "request.tool-input-output.wire.json")
}

func TestAnthropicAWSContractFixturesCoverUsageClassesLossErrorsAndContinuation(t *testing.T) {
	profile := anthropicAWSFixtureProfile(t)
	anthropicAWSFixture.assertUsageClassesLossAndContinuation(t, profile)
	assertAnthropicAWSFixtureClassifiedError(t)
	anthropicAWSFixture.assertRedaction(t)
	assertAnthropicAWSFixtureOpaqueThinkingAndToolOutput(t, profile)
}

func TestAnthropicAWSContractFixtureDecoderIsFragmentationInvariant(t *testing.T) {
	wire := anthropicAWSFixture.read(t, "stream.decoder.events")
	metadata := anthropicAWSFixture.loadMetadata(t)
	want := streamtest.FragmentationInvariant(t, wire, 31, 17, DecodeStream)
	if len(want) == 0 {
		t.Fatal("stream fixture decoded no events")
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
	if _, err := streamtest.Assemble("fixture-anthropic-aws-stream", want); err != nil {
		t.Fatal(err)
	}
	if err := contracttest.VerifyStreamAssemblyEquivalent(wire, anthropicAWSFixture.read(t, "stream.decoder.semantic.json"), func(events []byte) ([]byte, error) {
		return streamtest.AssembleJSON(events, "fixture-anthropic-aws-stream", DecodeStream)
	}, metadata.GeneratedFieldExemptions); err != nil {
		t.Fatal(err)
	}
}

func TestAnthropicAWSContractFixturesUseIndependentProfileTransport(t *testing.T) {
	request := anthropicAWSFixture.loadRequest(t, "request.semantic.json")
	responseBody := anthropicAWSFixture.read(t, "response.completed.json")
	var got *http.Request
	client := newAnthropicAWSFixtureClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		got = request
		return adaptertest.JSONResponse(request, http.StatusOK, string(responseBody), "Request-Id", "req-anthropic-aws-fixture"), nil
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
	anthropicAWSFixture.assertCanonical(t, body, "request.wire.json")
	if got.URL.Path != "/aws-contract/v1/messages" {
		t.Fatalf("Anthropic AWS transport path = %q", got.URL.Path)
	}
	gotSemantic, err := json.Marshal(result.Response)
	if err != nil {
		t.Fatal(err)
	}
	anthropicAWSFixture.assertCanonical(t, gotSemantic, "response.semantic.json")
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
	// Claude Platform on AWS has no fast mode, so the gateway offers no
	// priority class.
	profile.ServiceTiers[llm.ServiceClassPriority] = ""
	return mustProfile(t, profile)
}

func assertAnthropicAWSFixtureClassifiedError(t *testing.T) {
	t.Helper()
	calls := 0
	client := newAnthropicAWSFixtureClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		return adaptertest.JSONResponse(request, http.StatusUnauthorized, string(anthropicAWSFixture.read(t, "error.authentication.json")), "Request-Id", "aws-auth-fixture"), nil
	}))
	adapter, err := New(client, anthropicAWSFixtureEndpoint, anthropicAWSFixtureProfile(t))
	if err != nil {
		t.Fatal(err)
	}
	request := anthropicAWSFixture.loadRequest(t, "request.semantic.json")
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

func assertAnthropicAWSFixtureOpaqueThinkingAndToolOutput(t *testing.T, profile Profile) {
	t.Helper()
	response := anthropicAWSFixture.loadResponse(t, "response.thinking-tool.json")
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
