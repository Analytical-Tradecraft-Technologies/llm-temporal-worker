//go:build live

package live

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/anthropicmessages"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/openaichat"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/openairesponses"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

func TestFamilyForProfilePinsTheProtocol(t *testing.T) {
	want := map[string]provider.Family{
		"openai-responses":  provider.FamilyOpenAIResponses,
		"azure-responses":   provider.FamilyOpenAIResponses,
		"openai-chat":       provider.FamilyOpenAIChat,
		"openrouter-chat":   provider.FamilyOpenAIChat,
		"exa-chat":          provider.FamilyOpenAIChat,
		"anthropic-direct":  provider.FamilyAnthropicMessages,
		"anthropic-aws":     provider.FamilyAnthropicMessages,
		"bedrock-anthropic": provider.FamilyBedrockMessages,
		"bedrock-converse":  provider.FamilyBedrockConverse,
	}

	for _, profile := range Profiles() {
		if got := familyFor(profile); got != want[profile.ID] {
			t.Errorf("family for %s = %q, want %q", profile.ID, got, want[profile.ID])
		}
	}
}

func TestCompileProfileExercisesOmittedServiceClassAsStandard(t *testing.T) {
	profile := Profiles()[0]
	adapter := &recordingAdapter{}

	call, err := compileProfile(context.Background(), adapter, profile, requestFor(profile))
	testassert.NoError(t, err, "compile profile")
	if adapter.capabilityCalls != 1 || adapter.compileCalls != 1 {
		t.Fatalf("adapter calls = capabilities:%d compile:%d, want one each", adapter.capabilityCalls, adapter.compileCalls)
	}
	if adapter.query.EndpointID != profile.ID || adapter.query.Family != provider.FamilyOpenAIResponses || adapter.query.Model != profile.Model {
		t.Fatalf("capability query = %#v, want pinned openai responses profile", adapter.query)
	}
	if adapter.query.ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("capability query service class = %q, want %q", adapter.query.ServiceClass, llm.ServiceClassStandard)
	}
	if adapter.input.Request.ServiceClass != "" || !adapter.input.Strict {
		t.Fatalf("compile input = %#v, want omitted public class and strict compilation", adapter.input)
	}
	if call.ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("compiled call service class = %q, want %q", call.ServiceClass, llm.ServiceClassStandard)
	}
}

func TestUnsupportedContinuationProbeFailsBeforeProviderInvocation(t *testing.T) {
	profile := Profiles()[2]
	adapter := &recordingAdapter{rejectContinuation: true}

	testassert.NoError(t, preflightUnsupportedContinuation(context.Background(), adapter, profile), "preflight unsupported continuation")
	if adapter.compileCalls != 1 || adapter.capabilityCalls != 1 {
		t.Fatalf("adapter calls = capabilities:%d compile:%d, want one compile preflight", adapter.capabilityCalls, adapter.compileCalls)
	}
	if adapter.input.Request.Continuation == nil || !adapter.input.Request.Continuation.Pinned {
		t.Fatalf("continuation probe = %#v, want a pinned continuation", adapter.input.Request.Continuation)
	}
	if adapter.invokeCalls != 0 {
		t.Fatalf("provider invocations = %d, want no invocation during the continuation preflight", adapter.invokeCalls)
	}
}

func TestRunWithAdapterPreflightsUnsupportedContinuationBeforeInvocation(t *testing.T) {
	profile := Profiles()[2]
	adapter := &recordingAdapter{
		rejectContinuation: true,
		result:             provider.Result{Response: completedResponse()},
	}

	evidence, err := runWithAdapter(context.Background(), adapter, profile)
	testassert.NoError(t, err, "run profile")
	if adapter.capabilityCalls != 2 || adapter.compileCalls != 2 || adapter.invokeCalls != 1 {
		t.Fatalf("adapter calls = capabilities:%d compile:%d invoke:%d, want 2/2/1", adapter.capabilityCalls, adapter.compileCalls, adapter.invokeCalls)
	}
	if len(adapter.events) != 5 || adapter.events[len(adapter.events)-1] != "invoke" {
		t.Fatalf("adapter event order = %v, want both compiles before invoke", adapter.events)
	}
	if evidence.Profile != profile.ID || evidence.ContinuationVerified {
		t.Fatalf("evidence = %#v, want unsupported-continuation facts", evidence)
	}
}

func TestRunWithAdapterAllowsTextOnlyAnthropicResponseWithoutContinuation(t *testing.T) {
	profile := profileForTest(t, "anthropic-direct")
	const baseURL = "http://127.0.0.1/anthropic-live-contract"
	responseBody := `{"id":"msg_text_only","type":"message","role":"assistant","model":"claude-3-5-haiku-latest","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":1,"service_tier":"standard"}}`
	invocations := 0
	client, err := anthropicmessages.NewClient(anthropicmessages.ClientConfig{
		BaseURL: baseURL,
		APIKey:  "test-key",
		HTTPClient: &http.Client{Transport: testkit.RoundTripFunc(func(request *http.Request) (*http.Response, error) {
			invocations++
			if request.URL.Path != "/anthropic-live-contract/v1/messages" {
				t.Errorf("request path = %q, want Anthropic messages endpoint", request.URL.Path)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}, "Request-Id": []string{"req-text-only"}},
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Request:    request,
			}, nil
		})},
	})
	testassert.NoError(t, err, "new Anthropic client")
	providerProfile := anthropicmessages.DefaultProfile(profile.ID)
	providerProfile.ExpectedBaseURL = baseURL
	providerProfile.ExpectedModel = profile.Model
	adapter, err := anthropicmessages.New(client, profile.ID, providerProfile)
	testassert.NoError(t, err, "new Anthropic adapter")

	evidence, err := runWithAdapter(context.Background(), adapter, profile)
	testassert.NoError(t, err, "run text-only Anthropic profile")
	if invocations != 1 {
		t.Fatalf("Anthropic invocations = %d, want 1", invocations)
	}
	if evidence.ContinuationVerified {
		t.Fatalf("continuation verification = %t, want false for the adapter-lifted text-only response", evidence.ContinuationVerified)
	}
}

func TestRunWithAdapterRejectsMismatchedCompiledWireBeforeInvocation(t *testing.T) {
	profile := Profiles()[0]
	wrongCall := tierlessCompiledCall(profile)
	wrongCall.EndpointID = "unexpected-endpoint"
	assertCompiledCallRejectedBeforeInvocation(t, profile, wrongCall, "a mismatched compiled wire contract", "after a wire mismatch")
}

func TestRunWithAdapterRejectsMissingProviderTierBeforeInvocation(t *testing.T) {
	profile := Profiles()[0]
	assertCompiledCallRejectedBeforeInvocation(t, profile, tierlessCompiledCall(profile), "a compiled call with no explicit provider tier", "when the provider tier is omitted")
}

// tierlessCompiledCall is the call profile pins, minus the provider tier
// metadata that validateCompiledCall requires.
func tierlessCompiledCall(profile Profile) provider.Call {
	return provider.Call{
		EndpointID:   profile.ID,
		Family:       familyFor(profile),
		Model:        profile.Model,
		OperationKey: "live-contract-" + profile.ID,
		ServiceClass: llm.ServiceClassStandard,
	}
}

func assertCompiledCallRejectedBeforeInvocation(t *testing.T, profile Profile, call provider.Call, accepted, noInvocation string) {
	t.Helper()
	adapter := &recordingAdapter{
		callOverride: &call,
		result:       completedPinnedResult(profile),
	}

	if _, err := runWithAdapter(context.Background(), adapter, profile); err == nil {
		t.Fatalf("run profile unexpectedly accepted %s", accepted)
	}
	if adapter.invokeCalls != 0 {
		t.Fatalf("provider invocations = %d, want no invocation %s", adapter.invokeCalls, noInvocation)
	}
}

func TestChatProfilesRequireReportedActualTier(t *testing.T) {
	for _, liveProfile := range Profiles() {
		if familyFor(liveProfile) != provider.FamilyOpenAIChat {
			continue
		}
		profile, err := chatProfileFor(liveProfile)
		if err != nil {
			t.Fatalf("build %s chat profile: %v", liveProfile.ID, err)
		}
		if profile.MissingActualServiceClass != "" {
			t.Errorf("%s accepts a missing actual tier", liveProfile.ID)
		}
		if profile.ServiceTiers[llm.ServiceClassStandard] == "" {
			t.Errorf("%s has no explicit standard tier", liveProfile.ID)
		}
		if _, exists := profile.ActualServiceClasses[""]; exists {
			t.Errorf("%s maps a missing actual tier", liveProfile.ID)
		}
		if profile.Capabilities.Features[provider.FeatureContinuation].State != provider.CapabilityUnsupported {
			t.Errorf("%s continuation capability = %q, want unsupported", liveProfile.ID, profile.Capabilities.Features[provider.FeatureContinuation].State)
		}
		if liveProfile.ID == "openrouter-chat" {
			// The provider object is built per request, so read it from a
			// compiled call; compiling sends nothing.
			client, err := openaichat.NewOpenRouterClient(openaichat.OpenRouterClientConfig{BaseURL: liveOpenRouterBaseURL, APIKey: "live-test-key", HTTPClient: &http.Client{}})
			testassert.NoError(t, err)
			adapter, err := openaichat.New(client, liveProfile.ID, profile)
			testassert.NoError(t, err)
			capabilities := provider.CapabilitySet{Version: profile.Capabilities.Version, Features: map[provider.Feature]provider.Capability{}}
			for feature, capability := range profile.Capabilities.Features {
				capabilities.Features[feature] = capability
			}
			capabilities.Features[provider.FeatureToolCall] = provider.Capability{State: provider.CapabilityNative}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{
				Capability: capabilities,
				Request:    llm.Request{OperationKey: "live-profile", Model: liveProfile.Model, Input: []llm.Item{llm.HumanText("hello")}},
				Query:      provider.CapabilityQuery{EndpointID: liveProfile.ID, Family: provider.FamilyOpenAIChat, Model: liveProfile.Model},
			})
			testassert.NoError(t, err, "compile OpenRouter request")
			encoded, err := json.Marshal(call.SDKParams)
			testassert.NoError(t, err)
			var wire struct {
				Provider map[string]any `json:"provider"`
			}
			testassert.NoError(t, json.Unmarshal(encoded, &wire), "decode OpenRouter provider object")
			if wire.Provider["allow_fallbacks"] != false || wire.Provider["require_parameters"] != true || wire.Provider["only"] == nil {
				t.Errorf("OpenRouter provider object %v is not fail-closed", wire.Provider)
			}
		}
	}
}

func TestRunProfileWithFactoryFailsClosedBeforeAdapterConstruction(t *testing.T) {
	profile := Profiles()[0]
	for _, test := range []struct {
		name             string
		env              map[string]string
		wantFactoryCalls int
	}{
		{name: "suite flag missing", wantFactoryCalls: 0},
		{name: "authorization flag missing", env: map[string]string{EnableSuiteEnv: "1"}, wantFactoryCalls: 0},
		{name: "profile flag missing", env: map[string]string{EnableSuiteEnv: "1", AuthorizeEnv: "1"}, wantFactoryCalls: 0},
		{name: "all explicit gates", env: map[string]string{EnableSuiteEnv: "1", AuthorizeEnv: "1", profile.EnableEnv: "1"}, wantFactoryCalls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			credentialRead := false
			lookup := gateLookup(profile, test.env, &credentialRead)
			factoryCalls := 0
			factory := func(context.Context, Profile, func(string) (string, bool)) (provider.Adapter, error) {
				factoryCalls++
				return nil, errors.New("factory failure")
			}

			if _, err := runProfileWithFactory(context.Background(), profile, lookup, factory); err == nil {
				t.Fatal("run profile unexpectedly succeeded")
			}
			if factoryCalls != test.wantFactoryCalls {
				t.Fatalf("factory calls = %d, want %d", factoryCalls, test.wantFactoryCalls)
			}
			if credentialRead {
				t.Fatalf("credential %q was read before an adapter was constructed", profile.Credential.Environment)
			}
		})
	}
}

func TestAdapterForBuildsEnvironmentCredentialProfilesWithoutInvocation(t *testing.T) {
	values := map[string]string{
		"OPENAI_API_KEY":     "test-openai-key",
		"OPENROUTER_API_KEY": "test-openrouter-key",
		"EXA_API_KEY":        "test-exa-key",
		"ANTHROPIC_API_KEY":  "test-anthropic-key",
	}
	lookup := func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}

	for _, profile := range Profiles() {
		if profile.Credential.Kind != "bearer_env" && profile.Credential.Kind != "header_env" {
			continue
		}
		t.Run(profile.ID, func(t *testing.T) {
			adapter, err := adapterFor(context.Background(), profile, lookup)
			testassert.NoError(t, err, "build adapter")
			if adapter == nil || adapter.Name() == "" {
				t.Fatal("adapter is unavailable")
			}
		})
	}
}

func TestAdapterForRejectsMutatedProfileBeforeCredentialLookup(t *testing.T) {
	profile := Profiles()[0]
	profile.Model = "untrusted-model"
	profile.AllowedModels = map[string]bool{profile.Model: true}
	credentialRead := false
	lookup := func(name string) (string, bool) {
		if name == profile.Credential.Environment {
			credentialRead = true
		}
		return "test-key", true
	}

	if _, err := adapterFor(context.Background(), profile, lookup); err == nil {
		t.Fatal("mutated profile unexpectedly constructed an adapter")
	}
	if credentialRead {
		t.Fatal("adapter factory read a credential for a mutated profile")
	}
}

func TestAdapterForRejectsUntrustedAzureEndpointBeforeCredentialOrClientConstruction(t *testing.T) {
	var profile Profile
	for _, candidate := range Profiles() {
		if candidate.ID == "azure-responses" {
			profile = candidate
			break
		}
	}
	if profile.ID == "" {
		t.Fatal("azure live profile is unavailable")
	}

	for _, endpoint := range []string{
		liveOpenAIBaseURL,
		"https://attacker.example",
		"https://resource.openai.azure.com.attacker.example",
	} {
		t.Run(endpoint, func(t *testing.T) {
			credentialFactoryCalls := 0
			clientFactoryCalls := 0
			dependencies := defaultAzureAdapterDependencies()
			dependencies.newAzureCredential = func() (azcore.TokenCredential, error) {
				credentialFactoryCalls++
				return fakeAzureTokenCredential{}, nil
			}
			dependencies.newAzureTokenClient = func(openairesponses.AzureTokenClientConfig) (*openairesponses.Client, error) {
				clientFactoryCalls++
				return nil, errors.New("client factory must not run")
			}
			lookup := func(name string) (string, bool) {
				if name == liveAzureEndpointEnv {
					return endpoint, true
				}
				return "", false
			}

			_, err := adapterForWithAzureDependencies(context.Background(), profile, lookup, dependencies)
			if err == nil {
				t.Fatal("untrusted Azure endpoint unexpectedly constructed an adapter")
			}
			if strings.Contains(err.Error(), endpoint) {
				t.Fatal("untrusted Azure endpoint appeared in the adapter error")
			}
			if credentialFactoryCalls != 0 || clientFactoryCalls != 0 {
				t.Fatalf("credential/client factory calls = %d/%d, want 0/0", credentialFactoryCalls, clientFactoryCalls)
			}
		})
	}
}

func TestLiveAdapterCollapsesConstructionFailures(t *testing.T) {
	built := &recordingAdapter{}
	newAdapter := func(client string) (provider.Adapter, error) {
		if client != "client" {
			return nil, errors.New("adapter failure with SDK detail")
		}
		return built, nil
	}
	for _, test := range []struct {
		name      string
		client    string
		clientErr error
	}{
		{name: "client failure", client: "client", clientErr: errors.New("client failure with SDK detail")},
		{name: "adapter failure", client: "rejected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter, err := liveAdapter(test.client, test.clientErr, newAdapter)
			if adapter != nil || err == nil || err.Error() != "live adapter construction failed" {
				t.Fatalf("liveAdapter = (%v, %v), want only the content-free construction error", adapter, err)
			}
		})
	}
	if adapter, err := liveAdapter("client", nil, newAdapter); err != nil || adapter != built {
		t.Fatalf("liveAdapter = (%v, %v), want the constructed adapter", adapter, err)
	}
}

type recordingAdapter struct {
	capabilityCalls    int
	compileCalls       int
	invokeCalls        int
	query              provider.CapabilityQuery
	input              provider.CompileInput
	rejectContinuation bool
	callOverride       *provider.Call
	result             provider.Result
	events             []string
}

func (adapter *recordingAdapter) Name() string { return "recording" }

func (adapter *recordingAdapter) Capabilities(_ context.Context, query provider.CapabilityQuery) (provider.CapabilitySet, error) {
	adapter.capabilityCalls++
	adapter.query = query
	adapter.events = append(adapter.events, "capabilities")
	return provider.CapabilitySet{Version: "test"}, nil
}

func (adapter *recordingAdapter) Compile(_ context.Context, input provider.CompileInput) (provider.Call, error) {
	adapter.compileCalls++
	adapter.input = input
	adapter.events = append(adapter.events, "compile")
	if adapter.rejectContinuation && input.Request.Continuation != nil {
		return provider.Call{}, errors.New("continuations are unsupported")
	}
	if adapter.callOverride != nil {
		return *adapter.callOverride, nil
	}
	return provider.Call{
		EndpointID:   input.Query.EndpointID,
		Family:       input.Query.Family,
		Model:        input.Request.Model,
		OperationKey: input.Request.OperationKey,
		ServiceClass: input.Query.ServiceClass,
		Metadata:     provider.CallMetadata{ProviderTier: "standard"},
	}, nil
}

func (adapter *recordingAdapter) Invoke(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
	adapter.invokeCalls++
	adapter.events = append(adapter.events, "invoke")
	return adapter.result, nil
}

// completedResponse is a completed standard-class response that reports
// usage and both provider IDs but no continuation. Each call returns fresh
// pointers, so callers may mutate the result.
func completedResponse() llm.Response {
	actual := llm.ServiceClassStandard
	return llm.Response{
		Status: llm.ResponseStatusCompleted,
		Service: llm.ServiceFacts{
			Requested: llm.ServiceClassStandard,
			Attempted: llm.ServiceClassStandard,
			Actual:    &actual,
		},
		Usage:    llm.Usage{InputTokens: 3, OutputTokens: 2},
		Provider: llm.ProviderFacts{RequestID: "request-123", ResponseID: "response-123"},
	}
}

// completedPinnedResponse adds a continuation pinned to profile.
func completedPinnedResponse(profile Profile) llm.Response {
	response := completedResponse()
	response.Continuation = &llm.Continuation{
		Handle:     "continuation-123",
		EndpointID: profile.ID,
		Model:      profile.Model,
		Pinned:     true,
	}
	return response
}

func completedPinnedResult(profile Profile) provider.Result {
	return provider.Result{Response: completedPinnedResponse(profile)}
}

type fakeAzureTokenCredential struct{}

func (fakeAzureTokenCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{}, nil
}
