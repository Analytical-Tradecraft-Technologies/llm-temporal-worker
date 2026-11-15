package bedrockmessages

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/contracttest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

const (
	bedrockFixtureProfileID  = "bedrock-anthropic"
	bedrockFixtureEndpoint   = "bedrock-fixture"
	bedrockFixtureRequestID  = "bedrock-request-fixture"
	bedrockFixtureSourceDate = "2026-07-14"
)

func TestBedrockContractProfileIsEnforced(t *testing.T) {
	report := contract.ValidateRepository(t)
	for _, profile := range report.Enforced {
		if profile.ID == "bedrock-anthropic" {
			return
		}
	}
	t.Fatalf("%s must be enforced; bootstrap profiles: %#v", bedrockFixtureProfileID, report.Bootstrap)
}

func TestBedrockContractFixturesMatchCurrentLoweringAndLifting(t *testing.T) {
	requestFixture := mustReadBedrockFixture(t, "request.semantic.json")
	request := loadBedrockContractRequest(t, "request.semantic.json")
	adapter := fixtureBedrockAdapter(t)
	call := compileBedrockFixture(t, adapter, request)

	encodedWire := contract.MarshalJSON(t, call.SDKParams)
	assertBedrockFixtureJSON(t, encodedWire, "request.wire.json")

	response := loadBedrockContractResponse(t, "response.completed.json")
	lifted, err := adapter.profile.liftResponse(call, &response, bedrockFixtureRequestID)
	if err != nil {
		t.Fatal(err)
	}
	gotSemantic := contract.MarshalJSON(t, lifted)
	assertBedrockFixtureJSON(t, gotSemantic, "response.semantic.json")
	assertBedrockOpaqueStateFixture(t, lifted)

	semanticResponse := mustReadBedrockFixture(t, "response.semantic.json")
	roundTripSemantic, err := json.Marshal(struct {
		Request  json.RawMessage `json:"request"`
		Response json.RawMessage `json:"response"`
	}{Request: requestFixture, Response: semanticResponse})
	if err != nil {
		t.Fatal(err)
	}
	metadata := loadBedrockFixtureMetadata(t)
	if err := contracttest.VerifySemanticRoundTrip(roundTripSemantic, func(semantic []byte) ([]byte, error) {
		var fixture struct {
			Request  json.RawMessage `json:"request"`
			Response json.RawMessage `json:"response"`
		}
		if err := json.Unmarshal(semantic, &fixture); err != nil {
			return nil, err
		}
		var roundTripRequest llm.Request
		if err := json.Unmarshal(fixture.Request, &roundTripRequest); err != nil {
			return nil, err
		}
		roundTripCall, err := adapter.Compile(context.Background(), provider.CompileInput{
			Request: roundTripRequest,
			Query: provider.CapabilityQuery{
				EndpointID: bedrockFixtureEndpoint,
				Family:     provider.FamilyBedrockMessages,
				Model:      roundTripRequest.Model,
			},
			Strict: true,
		})
		if err != nil {
			return nil, err
		}
		roundTripped, err := adapter.profile.liftResponse(roundTripCall, &response, bedrockFixtureRequestID)
		if err != nil {
			return nil, err
		}
		fixture.Response, err = json.Marshal(roundTripped)
		if err != nil {
			return nil, err
		}
		return json.Marshal(fixture)
	}, metadata.GeneratedFieldExemptions); err != nil {
		t.Fatal(err)
	}
}

func TestBedrockContractFixturesCoverUsageClassesStrictLossErrorsAndContinuation(t *testing.T) {
	adapter := fixtureBedrockAdapter(t)
	assertBedrockClassFacts(t, adapter)
	assertBedrockPriorityDowngrade(t, adapter)
	assertBedrockUsageAndCost(t, adapter)
	assertBedrockStrictLoss(t, adapter)
	assertBedrockClassifiedError(t)
	assertBedrockContinuationCompatibility(t, adapter)
}

func assertBedrockPriorityDowngrade(t *testing.T, adapter *Adapter) {
	t.Helper()
	call := compileBedrockFixture(t, adapter, llm.Request{
		OperationKey: "fixture-bedrock-priority-downgrade",
		Model:        "claude-contract",
		ServiceClass: llm.ServiceClassPriority,
	})
	response := loadBedrockContractResponse(t, "priority-downgrade.response.json")
	lifted, err := adapter.profile.liftResponse(call, &response, bedrockFixtureRequestID)
	if err != nil {
		t.Fatal(err)
	}
	got := contract.MarshalJSON(t, lifted)
	assertBedrockFixtureJSON(t, got, "priority-downgrade.semantic.json")
	if lifted.Service.Actual == nil || *lifted.Service.Actual != llm.ServiceClassStandard {
		t.Fatalf("priority downgrade actual class = %#v, want standard", lifted.Service.Actual)
	}
}

func TestBedrockContractStreamFixturesRemainFragmentationInvariant(t *testing.T) {
	metadata := loadBedrockFixtureMetadata(t)
	for _, fixture := range []struct {
		name              string
		semantic          string
		operationKey      string
		wantToolFragments int
	}{
		{name: "full-stream.events", semantic: "full-stream.semantic.json", operationKey: "fixture-bedrock-full-stream"},
		{name: "fragmented-stream.events", semantic: "fragmented-stream.semantic.json", operationKey: "fixture-bedrock-fragmented-stream", wantToolFragments: 2},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			wire := mustReadBedrockFixture(t, fixture.name)
			want := streamtest.FragmentationInvariant(t, wire, 41, 11, DecodeStream)
			toolFragments := 0
			for _, event := range want {
				if _, ok := event.(provider.ToolArgumentsDelta); ok {
					toolFragments++
				}
			}
			if toolFragments != fixture.wantToolFragments {
				t.Fatalf("tool argument fragments = %d, want %d", toolFragments, fixture.wantToolFragments)
			}
			if err := contracttest.VerifyStreamAssemblyEquivalent(wire, mustReadBedrockFixture(t, fixture.semantic), func(events []byte) ([]byte, error) {
				return streamtest.AssembleJSON(events, fixture.operationKey, DecodeStream)
			}, metadata.GeneratedFieldExemptions); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBedrockContractFixturesDeclareSourceDateAndRedaction(t *testing.T) {
	metadata := loadBedrockFixtureMetadata(t)
	if metadata.UpstreamURL != "https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages.html" {
		t.Fatalf("upstream URL = %q", metadata.UpstreamURL)
	}
	if metadata.UpstreamDate != bedrockFixtureSourceDate {
		t.Fatalf("upstream date = %q, want %q", metadata.UpstreamDate, bedrockFixtureSourceDate)
	}
	if len(metadata.Redactions) < 2 {
		t.Fatalf("redaction policy = %#v, want explicit credentials and unstable identifiers", metadata.Redactions)
	}
	for _, name := range []string{"classified-error.wire.json", "security-redaction.wire.json"} {
		fixture := mustReadBedrockFixture(t, name)
		if !bytes.Contains(fixture, []byte("[REDACTED]")) {
			t.Fatalf("%s has no explicit redaction marker", name)
		}
		for _, unsafe := range []string{"AKIA", "contract-secret", "fixture-real-token", "Bearer live-"} {
			if bytes.Contains(fixture, []byte(unsafe)) {
				t.Fatalf("%s contains %q", name, unsafe)
			}
		}
	}
}

func fixtureBedrockAdapter(t *testing.T) *Adapter {
	t.Helper()
	return &Adapter{endpointID: bedrockFixtureEndpoint, profile: mustBedrockProfile(t, "")}
}

func compileBedrockFixture(t *testing.T, adapter *Adapter, request llm.Request) provider.Call {
	t.Helper()
	return adaptertest.CompileStrict(t, adapter, bedrockFixtureEndpoint, provider.FamilyBedrockMessages, request)
}

func loadBedrockContractRequest(t *testing.T, name string) llm.Request {
	t.Helper()
	return contract.DecodeJSON[llm.Request](t, mustReadBedrockFixture(t, name))
}

func loadBedrockContractResponse(t *testing.T, name string) anthropic.Message {
	t.Helper()
	return contract.DecodeJSON[anthropic.Message](t, mustReadBedrockFixture(t, name))
}

func loadBedrockFixtureMetadata(t *testing.T) contracttest.Metadata {
	t.Helper()
	return contract.DecodeMetadata(t, mustReadBedrockFixture(t, "metadata.yaml"))
}

func assertBedrockFixtureJSON(t *testing.T, got []byte, name string) {
	t.Helper()
	contract.AssertCanonicalJSON(t, got, mustReadBedrockFixture(t, name), name)
}

func assertBedrockOpaqueStateFixture(t *testing.T, response llm.Response) {
	t.Helper()
	states := make([]llm.ProviderState, 0, 2)
	for _, item := range response.Output {
		if state, ok := item.(llm.ProviderState); ok {
			states = append(states, state)
		}
	}
	if len(states) != 2 {
		t.Fatalf("opaque states = %#v, want thinking and redacted_thinking", states)
	}
	for index, state := range states {
		if state.Provider != "bedrock" || state.EndpointFamily != "messages" || state.MediaType != "application/vnd.anthropic.content-block+json" || !json.Valid(state.Opaque) {
			t.Fatalf("opaque state %d = %#v", index, state)
		}
	}
}

func assertBedrockClassFacts(t *testing.T, adapter *Adapter) {
	t.Helper()
	wire, actualByClass := contract.ClassFacts(t, mustReadBedrockFixture(t, "class-facts.semantic.json"), mustReadBedrockFixture(t, "class-facts.wire.json"))
	for _, fact := range wire {
		if !fact.Supported {
			continue
		}
		call := compileBedrockFixture(t, adapter, llm.Request{OperationKey: "fixture-bedrock-class-" + string(fact.Class), Model: "claude-contract", ServiceClass: fact.Class})
		if call.Metadata.ProviderTier != fact.RequestedTier {
			t.Fatalf("%s provider tier = %q, want %q", fact.Class, call.Metadata.ProviderTier, fact.RequestedTier)
		}
		params := contract.JSONObject(t, call.SDKParams)
		if tier, present := params["service_tier"]; present {
			t.Fatalf("%s wire body carried service_tier %#v; InvokeModel takes it as a header", fact.Class, tier)
		}
		actual := adapter.profile.actualClass(fact.ActualTier)
		if actual == nil || *actual != actualByClass[fact.Class] {
			t.Fatalf("%s actual class = %#v; want %q", fact.Class, actual, actualByClass[fact.Class])
		}
	}
}

func assertBedrockUsageAndCost(t *testing.T, adapter *Adapter) {
	t.Helper()
	response := loadBedrockContractResponse(t, "usage-cost.response.json")
	call := compileBedrockFixture(t, adapter, llm.Request{OperationKey: "fixture-bedrock-usage", Model: "claude-contract", ServiceClass: llm.ServiceClassStandard})
	lifted, err := adapter.profile.liftResponse(call, &response, bedrockFixtureRequestID)
	if err != nil {
		t.Fatal(err)
	}
	got := contract.MarshalJSON(t, lifted)
	assertBedrockFixtureJSON(t, got, "usage-cost.semantic.json")
	if lifted.Cost != (llm.Cost{}) {
		t.Fatalf("Bedrock cost should remain unreported: %#v", lifted.Cost)
	}
}

func assertBedrockStrictLoss(t *testing.T, adapter *Adapter) {
	t.Helper()
	request := loadBedrockContractRequest(t, "strict-loss.semantic.json")
	expected := contract.DecodeJSON[fixtureProviderError](t, mustReadBedrockFixture(t, "strict-loss.wire.json"))
	for _, portability := range []llm.PortabilityMode{llm.PortabilityStrict, llm.PortabilityBestEffort} {
		request.Portability = portability
		_, err := adaptertest.Compile(adapter, bedrockFixtureEndpoint, provider.FamilyBedrockMessages, request, true)
		var providerErr *provider.Error
		if !errors.As(err, &providerErr) {
			t.Fatalf("%s strict-loss error = %T %v, want provider error", portability, err, err)
		}
		assertBedrockProviderError(t, providerErr, expected)
	}
}

func assertBedrockClassifiedError(t *testing.T) {
	t.Helper()
	var fixture struct {
		Status    int                  `json:"status"`
		RequestID string               `json:"request_id"`
		Body      json.RawMessage      `json:"body"`
		Expected  fixtureProviderError `json:"expected"`
	}
	if err := json.Unmarshal(mustReadBedrockFixture(t, "classified-error.wire.json"), &fixture); err != nil {
		t.Fatal(err)
	}
	apiErr := contract.DecodeJSON[anthropic.Error](t, fixture.Body)
	apiErr.StatusCode = fixture.Status
	apiErr.RequestID = fixture.RequestID
	apiErr.Response = &http.Response{Header: http.Header{"X-Amzn-Requestid": []string{fixture.RequestID}}}
	mapped := mapAPIError(&apiErr, "bedrock.messages/bedrock-contract")
	assertBedrockProviderError(t, mapped, fixture.Expected)
	if mapped.Provider.RequestID != fixture.RequestID {
		t.Fatalf("classified request ID = %q, want %q", mapped.Provider.RequestID, fixture.RequestID)
	}
	encoded := contract.MarshalJSON(t, mapped)
	if bytes.Contains(encoded, []byte("fixture body [REDACTED]")) {
		t.Fatal("classified error leaked provider response body")
	}
}

func assertBedrockContinuationCompatibility(t *testing.T, adapter *Adapter) {
	t.Helper()
	var semantic struct {
		Replay       llm.Request `json:"replay"`
		Incompatible []struct {
			Name    string      `json:"name"`
			Request llm.Request `json:"request"`
		} `json:"incompatible"`
	}
	if err := json.Unmarshal(mustReadBedrockFixture(t, "continuation-compatibility.semantic.json"), &semantic); err != nil {
		t.Fatal(err)
	}
	var wire struct {
		ReplayMessages json.RawMessage `json:"replay_messages"`
		Rejections     []struct {
			Name     string               `json:"name"`
			Expected fixtureProviderError `json:"expected"`
		} `json:"rejections"`
	}
	if err := json.Unmarshal(mustReadBedrockFixture(t, "continuation-compatibility.wire.json"), &wire); err != nil {
		t.Fatal(err)
	}
	replay := compileBedrockFixture(t, adapter, semantic.Replay)
	if !replay.Metadata.OpaqueStateRequired {
		t.Fatal("Bedrock continuation call did not retain opaque-state requirement")
	}
	params := contract.JSONObject(t, replay.SDKParams)
	replayMessages := contract.MarshalJSON(t, params["messages"])
	contract.AssertCanonicalJSON(t, replayMessages, wire.ReplayMessages, "continuation replay messages")

	expectedByName := make(map[string]fixtureProviderError, len(wire.Rejections))
	for _, rejection := range wire.Rejections {
		expectedByName[rejection.Name] = rejection.Expected
	}
	for _, incompatible := range semantic.Incompatible {
		_, err := adaptertest.Compile(adapter, bedrockFixtureEndpoint, provider.FamilyBedrockMessages, incompatible.Request, true)
		var providerErr *provider.Error
		if !errors.As(err, &providerErr) {
			t.Fatalf("%s continuation error = %T %v, want provider error", incompatible.Name, err, err)
		}
		expected, ok := expectedByName[incompatible.Name]
		if !ok {
			t.Fatalf("%s has no expected rejection fixture", incompatible.Name)
		}
		assertBedrockProviderError(t, providerErr, expected)
	}
}

type fixtureProviderError struct {
	Code            provider.Code              `json:"code"`
	Phase           provider.Phase             `json:"phase"`
	Dispatch        provider.DispatchCertainty `json:"dispatch"`
	Retry           provider.RetryDisposition  `json:"retry"`
	MessageContains string                     `json:"message_contains"`
}

func assertBedrockProviderError(t *testing.T, got *provider.Error, want fixtureProviderError) {
	t.Helper()
	if got == nil {
		t.Fatal("provider error is nil")
	}
	if got.Code != want.Code || got.Phase != want.Phase || got.Dispatch != want.Dispatch || got.Retry != want.Retry {
		t.Fatalf("provider error = %#v, want %#v", got, want)
	}
	if want.MessageContains != "" && !strings.Contains(got.SafeMessage, want.MessageContains) {
		t.Fatalf("provider error message = %q, want substring %q", got.SafeMessage, want.MessageContains)
	}
}
