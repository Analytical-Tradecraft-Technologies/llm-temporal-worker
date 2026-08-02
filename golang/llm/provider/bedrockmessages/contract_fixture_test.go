package bedrockmessages

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
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

const (
	bedrockFixtureProfileID  = "bedrock-anthropic"
	bedrockFixtureEndpoint   = "bedrock-fixture"
	bedrockFixtureRequestID  = "bedrock-request-fixture"
	bedrockFixtureSourceDate = "2026-07-14"
)

func TestBedrockContractProfileIsEnforced(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	report, err := contracttest.ValidateRepository(root)
	if err != nil {
		t.Fatal(err)
	}
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

	encodedWire, err := json.Marshal(call.SDKParams)
	if err != nil {
		t.Fatal(err)
	}
	assertBedrockFixtureJSON(t, encodedWire, "request.wire.json")

	response := loadBedrockContractResponse(t, "response.completed.json")
	lifted, err := adapter.profile.liftResponse(call, &response, bedrockFixtureRequestID)
	if err != nil {
		t.Fatal(err)
	}
	gotSemantic, err := json.Marshal(lifted)
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := json.Marshal(lifted)
	if err != nil {
		t.Fatal(err)
	}
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
			want, err := DecodeStream(bytes.NewReader(wire))
			if err != nil {
				t.Fatal(err)
			}
			toolFragments := 0
			for _, event := range want {
				if _, ok := event.(provider.ToolArgumentsDelta); ok {
					toolFragments++
				}
			}
			if toolFragments != fixture.wantToolFragments {
				t.Fatalf("tool argument fragments = %d, want %d", toolFragments, fixture.wantToolFragments)
			}
			for split := 1; split < len(wire); split++ {
				got, err := DecodeStream(&bedrockChunkReader{chunks: [][]byte{wire[:split], {}, wire[split:]}})
				if err != nil {
					t.Fatalf("split %d: %v", split, err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("split %d changed decoded events\n got: %#v\nwant: %#v", split, got, want)
				}
			}
			for _, chunks := range [][][]byte{streamtest.Fragment(wire, 1), streamtest.RandomFragment(wire, 41, 11)} {
				got, err := DecodeStream(&bedrockChunkReader{chunks: chunks})
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("fragmented events differ\n got: %#v\nwant: %#v", got, want)
				}
			}
			if err := contracttest.VerifyStreamAssemblyEquivalent(wire, mustReadBedrockFixture(t, fixture.semantic), func(events []byte) ([]byte, error) {
				return assembleBedrockFixtureStream(events, fixture.operationKey)
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
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: bedrockFixtureEndpoint,
			Family:     provider.FamilyBedrockMessages,
			Model:      request.Model,
		},
		Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return call
}

func loadBedrockContractRequest(t *testing.T, name string) llm.Request {
	t.Helper()
	var request llm.Request
	if err := json.Unmarshal(mustReadBedrockFixture(t, name), &request); err != nil {
		t.Fatal(err)
	}
	return request
}

func loadBedrockContractResponse(t *testing.T, name string) anthropic.Message {
	t.Helper()
	var response anthropic.Message
	if err := json.Unmarshal(mustReadBedrockFixture(t, name), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func loadBedrockFixtureMetadata(t *testing.T) contracttest.Metadata {
	t.Helper()
	var metadata contracttest.Metadata
	if err := yaml.Unmarshal(mustReadBedrockFixture(t, "metadata.yaml"), &metadata); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func assertBedrockFixtureJSON(t *testing.T, got []byte, name string) {
	t.Helper()
	assertBedrockCanonicalJSON(t, got, mustReadBedrockFixture(t, name), name)
}

func assertBedrockCanonicalJSON(t *testing.T, got, want []byte, name string) {
	t.Helper()
	gotCanonical, err := llm.CanonicalJSON(got)
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical, err := llm.CanonicalJSON(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotCanonical, wantCanonical) {
		t.Fatalf("%s mismatch\n got: %s\nwant: %s", name, gotCanonical, wantCanonical)
	}
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
	var semantic []struct {
		Class  llm.ServiceClass `json:"class"`
		Actual llm.ServiceClass `json:"actual"`
	}
	if err := json.Unmarshal(mustReadBedrockFixture(t, "class-facts.semantic.json"), &semantic); err != nil {
		t.Fatal(err)
	}
	var wire []struct {
		Class         llm.ServiceClass `json:"class"`
		Supported     bool             `json:"supported"`
		RequestedTier string           `json:"requested_tier"`
		ActualTier    string           `json:"actual_tier"`
	}
	if err := json.Unmarshal(mustReadBedrockFixture(t, "class-facts.wire.json"), &wire); err != nil {
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
		call := compileBedrockFixture(t, adapter, llm.Request{OperationKey: "fixture-bedrock-class-" + string(fact.Class), Model: "claude-contract", ServiceClass: fact.Class})
		if call.Metadata.ProviderTier != fact.RequestedTier {
			t.Fatalf("%s provider tier = %q, want %q", fact.Class, call.Metadata.ProviderTier, fact.RequestedTier)
		}
		params := marshalBedrockWire(t, call.SDKParams)
		if params["service_tier"] != fact.RequestedTier {
			t.Fatalf("%s wire tier = %#v, want %q", fact.Class, params["service_tier"], fact.RequestedTier)
		}
		actual, err := adapter.profile.actualClass(fact.ActualTier)
		if err != nil || actual == nil || *actual != actualByClass[fact.Class] {
			t.Fatalf("%s actual class = %#v, %v; want %q", fact.Class, actual, err, actualByClass[fact.Class])
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
	got, err := json.Marshal(lifted)
	if err != nil {
		t.Fatal(err)
	}
	assertBedrockFixtureJSON(t, got, "usage-cost.semantic.json")
	if lifted.Cost != (llm.Cost{}) {
		t.Fatalf("Bedrock cost should remain unreported: %#v", lifted.Cost)
	}
}

func assertBedrockStrictLoss(t *testing.T, adapter *Adapter) {
	t.Helper()
	request := loadBedrockContractRequest(t, "strict-loss.semantic.json")
	var expected fixtureProviderError
	if err := json.Unmarshal(mustReadBedrockFixture(t, "strict-loss.wire.json"), &expected); err != nil {
		t.Fatal(err)
	}
	for _, portability := range []llm.PortabilityMode{llm.PortabilityStrict, llm.PortabilityBestEffort} {
		request.Portability = portability
		_, err := adapter.Compile(context.Background(), provider.CompileInput{
			Request: request,
			Query:   provider.CapabilityQuery{EndpointID: bedrockFixtureEndpoint, Family: provider.FamilyBedrockMessages, Model: request.Model},
			Strict:  true,
		})
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
	var apiErr anthropic.Error
	if err := json.Unmarshal(fixture.Body, &apiErr); err != nil {
		t.Fatal(err)
	}
	apiErr.StatusCode = fixture.Status
	apiErr.RequestID = fixture.RequestID
	apiErr.Response = &http.Response{Header: http.Header{"X-Amzn-Requestid": []string{fixture.RequestID}}}
	mapped := mapAPIError(&apiErr, "bedrock.messages/bedrock-contract")
	assertBedrockProviderError(t, mapped, fixture.Expected)
	if mapped.Provider.RequestID != fixture.RequestID {
		t.Fatalf("classified request ID = %q, want %q", mapped.Provider.RequestID, fixture.RequestID)
	}
	encoded, err := json.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
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
	params := marshalBedrockWire(t, replay.SDKParams)
	replayMessages, err := json.Marshal(params["messages"])
	if err != nil {
		t.Fatal(err)
	}
	assertBedrockCanonicalJSON(t, replayMessages, wire.ReplayMessages, "continuation replay messages")

	expectedByName := make(map[string]fixtureProviderError, len(wire.Rejections))
	for _, rejection := range wire.Rejections {
		expectedByName[rejection.Name] = rejection.Expected
	}
	for _, incompatible := range semantic.Incompatible {
		_, err := adapter.Compile(context.Background(), provider.CompileInput{
			Request: incompatible.Request,
			Query:   provider.CapabilityQuery{EndpointID: bedrockFixtureEndpoint, Family: provider.FamilyBedrockMessages, Model: incompatible.Request.Model},
			Strict:  true,
		})
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

func assembleBedrockFixtureStream(wire []byte, operationKey string) ([]byte, error) {
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
