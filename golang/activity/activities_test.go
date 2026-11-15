package activity

import (
	"context"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
)

type fakeEngine struct {
	response llm.Response
	err      error
	requests []llm.Request
}

func (engine *fakeEngine) Generate(_ context.Context, request llm.Request) (llm.Response, error) {
	engine.requests = append(engine.requests, request)
	if engine.err != nil {
		return llm.Response{}, engine.err
	}
	return engine.response, nil
}

var _ llm.Engine = (*fakeEngine)(nil)

type fakeHeartbeater struct{ progress []engine.Progress }

func (heartbeater *fakeHeartbeater) Beat(_ context.Context, progress engine.Progress) error {
	heartbeater.progress = append(heartbeater.progress, progress)
	return nil
}

type temporalActivityCaptureRegistry struct {
	name     string
	function any
}

func (registry *temporalActivityCaptureRegistry) RegisterActivity(function any) {
	registry.function = function
}

func (registry *temporalActivityCaptureRegistry) RegisterActivityWithOptions(function any, options sdkactivity.RegisterOptions) {
	registry.name = options.Name
	registry.function = function
}

func (*temporalActivityCaptureRegistry) RegisterDynamicActivity(any, sdkactivity.DynamicRegisterOptions) {
}

var _ worker.ActivityRegistry = (*temporalActivityCaptureRegistry)(nil)

func TestGenerateActivityCreatesAnIsolatedHeartbeaterPerInvocation(t *testing.T) {
	response := completedEngineResponse()
	var created []*fakeHeartbeater
	value := &fakeEngine{response: response}
	activities := Activities{
		Engine: value,
		HeartbeaterFactory: func() Heartbeater {
			heartbeater := &fakeHeartbeater{}
			created = append(created, heartbeater)
			return heartbeater
		},
	}
	payload := GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Model: "model-1", Input: []llm.Item{llm.HumanText("hello")}}}
	testassert.NoError(t, testassert.Err(activities.Generate(context.Background(), payload)))
	payload.Request.OperationKey = "operation-2"
	testassert.NoError(t, testassert.Err(activities.Generate(context.Background(), payload)))
	if len(created) != 2 || created[0] == created[1] {
		t.Fatalf("created heartbeater instances = %#v, want two distinct per-Activity instances", created)
	}
	if len(created[0].progress) == 0 || len(created[1].progress) == 0 {
		t.Fatalf("per-Activity heartbeat progress = %d, %d; want bounded lifecycle progress on each", len(created[0].progress), len(created[1].progress))
	}
	if got := len(value.requests); got != 2 {
		t.Fatalf("Generate calls = %d, want one per Activity invocation", got)
	}
}

func TestGenerateActivityMapsPayloadAndHeartbeats(t *testing.T) {
	heartbeater := &fakeHeartbeater{}
	value := &fakeEngine{response: completedEngineResponse()}
	activities := Activities{Engine: value, Heartbeater: heartbeater}
	response, err := activities.Generate(context.Background(), GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Model: "model-1", Input: []llm.Item{llm.HumanText("hello")}}})
	testassert.NoError(t, err)
	if response.Metadata.OperationID != "operation-id" || len(heartbeater.progress) != 2 || heartbeater.progress[0].Phase != "planning" || heartbeater.progress[1].Phase != "finalization" {
		t.Fatalf("response=%#v heartbeats=%#v", response, heartbeater.progress)
	}
	if got := value.requests; len(got) != 1 || got[0].OperationKey != "operation-1" || got[0].Model != "model-1" {
		t.Fatalf("Generate request = %#v, want validated Activity request", got)
	}
}

func TestGenerateActivityMapsEngineError(t *testing.T) {
	_, got := ambiguousEngineActivities().Generate(context.Background(), inputlessGeneratePayload())
	assertAmbiguousActivityError(t, got)
}

func TestGenerateTemporalReturnsPointerOnlyOnSuccess(t *testing.T) {
	want := completedEngineResponse()
	activities := Activities{Engine: &fakeEngine{response: want}}

	response, err := activities.generateTemporal(context.Background(), inputlessGeneratePayload())
	testassert.NoError(t, err)
	if response == nil {
		t.Fatal("Temporal Activity response = nil, want success response")
	}
	if response.Response.OperationKey != want.OperationKey || response.Response.OperationID != want.OperationID || response.Response.Status != want.Status {
		t.Fatalf("Temporal Activity response = %#v, want %#v", response.Response, want)
	}
}

func TestGenerateTemporalReturnsNilResultForAmbiguousError(t *testing.T) {
	response, got := ambiguousEngineActivities().generateTemporal(context.Background(), inputlessGeneratePayload())
	if response != nil {
		t.Fatalf("Temporal Activity response = %#v, want nil with error", response)
	}
	assertAmbiguousActivityError(t, got)
}

func TestRegisteredTemporalGeneratePreservesAmbiguousApplicationError(t *testing.T) {
	registry := &temporalActivityCaptureRegistry{}
	ambiguousEngineActivities().Register(registry)
	if registry.name != LegacyGenerateActivityName {
		t.Fatalf("registered Activity = %q, want %q", registry.name, LegacyGenerateActivityName)
	}
	generate, ok := registry.function.(func(context.Context, GenerateRequest) (*GenerateResponse, error))
	if !ok {
		t.Fatalf("registered Activity has type %T, want pointer response handler", registry.function)
	}

	_, got := executeTestActivity(nil, generate, registry.name, inputlessGeneratePayload())
	if got == nil {
		t.Fatal("registered Temporal Activity unexpectedly succeeded")
	}
	if strings.Contains(got.Error(), "response requires operation_key") {
		t.Fatalf("registered Temporal Activity serialized a zero response instead of returning ambiguity: %v", got)
	}
	assertAmbiguousActivityError(t, got)
}

// completedEngineResponse is the completed standard-service response the fake
// engines return for "operation-1".
func completedEngineResponse() llm.Response {
	return llm.Response{OperationKey: "operation-1", OperationID: "operation-id", Status: llm.ResponseStatusCompleted, Service: llm.ServiceFacts{Requested: llm.ServiceClassStandard, Attempted: llm.ServiceClassStandard}}
}

// inputlessGeneratePayload is a legacy Generate payload with no input items.
func inputlessGeneratePayload() GenerateRequest {
	return GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Model: "model-1"}}
}

// ambiguousEngineActivities wires an engine that always fails with an
// ambiguous-dispatch provider error.
func ambiguousEngineActivities() *Activities {
	err := provider.NewError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "safe")
	return &Activities{Engine: &fakeEngine{err: err}}
}

func assertAmbiguousActivityError(t *testing.T, got error) {
	t.Helper()
	assertApplicationError(t, got, ErrorTypeAmbiguous, true)
}
