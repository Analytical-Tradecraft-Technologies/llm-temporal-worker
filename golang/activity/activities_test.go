package activity

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
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
	response := llm.Response{OperationKey: "operation-1", OperationID: "operation-id", Status: llm.ResponseStatusCompleted, Service: llm.ServiceFacts{Requested: llm.ServiceClassStandard, Attempted: llm.ServiceClassStandard}}
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
	payload := GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Model: "model-1", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}}}
	if _, err := activities.Generate(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	payload.Request.OperationKey = "operation-2"
	if _, err := activities.Generate(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
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
	value := &fakeEngine{response: llm.Response{OperationKey: "operation-1", OperationID: "operation-id", Status: llm.ResponseStatusCompleted, Service: llm.ServiceFacts{Requested: llm.ServiceClassStandard, Attempted: llm.ServiceClassStandard}}}
	activities := Activities{Engine: value, Heartbeater: heartbeater}
	response, err := activities.Generate(context.Background(), GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Model: "model-1", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if response.Metadata.OperationID != "operation-id" || len(heartbeater.progress) != 2 || heartbeater.progress[0].Phase != "planning" || heartbeater.progress[1].Phase != "finalization" {
		t.Fatalf("response=%#v heartbeats=%#v", response, heartbeater.progress)
	}
	if got := value.requests; len(got) != 1 || got[0].OperationKey != "operation-1" || got[0].Model != "model-1" {
		t.Fatalf("Generate request = %#v, want validated Activity request", got)
	}
}

func TestGenerateActivityDurableCompositionRejectsLegacyProviderDispatch(t *testing.T) {
	value := &fakeEngine{}
	activities := Activities{Engine: value, RequireDurableProviderFence: true}
	payload := GenerateRequest{APIVersion: APIVersion, Request: llm.Request{
		OperationKey: "operation-1", Model: "model-1",
		Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
	}}
	if _, err := activities.Generate(context.Background(), payload); err == nil {
		t.Fatal("durable composition dispatched the legacy Generate surface")
	}
	if len(value.requests) != 0 {
		t.Fatalf("legacy engine calls = %d, want zero before a durable provider fence", len(value.requests))
	}
}

func TestGenerateActivityMapsEngineError(t *testing.T) {
	err := provider.NewError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "safe")
	activities := Activities{Engine: &fakeEngine{err: err}}
	_, got := activities.Generate(context.Background(), GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Model: "model-1"}})
	var applicationErr *temporal.ApplicationError
	if !errors.As(got, &applicationErr) {
		t.Fatalf("error = %v", got)
	}
	if applicationErr.Type() != ErrorTypeAmbiguous || !applicationErr.NonRetryable() {
		t.Fatalf("error type = %q non_retryable=%v", applicationErr.Type(), applicationErr.NonRetryable())
	}
}

func TestGenerateTemporalReturnsPointerOnlyOnSuccess(t *testing.T) {
	want := llm.Response{OperationKey: "operation-1", OperationID: "operation-id", Status: llm.ResponseStatusCompleted, Service: llm.ServiceFacts{Requested: llm.ServiceClassStandard, Attempted: llm.ServiceClassStandard}}
	activities := Activities{Engine: &fakeEngine{response: want}}

	response, err := activities.generateTemporal(context.Background(), GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Model: "model-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if response == nil {
		t.Fatal("Temporal Activity response = nil, want success response")
	}
	if response.Response.OperationKey != want.OperationKey || response.Response.OperationID != want.OperationID || response.Response.Status != want.Status {
		t.Fatalf("Temporal Activity response = %#v, want %#v", response.Response, want)
	}
}

func TestGenerateTemporalReturnsNilResultForAmbiguousError(t *testing.T) {
	err := provider.NewError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "safe")
	activities := Activities{Engine: &fakeEngine{err: err}}

	response, got := activities.generateTemporal(context.Background(), GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Model: "model-1"}})
	if response != nil {
		t.Fatalf("Temporal Activity response = %#v, want nil with error", response)
	}
	assertAmbiguousActivityError(t, got)
}

func TestRegisteredTemporalGeneratePreservesAmbiguousApplicationError(t *testing.T) {
	err := provider.NewError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "safe")
	activities := Activities{Engine: &fakeEngine{err: err}}
	registry := &temporalActivityCaptureRegistry{}
	activities.Register(registry)
	if registry.name != GenerateActivityName {
		t.Fatalf("registered Activity = %q, want %q", registry.name, GenerateActivityName)
	}
	generate, ok := registry.function.(func(context.Context, GenerateRequest) (*GenerateResponse, error))
	if !ok {
		t.Fatalf("registered Activity has type %T, want pointer response handler", registry.function)
	}

	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivityWithOptions(generate, sdkactivity.RegisterOptions{Name: registry.name})
	_, got := environment.ExecuteActivity(generate, GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Model: "model-1"}})
	if got == nil {
		t.Fatal("registered Temporal Activity unexpectedly succeeded")
	}
	if strings.Contains(got.Error(), "response requires operation_key") {
		t.Fatalf("registered Temporal Activity serialized a zero response instead of returning ambiguity: %v", got)
	}
	assertAmbiguousActivityError(t, got)
}

func assertAmbiguousActivityError(t *testing.T, got error) {
	t.Helper()
	var applicationErr *temporal.ApplicationError
	if !errors.As(got, &applicationErr) {
		t.Fatalf("error = %T %v, want Temporal application error", got, got)
	}
	if applicationErr.Type() != ErrorTypeAmbiguous || !applicationErr.NonRetryable() {
		t.Fatalf("error type = %q non_retryable=%v, want %q true", applicationErr.Type(), applicationErr.NonRetryable(), ErrorTypeAmbiguous)
	}
}
