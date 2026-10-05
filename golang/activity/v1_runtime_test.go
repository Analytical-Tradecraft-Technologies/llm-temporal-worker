package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

type v1RuntimeStub struct {
	generateCalls int
	compactCalls  int
	queryCalls    int
	err           error
	entered       chan struct{}
	release       chan struct{}
}

func (runtime *v1RuntimeStub) GenerateV1(_ context.Context, request llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	runtime.generateCalls++
	if runtime.entered != nil {
		runtime.entered <- struct{}{}
	}
	if runtime.release != nil {
		<-runtime.release
	}
	if runtime.err != nil {
		return llm.GenerateResponseV1{}, runtime.err
	}
	return llm.GenerateResponseV1{
		APIVersion: llm.APIVersion, OperationKey: request.OperationKey, OperationID: "op-generate",
		Status:     llm.ResponseStatusCompleted,
		Output:     []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "ok"}}}},
		Checkpoint: llm.CheckpointMetadata{Handle: "ckp-generate", Kind: "generation"},
		Cache:      llm.CacheDispositionV1{Disposition: "disabled"},
		Cost:       llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "provider_reported"},
	}, nil
}

func (runtime *v1RuntimeStub) CompactV1(_ context.Context, request llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	runtime.compactCalls++
	if runtime.err != nil {
		return llm.CompactResponseV1{}, runtime.err
	}
	return llm.CompactResponseV1{
		APIVersion: llm.CompactAPIVersion, OperationKey: request.OperationKey, OperationID: "op-compact",
		Checkpoint: llm.CheckpointMetadata{Handle: "ckp-compact", Parent: pointer(llm.CheckpointHandle(request.Parent)), Kind: "compaction"},
		Cache:      llm.CacheDispositionV1{Disposition: "disabled"},
		Cost:       llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "provider_reported"},
	}, nil
}

func (runtime *v1RuntimeStub) QueryV1(_ context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	runtime.queryCalls++
	if runtime.err != nil {
		return llm.QueryResponseV1{}, runtime.err
	}
	return llm.QueryResponseV1{
		APIVersion: llm.QueryAPIVersion, OperationKey: request.OperationKey, QueryExecutionID: "query-1",
		Kind: llm.QueryProviderStatus, ObservedAt: "2026-07-20T00:00:00Z", Source: "persisted", Freshness: "current", Complete: true,
		Result: llm.ProviderStatusPage{Routes: []json.RawMessage{}},
		Cost:   llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "control_query_zero"},
	}, nil
}

type v1Registry struct {
	names []string
	funcs []any
}

func (registry *v1Registry) RegisterActivity(value any) {
	registry.names = append(registry.names, "")
	registry.funcs = append(registry.funcs, value)
}

func (registry *v1Registry) RegisterActivityWithOptions(value any, options sdkactivity.RegisterOptions) {
	registry.names = append(registry.names, options.Name)
	registry.funcs = append(registry.funcs, value)
}

func (*v1Registry) RegisterDynamicActivity(any, sdkactivity.DynamicRegisterOptions) {}

func validGenerateV1Request() llm.GenerateRequestV1 {
	return llm.GenerateRequestV1{
		APIVersion: llm.APIVersion, OperationKey: "generate-1",
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "workflow"},
		Append:  []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
	}
}

func validCompactV1Request() llm.CompactRequestV1 {
	return llm.CompactRequestV1{
		APIVersion: llm.CompactAPIVersion, OperationKey: "compact-1", Parent: "ckp-parent",
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "workflow"},
	}
}

func validQueryV1Request() llm.QueryRequestV1 {
	return llm.QueryRequestV1{
		APIVersion: llm.QueryAPIVersion, OperationKey: "query-1", Kind: llm.QueryProviderStatus,
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "workflow"}, Query: []byte(`{"page_size":10}`),
	}
}

func TestRegisterV1InstallsExactActivityNames(t *testing.T) {
	registry := &v1Registry{}
	activities := &Activities{V1Runtime: &v1RuntimeStub{}}
	activities.Register(registry)
	want := []string{GenerateActivityName, CompactActivityName, QueryActivityName, PrepareActivityName, AcquireBudgetActivityName, PollActivityName, CompleteActivityName, PlanGenerationActivityName}
	if fmt.Sprint(registry.names) != fmt.Sprint(want) {
		t.Fatalf("registered names = %v, want %v", registry.names, want)
	}
	// Every v1 handler takes a raw payload and decodes it strictly inside the
	// Activity so decode failures map to typed, non-retryable errors.
	for index, registered := range registry.funcs {
		switch registered.(type) {
		case func(context.Context, converter.RawValue) (*llm.ExecutionResultV1, error),
			func(context.Context, converter.RawValue) (*llm.QueryResponseV1, error),
			func(context.Context, converter.RawValue) (*llm.GenerationPlanV1, error):
		default:
			t.Fatalf("%s registration has type %T, want a raw-payload handler", registry.names[index], registered)
		}
	}
}

func TestV1ActivitiesDispatchTypedRecords(t *testing.T) {
	runtime := &v1RuntimeStub{}
	activities := &Activities{V1Runtime: runtime}
	if _, err := activities.GenerateV1(context.Background(), validGenerateV1Request()); err != nil {
		t.Fatalf("GenerateV1 error = %v", err)
	}
	if _, err := activities.CompactV1(context.Background(), validCompactV1Request()); err != nil {
		t.Fatalf("CompactV1 error = %v", err)
	}
	if _, err := activities.QueryV1(context.Background(), validQueryV1Request()); err != nil {
		t.Fatalf("QueryV1 error = %v", err)
	}
	if runtime.generateCalls != 1 || runtime.compactCalls != 1 || runtime.queryCalls != 1 {
		t.Fatalf("runtime calls = generate:%d compact:%d query:%d", runtime.generateCalls, runtime.compactCalls, runtime.queryCalls)
	}
}

func TestUnconfiguredV1RuntimeFailsClosedForEveryOperation(t *testing.T) {
	runtime := UnconfiguredV1Runtime{}
	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "generate",
			call: func() error {
				_, err := runtime.GenerateV1(context.Background(), validGenerateV1Request())
				return err
			},
		},
		{
			name: "compact",
			call: func() error {
				_, err := runtime.CompactV1(context.Background(), validCompactV1Request())
				return err
			},
		},
		{
			name: "query",
			call: func() error {
				_, err := runtime.QueryV1(context.Background(), validQueryV1Request())
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			if err == nil {
				t.Fatal("unconfigured v1 runtime unexpectedly succeeded")
			}
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) {
				t.Fatalf("error = %T %v, want provider error", err, err)
			}
			if providerErr.Code != provider.CodeConfiguration ||
				providerErr.Phase != provider.PhaseStateLoad ||
				providerErr.Dispatch != provider.DispatchNotDispatched ||
				providerErr.Retry != provider.RetryNever {
				t.Fatalf("provider error = %#v, want configuration/state-load/not-dispatched/never", providerErr)
			}
			if got, want := err.Error(), "durable v1 runtime is not configured"; got != want {
				t.Fatalf("error text = %q, want stable redacted message %q", got, want)
			}
		})
	}
}

func TestV1GeneratePreservesHeartbeatLifecycleDuringRuntimeDispatch(t *testing.T) {
	ticker := newManualHeartbeatTicker()
	heartbeater := newRecordingHeartbeater()
	runtime := &v1RuntimeStub{entered: make(chan struct{}, 1), release: make(chan struct{})}
	activities := &Activities{
		V1Runtime:                  runtime,
		Heartbeater:                heartbeater,
		HeartbeatKeepaliveInterval: time.Second,
		heartbeatTickerFactory: func(time.Duration) heartbeatTicker {
			return ticker
		},
	}
	done := make(chan error, 1)
	go func() {
		_, err := activities.GenerateV1(context.Background(), validGenerateV1Request())
		done <- err
	}()
	select {
	case <-runtime.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for v1 runtime dispatch")
	}
	ticker.Tick()
	heartbeater.WaitForPhase(t, heartbeatProviderWaitPhase, 1)
	close(runtime.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for v1 Activity completion")
	}
	waitForKeepaliveEvent(t, ticker.stopped, "v1 keepalive ticker to stop")
}

func TestRegisteredV1GenerateExecutesThroughTemporalEnvironment(t *testing.T) {
	runtime := &executionRuntimeStub{result: pendingExecution()}
	activities := &Activities{V1Runtime: runtime}
	registry := &v1Registry{}
	activities.Register(registry)
	generate := registry.funcs[0]
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivityWithOptions(generate, sdkactivity.RegisterOptions{Name: GenerateActivityName})
	result, err := environment.ExecuteActivity(GenerateActivityName, validGenerateV1Request())
	if err != nil {
		t.Fatal(err)
	}
	var response llm.ExecutionResultV1
	if err := result.Get(&response); err != nil {
		t.Fatal(err)
	}
	if response.State != llm.ExecutionPending || runtime.calls != 1 {
		t.Fatalf("result=%#v calls=%d", response, runtime.calls)
	}
}

func TestV1ActivityRejectsOversizedPayloadBeforeDispatch(t *testing.T) {
	runtime := &v1RuntimeStub{}
	activities := &Activities{V1Runtime: runtime, PayloadLimits: PayloadLimits{MaxInlineBytes: 64}}
	request := validGenerateV1Request()
	request.Append = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: strings.Repeat("x", 256)}}}}
	_, err := activities.GenerateV1(context.Background(), request)
	if err == nil {
		t.Fatal("GenerateV1 unexpectedly accepted oversized payload")
	}
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) || applicationErr.Type() != ErrorTypeInvalidArgument {
		t.Fatalf("error = %T %v, want non-retryable invalid argument", err, err)
	}
	if runtime.generateCalls != 0 {
		t.Fatalf("runtime calls = %d, want zero before payload validation", runtime.generateCalls)
	}
}

func TestV1ActivityFailsClosedWithoutRuntime(t *testing.T) {
	_, err := (&Activities{}).GenerateV1(context.Background(), validGenerateV1Request())
	if err == nil {
		t.Fatal("GenerateV1 unexpectedly succeeded without a runtime")
	}
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) || applicationErr.Type() != ErrorTypeInvalidArgument {
		t.Fatalf("error = %T %v, want stable configuration error", err, err)
	}
}

func TestV1ActivityRedactsRuntimeError(t *testing.T) {
	secret := "provider-response-secret"
	runtime := &v1RuntimeStub{err: provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryAfter, secret)}
	_, err := (&Activities{V1Runtime: runtime}).GenerateV1(context.Background(), validGenerateV1Request())
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("error = %v, must redact runtime error details", err)
	}
}

func TestV1ActivityRedactsRuntimeErrorAcrossEveryActivity(t *testing.T) {
	secret := "provider-response-secret"
	cases := []struct {
		name  string
		call  func(*Activities) error
		calls func(*v1RuntimeStub) int
	}{
		{
			name: "generate",
			call: func(activities *Activities) error {
				_, err := activities.GenerateV1(context.Background(), validGenerateV1Request())
				return err
			},
			calls: func(runtime *v1RuntimeStub) int { return runtime.generateCalls },
		},
		{
			name: "compact",
			call: func(activities *Activities) error {
				_, err := activities.CompactV1(context.Background(), validCompactV1Request())
				return err
			},
			calls: func(runtime *v1RuntimeStub) int { return runtime.compactCalls },
		},
		{
			name: "query",
			call: func(activities *Activities) error {
				_, err := activities.QueryV1(context.Background(), validQueryV1Request())
				return err
			},
			calls: func(runtime *v1RuntimeStub) int { return runtime.queryCalls },
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			runtime := &v1RuntimeStub{err: provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryAfter, secret)}
			err := test.call(&Activities{V1Runtime: runtime})
			if err == nil || strings.Contains(err.Error(), secret) {
				t.Fatalf("error = %v, must redact runtime error details", err)
			}
			if got := test.calls(runtime); got != 1 {
				t.Fatalf("runtime calls = %d, want one dispatch", got)
			}
		})
	}
}

func pointer(value llm.CheckpointHandle) *llm.CheckpointHandle { return &value }
func stringPointer(value string) *string                       { return &value }
