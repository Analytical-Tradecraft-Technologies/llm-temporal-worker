package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

type resumableEngineAdapter struct {
	mu             sync.Mutex
	submits        int
	polls          int
	sharedCounters *resumableCallCounters
	terminal       bool
	submitErr      error
	pollErrors     []error
	pollResponses  []provider.ResumableResult
	recoveryPollID string
}

// resumableCallCounters are test instrumentation shared by two separately
// constructed adapters. The provider-owned operation state is represented by
// the stable operation ID, while these counters let the restart assertion
// prove that no second Submit crossed the process boundary.
type resumableCallCounters struct {
	mu      sync.Mutex
	submits int
	polls   int
}

func (adapter *resumableEngineAdapter) Name() string { return "resumable-fixture" }

func (adapter *resumableEngineAdapter) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	return provider.CapabilitySet{Version: "fixture", Features: map[provider.Feature]provider.Capability{provider.FeatureText: {State: provider.CapabilityNative}}}, nil
}

func (adapter *resumableEngineAdapter) Compile(_ context.Context, input provider.CompileInput) (provider.Call, error) {
	return provider.Call{EndpointID: input.Query.EndpointID, Family: input.Query.Family, Model: input.Query.Model, OperationKey: input.Request.OperationKey, ServiceClass: input.Query.ServiceClass, Metadata: input.Metadata}, nil
}

func (adapter *resumableEngineAdapter) Invoke(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
	return provider.Result{}, provider.NewError(provider.CodeInternal, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "fixture Invoke must not be called")
}

func (adapter *resumableEngineAdapter) Submit(ctx context.Context, _ provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
	if err := observer.BeforePossibleWrite(ctx); err != nil {
		return provider.ResumableResult{}, err
	}
	adapter.mu.Lock()
	adapter.submits++
	adapter.mu.Unlock()
	if adapter.sharedCounters != nil {
		adapter.sharedCounters.mu.Lock()
		adapter.sharedCounters.submits++
		adapter.sharedCounters.mu.Unlock()
	}
	if adapter.submitErr != nil {
		return provider.ResumableResult{}, adapter.submitErr
	}
	return provider.ResumableResult{State: provider.ResumablePending, ProviderOperationID: "fixture-provider-operation", Dispatch: provider.DispatchAccepted}, nil
}

func (adapter *resumableEngineAdapter) Poll(_ context.Context, _ provider.Call, id string, _ provider.Observer) (provider.ResumableResult, error) {
	adapter.mu.Lock()
	adapter.polls++
	poll := adapter.polls
	pollErrors := adapter.pollErrors
	responses := adapter.pollResponses
	recoveryPollID := adapter.recoveryPollID
	adapter.mu.Unlock()
	if adapter.sharedCounters != nil {
		adapter.sharedCounters.mu.Lock()
		adapter.sharedCounters.polls++
		adapter.sharedCounters.mu.Unlock()
	}
	if poll <= len(pollErrors) && pollErrors[poll-1] != nil {
		return provider.ResumableResult{}, pollErrors[poll-1]
	}
	if len(responses) > 0 {
		if recoveryPollID != "" && id != recoveryPollID {
			return provider.ResumableResult{}, provider.NewError(provider.CodeStateCorrupt, provider.PhasePoll, provider.DispatchAmbiguous, provider.RetryNever, "unexpected recovered provider id")
		}
		if poll > len(responses) {
			return provider.ResumableResult{}, context.Canceled
		}
		return responses[poll-1], nil
	}
	if id != "fixture-provider-operation" {
		return provider.ResumableResult{}, provider.NewError(provider.CodeStateCorrupt, provider.PhasePoll, provider.DispatchAmbiguous, provider.RetryNever, "unexpected fixture provider id")
	}
	if adapter.terminal {
		return provider.ResumableResult{State: provider.ResumableNotFound, Dispatch: provider.DispatchAmbiguous}, nil
	}
	if poll == 1 {
		// Simulate a worker/activity interruption after the durable ID was
		// written. The next Activity attempt must poll, never submit.
		return provider.ResumableResult{}, context.Canceled
	}
	response := successfulResponse()
	response.OperationKey = "resumable-retry"
	return provider.ResumableResult{State: provider.ResumableCompleted, ProviderOperationID: id, Dispatch: provider.DispatchAccepted, Result: provider.Result{Response: response}}, nil
}

type idempotencyRecoveryAdapter struct {
	*resumableEngineAdapter
	recovered  provider.ResumableResult
	recoveries int
}

func (adapter *idempotencyRecoveryAdapter) RecoverByIdempotencyKey(context.Context, provider.Call, provider.Observer) (provider.ResumableResult, error) {
	adapter.mu.Lock()
	adapter.recoveries++
	adapter.mu.Unlock()
	return adapter.recovered, nil
}

var _ provider.IdempotencyRecovery = (*idempotencyRecoveryAdapter)(nil)

var _ provider.ResumableAdapter = (*resumableEngineAdapter)(nil)

func TestGenerateResumesDurableProviderOperationWithoutSubmit(t *testing.T) {
	adapter := &resumableEngineAdapter{}
	harness := newHarness(t, adapter)
	metrics := newEngineTestMetrics(t)
	request := baseRequest("resumable-retry")
	ctx := observability.WithMetrics(context.Background(), metrics)
	if _, err := harness.engine.Generate(ctx, request); err == nil {
		t.Fatal("first attempt unexpectedly completed")
	}
	operation, err := harness.admission.Get(context.Background(), operationIDForTest(t, request))
	if err != nil {
		t.Fatal(err)
	}
	if string(operation.State) != "provider_pending" {
		t.Fatalf("operation state after interrupted poll = %q, want provider_pending", operation.State)
	}
	response, err := harness.engine.Generate(ctx, request)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if response.Status != llm.ResponseStatusCompleted {
		t.Fatalf("response status = %q, want completed", response.Status)
	}
	adapter.mu.Lock()
	submits, polls := adapter.submits, adapter.polls
	adapter.mu.Unlock()
	if submits != 1 {
		t.Fatalf("Submit calls = %d, want 1", submits)
	}
	if polls != 2 {
		t.Fatalf("Poll calls = %d, want 2", polls)
	}
	assertMetricCounter(t, metrics, "llmtw_provider_poll_total", map[string]string{"outcome": "started"}, 2)
	assertMetricCounter(t, metrics, "llmtw_provider_poll_total", map[string]string{"outcome": "retry"}, 1)
	assertMetricCounter(t, metrics, "llmtw_provider_poll_total", map[string]string{"outcome": "completed"}, 1)
}

func TestGenerateResumesDurableProviderOperationAfterEngineRestart(t *testing.T) {
	counters := &resumableCallCounters{}
	firstAdapter := &resumableEngineAdapter{
		sharedCounters: counters,
		pollErrors:     []error{context.Canceled},
	}
	first := newHarness(t, firstAdapter)
	request := baseRequest("resumable-engine-restart")

	if _, err := first.engine.Generate(context.Background(), request); err == nil {
		t.Fatal("first attempt unexpectedly completed")
	}
	operationID := operationIDForTest(t, request)
	operation, err := first.admission.Get(context.Background(), operationID)
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != admission.StateProviderPending {
		t.Fatalf("operation state after interrupted poll = %q, want provider_pending", operation.State)
	}
	pending, ok := first.admission.(admission.ProviderPendingStore)
	if !ok {
		t.Fatal("test admission store does not expose provider-pending recovery")
	}
	providerOperationID, err := pending.ProviderOperation(context.Background(), operationID)
	if err != nil {
		t.Fatalf("load persisted provider operation ID: %v", err)
	}
	if providerOperationID != "fixture-provider-operation" {
		t.Fatalf("persisted provider operation ID = %q, want fixture-provider-operation", providerOperationID)
	}

	// A replacement worker gets a fresh Engine, result repository, and provider
	// adapter while the durable operation ledger and provider-owned operation
	// remain available. Recovery must use the persisted provider operation ID
	// and poll it; it must not call Submit again after the process boundary.
	resumedResponse := successfulResponse()
	resumedResponse.OperationKey = request.OperationKey
	replacementAdapter := &resumableEngineAdapter{
		sharedCounters: counters,
		pollResponses: []provider.ResumableResult{{
			State:               provider.ResumableCompleted,
			ProviderOperationID: providerOperationID,
			Dispatch:            provider.DispatchAccepted,
			Result:              provider.Result{Response: resumedResponse},
		}},
		recoveryPollID: providerOperationID,
	}
	replacement := newHarnessWithAdmission(t, replacementAdapter, first.admission, func() time.Time { return first.clock })
	response, err := replacement.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatalf("resume after engine restart failed: %v", err)
	}
	if response.Status != llm.ResponseStatusCompleted {
		t.Fatalf("response status = %q, want completed", response.Status)
	}
	counters.mu.Lock()
	submits, polls := counters.submits, counters.polls
	counters.mu.Unlock()
	if submits != 1 {
		t.Fatalf("Submit calls across engine restart = %d, want exactly one", submits)
	}
	if polls != 2 {
		t.Fatalf("Poll calls across engine restart = %d, want initial and resumed poll", polls)
	}
}

func TestGenerateFinalizesTerminalFirstPollOutcome(t *testing.T) {
	adapter := &resumableEngineAdapter{terminal: true}
	harness := newHarness(t, adapter)
	request := baseRequest("resumable-terminal-poll")
	if _, err := harness.engine.Generate(context.Background(), request); err == nil {
		t.Fatal("terminal poll unexpectedly completed")
	}
	operation, err := harness.admission.Get(context.Background(), operationIDForTest(t, request))
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != admission.StateAmbiguous {
		t.Fatalf("operation state after terminal poll = %q, want ambiguous", operation.State)
	}
}

func TestGenerateRecoversAcceptedSubmitFailureByIdempotencyKey(t *testing.T) {
	completed := successfulResponse()
	completed.OperationKey = "resumable-submit-recovery"
	adapter := &idempotencyRecoveryAdapter{resumableEngineAdapter: &resumableEngineAdapter{
		submitErr:      provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAccepted, provider.RetryNever, "submit response was lost"),
		pollResponses:  []provider.ResumableResult{{State: provider.ResumableCompleted, ProviderOperationID: "recovered-provider-operation", Dispatch: provider.DispatchAccepted, Result: provider.Result{Response: completed}}},
		recoveryPollID: "recovered-provider-operation",
	}, recovered: provider.ResumableResult{State: provider.ResumablePending, ProviderOperationID: "recovered-provider-operation", Dispatch: provider.DispatchAccepted}}
	harness := newHarness(t, adapter)
	response, err := harness.engine.Generate(context.Background(), baseRequest("resumable-submit-recovery"))
	if err != nil {
		t.Fatalf("recovered submit failed: %v", err)
	}
	if response.Status != llm.ResponseStatusCompleted {
		t.Fatalf("response status = %q, want completed", response.Status)
	}
	adapter.mu.Lock()
	submits, recoveries, polls := adapter.submits, adapter.recoveries, adapter.polls
	adapter.mu.Unlock()
	if submits != 1 || recoveries != 1 || polls != 1 {
		t.Fatalf("submit/recovery/poll calls = %d/%d/%d, want 1/1/1", submits, recoveries, polls)
	}
}

func TestGenerateNotFoundRecoveryRemainsAmbiguous(t *testing.T) {
	adapter := &idempotencyRecoveryAdapter{resumableEngineAdapter: &resumableEngineAdapter{
		submitErr: provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "submit response was lost"),
	}, recovered: provider.ResumableResult{State: provider.ResumableNotFound, Dispatch: provider.DispatchAmbiguous}}
	harness := newHarness(t, adapter)
	_, err := harness.engine.Generate(context.Background(), baseRequest("resumable-submit-not-found"))
	if err == nil {
		t.Fatal("not-found recovery unexpectedly completed")
	}
	operation, getErr := harness.admission.Get(context.Background(), operationIDForTest(t, baseRequest("resumable-submit-not-found")))
	if getErr != nil {
		t.Fatal(getErr)
	}
	if operation.State != admission.StateAmbiguous {
		t.Fatalf("operation state = %q, want ambiguous", operation.State)
	}
	adapter.mu.Lock()
	submits, recoveries, polls := adapter.submits, adapter.recoveries, adapter.polls
	adapter.mu.Unlock()
	if submits != 1 || recoveries != 1 || polls != 0 {
		t.Fatalf("submit/recovery/poll calls = %d/%d/%d, want 1/1/0", submits, recoveries, polls)
	}
}

func TestGenerateAcceptedSubmitFailureWithoutRecoveryRemainsAmbiguous(t *testing.T) {
	adapter := &resumableEngineAdapter{submitErr: provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAccepted, provider.RetryNever, "submit response was lost")}
	harness := newHarness(t, adapter)
	request := baseRequest("resumable-submit-no-recovery")
	if _, err := harness.engine.Generate(context.Background(), request); err == nil {
		t.Fatal("unsupported recovery unexpectedly completed")
	}
	operation, err := harness.admission.Get(context.Background(), operationIDForTest(t, request))
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != admission.StateAmbiguous {
		t.Fatalf("operation state = %q, want ambiguous", operation.State)
	}
	adapter.mu.Lock()
	submits := adapter.submits
	adapter.mu.Unlock()
	if submits != 1 {
		t.Fatalf("submit calls = %d, want one with no recovery or resubmission", submits)
	}
}

func TestGenerateRecoversDispatchingOperationWithoutResubmitting(t *testing.T) {
	request := baseRequest("resumable-dispatching-restart")
	recoveredResponse := successfulResponse()
	recoveredResponse.OperationKey = request.OperationKey
	adapter := &idempotencyRecoveryAdapter{resumableEngineAdapter: &resumableEngineAdapter{}, recovered: provider.ResumableResult{
		State:               provider.ResumableCompleted,
		ProviderOperationID: "recovered-after-restart",
		Dispatch:            provider.DispatchAccepted,
		Result:              provider.Result{Response: recoveredResponse},
	}}
	harness := newHarness(t, adapter)
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := llm.RequestDigest(normalized)
	if err != nil {
		t.Fatal(err)
	}
	operationID, scopeKey := operationIdentity(normalized, digest)
	started, err := harness.admission.Begin(context.Background(), admission.BeginRequest{
		ID: operationID, ScopeKey: scopeKey, RequestDigest: digest,
		ExpiresAt: harness.clock.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("seed operation: %v", err)
	}
	if err := harness.admission.MarkDispatching(context.Background(), admission.DispatchRequest{
		OperationID: started.Operation.ID, DispatchToken: started.Operation.DispatchToken,
		Attempt:    admission.AttemptFacts{RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1", ServiceClass: "standard", Dispatch: admission.Accepted},
		LeaseUntil: harness.clock.Add(time.Minute),
	}); err != nil {
		t.Fatalf("seed dispatching operation: %v", err)
	}
	response, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatalf("dispatching recovery failed: %v", err)
	}
	if response.Status != llm.ResponseStatusCompleted {
		t.Fatalf("response status = %q, want completed", response.Status)
	}
	adapter.mu.Lock()
	submits, recoveries := adapter.submits, adapter.recoveries
	adapter.mu.Unlock()
	if submits != 0 {
		t.Fatalf("Submit calls after dispatching restart = %d, want zero", submits)
	}
	if recoveries != 1 {
		t.Fatalf("idempotency recovery calls = %d, want one", recoveries)
	}
}

func TestGenerateFailsClosedForDispatchingAdapterWithoutRecovery(t *testing.T) {
	request := baseRequest("resumable-dispatching-no-recovery")
	adapter := &resumableEngineAdapter{}
	harness := newHarness(t, adapter)
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := llm.RequestDigest(normalized)
	if err != nil {
		t.Fatal(err)
	}
	operationID, scopeKey := operationIdentity(normalized, digest)
	started, err := harness.admission.Begin(context.Background(), admission.BeginRequest{
		ID: operationID, ScopeKey: scopeKey, RequestDigest: digest,
		ExpiresAt: harness.clock.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("seed operation: %v", err)
	}
	if err := harness.admission.MarkDispatching(context.Background(), admission.DispatchRequest{
		OperationID: started.Operation.ID, DispatchToken: started.Operation.DispatchToken,
		Attempt:    admission.AttemptFacts{RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1", ServiceClass: "standard", Dispatch: admission.Accepted},
		LeaseUntil: harness.clock.Add(time.Minute),
	}); err != nil {
		t.Fatalf("seed dispatching operation: %v", err)
	}
	if _, err := harness.engine.Generate(context.Background(), request); err == nil {
		t.Fatal("dispatching operation without recovery unexpectedly completed")
	}
	operation, err := harness.admission.Get(context.Background(), operationID)
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != admission.StateAmbiguous {
		t.Fatalf("operation state = %q, want ambiguous", operation.State)
	}
	adapter.mu.Lock()
	submits := adapter.submits
	adapter.mu.Unlock()
	if submits != 0 {
		t.Fatalf("Submit calls without recovery = %d, want zero", submits)
	}
}

// operationIDForTest mirrors the engine's public operation identity inputs
// without exposing the internal hash helper to the fixture package.
func operationIDForTest(t *testing.T, request llm.Request) string {
	t.Helper()
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := llm.RequestDigest(normalized)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := operationIdentity(normalized, digest)
	return id
}
