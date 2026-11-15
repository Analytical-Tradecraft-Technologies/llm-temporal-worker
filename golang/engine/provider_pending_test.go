package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

type resumableTestAdapter struct {
	mu        sync.Mutex
	responses []provider.ResumableResult
	polls     int
	submits   int
	ids       []string
}

func (adapter *resumableTestAdapter) Name() string { return "resumable-test" }
func (adapter *resumableTestAdapter) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	return provider.CapabilitySet{}, nil
}
func (adapter *resumableTestAdapter) Compile(context.Context, provider.CompileInput) (provider.Call, error) {
	return provider.Call{OperationKey: "operation-key"}, nil
}
func (adapter *resumableTestAdapter) Invoke(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
	adapter.countSubmit()
	return provider.Result{}, nil
}
func (adapter *resumableTestAdapter) Submit(context.Context, provider.Call, provider.Observer) (provider.ResumableResult, error) {
	adapter.countSubmit()
	return provider.ResumableResult{}, nil
}

// countSubmit records a write-capable call; polling must never make one.
func (adapter *resumableTestAdapter) countSubmit() {
	adapter.mu.Lock()
	adapter.submits++
	adapter.mu.Unlock()
}
func (adapter *resumableTestAdapter) Poll(_ context.Context, _ provider.Call, id string, _ provider.Observer) (provider.ResumableResult, error) {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	adapter.polls++
	adapter.ids = append(adapter.ids, id)
	result := adapter.responses[adapter.polls-1]
	return result, nil
}
func (adapter *resumableTestAdapter) RecoverByIdempotencyKey(context.Context, provider.Call, provider.Observer) (provider.ResumableResult, error) {
	return provider.ResumableResult{State: provider.ResumableNotFound, Dispatch: provider.DispatchAmbiguous}, nil
}

var _ provider.ResumableAdapter = (*resumableTestAdapter)(nil)

// completedProviderOp is a completed poll of provider-op for operation-key.
// pendingProviderOp is an accepted, still-pending provider-op poll result.
func pendingProviderOp(nextPollAfter time.Duration) provider.ResumableResult {
	return provider.ResumableResult{State: provider.ResumablePending, ProviderOperationID: "provider-op", NextPollAfter: nextPollAfter, Dispatch: provider.DispatchAccepted}
}

func completedProviderOp() provider.ResumableResult {
	return provider.ResumableResult{
		State: provider.ResumableCompleted, ProviderOperationID: "provider-op", Dispatch: provider.DispatchAccepted,
		Result: provider.Result{Response: llm.Response{OperationKey: "operation-key", Status: llm.ResponseStatusCompleted}},
	}
}

// recordSleeps returns a poll sleeper that appends each delay to slept.
func recordSleeps(slept *[]time.Duration) func(context.Context, time.Duration) error {
	return func(_ context.Context, delay time.Duration) error {
		*slept = append(*slept, delay)
		return nil
	}
}

// pollOnceError polls provider-op once for call, requires an error, and
// returns the *provider.Error it is (nil when it is another type) and the raw
// error.
func pollOnceError(t *testing.T, ctx context.Context, adapter *resumableTestAdapter, call provider.Call, unexpectedSuccess string) (*provider.Error, error) {
	t.Helper()
	_, err := PollProviderOperation(ctx, adapter, call, "provider-op", nil, ProviderPollOptions{MaxPolls: 1})
	if err == nil {
		t.Fatal(unexpectedSuccess)
	}
	mapped, _ := err.(*provider.Error)
	return mapped, err
}

type pollProgressObserver struct {
	phases []string
}

func (observer *pollProgressObserver) BeforePossibleWrite(context.Context) error { return nil }
func (observer *pollProgressObserver) AfterResponseHeaders(context.Context, provider.ResponseMetadata) error {
	return nil
}
func (observer *pollProgressObserver) OnProgress(_ context.Context, progress provider.Progress) {
	observer.phases = append(observer.phases, progress.Phase)
}

func TestPollProviderOperationNeverSubmitsAndResumesPendingID(t *testing.T) {
	adapter := &resumableTestAdapter{responses: []provider.ResumableResult{
		pendingProviderOp(5 * time.Second),
		completedProviderOp(),
	}}
	observer := &pollProgressObserver{}
	var slept []time.Duration
	result, err := PollProviderOperation(context.Background(), adapter, provider.Call{OperationKey: "operation-key"}, "provider-op", observer, ProviderPollOptions{
		MaxPolls: 2,
		Sleep:    recordSleeps(&slept),
	})
	testassert.NoError(t, err)
	if result.Response.Status != llm.ResponseStatusCompleted {
		t.Fatalf("response status = %q, want completed", result.Response.Status)
	}
	if adapter.submits != 0 {
		t.Fatalf("submit calls = %d, want zero when resuming", adapter.submits)
	}
	if adapter.polls != 2 || len(adapter.ids) != 2 || adapter.ids[0] != "provider-op" || adapter.ids[1] != "provider-op" {
		t.Fatalf("poll calls/ids = %d/%v, want two polls of the persisted id", adapter.polls, adapter.ids)
	}
	if len(slept) != 1 || slept[0] != 5*time.Second {
		t.Fatalf("sleep durations = %v, want provider guidance", slept)
	}
	if len(observer.phases) != 1 || observer.phases[0] != string(provider.PhasePoll) {
		t.Fatalf("heartbeat phases = %v, want poll without provider id", observer.phases)
	}
}

func TestPollProviderOperationHonorsInitialProviderGuidance(t *testing.T) {
	adapter := &resumableTestAdapter{responses: []provider.ResumableResult{completedProviderOp()}}
	var slept []time.Duration
	result, err := PollProviderOperation(context.Background(), adapter, provider.Call{OperationKey: "operation-key"}, "provider-op", nil, ProviderPollOptions{
		MaxPolls:     1,
		InitialDelay: 5 * time.Second,
		Sleep:        recordSleeps(&slept),
	})
	testassert.NoError(t, err)
	if result.Response.Status != llm.ResponseStatusCompleted || adapter.polls != 1 {
		t.Fatalf("result/polls = %#v/%d, want completed/1", result.Response, adapter.polls)
	}
	if len(slept) != 1 || slept[0] != 5*time.Second {
		t.Fatalf("initial sleep = %v, want provider guidance", slept)
	}
}

func TestPollProviderOperationCapsInitialGuidance(t *testing.T) {
	adapter := &resumableTestAdapter{responses: []provider.ResumableResult{completedProviderOp()}}
	var slept []time.Duration
	testassert.NoError(t, testassert.Err(PollProviderOperation(context.Background(), adapter, provider.Call{OperationKey: "operation-key"}, "provider-op", nil, ProviderPollOptions{
		MaxPolls:        1,
		MaxPollInterval: time.Second,
		InitialDelay:    5 * time.Second,
		Sleep:           recordSleeps(&slept),
	})))
	if len(slept) != 1 || slept[0] != time.Second {
		t.Fatalf("initial sleep = %v, want capped guidance", slept)
	}
}

func TestPollProviderOperationCapsGuidanceAndLeavesPendingRetryable(t *testing.T) {
	adapter := &resumableTestAdapter{responses: []provider.ResumableResult{
		pendingProviderOp(10 * time.Second),
		pendingProviderOp(10 * time.Second),
	}}
	var slept []time.Duration
	_, err := PollProviderOperation(context.Background(), adapter, provider.Call{}, "provider-op", nil, ProviderPollOptions{
		MaxPolls:        2,
		MaxPollInterval: time.Second,
		Sleep:           recordSleeps(&slept),
	})
	if err == nil {
		t.Fatal("polling unexpectedly completed")
	}
	mapped, ok := err.(*provider.Error)
	if !ok {
		t.Fatalf("error type = %T, want provider error", err)
	}
	if mapped.Code != provider.CodeDeadlineExceeded || mapped.Retry != provider.RetrySameOperation || mapped.Dispatch != provider.DispatchAccepted {
		t.Fatalf("error = %#v, want retryable accepted deadline", mapped)
	}
	if len(slept) != 1 || slept[0] != time.Second {
		t.Fatalf("sleep durations = %v, want one capped interval", slept)
	}
}

func TestPollProviderOperationRejectsChangedProviderIdentity(t *testing.T) {
	adapter := &resumableTestAdapter{responses: []provider.ResumableResult{{State: provider.ResumablePending, ProviderOperationID: "other-op", Dispatch: provider.DispatchAccepted}}}
	mapped, err := pollOnceError(t, context.Background(), adapter, provider.Call{}, "changed provider operation identity was accepted")
	if mapped == nil || mapped.Code != provider.CodeStateCorrupt || mapped.Dispatch != provider.DispatchAmbiguous {
		t.Fatalf("error = %#v, want ambiguous state corruption", err)
	}
}

func TestPollProviderOperationRejectsMalformedPendingResult(t *testing.T) {
	adapter := &resumableTestAdapter{responses: []provider.ResumableResult{{
		State:    provider.ResumablePending,
		Dispatch: provider.DispatchAccepted,
	}}}
	mapped, err := pollOnceError(t, context.Background(), adapter, provider.Call{}, "pending result without a provider operation id was accepted")
	if mapped == nil || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("error = %#v, want ambiguous non-retryable invalid response", err)
	}
	if adapter.submits != 0 || adapter.polls != 1 {
		t.Fatalf("provider calls = submits:%d polls:%d, want no submit and one poll", adapter.submits, adapter.polls)
	}
}

func TestPollProviderOperationRejectsChangedTerminalIdentity(t *testing.T) {
	cases := []provider.ResumableState{provider.ResumableCompleted, provider.ResumableFailed}
	for _, state := range cases {
		t.Run(string(state), func(t *testing.T) {
			response := provider.ResumableResult{State: state, ProviderOperationID: "other-op", Dispatch: provider.DispatchAccepted}
			if state == provider.ResumableCompleted {
				response.Result = provider.Result{Response: llm.Response{OperationKey: "operation-key", Status: llm.ResponseStatusCompleted}}
			} else {
				response.Failure = provider.NewError(provider.CodeProviderUnavailable, provider.PhasePoll, provider.DispatchAccepted, provider.RetryNever, "provider operation failed")
			}
			adapter := &resumableTestAdapter{responses: []provider.ResumableResult{response}}
			mapped, err := pollOnceError(t, context.Background(), adapter, provider.Call{}, "changed terminal provider operation identity was accepted")
			if mapped == nil || mapped.Code != provider.CodeStateCorrupt || mapped.Dispatch != provider.DispatchAmbiguous {
				t.Fatalf("error = %#v, want ambiguous state corruption", err)
			}
		})
	}
}

func TestPollProviderOperationRejectsCompletedResponseForAnotherOperation(t *testing.T) {
	adapter := &resumableTestAdapter{responses: []provider.ResumableResult{{
		State:               provider.ResumableCompleted,
		ProviderOperationID: "provider-op",
		Dispatch:            provider.DispatchAccepted,
		Result: provider.Result{Response: llm.Response{
			OperationKey: "different-operation",
			Status:       llm.ResponseStatusCompleted,
		}},
	}}}
	mapped, err := pollOnceError(t, context.Background(), adapter, provider.Call{OperationKey: "requested-operation"}, "completed response for another operation was accepted")
	if mapped == nil || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Dispatch != provider.DispatchAmbiguous {
		t.Fatalf("error = %#v, want ambiguous invalid provider response", err)
	}
}

func TestPollProviderOperationPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	adapter := &resumableTestAdapter{responses: []provider.ResumableResult{pendingProviderOp(0)}}
	mapped, err := pollOnceError(t, ctx, adapter, provider.Call{}, "canceled polling unexpectedly completed")
	if mapped == nil || mapped.Code != provider.CodeCanceled || mapped.Retry != provider.RetryNever {
		t.Fatalf("error = %#v, want non-retryable canceled result", err)
	}
	if adapter.polls != 0 {
		t.Fatalf("poll calls = %d, want zero after cancellation", adapter.polls)
	}
}
