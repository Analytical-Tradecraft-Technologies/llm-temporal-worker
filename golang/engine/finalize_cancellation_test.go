package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// cancelOnSuccessAdapter cancels the caller's context just before handing
// back a valid provider result, as a Temporal cancellation landing at that
// instant would.
type cancelOnSuccessAdapter struct {
	*fakeAdapter
	cancel context.CancelFunc
}

func (adapter *cancelOnSuccessAdapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	result, err := adapter.fakeAdapter.Invoke(ctx, call, observer)
	adapter.cancel()
	return result, err
}

// contextCheckingHeartbeat fails on a cancelled context, as the Temporal
// Activity heartbeater does.
type contextCheckingHeartbeat struct{}

func (contextCheckingHeartbeat) Beat(ctx context.Context, _ Progress) error { return ctx.Err() }

func TestGenerateStoresPaidResultWhenCancelledBeforeFinalization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inner := &fakeAdapter{name: "cancel-on-success", response: successfulResponse()}
	harness := newHarness(t, &cancelOnSuccessAdapter{fakeAdapter: inner, cancel: cancel})

	response, err := harness.engine.Generate(WithHeartbeat(ctx, contextCheckingHeartbeat{}), baseRequest("cancel-on-success"))
	if err != nil {
		t.Fatalf("Generate() error = %v, want the paid result to be finalized", err)
	}
	if harness.results.puts != 1 {
		t.Fatalf("result writes = %d, want 1", harness.results.puts)
	}
	replay, err := harness.engine.Generate(WithHeartbeat(context.Background(), contextCheckingHeartbeat{}), baseRequest("cancel-on-success"))
	if err != nil {
		t.Fatalf("retry error = %v, want the stored result", err)
	}
	if replay.OperationID != response.OperationID || inner.invokes != 1 {
		t.Fatalf("retry operation = %q (first %q), provider invokes = %d; want a replay without a second call", replay.OperationID, response.OperationID, inner.invokes)
	}
}

// cancelBeforeCallbacksAdapter cancels the caller's context after the
// provider has answered but before the adapter runs its post-response
// observer callbacks, as a one-shot SDK that returns before the adapter
// reports headers and progress does.
type cancelBeforeCallbacksAdapter struct {
	*fakeAdapter
	cancel context.CancelFunc
}

func (adapter *cancelBeforeCallbacksAdapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	adapter.mu.Lock()
	adapter.invokes++
	response := adapter.response
	adapter.mu.Unlock()
	if err := observer.BeforePossibleWrite(ctx); err != nil {
		return provider.Result{}, err
	}
	adapter.cancel()
	if err := observer.AfterResponseHeaders(ctx, provider.ResponseMetadata{}); err != nil {
		return provider.Result{}, err
	}
	observer.OnProgress(ctx, provider.Progress{Phase: string(provider.PhaseLift), OutputItems: len(response.Output)})
	return provider.Result{Response: response}, nil
}

func TestGenerateStoresPaidResultWhenCancelledBeforeResponseCallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inner := &fakeAdapter{name: "cancel-before-callbacks", response: successfulResponse()}
	harness := newHarness(t, &cancelBeforeCallbacksAdapter{fakeAdapter: inner, cancel: cancel})
	if _, err := harness.engine.Generate(WithHeartbeat(ctx, contextCheckingHeartbeat{}), baseRequest("cancel-before-callbacks")); err != nil {
		t.Fatalf("Generate() error = %v, want the paid result to be finalized", err)
	}
	if harness.results.puts != 1 {
		t.Fatalf("result writes = %d, want 1", harness.results.puts)
	}
}

// blockingHeartbeat blocks until its context ends, as a heartbeat stuck in
// its transport would.
type blockingHeartbeat struct{}

func (blockingHeartbeat) Beat(ctx context.Context, _ Progress) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestShieldedHeartbeatIsBoundedByTheFinalizationTimeout(t *testing.T) {
	harness := newHarness(t, &fakeAdapter{name: "bounded-heartbeat", response: successfulResponse()})
	harness.engine.dependencies.FinalizationTimeout = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(WithHeartbeat(context.Background(), blockingHeartbeat{}))
	cancel()
	done := make(chan error, 1)
	go func() { done <- harness.engine.shieldedBeat(ctx, Progress{Phase: "finalization"}) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked heartbeat reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("shielded heartbeat ignored the finalization timeout")
	}
}
