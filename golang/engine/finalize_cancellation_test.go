package engine

import (
	"context"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
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
