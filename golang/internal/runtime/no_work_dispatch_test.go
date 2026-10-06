package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// A zero-cost compaction makes no provider call, so a publication failure on
// it must not report dispatch=accepted (#1165).
func TestNoWorkCompactionPublicationFailureIsNotDispatched(t *testing.T) {
	f := compactionTriggerFixture(t, `{"recent_turns":100}`)
	f.turn(t, "turn-1", "hello")
	// Fail checkpoint publication itself: without a keyring it cannot issue
	// the child handle.
	f.runtime.publication.keyring = nil
	ctx := context.Background()
	request := llm.CompactRequestV1{OperationKey: "no-work", Context: f.request.Context, Parent: *f.request.Parent, Cache: &llm.CachePolicyV1{}}
	before := f.submits.Load()
	_, err := f.runtime.CompactStepV1(ctx, request)
	var failure *provider.Error
	if !errors.As(err, &failure) {
		t.Fatalf("compact = %v, want a publication failure", err)
	}
	if failure.Dispatch != provider.DispatchNotDispatched || f.submits.Load() != before {
		t.Fatalf("no-work publication failure dispatch = %s, submits %d -> %d", failure.Dispatch, before, f.submits.Load())
	}
}
