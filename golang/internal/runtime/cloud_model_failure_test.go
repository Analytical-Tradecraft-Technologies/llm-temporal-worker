package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestCloudModelProcessingFailureRetainsPaidClaim(t *testing.T) {
	ctx := context.Background()
	f := boundedCloud(t, false)
	f.adapter.invoke = invokeFailingAfterWrite(&f.submits, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetrySameOperation, "provider model processing failed"))
	result, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, result, err, llm.ExecutionPending)
	ref, scope, first := f.firstAttempt(t, ctx, result)
	saved, err := f.repository.LoadProviderExecution(ctx, scope, first.ID)
	if err != nil || saved.Execution.Claim == nil || saved.Execution.Settled {
		t.Fatalf("model failure lost its paid claim: %v", err)
	}
	f.now = f.now.Add(16 * time.Minute)
	result, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, result, err, llm.ExecutionAcquired)
	next, err := f.repository.LoadRequestAttempt(ctx, scope, first.RootID)
	if err != nil || next.ID == first.ID {
		t.Fatalf("retry reused paid attempt: %v", err)
	}
	original, err := f.repository.LoadProviderExecution(ctx, scope, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertUnknownWorkCharged(t, original)
	if f.submits.Load() != 1 {
		t.Fatal("budget acquisition resubmitted provider work")
	}
}
