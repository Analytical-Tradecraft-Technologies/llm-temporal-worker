package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestCloudModelProcessingFailureRetainsPaidClaim(t *testing.T) {
	ctx := context.Background()
	f := boundedCloud(t, false)
	f.adapter.invoke = invokeFailingAfterWrite(&f.submits, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetrySameOperation, "provider model processing failed"))
	result := f.step(t, ctx, f.request, llm.ExecutionPending)
	ref, scope, first := f.firstAttempt(t, ctx, result)
	saved, err := f.repository.LoadProviderExecution(ctx, scope, first.ID)
	if err != nil || saved.Execution.Claim == nil || saved.Execution.Settled {
		t.Fatalf("model failure lost its paid claim: %v", err)
	}
	f.now = f.now.Add(16 * time.Minute)
	f.acquire(t, ctx, ref, llm.ExecutionAcquired)
	next, err := f.repository.LoadRequestAttempt(ctx, scope, first.RootID)
	if err != nil || next.ID == first.ID {
		t.Fatalf("retry reused paid attempt: %v", err)
	}
	original, err := f.repository.LoadProviderExecution(ctx, scope, first.ID)
	testassert.NoError(t, err)
	assertUnknownWorkCharged(t, original)
	requireCount(t, &f.submits, 1, "budget acquisition resubmitted provider work")
}
