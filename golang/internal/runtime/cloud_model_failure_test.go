package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

func TestCloudModelProcessingFailureRetainsPaidClaim(t *testing.T) {
	ctx := context.Background()
	f := boundedCloud(t, false)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := observer.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetrySameOperation, "provider model processing failed")
	}
	result, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, result, err, llm.ExecutionPending)
	ref := llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context}
	scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
	first, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(result.RequestID))
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil || original.Execution.Settled || original.Execution.Claim == nil {
		t.Fatalf("new budget acquisition refunded unknown work: %v", err)
	}
	if f.submits.Load() != 1 {
		t.Fatal("budget acquisition resubmitted provider work")
	}
}
