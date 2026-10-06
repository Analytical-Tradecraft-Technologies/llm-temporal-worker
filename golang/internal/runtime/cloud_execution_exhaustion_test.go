package runtime

import (
	"context"
	"errors"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"testing"
	"time"
)

func TestCloudExecutionExhaustionRetainsUnknownPaidWork(t *testing.T) {
	f := boundedCloud(t, false)
	f.options.MaxAttempts = 2
	f.restart(t)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, errors.New("lost paid response")
	}
	ctx := context.Background()
	scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
	var ref llm.ExecutionReferenceV1
	var attempts []cloudstate.RequestAttempt
	for i := 0; i < 2; i++ {
		result, err := f.runtime.GenerateStepV1(ctx, f.request)
		boundedState(t, result, err, llm.ExecutionPending)
		ref = llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context}
		attempt, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(result.RequestID))
		if err != nil {
			t.Fatal(err)
		}
		attempts = append(attempts, attempt)
		f.now = f.now.Add(16 * time.Minute)
		f.restart(t)
		result, err = f.runtime.AcquireBudgetV1(ctx, ref)
		if i == 0 {
			boundedState(t, result, err, llm.ExecutionAcquired)
		} else {
			boundedState(t, result, err, llm.ExecutionFailed)
			if result.Retryable {
				t.Fatal("exhausted request remains retryable")
			}
		}
	}
	f.restart(t)
	for i := 0; i < 3; i++ {
		result, err := f.runtime.AcquireBudgetV1(ctx, ref)
		boundedState(t, result, err, llm.ExecutionFailed)
	}
	if f.submits.Load() != 2 {
		t.Fatal("attempt limit exceeded", f.submits.Load())
	}
	for _, attempt := range attempts {
		saved, err := f.repository.LoadProviderExecution(ctx, scope, attempt.ID)
		if err != nil {
			t.Fatal(err)
		}
		assertUnknownWorkCharged(t, saved)
		shard, _ := cloudstate.PendingShard(attempt.ID)
		page, err := f.repository.ListPending(ctx, shard, 100, "")
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, entry := range page.Requests {
			if entry.ID == attempt.ID {
				found = true
			}
		}
		if !found {
			t.Fatal("unknown paid work removed from recovery")
		}
	}
}

func TestCloudExecutionExhaustionAfterRetryableRejection(t *testing.T) {
	f := boundedCloud(t, false)
	f.options.MaxAttempts = 1
	f.restart(t)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "unavailable")
	}
	ctx := context.Background()
	result, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, result, err, llm.ExecutionFailed)
	if !result.Retryable {
		t.Fatal("fixture rejection not retryable")
	}
	f.now = f.now.Add(time.Minute)
	ref := llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context}
	result, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, result, err, llm.ExecutionFailed)
	if result.Retryable {
		t.Fatal("limit ignored")
	}
	// The terminal failure keeps the last attempt's facts (#1001).
	wantDetails := func(result llm.ExecutionResultV1) {
		t.Helper()
		if result.FailureCode != "provider_rejected" || result.ErrorCode != string(provider.CodeProviderUnavailable) || result.Dispatch != string(provider.DispatchRejected) {
			t.Fatalf("exhausted failure = %+v, want the last attempt's code and dispatch", result)
		}
	}
	wantDetails(result)
	f.restart(t)
	result, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, result, err, llm.ExecutionFailed)
	wantDetails(result)
	if f.submits.Load() != 1 {
		t.Fatal("terminal request submitted again")
	}
}
