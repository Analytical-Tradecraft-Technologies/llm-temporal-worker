package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

type advanceBeforeFailureStore struct {
	cloudExecutionStore
	advance    func()
	conflicted bool
}

func (s *advanceBeforeFailureStore) FinishRequestFailure(ctx context.Context, scope cloudstate.Scope, root, attempt cloudstate.RequestID, failure llm.ExecutionResultV1, now time.Time) error {
	if s.advance != nil {
		advance := s.advance
		s.advance = nil
		advance()
	}
	err := s.cloudExecutionStore.FinishRequestFailure(ctx, scope, root, attempt, failure, now)
	s.conflicted = s.conflicted || errors.Is(err, contracts.ErrConflict)
	return err
}

func TestCloudConcurrentFailureFinalization(t *testing.T) {
	for _, step := range []string{"acquire", "generate", "poll", "complete"} {
		t.Run(step, func(t *testing.T) {
			f := boundedCloud(t, false)
			f.request.Cache = &llm.CachePolicyV1{}
			f.adapter.invoke = func(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
				f.submits.Add(1)
				failure := provider.NewError(provider.CodeProviderRateLimited, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryAfter, "private")
				failure.RetryAfter = time.Second
				return provider.Result{}, failure
			}
			ctx := context.Background()
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
			scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
			f.now = f.now.Add(time.Second)
			// Force another acquisition to advance the root between this caller's
			// load of the failed attempt and its idempotent failure finalization.
			other := *f.runtime
			var winner cloudstate.RequestAttempt
			store := &advanceBeforeFailureStore{cloudExecutionStore: f.runtime.store}
			store.advance = func() {
				v, err := other.AcquireBudgetV1(ctx, ref)
				boundedState(t, v, err, llm.ExecutionAcquired)
				winner, err = f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(ref.RequestID))
				if err != nil || winner.Number != 2 {
					t.Fatal("competing acquisition did not advance the attempt", err)
				}
			}
			f.runtime.store = store
			switch step {
			case "acquire":
				v, err = f.runtime.AcquireBudgetV1(ctx, ref)
			case "generate":
				v, err = f.runtime.GenerateStepV1(ctx, f.request)
			case "poll":
				v, err = f.runtime.PollExecutionV1(ctx, ref)
			case "complete":
				v, err = f.runtime.CompleteExecutionV1(ctx, ref)
			}
			if step == "acquire" {
				boundedState(t, v, err, llm.ExecutionAcquired)
			} else {
				var mapped *provider.Error
				if !errors.As(err, &mapped) || mapped.Code != provider.CodeOperationConflict || mapped.Retry != provider.RetryNever {
					t.Fatal("non-acquisition step followed the new attempt", v, err)
				}
			}
			current, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(ref.RequestID))
			if err != nil || current.ID != winner.ID || !store.conflicted || f.submits.Load() != 1 {
				t.Fatal("stale acquisition did not reuse the winner without dispatch", current, err, store.conflicted, f.submits.Load())
			}
		})
	}
}

func TestCloudFailureRetriesUseSettledIndependentAttempts(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(map[bool]string{false: "rejected", true: "failed-job"}[async], func(t *testing.T) {
			f := boundedCloud(t, async)
			f.request.Cache = &llm.CachePolicyV1{}
			original := f.adapter.invoke
			failure := provider.NewError(provider.CodeProviderRateLimited, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryAfter, "private provider message")
			failure.RetryAfter = 10 * time.Second
			if async {
				f.adapter.poll = func(ctx context.Context, call provider.Call, id string, o provider.Observer) (provider.ResumableResult, error) {
					f.polls.Add(1)
					return provider.ResumableResult{State: provider.ResumableFailed, Dispatch: provider.DispatchAccepted, ProviderOperationID: id, Failure: failure}, nil
				}
			} else {
				f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
					f.submits.Add(1)
					return provider.Result{}, failure
				}
			}
			ctx := context.Background()
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
			if async {
				boundedState(t, v, err, llm.ExecutionPending)
				f.now = f.now.Add(2 * time.Second)
				v, err = f.runtime.PollExecutionV1(ctx, ref)
			}
			boundedState(t, v, err, llm.ExecutionFailed)
			if !v.Retryable {
				t.Fatal("lost retry classification")
			}
			scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
			first, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
			if err != nil {
				t.Fatal(err)
			}
			saved, err := f.repository.LoadProviderExecution(ctx, scope, first.ID)
			if err != nil || !saved.Execution.Settled || !saved.Execution.Failure.Retryable {
				t.Fatal("retry exposed before settlement", err)
			}
			assertCloudStatus(t, f, first.ID, cloudstate.StatusFailed)
			assertCloudStatus(t, f, first.RootID, cloudstate.StatusRunning)
			f.restart(t)
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			if f.submits.Load() != 1 {
				t.Fatal("activity retry submitted")
			}
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionBudgetWait)
			if v.RetryAfterSeconds != 10 {
				t.Fatal("provider delay discarded", v)
			}
			f.now = f.now.Add(10 * time.Second)
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					result, err := f.runtime.AcquireBudgetV1(ctx, ref)
					if err != nil || result.State != llm.ExecutionAcquired {
						t.Errorf("concurrent acquisition: %+v %v", result, err)
					}
				}()
			}
			wg.Wait()
			second, err := f.repository.LoadRequestAttempt(ctx, scope, first.RootID)
			if err != nil || second.ID == first.ID || second.Number != 2 {
				t.Fatal("failed attempt was reused", err)
			}
			// A stale completion must not fail or close the now-current second attempt.
			stale := llm.ExecutionResultV1{RequestID: string(first.RootID), Kind: "generate", State: llm.ExecutionFailed, FailureCode: "provider_error", Retryable: true}
			if err := f.repository.FinishRequestFailure(ctx, scope, first.RootID, first.ID, stale, f.now); err == nil {
				t.Fatal("stale child modified root")
			}
			f.adapter.invoke = original
			f.adapter.poll = func(ctx context.Context, call provider.Call, id string, o provider.Observer) (provider.ResumableResult, error) {
				f.polls.Add(1)
				v := executionResponse(call)
				v.ProviderOperationID = id
				return v, nil
			}
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			if async {
				boundedState(t, v, err, llm.ExecutionPending)
				f.now = f.now.Add(2 * time.Second)
				v, err = f.runtime.PollExecutionV1(ctx, ref)
			}
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			v, err = f.runtime.CompleteExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionCompleted)
			paid, err := f.repository.LoadProviderExecution(ctx, scope, second.ID)
			if err != nil || !paid.Execution.Settled || paid.Execution.Reservation.OperationID == saved.Execution.Reservation.OperationID {
				t.Fatal("retry reused reservation", err)
			}
			assertCloudStatus(t, f, first.ID, cloudstate.StatusFailed)
			assertCloudStatus(t, f, first.RootID, cloudstate.StatusCompleted)
			if f.submits.Load() != 2 {
				t.Fatal("wrong paid submission count")
			}
		})
	}
}

func assertCloudStatus(t *testing.T, f *boundedCloudFixture, id cloudstate.RequestID, want cloudstate.Status) {
	t.Helper()
	ctx := context.Background()
	record, err := f.repository.Read(ctx, cloudstate.Scope{Tenant: "tenant", Project: "project"}, id)
	if err != nil || record.Status != want {
		t.Fatal("unexpected durable status", record.Status, want, err)
	}
	if want == cloudstate.StatusFailed || want == cloudstate.StatusCompleted {
		shard, _ := cloudstate.PendingShard(id)
		page, err := f.repository.ListPending(ctx, shard, 100, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, pending := range page.Requests {
			if pending.ID == id {
				t.Fatal("terminal record still pending")
			}
		}
	}
}

func TestCloudPermanentFailureReplaysWithoutParentOrProvider(t *testing.T) {
	f := boundedCloud(t, false)
	ctx := context.Background()
	f.adapter.invoke = func(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		return provider.Result{}, provider.NewError(provider.CodePermissionDenied, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNever, "private")
	}
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
	first, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	assertCloudStatus(t, f, first.ID, cloudstate.StatusFailed)
	assertCloudStatus(t, f, first.RootID, cloudstate.StatusFailed)
	f.now = f.now.Add(48 * time.Hour)
	f.restart(t)
	for _, run := range []func() (llm.ExecutionResultV1, error){
		func() (llm.ExecutionResultV1, error) {
			return f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
		},
		func() (llm.ExecutionResultV1, error) { return f.runtime.AcquireBudgetV1(ctx, ref) },
		func() (llm.ExecutionResultV1, error) { return f.runtime.GenerateStepV1(ctx, f.request) },
		func() (llm.ExecutionResultV1, error) { return f.runtime.PollExecutionV1(ctx, ref) },
		func() (llm.ExecutionResultV1, error) { return f.runtime.CompleteExecutionV1(ctx, ref) },
	} {
		result, err := run()
		boundedState(t, result, err, llm.ExecutionFailed)
		if result.Retryable || result.FailureCode != "provider_error" {
			t.Fatal("failure changed")
		}
	}
	if f.submits.Load() != 1 {
		t.Fatal("permanent failure resubmitted")
	}
	denied := f.options
	denied.ResolveScope = func(context.Context, llm.RequestContext) (string, error) { return "", errors.New("denied") }
	r, err := f.cap.NewCloudExecutionRuntime(ctx, denied)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.AcquireBudgetV1(ctx, ref); err == nil {
		t.Fatal("failed result bypassed authorization")
	}
}
