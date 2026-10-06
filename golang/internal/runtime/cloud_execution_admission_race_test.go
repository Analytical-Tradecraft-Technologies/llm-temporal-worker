package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

// Advance the durable request after a caller reads that execution is absent,
// or immediately before Reserve rechecks the plan. Both reads must remain
// fenced by storage; the runtime must resume the winner instead of admitting
// the stale caller again.
type admissionRaceStore struct {
	cloudExecutionStore
	afterMissing func()
	beforePlan   func(int)
	planReads    int
}

func (s *admissionRaceStore) LoadProviderExecution(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.SavedProviderExecution, error) {
	v, err := s.cloudExecutionStore.LoadProviderExecution(ctx, scope, id)
	if errors.Is(err, cloudstate.ErrProviderExecutionMissing) && s.afterMissing != nil {
		hook := s.afterMissing
		s.afterMissing = nil
		hook()
	}
	return v, err
}

func (s *admissionRaceStore) LoadBudgetPlan(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.BudgetPlan, error) {
	s.planReads++
	if s.beforePlan != nil {
		s.beforePlan(s.planReads)
	}
	return s.cloudExecutionStore.LoadBudgetPlan(ctx, scope, id)
}

func TestCloudExecutionAdmissionResumesConcurrentWinner(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		for _, mode := range []string{"sync", "async"} {
			for _, window := range []string{"prepare", "reserve"} {
				for _, step := range []cloudStep{cloudPrepare, cloudAcquire, cloudSubmit, cloudPoll, cloudComplete} {
					if window == "reserve" && step != cloudAcquire && step != cloudSubmit {
						continue
					}
					names := map[cloudStep]string{cloudPrepare: "prepare", cloudAcquire: "acquire", cloudSubmit: "submit", cloudPoll: "poll", cloudComplete: "complete"}
					t.Run(kind+"/"+mode+"/before-"+window+"/"+names[step], func(t *testing.T) {
						f := boundedCloud(t, mode == "async")
						f.request.Cache = &llm.CachePolicyV1{}
						ctx := context.Background()
						input := llm.PrepareExecutionV1{Generate: &f.request}
						if kind == "compact" {
							policy := json.RawMessage(`{"recent_turns":0}`)
							f.request.SettingsPatch.CompactionPolicy.Set = &policy
							parent := f.finish(t)
							f.now = f.now.Add(time.Minute)
							input = llm.PrepareExecutionV1{Compact: &llm.CompactRequestV1{OperationKey: "compact", Context: f.request.Context, Parent: parent.Generate.Checkpoint.Handle, Cache: &llm.CachePolicyV1{}}}
						}
						v, err := f.runtime.PrepareExecutionV1(ctx, input)
						boundedState(t, v, err, llm.ExecutionBudgetRequired)
						ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
						v, err = f.runtime.AcquireBudgetV1(ctx, ref)
						boundedState(t, v, err, llm.ExecutionAcquired)
						winner := f.runtime
						f.restart(t)
						before := f.submits.Load()
						hooks := 0
						startWinner := func() {
							hooks++
							v, err := winner.prepareStep(ctx, input, cloudSubmit)
							want := llm.ExecutionProviderCompleted
							if mode == "async" {
								want = llm.ExecutionPending
							}
							boundedState(t, v, err, want)
						}
						store := &admissionRaceStore{cloudExecutionStore: f.runtime.store}
						if window == "prepare" {
							store.afterMissing = startWinner
						} else {
							store.beforePlan = func(n int) {
								if n == 2 {
									startWinner()
								}
							}
						}
						f.runtime.store, f.runtime.execution.admission.store = store, store
						v, err = f.runtime.prepareStep(ctx, input, step)
						want := llm.ExecutionProviderCompleted
						if mode == "async" {
							want = llm.ExecutionPending
						} else if step == cloudComplete {
							want = llm.ExecutionCompleted
						}
						boundedState(t, v, err, want)
						if hooks != 1 || f.submits.Load() != before+1 || v.RequestID != ref.RequestID {
							t.Fatal("stale admission changed the winner or submitted again")
						}
						if mode == "async" {
							f.now = f.now.Add(2 * time.Second)
							v, err = f.runtime.PollExecutionV1(ctx, ref)
							boundedState(t, v, err, llm.ExecutionProviderCompleted)
						}
						v, err = f.runtime.CompleteExecutionV1(ctx, ref)
						boundedState(t, v, err, llm.ExecutionCompleted)
					})
				}
			}
		}
	}
}

func TestCloudExecutionStaleAdmissionCannotSubmitReplacementAttempt(t *testing.T) {
	f := boundedCloud(t, false)
	ctx := context.Background()
	f.adapter.invoke = func(ctx context.Context, _ provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, errors.New("lost paid response")
	}
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	winner := f.runtime
	f.restart(t)
	store := &admissionRaceStore{cloudExecutionStore: f.runtime.store, beforePlan: func(n int) {
		if n != 2 {
			return
		}
		v, err := winner.GenerateStepV1(ctx, f.request)
		boundedState(t, v, err, llm.ExecutionPending)
		f.now = f.now.Add(16 * time.Minute)
		v, err = winner.AcquireBudgetV1(ctx, ref)
		boundedState(t, v, err, llm.ExecutionAcquired)
	}}
	f.runtime.store, f.runtime.execution.admission.store = store, store
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionOutcomeUnknown)
	if f.submits.Load() != 1 {
		t.Fatal("stale submission dispatched a replacement paid attempt")
	}
}

type conflictingAdmissionStore struct{ cloudAdmissionStore }

func (s conflictingAdmissionStore) LoadBudgetPlan(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.BudgetPlan, error) {
	return cloudstate.BudgetPlan{}, contracts.ErrConflict
}

func TestCloudExecutionAdmissionConflictWithoutWinnerStillFails(t *testing.T) {
	f := boundedCloud(t, false)
	f.runtime.execution.admission.store = conflictingAdmissionStore{f.runtime.execution.admission.store}
	_, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	var classified *provider.Error
	if !errors.As(err, &classified) || classified.Code != provider.CodeOperationConflict || classified.Retry != provider.RetryNever || f.submits.Load() != 0 {
		t.Fatalf("unproven conflict was ignored: %v", err)
	}
}
