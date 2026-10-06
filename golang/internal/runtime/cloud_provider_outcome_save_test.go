package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// cloudStorageFault fails writes of one kind once armed. Each save is a blob
// create, the request event create that commits the revision, then the
// pending index replace; skip selects which of those writes fails first.
type cloudStorageFault struct {
	kind    string
	skip    int
	script  []bool // per injected failure: true applies the write before failing
	forever bool
	armed   bool
	seen    int
	fired   int
}

func (s *cloudStorageFault) next(kind string) (before, after error) {
	if !s.armed || kind != s.kind {
		return nil, nil
	}
	s.seen++
	if s.seen <= s.skip || (!s.forever && s.fired >= len(s.script)) {
		return nil, nil
	}
	applied := s.fired < len(s.script) && s.script[s.fired]
	s.fired++
	err := errors.New("throttled: transient storage failure")
	if applied {
		return nil, err
	}
	return err, nil
}

func (f *boundedCloudFixture) inject(fault *cloudStorageFault) {
	f.table.hook = func(string, kv.KeyValueItem) (error, error) { return fault.next("kv") }
	f.blobs.hook = func(blob.BlobKey) (error, error) { return fault.next("blob") }
}

type countingReconciles struct {
	durable.BudgetLeaser
	reconciles atomic.Int32
}

func (c *countingReconciles) Reconcile(ctx context.Context, request durable.ReconcileRequest) error {
	c.reconciles.Add(1)
	return c.BudgetLeaser.Reconcile(ctx, request)
}

// acquiredForSaveFaults reserves budget for the request and returns the
// provider-side reconcile counter. Retry delays are shortened, not removed.
func (f *boundedCloudFixture) acquiredForSaveFaults(t *testing.T) (llm.ExecutionReferenceV1, *countingReconciles) {
	t.Helper()
	budgets := &countingReconciles{BudgetLeaser: f.cap.Budgets}
	f.cap.Budgets = budgets
	f.restart(t)
	f.runtime.execution.saveBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	ctx := context.Background()
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	return ref, budgets
}

func (f *boundedCloudFixture) savedExecution(t *testing.T, ref llm.ExecutionReferenceV1) cloudstate.SavedProviderExecution {
	t.Helper()
	scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
	attempt, err := f.repository.LoadRequestAttempt(context.Background(), scope, cloudstate.RequestID(ref.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	saved, err := f.repository.LoadProviderExecution(context.Background(), scope, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	return saved
}

func TestCloudProviderOutcomeSaveRetriesTransientStorageFailure(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, test := range []struct {
			name    string
			kind    string
			skip    int
			applied bool
		}{
			{"blob-before", "blob", 0, false},
			{"blob-lost-acknowledgement", "blob", 0, true},
			{"record-before", "kv", 0, false},
			{"record-lost-acknowledgement", "kv", 0, true},
			{"index-before", "kv", 1, false},
			{"index-lost-acknowledgement", "kv", 1, true},
		} {
			t.Run(map[bool]string{false: "sync/", true: "async/"}[async]+test.name, func(t *testing.T) {
				f := boundedCloud(t, async)
				ref, budgets := f.acquiredForSaveFaults(t)
				// A fresh single failure is armed only once the provider answered,
				// so it lands on the save of that answer.
				var faults []*cloudStorageFault
				arm := func() {
					fault := &cloudStorageFault{kind: test.kind, skip: test.skip, script: []bool{test.applied}, armed: true}
					faults = append(faults, fault)
					f.inject(fault)
				}
				invoke, submit, poll := f.adapter.invoke, f.adapter.submit, f.adapter.poll
				f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
					result, err := invoke(ctx, call, o)
					arm()
					return result, err
				}
				f.adapter.submit = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.ResumableResult, error) {
					result, err := submit(ctx, call, o)
					arm()
					return result, err
				}
				f.adapter.poll = func(ctx context.Context, call provider.Call, id string, o provider.Observer) (provider.ResumableResult, error) {
					result, err := poll(ctx, call, id, o)
					arm()
					return result, err
				}
				ctx := context.Background()
				started := f.now
				v, err := f.runtime.GenerateStepV1(ctx, f.request)
				if async {
					boundedState(t, v, err, llm.ExecutionPending)
					if saved := f.savedExecution(t, ref); saved.Execution.ProviderOperationID != "job" {
						t.Fatal("accepted provider job was not saved")
					}
					f.now = f.now.Add(2 * time.Second)
					v, err = f.runtime.PollExecutionV1(ctx, ref)
				}
				boundedState(t, v, err, llm.ExecutionProviderCompleted)
				for _, fault := range faults {
					if fault.fired != 1 {
						t.Fatalf("storage fault fired %d times", fault.fired)
					}
				}
				saved := f.savedExecution(t, ref)
				if saved.Execution.Stage != cloudstate.ExecutionSucceeded || !saved.Execution.Settled || saved.Execution.Claim == nil {
					t.Fatalf("paid response not saved and settled: %+v", saved.Execution)
				}
				if f.submits.Load() != 1 || budgets.reconciles.Load() != 1 || f.now.Sub(started) > 2*time.Second {
					t.Fatalf("submits=%d reconciles=%d waited=%s", f.submits.Load(), budgets.reconciles.Load(), f.now.Sub(started))
				}
				v, err = f.runtime.CompleteExecutionV1(ctx, ref)
				boundedState(t, v, err, llm.ExecutionCompleted)
				if f.submits.Load() != 1 {
					t.Fatal("completion dispatched again")
				}
			})
		}
	}
}

func TestCloudProviderOutcomeSavePersistentFailureStaysUnknown(t *testing.T) {
	f := boundedCloud(t, false)
	ref, budgets := f.acquiredForSaveFaults(t)
	fault := &cloudStorageFault{kind: "kv", forever: true}
	f.inject(fault)
	invoke := f.adapter.invoke
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		result, err := invoke(ctx, call, o)
		fault.armed = true
		return result, err
	}
	ctx := context.Background()
	_, err := f.runtime.GenerateStepV1(ctx, f.request)
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeStateUnavailable || mapped.Retry != provider.RetrySameOperation {
		t.Fatalf("exhausted outcome save = %#v, want retryable state_unavailable", err)
	}
	if want := len(f.runtime.execution.saveBackoff) + 1; fault.fired != want {
		t.Fatalf("outcome save attempts = %d, want %d", fault.fired, want)
	}
	fault.armed = false
	// The Activity retry finds only the durable submitting marker. It must wait
	// for recovery and report the outcome unknown; it can never dispatch again.
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	f.now = f.now.Add(16 * time.Minute)
	v, err = f.runtime.PollExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionOutcomeUnknown)
	saved := f.savedExecution(t, ref)
	if saved.Execution.Stage != cloudstate.ExecutionUnknown || saved.Execution.Claim == nil || saved.Execution.Settled {
		t.Fatalf("unknown paid attempt = %+v", saved.Execution)
	}
	if f.submits.Load() != 1 || budgets.reconciles.Load() != 0 {
		t.Fatalf("submits=%d reconciles=%d", f.submits.Load(), budgets.reconciles.Load())
	}
}

// reencodedReplayStore fails the first save of a provider result and then
// refuses the identical revision as a conflict, as the repository does when
// the stored encoding differs from the in-memory one.
type reencodedReplayStore struct {
	CloudProviderExecutionStore
	applied bool
	saves   int
}

func (s *reencodedReplayStore) SaveProviderExecution(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, previous uint64, execution cloudstate.ProviderExecution) error {
	if execution.Stage != cloudstate.ExecutionSucceeded || execution.Settled {
		return s.CloudProviderExecutionStore.SaveProviderExecution(ctx, scope, id, previous, execution)
	}
	s.saves++
	if s.saves > 1 {
		return contracts.ErrConflict
	}
	if s.applied {
		if err := s.CloudProviderExecutionStore.SaveProviderExecution(ctx, scope, id, previous, execution); err != nil {
			return err
		}
	}
	return errors.New("lost outcome acknowledgement")
}

func TestCloudProviderOutcomeSaveConflictAfterRetryIsVerifiedByReload(t *testing.T) {
	for _, applied := range []bool{true, false} {
		t.Run(map[bool]string{true: "own-write-applied", false: "not-applied"}[applied], func(t *testing.T) {
			f := boundedCloud(t, false)
			ref, budgets := f.acquiredForSaveFaults(t)
			store := &reencodedReplayStore{CloudProviderExecutionStore: f.runtime.execution.store, applied: applied}
			f.runtime.execution.store = store
			v, err := f.runtime.GenerateStepV1(context.Background(), f.request)
			if applied {
				boundedState(t, v, err, llm.ExecutionProviderCompleted)
				if saved := f.savedExecution(t, ref); !saved.Execution.Settled || budgets.reconciles.Load() != 1 {
					t.Fatal("applied outcome was not settled once")
				}
			} else {
				// A conflict is success only when the durable record is this result.
				var mapped *provider.Error
				if !errors.As(err, &mapped) || mapped.Code != provider.CodeStateUnavailable || mapped.Retry != provider.RetrySameOperation {
					t.Fatalf("unverified conflict = %#v, want retryable state_unavailable", err)
				}
				if saved := f.savedExecution(t, ref); saved.Execution.Stage != cloudstate.ExecutionSubmitting || budgets.reconciles.Load() != 0 {
					t.Fatal("unverified conflict was treated as a saved outcome")
				}
			}
			if store.saves != 2 || f.submits.Load() != 1 {
				t.Fatalf("saves=%d submits=%d", store.saves, f.submits.Load())
			}
		})
	}
}

// lostMarkerStore applies the pre-HTTP marker but never acknowledges it.
type lostMarkerStore struct {
	CloudProviderExecutionStore
	lost int
}

func (s *lostMarkerStore) SaveProviderExecution(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, previous uint64, execution cloudstate.ProviderExecution) error {
	err := s.CloudProviderExecutionStore.SaveProviderExecution(ctx, scope, id, previous, execution)
	if err == nil && execution.Stage == cloudstate.ExecutionSubmitting {
		s.lost++
		return errors.New("lost marker acknowledgement")
	}
	return err
}

func TestCloudProviderRefusedMarkerSaveSettlesWithoutDispatch(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "marker-not-applied", true: "marker-applied"}[applied], func(t *testing.T) {
			f := boundedCloud(t, false)
			ref, budgets := f.acquiredForSaveFaults(t)
			attempts := len(f.runtime.execution.saveBackoff) + 1
			fault := &cloudStorageFault{kind: "kv", script: make([]bool, attempts)}
			lost := &lostMarkerStore{CloudProviderExecutionStore: f.runtime.execution.store}
			if applied {
				f.runtime.execution.store = lost
			} else {
				f.inject(fault)
			}
			var dispatched atomic.Int32
			f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
				fault.armed = true
				if err := o.BeforePossibleWrite(ctx); err != nil {
					return provider.Result{}, err
				}
				dispatched.Add(1)
				return executionResponse(call).Result, nil
			}
			ctx := context.Background()
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			if !v.Retryable || dispatched.Load() != 0 || fault.fired+lost.lost != attempts {
				t.Fatalf("result=%+v dispatched=%d marker failures=%d", v, dispatched.Load(), fault.fired+lost.lost)
			}
			execution := f.savedExecution(t, ref).Execution
			if execution.Stage != cloudstate.ExecutionFailed || execution.Failure.Dispatch != provider.DispatchNotDispatched ||
				execution.Claim == nil || execution.Settlement == nil || !execution.Settled || budgets.reconciles.Load() != 1 {
				t.Fatalf("claimed reservation not released: %+v reconciles=%d", execution, budgets.reconciles.Load())
			}
			for _, event := range execution.Settlement.Events {
				if event.Kind != budget.JournalFinalizeExact || event.ActualCostUSD == nil || !event.ActualCostUSD.IsZero() {
					t.Fatalf("undispatched attempt charged: %+v", event)
				}
			}
			// The workflow's short retry delay replaces the 15-minute recovery wait.
			f.runtime.execution.store = lost.CloudProviderExecutionStore
			f.now = f.now.Add(2 * time.Second)
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionAcquired)
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			v, err = f.runtime.CompleteExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionCompleted)
			if dispatched.Load() != 1 {
				t.Fatalf("dispatched %d times", dispatched.Load())
			}
		})
	}
}
