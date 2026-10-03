package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// Cross-process storage seam. cloudstate tests separately exercise the real
// CAS/event/blob/index implementation, including uncertain backend writes.
type executionTestStore struct {
	*admissionPlanStore
	lock          sync.Mutex
	execution     []byte
	before, after func(cloudstate.ProviderExecution) error
}

func (s *executionTestStore) LoadProviderExecution(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.SavedProviderExecution, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := ctx.Err(); err != nil {
		return cloudstate.SavedProviderExecution{}, err
	}
	if scope != s.record.Request.Scope || id != s.record.Request.ID {
		return cloudstate.SavedProviderExecution{}, contracts.ErrNotFound
	}
	if s.execution == nil {
		return cloudstate.SavedProviderExecution{}, cloudstate.ErrProviderExecutionMissing
	}
	var saved cloudstate.SavedProviderExecution
	_ = json.Unmarshal(s.data, &saved.Plan)
	_ = json.Unmarshal(s.execution, &saved.Execution)
	return saved, nil
}
func (s *executionTestStore) BeginProviderExecution(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, reservation durable.ReserveResult, now time.Time) (cloudstate.SavedProviderExecution, bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := ctx.Err(); err != nil {
		return cloudstate.SavedProviderExecution{}, false, err
	}
	var saved cloudstate.SavedProviderExecution
	_ = json.Unmarshal(s.data, &saved.Plan)
	if s.execution != nil {
		_ = json.Unmarshal(s.execution, &saved.Execution)
		return saved, false, nil
	}
	saved.Execution = cloudstate.ProviderExecution{Version: 1, StartToken: uuid.NewString(), Revision: 1, Stage: cloudstate.ExecutionClaiming, Reservation: reservation, StartedAt: now, UpdatedAt: now, RecoverAfter: now.Add(durable.BudgetStartLease)}
	if err := saved.Execution.Validate(saved.Plan); err != nil {
		return saved, false, err
	}
	if s.before != nil {
		if err := s.before(saved.Execution); err != nil {
			return saved, false, err
		}
	}
	s.execution, _ = json.Marshal(saved.Execution)
	if s.after != nil {
		if err := s.after(saved.Execution); err != nil {
			return saved, false, err
		}
	}
	return saved, true, nil
}
func (s *executionTestStore) SaveProviderExecution(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, previous uint64, next cloudstate.ProviderExecution) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	var old cloudstate.ProviderExecution
	_ = json.Unmarshal(s.execution, &old)
	if old.Revision != previous || next.Revision != previous+1 {
		return contracts.ErrConflict
	}
	var plan cloudstate.BudgetPlan
	_ = json.Unmarshal(s.data, &plan)
	if err := next.Validate(plan); err != nil {
		return err
	}
	if s.before != nil {
		if err := s.before(next); err != nil {
			return err
		}
	}
	s.execution, _ = json.Marshal(next)
	if s.after != nil {
		return s.after(next)
	}
	return nil
}

type executionSyncAdapter struct {
	*planningAdapter
	invoke func(context.Context, provider.Call, provider.Observer) (provider.Result, error)
}

func (a *executionSyncAdapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	return a.invoke(ctx, call, observer)
}

type executionAsyncAdapter struct {
	*executionSyncAdapter
	submit func(context.Context, provider.Call, provider.Observer) (provider.ResumableResult, error)
	poll   func(context.Context, provider.Call, string, provider.Observer) (provider.ResumableResult, error)
}

func (a *executionAsyncAdapter) Submit(ctx context.Context, call provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
	return a.submit(ctx, call, observer)
}
func (a *executionAsyncAdapter) Poll(ctx context.Context, call provider.Call, id string, observer provider.Observer) (provider.ResumableResult, error) {
	return a.poll(ctx, call, id, observer)
}

type executionRecoveryAdapter struct {
	*executionAsyncAdapter
	recover func(context.Context, provider.Call, provider.Observer) (provider.ResumableResult, error)
}

func (a *executionRecoveryAdapter) RecoverByIdempotencyKey(ctx context.Context, call provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
	return a.recover(ctx, call, observer)
}

type executionLeaser struct {
	durable.BudgetLeaser
	reconcile func(context.Context, durable.ReconcileRequest) error
}

func (l *executionLeaser) Reconcile(ctx context.Context, r durable.ReconcileRequest) error {
	return l.reconcile(ctx, r)
}

type providerExecutionFixture struct {
	*cloudAdmissionFixture
	executor                    *CloudProviderExecution
	store                       *executionTestStore
	adapter                     *executionAsyncAdapter
	call                        *CloudBudgetCall
	reservation                 durable.ReserveResult
	now                         time.Time
	submits, polls, settlements atomic.Int32
}

func executionResponse(call provider.Call) provider.ResumableResult {
	return provider.ResumableResult{State: provider.ResumableCompleted, Dispatch: provider.DispatchAccepted, ProviderOperationID: "private-job", Result: provider.Result{Response: llm.Response{OperationKey: call.OperationKey, Status: llm.ResponseStatusCompleted, Usage: llm.Usage{InputTokens: 10, OutputTokens: 5}}}}
}

func newProviderExecutionFixture(t *testing.T, kind string, async bool, configure ...func(*budgetPlanningFixture)) *providerExecutionFixture {
	t.Helper()
	f := &providerExecutionFixture{cloudAdmissionFixture: newCloudAdmissionFixture(t, kind, configure...)}
	f.now = f.attempt.QuotedAt
	f.store = &executionTestStore{admissionPlanStore: f.cloudAdmissionFixture.store}
	f.executor = &CloudProviderExecution{store: f.store, admission: f.helper, clock: func() time.Time { return f.now }}
	f.adapter = &executionAsyncAdapter{executionSyncAdapter: &executionSyncAdapter{planningAdapter: &planningAdapter{version: "profile/v1"}}}
	f.adapter.invoke = func(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := observer.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return executionResponse(call).Result, nil
	}
	f.adapter.submit = func(ctx context.Context, call provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
		f.submits.Add(1)
		if err := observer.BeforePossibleWrite(ctx); err != nil {
			return provider.ResumableResult{}, err
		}
		return provider.ResumableResult{State: provider.ResumablePending, Dispatch: provider.DispatchAccepted, ProviderOperationID: "private-job", NextPollAfter: 2 * time.Second}, nil
	}
	f.adapter.poll = func(_ context.Context, call provider.Call, id string, _ provider.Observer) (provider.ResumableResult, error) {
		f.polls.Add(1)
		if id != "private-job" {
			t.Fatal("replaced provider job ID")
		}
		return executionResponse(call), nil
	}
	var adapter provider.Adapter = f.adapter
	if !async {
		adapter = f.adapter.executionSyncAdapter
	}
	f.helper.planning.providers.adapters = engine.AdapterMap{"endpoint": adapter}
	underlying := f.helper.admit.boundary.Materializer
	f.helper.admit.boundary.Materializer = &executionLeaser{BudgetLeaser: underlying, reconcile: func(ctx context.Context, request durable.ReconcileRequest) error {
		f.settlements.Add(1)
		saved, err := f.store.LoadProviderExecution(ctx, f.store.record.Request.Scope, f.store.record.Request.ID)
		if err != nil || saved.Execution.Settlement == nil || !reflect.DeepEqual(*saved.Execution.Settlement, request) {
			t.Fatal("settled before saving exact batch", err)
		}
		return underlying.Reconcile(ctx, request)
	}}
	var err error
	f.call, err = f.prepare(context.Background(), f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	f.reservation, err = f.helper.Reserve(context.Background(), f.call)
	if err != nil || f.reservation.Accepted != f.call.Plan().RequiresReservation() {
		t.Fatal("reserve", err)
	}
	return f
}

func (f *providerExecutionFixture) submit(ctx context.Context) (ProviderExecutionResult, error) {
	return f.executor.Submit(ctx, f.call, f.reservation)
}
func (f *providerExecutionFixture) resume(ctx context.Context) (ProviderExecutionResult, error) {
	return f.executor.Resume(ctx, f.store.record.Request.Scope, f.store.record.Request.ID, durable.GenerateReplay{}, f.replay)
}

func TestCloudProviderExecutionBothKindsSyncAndAsync(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		for _, async := range []bool{false, true} {
			t.Run(kind+map[bool]string{true: "/async", false: "/sync"}[async], func(t *testing.T) {
				f := newProviderExecutionFixture(t, kind, async)
				result, err := f.submit(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if async {
					if result.Saved.Execution.Stage != cloudstate.ExecutionPending || result.RetryAfter != 2*time.Second || f.polls.Load() != 0 || f.settlements.Load() != 0 {
						t.Fatal("submission polled/waited/settled pending work")
					}
					if _, err = f.resume(context.Background()); err != nil || f.polls.Load() != 0 {
						t.Fatal("ignored provider poll guidance", err)
					}
					f.now = f.now.Add(3 * time.Second)
					result, err = f.resume(context.Background())
				}
				if err != nil || result.Saved.Execution.Stage != cloudstate.ExecutionSucceeded || !result.Saved.Execution.Settled || f.submits.Load() != 1 || f.settlements.Load() != 1 {
					t.Fatalf("completion: %+v %v", result, err)
				}
				if result.Saved.Execution.Response.Cost.ActualCostUSD == nil {
					t.Fatal("successful usage not priced")
				}
				for range 3 {
					if _, err := f.resume(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				if f.submits.Load() != 1 || f.settlements.Load() != 1 {
					t.Fatal("replayed paid work or settled twice")
				}
			})
		}
	}
}

func TestCloudProviderExecutionLostAcknowledgementsNeverResubmit(t *testing.T) {
	for _, stage := range []cloudstate.ExecutionStage{cloudstate.ExecutionClaiming, cloudstate.ExecutionSubmitting, cloudstate.ExecutionPending, cloudstate.ExecutionSucceeded} {
		t.Run(string(stage), func(t *testing.T) {
			f := newProviderExecutionFixture(t, "generate", stage != cloudstate.ExecutionSucceeded)
			f.store.after = func(next cloudstate.ProviderExecution) error {
				if next.Stage == stage {
					return errors.New("sensitive lost storage reply")
				}
				return nil
			}
			_, err := f.submit(context.Background())
			if err == nil || strings.Contains(err.Error(), "sensitive") {
				t.Fatal("uncertain write not safely reported", err)
			}
			f.store.after = nil
			before := f.submits.Load()
			f.now = f.now.Add(16 * time.Minute)
			result, err := f.resume(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if f.submits.Load() != before {
				t.Fatal("recovery submitted again")
			}
			if stage == cloudstate.ExecutionClaiming || stage == cloudstate.ExecutionSubmitting {
				if result.Saved.Execution.Stage != cloudstate.ExecutionUnknown || f.settlements.Load() != 0 {
					t.Fatal("uncertain submission refunded or guessed")
				}
			} else if result.Saved.Execution.Stage != cloudstate.ExecutionSucceeded || !result.Saved.Execution.Settled {
				t.Fatal("saved work did not complete")
			}
		})
	}
}

func TestCloudProviderExecutionLostClaimReplyRetainsBudget(t *testing.T) {
	f := newProviderExecutionFixture(t, "generate", true)
	f.leaser.claim = func(ctx context.Context, request durable.ClaimRequest) (durable.ClaimReceipt, error) {
		_, err := f.leaser.BudgetLeaser.Claim(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		return durable.ClaimReceipt{}, errors.New("lost claim")
	}
	result, err := f.submit(context.Background())
	if err != nil || result.Saved.Execution.Stage != cloudstate.ExecutionUnknown || f.submits.Load() != 0 || f.settlements.Load() != 0 {
		t.Fatal("uncertain claim dispatched/refunded", err)
	}
	if _, err := f.resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.submits.Load() != 0 || f.settlements.Load() != 0 {
		t.Fatal("recovery reused uncertain claim")
	}
}

func TestCloudProviderExecutionReplaysExactSettlementAfterLostRedisReply(t *testing.T) {
	f := newProviderExecutionFixture(t, "generate", false)
	leaser := f.helper.admit.boundary.Materializer.(*executionLeaser)
	settle := leaser.reconcile
	var original durable.ReconcileRequest
	leaser.reconcile = func(ctx context.Context, request durable.ReconcileRequest) error {
		original = request
		if err := settle(ctx, request); err != nil {
			return err
		}
		return errors.New("lost settlement reply")
	}
	if _, err := f.submit(context.Background()); err == nil {
		t.Fatal("lost settlement reply hidden")
	}
	leaser.reconcile = func(ctx context.Context, request durable.ReconcileRequest) error {
		if !reflect.DeepEqual(original, request) {
			t.Fatal("settlement changed after restart")
		}
		return settle(ctx, request)
	}
	completedAt := f.now
	f.now = f.now.Add(time.Hour)
	result, err := f.resume(context.Background())
	if err != nil || !result.Saved.Execution.CompletedAt.Equal(completedAt) || !result.Saved.Execution.Settled || f.submits.Load() != 1 {
		t.Fatal("settlement recovery resubmitted", err)
	}
}

func TestCloudProviderExecutionConcurrentSubmitsClaimAndCallOnce(t *testing.T) {
	f := newProviderExecutionFixture(t, "generate", true)
	var group sync.WaitGroup
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := f.submit(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	if f.submits.Load() != 1 {
		t.Fatal("duplicate submission", f.submits.Load())
	}
}

func TestCloudProviderExecutionPollFailurePreservesKnownJob(t *testing.T) {
	for _, mode := range []string{"transport", "identity", "operation", "not_found", "pending"} {
		t.Run(mode, func(t *testing.T) {
			f := newProviderExecutionFixture(t, "generate", true)
			if _, err := f.submit(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.now = f.now.Add(time.Minute)
			f.adapter.poll = func(_ context.Context, call provider.Call, id string, _ provider.Observer) (provider.ResumableResult, error) {
				f.polls.Add(1)
				outcome := executionResponse(call)
				switch mode {
				case "transport":
					return outcome, errors.New("sensitive transport")
				case "identity":
					outcome.ProviderOperationID = "other"
				case "operation":
					outcome.Result.Response.OperationKey = "other"
				case "not_found":
					outcome = provider.ResumableResult{State: provider.ResumableNotFound, Dispatch: provider.DispatchAmbiguous}
				case "pending":
					outcome = provider.ResumableResult{State: provider.ResumablePending, Dispatch: provider.DispatchAccepted, ProviderOperationID: id, NextPollAfter: time.Hour}
				}
				return outcome, nil
			}
			result, err := f.resume(context.Background())
			if mode == "not_found" {
				if err != nil || result.Saved.Execution.Stage != cloudstate.ExecutionUnknown {
					t.Fatal(err)
				}
			} else if mode == "pending" {
				if err != nil || result.RetryAfter != time.Minute {
					t.Fatal("poll guidance not bounded", err)
				}
			} else if err == nil || strings.Contains(err.Error(), "sensitive") {
				t.Fatal("invalid poll result accepted", err)
			}
			saved, err := f.store.LoadProviderExecution(context.Background(), f.call.scope, f.call.id)
			if err != nil || saved.Execution.ProviderOperationID != "private-job" || f.polls.Load() != 1 || f.submits.Load() != 1 || f.settlements.Load() != 0 {
				t.Fatal("poll destroyed or resubmitted job", err)
			}
		})
	}
}

func TestCloudProviderExecutionUnknownSubmissionCanRecoverButNeverResubmit(t *testing.T) {
	f := newProviderExecutionFixture(t, "generate", true)
	f.adapter.submit = func(ctx context.Context, call provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
		f.submits.Add(1)
		if err := observer.BeforePossibleWrite(ctx); err != nil {
			return provider.ResumableResult{}, err
		}
		return provider.ResumableResult{}, errors.New("connection lost after accepting")
	}
	var recoveries int
	recovery := &executionRecoveryAdapter{executionAsyncAdapter: f.adapter, recover: func(_ context.Context, call provider.Call, _ provider.Observer) (provider.ResumableResult, error) {
		recoveries++
		return executionResponse(call), nil
	}}
	f.helper.planning.providers.adapters = engine.AdapterMap{"endpoint": recovery}
	f.call.provider.Adapter = recovery
	result, err := f.submit(context.Background())
	if err != nil || result.Saved.Execution.Stage != cloudstate.ExecutionUnknown {
		t.Fatal(err)
	}
	result, err = f.resume(context.Background())
	if err != nil || result.Saved.Execution.Stage != cloudstate.ExecutionSucceeded || !result.Saved.Execution.Settled || f.submits.Load() != 1 || recoveries != 1 {
		t.Fatal("idempotency recovery failed", err)
	}
}

func TestCloudProviderExecutionCanceledCallStillSavesPaidOutcome(t *testing.T) {
	f := newProviderExecutionFixture(t, "compact", false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.adapter.invoke = func(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := observer.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		cancel()
		return executionResponse(call).Result, nil
	}
	result, err := f.submit(ctx)
	if err != nil || !result.Saved.Execution.Settled {
		t.Fatal("cancellation abandoned paid result", err)
	}
}

func TestCloudProviderExecutionFailureAndIncompleteAccounting(t *testing.T) {
	for _, mode := range []string{"rejected", "preflight", "ambiguous", "failed_accepted", "unknown_cost", "length", "tools"} {
		t.Run(mode, func(t *testing.T) {
			f := newProviderExecutionFixture(t, "generate", true)
			f.adapter.submit = func(ctx context.Context, call provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
				f.submits.Add(1)
				if mode != "preflight" {
					if err := observer.BeforePossibleWrite(ctx); err != nil {
						return provider.ResumableResult{}, err
					}
				}
				if mode == "preflight" || mode == "rejected" || mode == "ambiguous" {
					dispatch := provider.DispatchAmbiguous
					if mode == "preflight" {
						dispatch = provider.DispatchNotDispatched
					}
					if mode == "rejected" {
						dispatch = provider.DispatchRejected
					}
					return provider.ResumableResult{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, dispatch, provider.RetryNever, "sensitive diagnostic")
				}
				outcome := executionResponse(call)
				switch mode {
				case "unknown_cost":
					outcome.Result.Response.Cost.Status = llm.CostStatusUnknown
				case "length":
					outcome.Result.Response.Status = llm.ResponseStatusLength
				case "tools":
					outcome.Result.Response.Status = llm.ResponseStatusToolCalls
				case "failed_accepted":
					outcome = provider.ResumableResult{State: provider.ResumableFailed, Dispatch: provider.DispatchAccepted, Failure: provider.NewError(provider.CodeProviderUnavailable, provider.PhasePoll, provider.DispatchAccepted, provider.RetryNever, "sensitive diagnostic")}
				}
				return outcome, nil
			}
			result, err := f.submit(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(result.Saved.Execution)
			if strings.Contains(string(data), "sensitive") {
				t.Fatal("stored provider error diagnostic")
			}
			if mode == "ambiguous" {
				if result.Saved.Execution.Stage != cloudstate.ExecutionUnknown || f.settlements.Load() != 0 {
					t.Fatal("ambiguous cost released")
				}
				return
			}
			if !result.Saved.Execution.Settled || f.settlements.Load() != 1 {
				t.Fatal("terminal accounting not settled")
			}
			for _, event := range result.Saved.Execution.Settlement.Events {
				if mode == "unknown_cost" || mode == "failed_accepted" {
					if event.CostStatus != budget.CostUnknown || event.ActualCostUSD != nil || event.AccountedIncreaseUSD.Cmp(event.ReservedDecreaseUSD) != 0 {
						t.Fatal("unknown cost undercounted")
					}
				} else if mode == "preflight" || mode == "rejected" {
					if event.ActualCostUSD == nil || !event.ActualCostUSD.IsZero() {
						t.Fatal("proven zero cost retained")
					}
				} else if event.ActualCostUSD == nil || event.ActualCostUSD.IsZero() {
					t.Fatal("incomplete or tool response treated as free")
				}
			}
			if mode == "length" && result.Saved.Execution.Response.Status != llm.ResponseStatusLength {
				t.Fatal("truncation became normal success")
			}
			if mode == "tools" && result.Saved.Execution.Response.Status != llm.ResponseStatusToolCalls {
				t.Fatal("tool calls not returned")
			}
		})
	}
}

func TestCloudProviderExecutionRefusesHTTPWhenSubmissionStateCannotBeSaved(t *testing.T) {
	f := newProviderExecutionFixture(t, "generate", true)
	f.store.before = func(next cloudstate.ProviderExecution) error {
		if next.Stage == cloudstate.ExecutionSubmitting {
			return contracts.ErrUnavailable
		}
		return nil
	}
	var writes int
	f.adapter.submit = func(ctx context.Context, _ provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
		if err := observer.BeforePossibleWrite(ctx); err != nil {
			return provider.ResumableResult{}, err
		}
		writes++
		return provider.ResumableResult{}, nil
	}
	if _, err := f.submit(context.Background()); err == nil || writes != 0 {
		t.Fatal("HTTP crossed failed durable boundary", err)
	}
}
