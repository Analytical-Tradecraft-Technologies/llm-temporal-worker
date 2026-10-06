package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

// Limits sized for exactly one reservation of the fixture's quote.
func singleReservationLimits(b *budgetPlanningFixture) {
	for i := range b.source.value.BudgetPolicies[0].Windows {
		b.source.value.BudgetPolicies[0].Windows[i].LimitUSD = pricing.MustUSD("0.0001")
	}
}

// An unresolved paid attempt is charged at its reservation once its recovery
// window has elapsed, whichever step observes that. The conservative charge
// then ages out of every budget window instead of holding its claim forever.
func TestCloudExecutionRuntimeUnknownOutcomeSettlesAfterRecoveryWindow(t *testing.T) {
	for _, step := range []string{"poll", "acquire"} {
		t.Run(step, func(t *testing.T) {
			f := boundedCloud(t, false, singleReservationLimits)
			original := f.adapter.invoke
			f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
				f.submits.Add(1)
				if err := o.BeforePossibleWrite(ctx); err != nil {
					return provider.Result{}, err
				}
				return provider.Result{}, errors.New("lost paid response")
			}
			ctx := context.Background()
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionPending)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
			scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
			first, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
			if err != nil {
				t.Fatal(err)
			}
			// Inside the recovery window the claim is retained as it is.
			v, err = f.runtime.PollExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionPending)
			saved, err := f.repository.LoadProviderExecution(ctx, scope, first.ID)
			if err != nil || saved.Execution.Claim == nil || saved.Execution.Settlement != nil || saved.Execution.Settled {
				t.Fatal("unknown claim settled inside the recovery window", err)
			}
			f.now = f.now.Add(16 * time.Minute)
			f.restart(t)
			if step == "poll" {
				v, err = f.runtime.PollExecutionV1(ctx, ref)
				boundedState(t, v, err, llm.ExecutionOutcomeUnknown)
			} else {
				// The retry stays behind its own conservative charge for the window.
				v, err = f.runtime.AcquireBudgetV1(ctx, ref)
				boundedState(t, v, err, llm.ExecutionBudgetWait)
			}
			saved, err = f.repository.LoadProviderExecution(ctx, scope, first.ID)
			if err != nil || saved.Execution.Stage != cloudstate.ExecutionUnknown || saved.Execution.Settlement == nil || !saved.Execution.Settled {
				t.Fatalf("unknown claim not settled after the recovery window: %+v %v", saved.Execution, err)
			}
			if len(saved.Execution.Settlement.Events) != len(saved.Execution.Reservation.Events) {
				t.Fatal("settlement does not cover every reserved window")
			}
			for _, event := range saved.Execution.Settlement.Events {
				if event.Kind != budget.JournalFinalizeUnknown || event.AccountedIncreaseUSD.Cmp(event.ReservedDecreaseUSD) != 0 {
					t.Fatalf("unknown outcome settled below its reservation: %+v", event)
				}
			}
			assertCloudStatus(t, f, first.ID, cloudstate.StatusOutcomeUnknown)
			// Settlement is idempotent across activity retries and never reopens
			// recovery. After acquisition the request's current attempt is the
			// replacement, so only the poll path re-observes the old outcome.
			f.restart(t)
			if step == "poll" {
				v, err = f.runtime.PollExecutionV1(ctx, ref)
				boundedState(t, v, err, llm.ExecutionOutcomeUnknown)
			}
			// An independent request after every window has rolled over gets its capacity back.
			f.now = f.now.Add(48 * time.Hour)
			f.adapter.invoke = original
			f.restart(t)
			f.request.OperationKey = "independent"
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			if f.submits.Load() != 2 {
				t.Fatal("unknown attempt resubmitted", f.submits.Load())
			}
		})
	}
}

// assertUnknownWorkCharged requires an unresolved paid attempt to keep its
// claim and, once settled, to be charged at its full reservation: settling
// unknown work may never refund it.
func assertUnknownWorkCharged(t *testing.T, saved cloudstate.SavedProviderExecution) {
	t.Helper()
	if saved.Execution.Claim == nil {
		t.Fatal("unknown work lost its claim")
	}
	if saved.Execution.Settlement == nil {
		if saved.Execution.Settled {
			t.Fatal("unknown work settled without a settlement")
		}
		return
	}
	for _, event := range saved.Execution.Settlement.Events {
		if event.Kind != budget.JournalFinalizeUnknown || event.AccountedIncreaseUSD.Cmp(event.ReservedDecreaseUSD) != 0 {
			t.Fatalf("unknown work settled below its reservation: %+v", event)
		}
	}
}

// Settling an expired unknown attempt is accounting only, so a worker with a
// different configuration settles it while still leaving the request itself
// to a compatible worker. A drained rollout must not hold the claim forever.
func TestCloudExecutionRuntimeSettlesExpiredUnknownAcrossConfigRollout(t *testing.T) {
	f := boundedCloud(t, false, singleReservationLimits)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, errors.New("lost paid response")
	}
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
	first, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(16 * time.Minute)
	rolledOut := f.cap
	rolledOut.ConfigDigest = [32]byte{42}
	source := *f.cap.Snapshot.(*planningSource)
	source.value.ConfigDigest = rolledOut.ConfigDigest
	rolledOut.Snapshot = &source
	if rolledOut.composition != nil {
		composition := *rolledOut.composition
		composition.Identity.ConfigDigest = rolledOut.ConfigDigest
		rolledOut.composition = &composition
	}
	f.cap = rolledOut
	f.restart(t)
	if _, err := f.runtime.AcquireBudgetV1(ctx, ref); err == nil {
		t.Fatal("incompatible worker advanced the request")
	}
	saved, err := f.repository.LoadProviderExecution(ctx, scope, first.ID)
	if err != nil || saved.Execution.Stage != cloudstate.ExecutionUnknown || saved.Execution.Settlement == nil || !saved.Execution.Settled {
		t.Fatalf("expired unknown claim not settled by an incompatible worker: %+v %v", saved.Execution, err)
	}
	assertUnknownWorkCharged(t, saved)
	if f.submits.Load() != 1 {
		t.Fatal("incompatible worker resubmitted", f.submits.Load())
	}
}

// When recovery resolves an expired unknown attempt during acquisition, the
// recovered outcome is finished instead of replacing the attempt.
func TestCloudExecutionRuntimeAcquireFinishesARecoveredUnknownAttempt(t *testing.T) {
	f := boundedCloud(t, true)
	f.adapter.submit = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.ResumableResult, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.ResumableResult{}, err
		}
		return provider.ResumableResult{}, errors.New("connection lost after accepting")
	}
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
	first, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	recoveries := 0
	recovery := &executionRecoveryAdapter{executionAsyncAdapter: f.adapter, recover: func(_ context.Context, call provider.Call, _ provider.Observer) (provider.ResumableResult, error) {
		recoveries++
		v := executionResponse(call)
		v.Result.Response.Output = []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "answer"}}}}
		return v, nil
	}}
	f.cap.Adapters = engine.AdapterMap{"endpoint": recovery}
	f.now = f.now.Add(16 * time.Minute)
	f.restart(t)
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	attempt, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(ref.RequestID))
	if err != nil || attempt.ID != first.ID {
		t.Fatalf("recovered attempt was replaced: %v", err)
	}
	saved, err := f.repository.LoadProviderExecution(ctx, scope, first.ID)
	if err != nil || saved.Execution.Stage != cloudstate.ExecutionSucceeded || recoveries != 1 || f.submits.Load() != 1 {
		t.Fatalf("recovered execution = %+v, recoveries=%d submits=%d, %v", saved.Execution.Stage, recoveries, f.submits.Load(), err)
	}
}
