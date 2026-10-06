package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
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
