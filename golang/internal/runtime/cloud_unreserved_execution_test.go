package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func unreservedPlanning(t *testing.T, mode string) func(*budgetPlanningFixture) {
	t.Helper()
	return func(f *budgetPlanningFixture) {
		switch mode {
		case "free":
			f.entry.Prices = pricing.UnitPrices{}
			f.prices(t, []pricing.Entry{f.entry})
		case "unmatched", "unpriced":
			f.source.value.RequireBudgetMatch = false
			f.source.value.BudgetPolicies = nil
			if mode == "unpriced" {
				f.source.value.RequirePriceWhenBudgeted = true
				f.prices(t, nil)
			}
		default:
			t.Fatal("unknown test mode")
		}
	}
}

func TestCloudUnreservedAdmissionPersistsExplicitModeWithoutRedis(t *testing.T) {
	for _, mode := range []string{"free", "unmatched", "unpriced"} {
		for _, kind := range []string{"generate", "compact"} {
			t.Run(mode+"/"+kind, func(t *testing.T) {
				f := newCloudAdmissionFixture(t, kind, unreservedPlanning(t, mode))
				// Any unintended Accept/Claim/Reconcile call panics through the nil port.
				f.helper.admit.boundary.Materializer = &executionLeaser{}
				call, err := f.prepare(context.Background(), f.attempt)
				if err != nil {
					t.Fatal(err)
				}
				plan := call.Plan()
				if plan.RequiresReservation() || plan.Unpriced != (mode == "unpriced") || (plan.Mode == cloudstate.BudgetFree) != (mode == "free") {
					t.Fatal("lost explicit admission policy")
				}
				reservation, err := f.helper.Reserve(context.Background(), call)
				if err != nil || !reflect.DeepEqual(reservation, durable.ReserveResult{}) {
					t.Fatal("fabricated reservation", err)
				}
				if _, err := f.helper.Claim(context.Background(), call, reservation); err == nil {
					t.Fatal("fabricated claim")
				}
				// Recovery uses the original mode even if today's policies and prices change.
				f.source.value.RequireBudgetMatch = true
				f.prices(t, nil)
				f.helper, err = f.cap.NewCloudBudgetAdmission(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				recovered, err := f.prepare(context.Background(), f.attempt)
				if err != nil || !reflect.DeepEqual(recovered.Plan(), plan) {
					t.Fatal("changed saved policy", err)
				}
			})
		}
	}
}

func TestCloudUnreservedExecutionBothKindsSyncAndAsync(t *testing.T) {
	for _, mode := range []string{"free", "unmatched", "unpriced"} {
		for _, kind := range []string{"generate", "compact"} {
			for _, async := range []bool{false, true} {
				t.Run(mode+"/"+kind+map[bool]string{false: "/sync", true: "/async"}[async], func(t *testing.T) {
					f := newProviderExecutionFixture(t, kind, async, unreservedPlanning(t, mode))
					f.helper.admit.boundary.Materializer = &executionLeaser{}
					result, err := f.submit(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					if async {
						if result.Saved.Execution.Stage != cloudstate.ExecutionPending || f.polls.Load() != 0 {
							t.Fatal("submit polled")
						}
						f.now = f.now.Add(3 * time.Second)
						result, err = f.resume(context.Background())
						if err != nil || f.polls.Load() != 1 {
							t.Fatal("poll once", err)
						}
					}
					execution := result.Saved.Execution
					if execution.Stage != cloudstate.ExecutionSucceeded || !execution.Settled || execution.Claim != nil || execution.Settlement != nil || !execution.CompletedAt.Equal(f.now) || execution.Response.Cost.ReservedCostUSD != nil {
						t.Fatal("invalid unreserved completion")
					}
					if mode == "unpriced" {
						if execution.Response.Cost.Status != llm.CostStatusUnknown || execution.Response.Cost.ActualCostUSD != nil {
							t.Fatal("unknown price became free")
						}
					} else if execution.Response.Cost.Status != llm.CostStatusKnown || (execution.Response.Cost.ActualCostUSD.IsZero() != (mode == "free")) {
						t.Fatal("incorrect accounting")
					}
					f.now = f.now.Add(time.Hour)
					replay, err := f.resume(context.Background())
					if err != nil || !equalFinalizationValue(replay.Saved.Execution, execution) || f.submits.Load() != 1 || f.settlements.Load() != 0 {
						t.Fatal("replay changed completed work", err)
					}
				})
			}
		}
	}
}

func TestCloudUnreservedAmbiguousSubmissionNeverRepeats(t *testing.T) {
	f := newProviderExecutionFixture(t, "generate", true, unreservedPlanning(t, "free"))
	f.helper.admit.boundary.Materializer = &executionLeaser{}
	f.adapter.submit = func(ctx context.Context, call provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
		f.submits.Add(1)
		if err := observer.BeforePossibleWrite(ctx); err != nil {
			return provider.ResumableResult{}, err
		}
		return provider.ResumableResult{}, errors.New("lost provider reply")
	}
	result, err := f.submit(context.Background())
	if err != nil || result.Saved.Execution.Stage != cloudstate.ExecutionUnknown {
		t.Fatal("ambiguous result", err)
	}
	f.now = f.now.Add(time.Hour)
	for range 2 {
		result, err = f.resume(context.Background())
		if err != nil || result.Saved.Execution.Stage != cloudstate.ExecutionUnknown {
			t.Fatal("lost uncertainty", err)
		}
	}
	if f.submits.Load() != 1 || f.polls.Load() != 0 || f.settlements.Load() != 0 {
		t.Fatal("ambiguous attempt repeated")
	}
}

func TestCloudFreeExecutionPreservesProviderReportedCharge(t *testing.T) {
	f := newProviderExecutionFixture(t, "generate", false, unreservedPlanning(t, "free"))
	f.adapter.invoke = func(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
		if err := observer.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		result := executionResponse(call).Result
		actual := pricing.MustUSD("0.17")
		result.Response.Cost.ActualCostUSD = &actual
		return result, nil
	}
	result, err := f.submit(context.Background())
	if err != nil || result.Saved.Execution.Response.Cost.ActualCostUSD.Cmp(pricing.MustUSD("0.17")) != 0 || result.Saved.Execution.Response.Cost.Method != "provider_reported" {
		t.Fatalf("discarded reported charge: %+v, %v", result.Saved.Execution.Response.Cost, err)
	}
}
