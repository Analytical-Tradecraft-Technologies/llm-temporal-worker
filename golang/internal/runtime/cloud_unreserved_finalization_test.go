package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

type unreservedFinalizationStore struct {
	*recordingCloudRequests
	saved cloudstate.SavedProviderExecution
}

func (s *unreservedFinalizationStore) LoadProviderExecution(_ context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.SavedProviderExecution, error) {
	if scope != s.record.Request.Scope || id != s.record.Request.ID {
		return cloudstate.SavedProviderExecution{}, cloudstate.ErrCorrupt
	}
	return s.saved, nil
}

func unreservedFinalizationFixture(t *testing.T, kind string) (*cloudRequestRuntime, *unreservedFinalizationStore, *replayEffectsStore, func() ([]byte, error), func(FinalizationEffects) error, FinalizationEffects) {
	t.Helper()
	r, repository, store, run, save, effects := finalizationFixture(t, kind, "provider")
	f := newProviderExecutionFixture(t, kind, false, unreservedPlanning(t, "free"))
	result, err := f.submit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	saved := result.Saved
	paid := effects.Provider
	paid.Unreserved = true
	paid.Budget = durable.ReconcileRequest{}
	paid.Lease.Key.Route = saved.Plan.Route.CacheIdentity
	paid.Entry.Key = paid.Lease.Key
	saved.Plan.Route.OperationID = durable.OperationID(paid.Lease.OperationID)
	saved.Plan.Route.GenerationID = durable.GenerationID(paid.Lease.Attempt)
	saved.Plan.Reservation.OperationID = saved.Plan.Route.OperationID
	saved.Plan.Reservation.GenerationID = saved.Plan.Route.GenerationID
	saved.Plan.QuotedAt = paid.Lease.AcquiredAt
	saved.Plan.Reservation.ExpiresAt = paid.Lease.ExpiresAt
	saved.Execution.StartedAt = paid.Lease.AcquiredAt
	saved.Execution.RecoverAfter = paid.Lease.AcquiredAt.Add(durable.BudgetStartLease)
	saved.Execution.CompletedAt = paid.Completion.CompletedAt
	saved.Execution.UpdatedAt = paid.Completion.CompletedAt
	var cost llm.CostV1
	if kind == "generate" {
		var response llm.GenerateResponseV1
		if err := json.Unmarshal(paid.Entry.Response, &response); err != nil {
			t.Fatal(err)
		}
		saved.Execution.Response.Status = response.Status
		saved.Execution.Response.Output = response.Output
		cost = response.Cost
	} else {
		var response llm.CompactResponseV1
		if err := json.Unmarshal(paid.Entry.Response, &response); err != nil {
			t.Fatal(err)
		}
		cost = response.Cost
	}
	actual := pricing.MustUSD(*cost.ActualCostUSD)
	saved.Execution.Response.Cost = llm.Cost{Status: llm.CostStatusKnown, ActualCostUSD: &actual, Method: cost.Method, CatalogVersion: cost.CatalogVersion}
	if err := saved.Execution.Validate(saved.Plan); err != nil {
		t.Fatal(err)
	}
	wrapper := &unreservedFinalizationStore{recordingCloudRequests: repository, saved: saved}
	r.finalizer.requests = wrapper
	r.finalizer.budgets = nil
	return r, wrapper, store, run, save, effects
}

func TestCloudUnreservedFinalizationReplaysWithoutBudget(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		for _, fault := range []string{"save", "publish", "fill", "terminal"} {
			t.Run(kind+"/"+fault, func(t *testing.T) {
				_, repository, store, run, save, effects := unreservedFinalizationFixture(t, kind)
				if fault == "save" {
					repository.saveHandoffErr = cloudstate.ErrIndexPending
				}
				err := save(effects)
				if (err != nil) != (fault == "save") {
					t.Fatal("save", err)
				}
				repository.saveHandoffErr = nil
				if fault == "terminal" {
					repository.completeErr = cloudstate.ErrIndexPending
				} else if fault != "save" {
					store.fail = fault
				}
				if fault != "save" {
					if _, err := run(); err == nil {
						t.Fatal("lost acknowledgment hidden")
					}
				}
				repository.completeErr = nil
				store.fail = ""
				first, err := run()
				if err != nil {
					t.Fatal(err)
				}
				second, err := run()
				if err != nil || !equalFinalizationJSON(first, second) {
					t.Fatal("replay changed response", err)
				}
				for _, event := range store.events {
					if event == "budget" {
						t.Fatal("unreserved work touched Redis")
					}
				}
				for _, completion := range store.completions {
					if !completion.CompletedAt.Equal(repository.saved.Execution.CompletedAt) {
						t.Fatal("cache age moved")
					}
				}
			})
		}
	}
}

func TestCloudUnreservedFinalizationRejectsUnprovenBypass(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		for _, fault := range []string{"missing_execution", "pending", "timestamp", "operation", "generation", "route", "cost", "claim", "mode"} {
			t.Run(kind+"/"+fault, func(t *testing.T) {
				r, repository, store, _, save, effects := unreservedFinalizationFixture(t, kind)
				switch fault {
				case "missing_execution":
					r.finalizer.requests = repository.recordingCloudRequests
				case "pending":
					repository.saved.Execution.Stage = cloudstate.ExecutionPending
				case "timestamp":
					effects.Provider.Completion.CompletedAt = effects.Provider.Completion.CompletedAt.Add(time.Minute)
					effects.Provider.Entry.CompletedAt = effects.Provider.Completion.CompletedAt
				case "operation":
					repository.saved.Plan.Route.OperationID = "other"
				case "generation":
					effects.Provider.Lease.Attempt = "other"
				case "route":
					effects.Provider.Lease.Key.Route.Model = "other"
					effects.Provider.Entry.Key = effects.Provider.Lease.Key
				case "cost":
					actual := pricing.MustUSD("1")
					repository.saved.Execution.Response.Cost.ActualCostUSD = &actual
				case "claim":
					repository.saved.Execution.Claim = &durable.ClaimReceipt{}
				case "mode":
					repository.saved.Plan.Mode = cloudstate.BudgetReserved
				}
				if err := save(effects); err == nil {
					t.Fatal("unverified bypass accepted")
				}
				if repository.handoff != nil || len(store.events) != 0 {
					t.Fatal("invalid execution published")
				}
			})
		}
	}
}
