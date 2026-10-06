package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

// CloudBudgetPlanStore is supplied by the configured request repository. It
// preserves initial planning facts; Redis remains the budget/claim authority.
type CloudBudgetPlanStore interface {
	SaveBudgetPlan(context.Context, cloudstate.Scope, cloudstate.RequestID, cloudstate.BudgetPlan, time.Time) error
	LoadBudgetPlan(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.BudgetPlan, error)
}

// CloudBudgetPlans binds plan persistence to the same cloud stores as request
// discovery. It neither owns clients nor activates phase factories.
type CloudBudgetPlans struct {
	store        CloudBudgetPlanStore
	configDigest [32]byte
}

func (capabilities V1RuntimeCapabilities) NewCloudBudgetPlans() (*CloudBudgetPlans, error) {
	store, ok := capabilities.Requests.(CloudBudgetPlanStore)
	if !ok || isNilCapability(store) || capabilities.ConfigDigest == ([32]byte{}) {
		return nil, errors.New("cloud budget plans require request storage and a configuration identity")
	}
	return &CloudBudgetPlans{store: store, configDigest: capabilities.ConfigDigest}, nil
}

// BudgetPlan projects only durable planning facts. Adapter clients, SDK params,
// and provider operation keys are deliberately absent. An explicit mode
// preserves the distinction between reserved, free and unmatched calls.
func (planned PlannedBudgetCall) BudgetPlan(kind string) (cloudstate.BudgetPlan, error) {
	if planned.mode == "" || (planned.Quote == nil) != planned.unpriced {
		return cloudstate.BudgetPlan{}, budgetPlanningError(provider.CodeInvalidArgument)
	}
	providerPlan := planned.Provider
	route, err := providerPlan.Route(planned.Route.OperationID, planned.Route.GenerationID)
	if err != nil {
		return cloudstate.BudgetPlan{}, err
	}
	// A route without a configured price version acquires the actual quote's
	// version during budget planning; all other compiled bindings are unchanged.
	if planned.Quote != nil {
		route.PriceVersion = planned.Quote.Entry.Version
	}
	if route != planned.Route {
		return cloudstate.BudgetPlan{}, budgetPlanningError(provider.CodeConfiguration)
	}
	plan := cloudstate.BudgetPlan{Mode: planned.mode, Unpriced: planned.unpriced, Version: 1, Kind: kind, ConfigDigest: providerPlan.ConfigDigest, ConfigEpoch: providerPlan.ConfigEpoch,
		RequestDigest: providerPlan.Call.Metadata.SchemaDigest, CapabilityVersion: providerPlan.CapabilityVersion, CompilerVersion: cloudCompilerVersion,
		Family: providerPlan.Candidate.Family, ProviderTier: providerPlan.Candidate.ProviderTier,
		RequestedClass: providerPlan.Candidate.RequestedClass, AttemptedClass: providerPlan.Candidate.AttemptedClass,
		Route: planned.Route, Estimate: planned.Estimate, Reservation: planned.Reservation, QuotedAt: planned.QuotedAt, ClassEntries: planned.ClassEntries}
	if planned.Quote != nil {
		plan.Quote = *planned.Quote
	}
	if plan.Validate() != nil {
		return cloudstate.BudgetPlan{}, budgetPlanningError(provider.CodeConfiguration)
	}
	// Detach caller-owned vectors before passing the plan to any store.
	plan.Reservation.Reservations = append(plan.Reservation.Reservations[:0:0], plan.Reservation.Reservations...)
	plan.Quote.Entry.UnknownComponents = append(plan.Quote.Entry.UnknownComponents[:0:0], plan.Quote.Entry.UnknownComponents...)
	return plan, nil
}

func (plans *CloudBudgetPlans) Save(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, kind string, planned PlannedBudgetCall, now time.Time) (cloudstate.BudgetPlan, error) {
	if ctx == nil || plans == nil || isNilCapability(plans.store) {
		return cloudstate.BudgetPlan{}, cloudRuntimeError(cloudstate.ErrInvalid, false)
	}
	if err := ctx.Err(); err != nil {
		return cloudstate.BudgetPlan{}, err
	}
	plan, err := planned.BudgetPlan(kind)
	if err != nil {
		return cloudstate.BudgetPlan{}, err
	}
	if plan.ConfigDigest != plans.configDigest {
		return cloudstate.BudgetPlan{}, budgetPlanningError(provider.CodeConfiguration)
	}
	if err := plans.store.SaveBudgetPlan(ctx, scope, id, plan, now); err != nil {
		return cloudstate.BudgetPlan{}, cloudRuntimeError(err, false)
	}
	return plan, nil
}

// Load deliberately does not compare the saved config digest to today's
// snapshot or consult its price resolver. Recovery must use the saved identity;
// it must not recompile under changed settings without verifying compatibility.
func (plans *CloudBudgetPlans) Load(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.BudgetPlan, error) {
	if ctx == nil || plans == nil || isNilCapability(plans.store) {
		return cloudstate.BudgetPlan{}, cloudRuntimeError(cloudstate.ErrInvalid, false)
	}
	if err := ctx.Err(); err != nil {
		return cloudstate.BudgetPlan{}, err
	}
	plan, err := plans.store.LoadBudgetPlan(ctx, scope, id)
	if errors.Is(err, cloudstate.ErrBudgetPlanMissing) {
		return cloudstate.BudgetPlan{}, cloudstate.ErrBudgetPlanMissing
	}
	if err != nil {
		return cloudstate.BudgetPlan{}, cloudRuntimeError(err, false)
	}
	if plan.Validate() != nil {
		return cloudstate.BudgetPlan{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	return plan, nil
}
