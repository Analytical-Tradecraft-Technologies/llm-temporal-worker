package durable

import (
	"context"
	"errors"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/budget"
)

// UnknownCostResolution identifies one exact billing correction to Redis accounting.
type UnknownCostResolution struct {
	OperationID   OperationID
	GenerationID  GenerationID
	IncarnationID IncarnationID
	Events        []budget.CompletionEvent
}

func (resolution UnknownCostResolution) Validate() error {
	if err := resolution.OperationID.Validate(); err != nil {
		return fmt.Errorf("unknown-cost operation: %w", err)
	}
	if err := resolution.GenerationID.Validate(); err != nil {
		return fmt.Errorf("unknown-cost generation: %w", err)
	}
	if err := resolution.IncarnationID.Validate(); err != nil {
		return fmt.Errorf("unknown-cost incarnation: %w", err)
	}
	if len(resolution.Events) == 0 {
		return errors.New("unknown-cost resolution requires at least one event")
	}
	reconcile := ReconcileRequest{
		OperationID:   resolution.OperationID,
		GenerationID:  resolution.GenerationID,
		IncarnationID: resolution.IncarnationID,
		Events:        resolution.Events,
	}
	if err := reconcile.Validate(); err != nil {
		return fmt.Errorf("unknown-cost reconciliation: %w", err)
	}
	seen := make(map[string]struct{}, len(resolution.Events))
	var amount = resolution.Events[0].ActualCostUSD
	for index, event := range resolution.Events {
		if event.Kind != budget.JournalResolveUnknownExact || event.CostStatus != budget.CostExact || event.ActualCostUSD == nil {
			return fmt.Errorf("unknown-cost event %d must be an exact resolve_unknown_exact event", index)
		}
		if _, exists := seen[event.EventID]; exists {
			return fmt.Errorf("unknown-cost event %d repeats event ID", index)
		}
		seen[event.EventID] = struct{}{}
		if amount == nil || event.ActualCostUSD.Cmp(*amount) != 0 {
			return errors.New("unknown-cost events must use one exact amount")
		}
	}
	return nil
}

// UnknownCostResolutionResult tells callers whether to retry the same event
// payload after a Redis failure. Event identity makes retries idempotent.
type UnknownCostResolutionResult struct {
	ReconcilePending bool
	Reconciled       bool
}

// UnknownCostBoundary applies exact corrections through the same Redis budget
// authority that retains the conservative charge while the cost is unknown.
type UnknownCostBoundary struct {
	Identity     StateIdentity
	Materializer BudgetMaterializer
}

func (boundary UnknownCostBoundary) Validate() error {
	if err := boundary.Identity.ValidateBudget(); err != nil {
		return fmt.Errorf("%w: identity: %v", ErrBudgetBoundaryInvalid, err)
	}
	if isNilPort(boundary.Materializer) {
		return fmt.Errorf("%w: Redis budget materializer is required", ErrBudgetBoundaryInvalid)
	}
	return nil
}

// Resolve atomically applies an exact correction in Redis. ErrReconcilePending
// tells the caller to retry the identical payload without dispatching again.
func (boundary UnknownCostBoundary) Resolve(ctx context.Context, resolution UnknownCostResolution) (UnknownCostResolutionResult, error) {
	var result UnknownCostResolutionResult
	if ctx == nil {
		return result, fmt.Errorf("%w: context is nil", ErrBudgetBoundaryInvalid)
	}
	if err := boundary.Validate(); err != nil {
		return result, err
	}
	if err := resolution.Validate(); err != nil {
		return result, fmt.Errorf("%w: %v", ErrBudgetBoundaryInvalid, err)
	}
	reconcile := ReconcileRequest{
		OperationID:   resolution.OperationID,
		GenerationID:  resolution.GenerationID,
		IncarnationID: resolution.IncarnationID,
		Events:        append([]budget.CompletionEvent(nil), resolution.Events...),
	}
	if err := boundary.Materializer.Reconcile(ctx, reconcile); err != nil {
		result.ReconcilePending = true
		return result, fmt.Errorf("%w: %v", ErrReconcilePending, err)
	}
	result.Reconciled = true
	return result, nil
}
