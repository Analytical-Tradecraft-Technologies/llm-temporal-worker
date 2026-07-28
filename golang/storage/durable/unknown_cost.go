package durable

import (
	"context"
	"errors"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

// UnknownCostJournal is the PostgreSQL side of the authoritative billing
// handoff. Implementations must append resolve_unknown_exact events and update
// the operation and budget projections in one idempotent transaction.
type UnknownCostJournal interface {
	ResolveUnknownExact(context.Context, []budget.CompletionEvent) ([]postgresstore.JournalRecord, error)
}

// UnknownCostResolution identifies one authoritative billing correction. The
// exact events are committed to PostgreSQL before they are reconciled to the
// Redis materialization.
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

// UnknownCostResolutionResult exposes the committed PostgreSQL receipt even
// when Redis reconciliation is pending. A caller can retry Resolve with the
// same event payload; PostgreSQL replay is idempotent and Redis reconciliation
// remains fenced by the generation/incarnation identity.
type UnknownCostResolutionResult struct {
	JournalRecords   []postgresstore.JournalRecord
	ReconcilePending bool
	Reconciled       bool
}

// UnknownCostBoundary composes the authoritative PostgreSQL correction with
// the active Redis budget materializer. It never treats the two writes as a
// cross-store transaction: PostgreSQL commits first, then Redis is retried
// independently while its conservative bound remains in force.
type UnknownCostBoundary struct {
	Identity     StateIdentity
	Journal      UnknownCostJournal
	Materializer BudgetMaterializer
}

func (boundary UnknownCostBoundary) Validate() error {
	if err := boundary.Identity.Validate(); err != nil {
		return fmt.Errorf("%w: identity: %v", ErrBudgetBoundaryInvalid, err)
	}
	if isNilPort(boundary.Journal) {
		return fmt.Errorf("%w: PostgreSQL unknown-cost journal is required", ErrBudgetBoundaryInvalid)
	}
	if isNilPort(boundary.Materializer) {
		return fmt.Errorf("%w: Redis budget materializer is required", ErrBudgetBoundaryInvalid)
	}
	return nil
}

// Resolve commits the exact correction to PostgreSQL and only then asks Redis
// to reconcile. A PostgreSQL failure never invokes Redis. If Redis fails after
// the commit, the returned receipt is authoritative and ErrReconcilePending
// tells the caller to retry without weakening the prior conservative bound.
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
	records, err := boundary.Journal.ResolveUnknownExact(ctx, resolution.Events)
	if err != nil {
		return result, err
	}
	result.JournalRecords = append([]postgresstore.JournalRecord(nil), records...)
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
