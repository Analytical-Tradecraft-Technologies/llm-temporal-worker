package durable

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

// PollSettlement deterministically derives the same Redis settlement on every
// terminal poll, including retries after a lost acknowledgement. It never uses
// a poll attempt ID or current time. finalizedAt must be the persisted terminal
// timestamp, never the time of the current polling attempt. A nil actual cost retains the entire hold;
// terminal failure alone is not proof that the provider charged zero.
func PollSettlement(reservation ReserveResult, actual *pricing.USD, finalizedAt time.Time) (ReconcileRequest, error) {
	result := ReconcileRequest{OperationID: reservation.OperationID, GenerationID: reservation.GenerationID, IncarnationID: reservation.IncarnationID}
	if finalizedAt.IsZero() {
		return result, fmt.Errorf("persisted finalization time is required")
	}
	if !reservation.Accepted || len(reservation.Events) == 0 {
		return result, fmt.Errorf("poll requires an accepted reservation")
	}
	seen := make(map[string]bool, len(reservation.Events))
	for _, r := range reservation.Events {
		if seen[r.EventID] {
			return result, fmt.Errorf("duplicate reservation event")
		}
		seen[r.EventID] = true
		if err := r.Validate(); err != nil {
			return result, err
		}
		if r.OperationID != string(reservation.OperationID) || r.GenerationID != string(reservation.GenerationID) {
			return result, fmt.Errorf("poll reservation identity mismatch")
		}
		event := budget.CompletionEvent{EventID: fmt.Sprintf("%x", sha256.Sum256([]byte("poll-settlement/v1:"+r.EventID))), GenerationID: r.GenerationID, OperationID: r.OperationID, WindowID: r.WindowID, BucketStart: r.BucketStart, ReservationRevision: r.ReservationRevision + 1, OccurredAt: finalizedAt}
		if actual == nil {
			event.Kind = budget.JournalRetainAmbiguous
			event.CostStatus = budget.CostUnknown
			event.UnknownReasonCode = "provider_did_not_report_cost"
		} else {
			cost := *actual
			event.Kind = budget.JournalFinalizeExact
			event.CostStatus = budget.CostExact
			event.ActualCostUSD = &cost
			event.AccountedIncreaseUSD = cost
			event.ReservedDecreaseUSD = r.AmountUSD
		}
		if err := event.Validate(); err != nil {
			return result, err
		}
		result.Events = append(result.Events, event)
	}
	return result, result.Validate()
}

// ReconcilePollBudget uses Redis as the once-only settlement authority. There
// is deliberately no PostgreSQL "already released?" check and no check/delete
// race. Redis applies the event set atomically or recognizes the same events.
// Callers persist the terminal outcome first, so another worker can reproduce
// this handoff after a crash. Unknown costs require the existing reconciliation
// path before any later release; they cannot be silently changed by a poll.
func ReconcilePollBudget(ctx context.Context, materializer BudgetMaterializer, reservation ReserveResult, actual *pricing.USD, finalizedAt time.Time) error {
	if isNilPort(materializer) {
		return fmt.Errorf("Redis budget materializer is required")
	}
	request, err := PollSettlement(reservation, actual, finalizedAt)
	if err != nil {
		return err
	}
	return materializer.Reconcile(ctx, request)
}
