package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

var (
	// ErrUnknownCostConflict means the operation is no longer an unknown-cost
	// terminal operation, or the requested exact amount differs from an amount
	// already recorded by an idempotent retry.
	ErrUnknownCostConflict = errors.New("unknown-cost resolution conflicts with operation state")
	// ErrUnknownCostOperationMissing is returned without exposing operation or
	// tenant identifiers in the error text.
	ErrUnknownCostOperationMissing = errors.New("unknown-cost operation is missing")
)

// ResolveUnknownExact atomically records authoritative billing for one
// operation and all of its budget-window reservations. Each event must be a
// validated resolve_unknown_exact revision for the same operation and exact
// amount. The operation row, journal events, reservation projections, and
// bucket projections commit together; a failure leaves the conservative
// unknown bound untouched.
//
// Redis reconciliation is deliberately outside this method. The caller must
// reconcile Redis only after this transaction commits, using its own fenced
// generation protocol. This boundary never accepts provider credentials or
// billing payloads beyond the validated exact USD amount.
func (repository *BudgetJournalRepository) ResolveUnknownExact(ctx context.Context, events []budget.CompletionEvent) ([]JournalRecord, error) {
	if err := repository.validate(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, errors.New("unknown-cost resolution requires at least one event")
	}
	operationID, amount, err := validateUnknownResolutionEvents(events)
	if err != nil {
		return nil, err
	}
	journalTable, err := repository.Namespace.Render("budget_journal_events")
	if err != nil {
		return nil, err
	}
	bucketTable, err := repository.Namespace.Render("budget_buckets")
	if err != nil {
		return nil, err
	}
	reservationTable, err := repository.Namespace.Render("operation_budget_reservations")
	if err != nil {
		return nil, err
	}
	operationsTable, err := repository.Namespace.Render("operations")
	if err != nil {
		return nil, err
	}
	now := events[0].OccurredAt.UTC()
	if repository.Now != nil {
		now = repository.Now().UTC()
	}
	relations := journalRelations{journal: journalTable, buckets: bucketTable, reservations: reservationTable}
	results := make([]JournalRecord, 0, len(events))
	err = WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var state, costStatus string
		var actual *string
		if err := tx.QueryRow(ctx, "SELECT state, cost_status, actual_cost_usd::text FROM "+operationsTable+" WHERE operation_id=$1 FOR UPDATE", operationID).Scan(&state, &costStatus, &actual); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrUnknownCostOperationMissing
			}
			return redactPostgresError(fmt.Errorf("lock unknown-cost operation: %w", err))
		}
		if costStatus != string(budget.CostUnknown) && costStatus != string(budget.CostExact) {
			return ErrUnknownCostConflict
		}
		if state != "completed" && state != "ambiguous" {
			return ErrUnknownCostConflict
		}
		if costStatus == string(budget.CostExact) {
			encoded, encodeErr := EncodeUSD(amount)
			if encodeErr != nil || actual == nil || *actual != encoded {
				return ErrUnknownCostConflict
			}
		}
		for _, event := range events {
			input := journalInput{
				eventID: event.EventID, generationID: event.GenerationID, operationID: event.OperationID, windowID: event.WindowID,
				bucketStart: event.BucketStart, revision: event.ReservationRevision, kind: event.Kind,
				reservedDecrease: event.ReservedDecreaseUSD, accountedIncrease: event.AccountedIncreaseUSD,
				accountedDecrease: event.AccountedDecreaseUSD, actual: event.ActualCostUSD, costStatus: event.CostStatus,
				unknownReason: event.UnknownReasonCode, occurredAt: event.OccurredAt,
			}
			record, appendErr := repository.appendTx(ctx, tx, input, relations, now)
			if appendErr != nil {
				return appendErr
			}
			results = append(results, record)
		}
		if costStatus == string(budget.CostUnknown) {
			encoded, encodeErr := EncodeUSD(amount)
			if encodeErr != nil {
				return encodeErr
			}
			tag, updateErr := tx.Exec(ctx, unknownCostOperationUpdateSQL(operationsTable), operationID, encoded)
			if updateErr != nil {
				return redactPostgresError(fmt.Errorf("resolve unknown operation cost: %w", updateErr))
			}
			if tag.RowsAffected() != 1 {
				return ErrUnknownCostConflict
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func unknownCostOperationUpdateSQL(operationsTable string) string {
	return "UPDATE " + operationsTable + " SET actual_cost_usd=$2, cost_status='exact', cost_method='provider_reported', cost_unknown_reason_code=NULL, updated_at=clock_timestamp() WHERE operation_id=$1 AND cost_status='unknown'"
}

func validateUnknownResolutionEvents(events []budget.CompletionEvent) (uuid.UUID, pricing.USD, error) {
	operationID := uuid.Nil
	amount := pricing.USD{}
	seenEvents := make(map[string]struct{}, len(events))
	for index, event := range events {
		if err := event.Validate(); err != nil {
			return uuid.Nil, pricing.USD{}, fmt.Errorf("unknown-cost event %d: %w", index, err)
		}
		if event.Kind != budget.JournalResolveUnknownExact || event.CostStatus != budget.CostExact || event.ActualCostUSD == nil {
			return uuid.Nil, pricing.USD{}, errors.New("unknown-cost resolution requires exact resolve events")
		}
		if _, exists := seenEvents[event.EventID]; exists {
			return uuid.Nil, pricing.USD{}, errors.New("unknown-cost resolution event IDs must be unique")
		}
		seenEvents[event.EventID] = struct{}{}
		parsedOperation, err := uuid.Parse(event.OperationID)
		if err != nil {
			return uuid.Nil, pricing.USD{}, fmt.Errorf("unknown-cost operation ID: %w", err)
		}
		if index == 0 {
			operationID = parsedOperation
			amount = *event.ActualCostUSD
			continue
		}
		if parsedOperation != operationID {
			return uuid.Nil, pricing.USD{}, errors.New("unknown-cost events must target one operation")
		}
		if event.ActualCostUSD.Cmp(amount) != 0 {
			return uuid.Nil, pricing.USD{}, errors.New("unknown-cost events must use one exact amount")
		}
	}
	return operationID, amount, nil
}
