package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

var (
	ErrBudgetJournalConflict           = errors.New("budget journal idempotency conflict")
	ErrBudgetJournalMissingReservation = errors.New("budget journal reservation is missing")
)

// BudgetJournal is the durable write port used after Redis accepts a budget
// decision. It intentionally has no active-budget read method: normal
// admission and completion must not query PostgreSQL budget tables.
type BudgetJournal interface {
	AppendReservation(context.Context, budget.ReservationEvent) (JournalRecord, error)
	AppendCompletion(context.Context, budget.CompletionEvent) (JournalRecord, error)
}

// JournalRecord identifies the append result. Existing is true when a retry
// presented the same operation/window/revision and event identity; projection
// writes are skipped in that case.
type JournalRecord struct {
	JournalID int64
	EventID   uuid.UUID
	Existing  bool
}

// BudgetJournalRepository implements the write-only journal port. The caller
// must have already acquired the matching Redis reservation; this repository
// never calls Redis or dispatches a provider operation.
type BudgetJournalRepository struct {
	Pool      *pgxpool.Pool
	Namespace Namespace
	Now       func() time.Time
}

var _ BudgetJournal = (*BudgetJournalRepository)(nil)

func (repository *BudgetJournalRepository) validate() error {
	if repository == nil || repository.Pool == nil {
		return errors.New("budget journal PostgreSQL pool is nil")
	}
	return repository.Namespace.Validate()
}

func (repository *BudgetJournalRepository) AppendReservation(ctx context.Context, event budget.ReservationEvent) (JournalRecord, error) {
	if err := event.Validate(); err != nil {
		return JournalRecord{}, err
	}
	return repository.append(ctx, journalInput{
		eventID: event.EventID, generationID: event.GenerationID, operationID: event.OperationID, windowID: event.WindowID,
		bucketStart: event.BucketStart, revision: event.ReservationRevision, kind: budget.JournalReserve,
		reservedIncrease: event.AmountUSD, costStatus: budget.CostPending, occurredAt: event.OccurredAt,
		reserve: true,
	})
}

func (repository *BudgetJournalRepository) AppendCompletion(ctx context.Context, event budget.CompletionEvent) (JournalRecord, error) {
	if err := event.Validate(); err != nil {
		return JournalRecord{}, err
	}
	return repository.append(ctx, journalInput{
		eventID: event.EventID, generationID: event.GenerationID, operationID: event.OperationID, windowID: event.WindowID,
		bucketStart: event.BucketStart, revision: event.ReservationRevision, kind: event.Kind,
		reservedDecrease: event.ReservedDecreaseUSD, accountedIncrease: event.AccountedIncreaseUSD,
		accountedDecrease: event.AccountedDecreaseUSD, actual: event.ActualCostUSD, costStatus: event.CostStatus,
		unknownReason: event.UnknownReasonCode, occurredAt: event.OccurredAt,
	})
}

type journalInput struct {
	eventID, generationID, operationID, windowID string
	bucketStart                                  time.Time
	revision                                     int
	kind                                         budget.JournalEventKind
	reservedIncrease, reservedDecrease           pricing.USD
	accountedIncrease, accountedDecrease         pricing.USD
	actual                                       *pricing.USD
	costStatus                                   budget.ActualCostStatus
	unknownReason                                string
	occurredAt                                   time.Time
	reserve                                      bool
}

func (repository *BudgetJournalRepository) append(ctx context.Context, input journalInput) (result JournalRecord, err error) {
	if err := repository.validate(); err != nil {
		return result, err
	}
	if _, _, _, _, err := parseJournalUUIDs(input); err != nil {
		return result, err
	}
	journalTable, err := repository.Namespace.Render("budget_journal_events")
	if err != nil {
		return result, err
	}
	bucketTable, err := repository.Namespace.Render("budget_buckets")
	if err != nil {
		return result, err
	}
	reservationTable, err := repository.Namespace.Render("operation_budget_reservations")
	if err != nil {
		return result, err
	}
	now := input.occurredAt
	if repository.Now != nil {
		now = repository.Now()
	}
	relations := journalRelations{journal: journalTable, buckets: bucketTable, reservations: reservationTable}
	err = WithTransaction(ctx, repository.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		result, err = repository.appendTx(ctx, tx, input, relations, now)
		return err
	})
	if err != nil {
		return JournalRecord{}, err
	}
	return result, nil
}

type journalRelations struct {
	journal, buckets, reservations string
}

// appendTx appends one journal event and updates its projections. The caller
// owns the transaction so a higher-level operation (such as exact unknown-cost
// reconciliation) can update its operation row atomically with all events.
func (repository *BudgetJournalRepository) appendTx(ctx context.Context, tx pgx.Tx, input journalInput, relations journalRelations, now time.Time) (result JournalRecord, err error) {
	eventID, generationID, operationID, windowID, err := parseJournalUUIDs(input)
	if err != nil {
		return result, err
	}
	reservedIncrease, err := EncodeUSD(input.reservedIncrease)
	if err != nil {
		return result, fmt.Errorf("encode reserved increase: %w", err)
	}
	reservedDecrease, err := EncodeUSD(input.reservedDecrease)
	if err != nil {
		return result, fmt.Errorf("encode reserved decrease: %w", err)
	}
	accountedIncrease, err := EncodeUSD(input.accountedIncrease)
	if err != nil {
		return result, fmt.Errorf("encode accounted increase: %w", err)
	}
	accountedDecrease, err := EncodeUSD(input.accountedDecrease)
	if err != nil {
		return result, fmt.Errorf("encode accounted decrease: %w", err)
	}
	var actual any
	if input.actual != nil {
		actual, err = EncodeUSD(*input.actual)
		if err != nil {
			return result, fmt.Errorf("encode actual cost: %w", err)
		}
	}
	unknownReason := nullableReason(input.unknownReason)
	var inserted bool
	insertErr := tx.QueryRow(ctx, journalAppendSQL(relations.journal),
		eventID, generationID, operationID, windowID, input.bucketStart.UTC(), input.revision, string(input.kind),
		reservedIncrease, reservedDecrease, accountedIncrease, accountedDecrease, actual, string(input.costStatus), unknownReason, input.occurredAt.UTC()).Scan(&result.JournalID, &result.EventID, &inserted)
	if errors.Is(insertErr, pgx.ErrNoRows) {
		return result, ErrBudgetJournalConflict
	}
	if isPostgresUniqueViolation(insertErr) {
		return result, ErrBudgetJournalConflict
	}
	if insertErr != nil {
		return result, redactPostgresError(fmt.Errorf("append budget journal event: %w", insertErr))
	}
	result.Existing = !inserted
	if result.Existing {
		return result, nil
	}
	if _, err := tx.Exec(ctx, budgetBucketUpsertSQL(relations.buckets),
		windowID, input.bucketStart.UTC(), reservedIncrease, accountedIncrease, result.JournalID, reservedDecrease, accountedDecrease); err != nil {
		return result, redactPostgresError(fmt.Errorf("update budget bucket projection: %w", err))
	}
	if input.reserve {
		tag, err := tx.Exec(ctx, reservationAppendSQL(relations.reservations),
			operationID, windowID, input.bucketStart.UTC(), reservedIncrease, input.revision, result.JournalID, now.UTC())
		if err != nil {
			return result, redactPostgresError(fmt.Errorf("insert budget reservation projection: %w", err))
		}
		if tag.RowsAffected() != 1 {
			return result, ErrBudgetJournalConflict
		}
		return result, nil
	}
	state, basis, finalizedAt := completionProjection(input)
	tag, err := tx.Exec(ctx, reservationCompletionSQL(relations.reservations, input.kind),
		operationID, windowID, state, actual, string(input.costStatus), unknownReason, journalCharge(input), basis, input.revision, result.JournalID, finalizedAt)
	if err != nil {
		return result, redactPostgresError(fmt.Errorf("update budget reservation projection: %w", err))
	}
	if tag.RowsAffected() != 1 {
		return result, ErrBudgetJournalMissingReservation
	}
	return result, nil
}

// journalAppendSQL is deliberately one write statement. A retry may update a
// row only when both unique identities identify that same event; its returned
// system-column flag distinguishes that harmless idempotency update from a new
// append. A conflict that does not return a row is a mismatched event identity.
func journalAppendSQL(journalTable string) string {
	return "INSERT INTO " + journalTable +
		" (event_id, redis_generation_id, operation_id, window_id, bucket_start, reservation_revision, event_kind," +
		" reserved_increase_usd, reserved_decrease_usd, accounted_increase_usd, accounted_decrease_usd," +
		" actual_cost_usd, actual_cost_status, actual_cost_unknown_reason_code, occurred_at)" +
		" VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)" +
		" ON CONFLICT (operation_id, window_id, reservation_revision) DO UPDATE SET event_id = " + journalTable + ".event_id" +
		" WHERE " + journalTable + ".event_id = EXCLUDED.event_id" +
		" AND " + journalTable + ".redis_generation_id = EXCLUDED.redis_generation_id" +
		" AND " + journalTable + ".operation_id = EXCLUDED.operation_id" +
		" AND " + journalTable + ".window_id = EXCLUDED.window_id" +
		" AND " + journalTable + ".bucket_start = EXCLUDED.bucket_start" +
		" AND " + journalTable + ".reservation_revision = EXCLUDED.reservation_revision" +
		" AND " + journalTable + ".event_kind = EXCLUDED.event_kind" +
		" AND " + journalTable + ".reserved_increase_usd = EXCLUDED.reserved_increase_usd" +
		" AND " + journalTable + ".reserved_decrease_usd = EXCLUDED.reserved_decrease_usd" +
		" AND " + journalTable + ".accounted_increase_usd = EXCLUDED.accounted_increase_usd" +
		" AND " + journalTable + ".accounted_decrease_usd = EXCLUDED.accounted_decrease_usd" +
		" AND " + journalTable + ".actual_cost_usd IS NOT DISTINCT FROM EXCLUDED.actual_cost_usd" +
		" AND " + journalTable + ".actual_cost_status = EXCLUDED.actual_cost_status" +
		" AND " + journalTable + ".actual_cost_unknown_reason_code IS NOT DISTINCT FROM EXCLUDED.actual_cost_unknown_reason_code" +
		" AND " + journalTable + ".occurred_at = EXCLUDED.occurred_at" +
		" RETURNING journal_id, event_id, (xmax = 0) AS inserted"
}

func isPostgresUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func budgetBucketUpsertSQL(bucketTable string) string {
	return "INSERT INTO " + bucketTable +
		" (window_id, bucket_start, reserved_cost_usd, accounted_cost_usd, last_journal_id)" +
		" VALUES ($1,$2,$3,$4,$5) ON CONFLICT (window_id, bucket_start) DO UPDATE SET" +
		" reserved_cost_usd = " + bucketTable + ".reserved_cost_usd + EXCLUDED.reserved_cost_usd - $6," +
		" accounted_cost_usd = " + bucketTable + ".accounted_cost_usd + EXCLUDED.accounted_cost_usd - $7," +
		" last_journal_id = GREATEST(" + bucketTable + ".last_journal_id, EXCLUDED.last_journal_id), updated_at = clock_timestamp()"
}

func reservationAppendSQL(reservationTable string) string {
	return "INSERT INTO " + reservationTable +
		" (operation_id, window_id, bucket_start, state, reserved_cost_usd, actual_cost_status," +
		" budget_charge_usd, budget_charge_basis, reservation_revision, last_journal_id, created_at)" +
		" VALUES ($1,$2,$3,'reserved',$4,'pending',0,'reserved',$5,$6,$7) ON CONFLICT (operation_id, window_id) DO NOTHING"
}

func reservationCompletionSQL(reservationTable string, kind budget.JournalEventKind) string {
	allowedStates := "'reserved'"
	if kind == budget.JournalResolveUnknownExact {
		// JournalFinalizeUnknown leaves a reservation finalized while retaining
		// its conservative bound.  It is still eligible for one authoritative
		// exact-cost resolution.  Keep finalized exact rows fenced so a new
		// resolve revision cannot rewrite an already-settled reservation; an
		// idempotent replay returns before this projection update is reached.
		allowedStates = "'reserved', 'retained_ambiguous', 'finalized'"
	}
	stateFence := "state IN (" + allowedStates + ")"
	if kind == budget.JournalResolveUnknownExact {
		stateFence += " AND (state <> 'finalized' OR actual_cost_status = 'unknown')"
	}
	return "UPDATE " + reservationTable + " SET state=$3, actual_cost_usd=$4, actual_cost_status=$5," +
		" actual_cost_unknown_reason_code=$6, budget_charge_usd=CASE WHEN $7::numeric IS NULL THEN reserved_cost_usd ELSE $7::numeric END, budget_charge_basis=$8," +
		" reservation_revision=$9, last_journal_id=$10, finalized_at=$11" +
		" WHERE operation_id=$1 AND window_id=$2 AND reservation_revision < $9 AND " + stateFence
}

func parseJournalUUIDs(input journalInput) (uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, error) {
	eventID, err := uuid.Parse(input.eventID)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("event ID: %w", err)
	}
	generationID, err := uuid.Parse(input.generationID)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("generation ID: %w", err)
	}
	operationID, err := uuid.Parse(input.operationID)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("operation ID: %w", err)
	}
	windowID, err := uuid.Parse(input.windowID)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("window ID: %w", err)
	}
	return eventID, generationID, operationID, windowID, nil
}

func completionProjection(input journalInput) (state, basis string, finalizedAt any) {
	switch input.kind {
	case budget.JournalRetainAmbiguous:
		return "retained_ambiguous", "retained_bound", input.occurredAt.UTC()
	case budget.JournalRelease:
		return "released", "released", input.occurredAt.UTC()
	case budget.JournalFinalizeUnknown:
		return "finalized", "retained_bound", input.occurredAt.UTC()
	default:
		return "finalized", "exact_actual", input.occurredAt.UTC()
	}
}

// journalCharge is passed as a nullable numeric value. Unknown/ambiguous
// transitions use NULL so SQL retains the original reservation bound; exact
// and release transitions pass their explicit actual amount.
func journalCharge(input journalInput) any {
	if input.kind == budget.JournalRetainAmbiguous || input.kind == budget.JournalFinalizeUnknown {
		return nil
	}
	if input.actual == nil {
		return "0.000000000000000000"
	}
	value, _ := EncodeUSD(*input.actual)
	return value
}

func nullableReason(value string) any {
	if value == "" {
		return nil
	}
	return value
}
