package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// TestBudgetJournalAppendReplayAndFinalize proves the write-only journal
// boundary against PostgreSQL. Redis acceptance and provider dispatch are
// deliberately outside this test: those runtime composition steps remain a
// separate production task. The test instead verifies the durable contract
// after a reservation has already been accepted.
func TestBudgetJournalAppendReplayAndFinalize(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	// Begin supplies the operation and configuration foreign keys without
	// bypassing the operation repository's encrypted request envelope.
	operationKey := "budget-journal-integration-" + uuid.NewString()
	configDigest := sha256.Sum256([]byte(operationKey))
	started, err := repository.Begin(ctx, admission.BeginRequest{
		ID:              operationKey,
		ScopeKey:        "budget-journal/fixtures",
		RequestDigest:   admission.Digest([]byte(operationKey)),
		ReservationUSD:  pricing.MustUSD("0"),
		ConfigVersion:   operationKey,
		ConfigDigest:    configDigest,
		ExpiresAt:       time.Now().UTC().Add(time.Hour),
		RequestManifest: []byte(`{"model":"fixture"}`),
	})
	if err != nil {
		t.Fatalf("begin operation: %v", err)
	}
	if started.Existing {
		t.Fatal("new integration operation unexpectedly replayed")
	}
	scope, err := repository.Scopes.Ensure(ctx, "budget-journal", "fixtures")
	if err != nil {
		t.Fatalf("ensure budget scope: %v", err)
	}

	generationID := uuid.New()
	policyID := uuid.New()
	windowID := uuid.New()
	selectorDigest := sha256.Sum256([]byte(operationKey + ":selector"))
	now := time.Now().UTC().Truncate(time.Microsecond)
	bucketStart := now.Truncate(time.Hour)
	if err := insertBudgetJournalFixtures(ctx, repository, scope.ID, generationID, policyID, windowID, configDigest, selectorDigest, now); err != nil {
		t.Fatalf("insert budget journal fixtures: %v", err)
	}

	amount := pricing.MustUSD("1.250000000000000000")
	reserveEvent := budget.ReservationEvent{
		EventID:             uuid.NewString(),
		GenerationID:        generationID.String(),
		OperationID:         operationUUID(operationKey).String(),
		WindowID:            windowID.String(),
		BucketStart:         bucketStart,
		ReservationRevision: 1,
		AmountUSD:           amount,
		OccurredAt:          now,
	}
	journal := &BudgetJournalRepository{Pool: repository.Pool, Namespace: repository.Namespace}
	first, err := journal.AppendReservation(ctx, reserveEvent)
	if err != nil {
		t.Fatalf("append reservation: %v", err)
	}
	if first.Existing || first.JournalID == 0 || first.EventID != uuid.MustParse(reserveEvent.EventID) {
		t.Fatalf("first reservation append = %#v", first)
	}
	replay, err := journal.AppendReservation(ctx, reserveEvent)
	if err != nil {
		t.Fatalf("replay reservation: %v", err)
	}
	if !replay.Existing || replay.JournalID != first.JournalID || replay.EventID != first.EventID {
		t.Fatalf("reservation replay = %#v, first=%#v", replay, first)
	}
	changed := reserveEvent
	changed.AmountUSD = pricing.MustUSD("1.500000000000000000")
	if _, err := journal.AppendReservation(ctx, changed); !errors.Is(err, ErrBudgetJournalConflict) {
		t.Fatalf("changed reservation payload error = %v, want ErrBudgetJournalConflict", err)
	}

	actual := pricing.MustUSD("0.250000000000000000")
	completion := budget.CompletionEvent{
		EventID:              uuid.NewString(),
		GenerationID:         reserveEvent.GenerationID,
		OperationID:          reserveEvent.OperationID,
		WindowID:             reserveEvent.WindowID,
		BucketStart:          bucketStart,
		ReservationRevision:  2,
		Kind:                 budget.JournalFinalizeExact,
		ReservedDecreaseUSD:  amount,
		AccountedIncreaseUSD: actual,
		ActualCostUSD:        &actual,
		CostStatus:           budget.CostExact,
		OccurredAt:           now.Add(time.Second),
	}
	finalized, err := journal.AppendCompletion(ctx, completion)
	if err != nil {
		t.Fatalf("append exact completion: %v", err)
	}
	if finalized.Existing || finalized.JournalID <= first.JournalID {
		t.Fatalf("completion append = %#v, reservation=%#v", finalized, first)
	}

	assertBudgetJournalProjection(t, ctx, repository, journal, operationUUID(operationKey), windowID, bucketStart, first.JournalID, finalized.JournalID)
}

// TestBudgetJournalResolveFinalizedUnknown proves that an unknown cost which
// already finalized its reservation bound can still be reconciled to the
// authoritative provider amount.  This is distinct from an ambiguous
// reservation: JournalFinalizeUnknown leaves state=finalized and retains the
// bound in the accounted projection.
func TestBudgetJournalResolveFinalizedUnknown(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	operationKey := "budget-journal-resolve-unknown-" + uuid.NewString()
	configDigest := sha256.Sum256([]byte(operationKey))
	now := time.Now().UTC().Truncate(time.Microsecond)
	started, err := repository.Begin(ctx, admission.BeginRequest{
		ID: operationKey, ScopeKey: "budget-journal/resolve", RequestDigest: admission.Digest([]byte(operationKey)),
		ReservationUSD: pricing.MustUSD("0"), ConfigVersion: operationKey, ConfigDigest: configDigest,
		ExpiresAt: now.Add(time.Hour), RequestManifest: []byte(`{"model":"fixture"}`),
	})
	if err != nil || started.Existing {
		t.Fatalf("begin operation = %#v, %v", started, err)
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: operationKey, DispatchToken: started.Operation.DispatchToken}); err != nil {
		t.Fatalf("dispatch operation: %v", err)
	}
	result := &state.BlobRef{Digest: admission.Digest([]byte(operationKey + ":result")), Size: 6, Media: "application/json"}
	if err := repository.Complete(ctx, admission.CompleteRequest{
		OperationID: operationKey, DispatchToken: started.Operation.DispatchToken, ResultRef: result,
		CostStatus: "unknown", UnknownReason: "provider_timeout",
	}); err != nil {
		t.Fatalf("complete unknown operation: %v", err)
	}

	scope, err := repository.Scopes.Ensure(ctx, "budget-journal", "resolve")
	if err != nil {
		t.Fatalf("ensure budget scope: %v", err)
	}
	generationID, policyID, windowID := uuid.New(), uuid.New(), uuid.New()
	selectorDigest := sha256.Sum256([]byte(operationKey + ":selector"))
	bucketStart := now.Truncate(time.Hour)
	if err := insertBudgetJournalFixtures(ctx, repository, scope.ID, generationID, policyID, windowID, configDigest, selectorDigest, now); err != nil {
		t.Fatalf("insert budget journal fixtures: %v", err)
	}

	amount := pricing.MustUSD("1.250000000000000000")
	operationID := operationUUID(operationKey).String()
	reserve := budget.ReservationEvent{
		EventID: uuid.NewString(), GenerationID: generationID.String(), OperationID: operationID, WindowID: windowID.String(),
		BucketStart: bucketStart, ReservationRevision: 1, AmountUSD: amount, OccurredAt: now,
	}
	journal := &BudgetJournalRepository{Pool: repository.Pool, Namespace: repository.Namespace}
	reserved, err := journal.AppendReservation(ctx, reserve)
	if err != nil {
		t.Fatalf("append reservation: %v", err)
	}
	if reserved.Existing || reserved.JournalID == 0 {
		t.Fatalf("reservation journal record = %#v", reserved)
	}
	unknown := budget.CompletionEvent{
		EventID: uuid.NewString(), GenerationID: reserve.GenerationID, OperationID: operationID, WindowID: reserve.WindowID,
		BucketStart: bucketStart, ReservationRevision: 2, Kind: budget.JournalFinalizeUnknown,
		ReservedDecreaseUSD: amount, AccountedIncreaseUSD: amount, UnknownReasonCode: "provider_timeout",
		CostStatus: budget.CostUnknown, OccurredAt: now.Add(time.Second),
	}
	unknownRecord, err := journal.AppendCompletion(ctx, unknown)
	if err != nil {
		t.Fatalf("append unknown completion: %v", err)
	}

	actual := pricing.MustUSD("0.250000000000000000")
	resolve := budget.CompletionEvent{
		EventID: uuid.NewString(), GenerationID: reserve.GenerationID, OperationID: operationID, WindowID: reserve.WindowID,
		BucketStart: bucketStart, ReservationRevision: 3, Kind: budget.JournalResolveUnknownExact,
		// FinalizeUnknown already moved the full bound into accounted_cost_usd;
		// remove that bound before adding the authoritative exact amount.
		AccountedDecreaseUSD: amount, AccountedIncreaseUSD: actual, ActualCostUSD: &actual,
		CostStatus: budget.CostExact, OccurredAt: now.Add(2 * time.Second),
	}
	resolved, err := journal.ResolveUnknownExact(ctx, []budget.CompletionEvent{resolve})
	if err != nil {
		t.Fatalf("resolve finalized unknown cost: %v", err)
	}
	if len(resolved) != 1 || resolved[0].Existing || resolved[0].JournalID <= unknownRecord.JournalID {
		t.Fatalf("resolved journal record = %#v, unknown=%#v", resolved, unknownRecord)
	}
	if _, err := journal.ResolveUnknownExact(ctx, []budget.CompletionEvent{resolve}); err != nil {
		t.Fatalf("idempotent unknown-cost resolution: %v", err)
	}

	completed, err := repository.Get(ctx, operationKey)
	if err != nil || completed.ActualCostUSD == nil || completed.ActualCostUSD.Cmp(actual) != 0 {
		t.Fatalf("operation exact cost = %#v, %v", completed, err)
	}
	bucketTable, err := journal.Namespace.Render("budget_buckets")
	if err != nil {
		t.Fatal(err)
	}
	var reservedCost, accountedCost string
	if err := repository.Pool.QueryRow(ctx, "SELECT reserved_cost_usd::text, accounted_cost_usd::text FROM "+bucketTable+" WHERE window_id=$1 AND bucket_start=$2", windowID, bucketStart).Scan(&reservedCost, &accountedCost); err != nil {
		t.Fatalf("read resolved bucket: %v", err)
	}
	if reservedCost != "0.000000000000000000" || accountedCost != actual.String() {
		t.Fatalf("resolved bucket = reserved %s accounted %s, want zero/%s", reservedCost, accountedCost, actual.String())
	}
	reservationTable, err := journal.Namespace.Render("operation_budget_reservations")
	if err != nil {
		t.Fatal(err)
	}
	var state, status, basis, charge string
	if err := repository.Pool.QueryRow(ctx, "SELECT state, actual_cost_status, budget_charge_basis, budget_charge_usd::text FROM "+reservationTable+" WHERE operation_id=$1 AND window_id=$2", operationUUID(operationKey), windowID).Scan(&state, &status, &basis, &charge); err != nil {
		t.Fatalf("read resolved reservation: %v", err)
	}
	if state != "finalized" || status != "exact" || basis != "exact_actual" || charge != actual.String() {
		t.Fatalf("resolved reservation = state=%s status=%s basis=%s charge=%s, want finalized/exact/exact_actual/%s", state, status, basis, charge, actual.String())
	}
}

func insertBudgetJournalFixtures(ctx context.Context, repository OperationRepository, scopeID, generationID, policyID, windowID uuid.UUID, configDigest, selectorDigest [32]byte, now time.Time) error {
	generation, err := repository.Namespace.Render("budget_redis_generations")
	if err != nil {
		return err
	}
	policy, err := repository.Namespace.Render("budget_policies")
	if err != nil {
		return err
	}
	window, err := repository.Namespace.Render("budget_windows")
	if err != nil {
		return err
	}
	manifestDigest := sha256.Sum256([]byte(generationID.String()))
	// The generation is only a foreign-key fixture; keep it superseded so
	// repeated or parallel integration runs never contend on the single active
	// generation slot.
	if _, err := repository.Pool.Exec(ctx, "INSERT INTO "+generation+" (generation_id, reason, state, source_journal_id, coverage_start, coverage_end, manifest_digest, completed_at) VALUES ($1,'initial_cold_start','superseded',0,$2,$3,$4,$3)", generationID, now.Add(-time.Hour), now.Add(time.Hour), manifestDigest[:]); err != nil {
		return err
	}
	if _, err := repository.Pool.Exec(ctx, "INSERT INTO "+policy+" (policy_id, scope_id, policy_key, config_digest, selector_digest, sanitized_selector, priority, enabled, effective_from) VALUES ($1,$2,$3,$4,$5,'{}'::jsonb,1,true,$6)", policyID, scopeID, generationID.String(), configDigest[:], selectorDigest[:], now.Add(-time.Hour)); err != nil {
		return err
	}
	_, err = repository.Pool.Exec(ctx, "INSERT INTO "+window+" (window_id, policy_id, window_key, duration_seconds, bucket_seconds, limit_usd) VALUES ($1,$2,'hour',3600,60,10.000000000000000000)", windowID, policyID)
	return err
}

func assertBudgetJournalProjection(t *testing.T, ctx context.Context, repository OperationRepository, journal *BudgetJournalRepository, operationID, windowID uuid.UUID, bucketStart time.Time, reservationJournalID, completionJournalID int64) {
	t.Helper()
	journalTable, err := journal.Namespace.Render("budget_journal_events")
	if err != nil {
		t.Fatal(err)
	}
	var journalCount int
	if err := repository.Pool.QueryRow(ctx, "SELECT count(*) FROM "+journalTable+" WHERE operation_id=$1", operationID).Scan(&journalCount); err != nil {
		t.Fatal(err)
	}
	if journalCount != 2 {
		t.Fatalf("journal rows = %d, want 2 after idempotent replay", journalCount)
	}

	bucketTable, err := journal.Namespace.Render("budget_buckets")
	if err != nil {
		t.Fatal(err)
	}
	var reserved, accounted string
	var lastJournalID int64
	if err := repository.Pool.QueryRow(ctx, "SELECT reserved_cost_usd::text, accounted_cost_usd::text, last_journal_id FROM "+bucketTable+" WHERE window_id=$1 AND bucket_start=$2", windowID, bucketStart).Scan(&reserved, &accounted, &lastJournalID); err != nil {
		t.Fatal(err)
	}
	if reserved != "0.000000000000000000" || accounted != "0.250000000000000000" || lastJournalID != completionJournalID {
		t.Fatalf("bucket projection = reserved %s accounted %s last_journal_id %d, want zero/0.25/%d", reserved, accounted, lastJournalID, completionJournalID)
	}

	reservationTable, err := journal.Namespace.Render("operation_budget_reservations")
	if err != nil {
		t.Fatal(err)
	}
	var state, status, basis, charge string
	var revision, reservationLastJournalID int
	if err := repository.Pool.QueryRow(ctx, "SELECT state, actual_cost_status, budget_charge_basis, budget_charge_usd::text, reservation_revision, last_journal_id FROM "+reservationTable+" WHERE operation_id=$1 AND window_id=$2", operationID, windowID).Scan(&state, &status, &basis, &charge, &revision, &reservationLastJournalID); err != nil {
		t.Fatal(err)
	}
	if state != "finalized" || status != "exact" || basis != "exact_actual" || charge != "0.250000000000000000" || revision != 2 || int64(reservationLastJournalID) != completionJournalID {
		t.Fatalf("reservation projection = state=%s status=%s basis=%s charge=%s revision=%d journal=%d, want finalized/exact/exact_actual/0.25/2/%d", state, status, basis, charge, revision, reservationLastJournalID, completionJournalID)
	}
	if reservationJournalID >= completionJournalID {
		t.Fatalf("journal IDs are not increasing: reservation=%d completion=%d", reservationJournalID, completionJournalID)
	}
}
