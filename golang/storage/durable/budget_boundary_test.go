package durable

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

type boundaryMaterializer struct {
	result       ReserveResult
	acceptErr    error
	reconcileErr error
	calls        []string
}

func (m *boundaryMaterializer) Accept(_ context.Context, _ ReserveRequest) (ReserveResult, error) {
	m.calls = append(m.calls, "accept")
	return m.result, m.acceptErr
}

func (m *boundaryMaterializer) Reconcile(_ context.Context, _ ReconcileRequest) error {
	m.calls = append(m.calls, "reconcile")
	return m.reconcileErr
}

type boundaryJournal struct {
	reservations []budget.ReservationEvent
	completions  []budget.CompletionEvent
	failReserve  int
	failComplete int
	calls        *[]string
}

func (j *boundaryJournal) AppendReservation(_ context.Context, event budget.ReservationEvent) (postgresstore.JournalRecord, error) {
	*j.calls = append(*j.calls, "reservation:"+event.EventID)
	j.reservations = append(j.reservations, event)
	if j.failReserve > 0 && len(j.reservations) == j.failReserve {
		return postgresstore.JournalRecord{}, errors.New("reservation journal unavailable")
	}
	return postgresstore.JournalRecord{JournalID: int64(len(j.reservations))}, nil
}

func (j *boundaryJournal) AppendCompletion(_ context.Context, event budget.CompletionEvent) (postgresstore.JournalRecord, error) {
	*j.calls = append(*j.calls, "completion:"+event.EventID)
	j.completions = append(j.completions, event)
	if j.failComplete > 0 && len(j.completions) == j.failComplete {
		return postgresstore.JournalRecord{}, errors.New("completion journal unavailable")
	}
	return postgresstore.JournalRecord{JournalID: int64(len(j.completions))}, nil
}

func boundaryRequest(now time.Time) ReserveRequest {
	return ReserveRequest{
		OperationID:  "operation-1",
		GenerationID: "generation-1",
		ExpiresAt:    now.Add(time.Hour),
		Reservations: []admission.WindowReservation{{
			PolicyID: "policy", WindowID: "window", Bucket: now.Unix() / 60,
			AmountUSD: pricing.MustUSD("0.10"), LimitUSD: pricing.MustUSD("1.00"),
			BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour),
		}},
	}
}

func boundaryAcceptedResult(request ReserveRequest, now time.Time) ReserveResult {
	return ReserveResult{
		OperationID: request.OperationID, Accepted: true, GenerationID: request.GenerationID,
		IncarnationID: "incarnation-1",
		Events: []budget.ReservationEvent{{
			EventID: "reservation-event-1", GenerationID: string(request.GenerationID), OperationID: string(request.OperationID),
			WindowID: "window", BucketStart: time.Unix((now.Unix()/60)*60, 0).UTC(),
			ReservationRevision: 1, AmountUSD: pricing.MustUSD("0.10"), OccurredAt: now,
		}},
	}
}

func boundaryCompletion(request ReserveRequest, now time.Time) budget.CompletionEvent {
	cost := pricing.MustUSD("0.07")
	return budget.CompletionEvent{
		EventID: "completion-event-1", GenerationID: string(request.GenerationID), OperationID: string(request.OperationID),
		WindowID: "window", BucketStart: time.Unix((now.Unix()/60)*60, 0).UTC(), ReservationRevision: 2,
		Kind: budget.JournalFinalizeExact, ReservedDecreaseUSD: pricing.MustUSD("0.10"),
		AccountedIncreaseUSD: cost, ActualCostUSD: &cost, CostStatus: budget.CostExact, OccurredAt: now,
	}
}

func newBoundary(materializer BudgetMaterializer, journal Journal) BudgetBoundary {
	return BudgetBoundary{
		Identity: StateIdentity{
			Postgres:     PostgresIdentity{Database: "llmtw", Schema: "worker", TablePrefix: "prod_"},
			Redis:        RedisIdentity{KeyPrefix: "llmtw", HashTag: "admission"},
			ConfigDigest: sha256.Sum256([]byte("snapshot")),
		},
		Materializer: materializer, Journal: journal,
	}
}

func newLifecycle(t *testing.T) *Lifecycle {
	t.Helper()
	lifecycle := new(Lifecycle)
	if err := lifecycle.Advance(PhaseOperationReplay); err != nil {
		t.Fatal(err)
	}
	return lifecycle
}

func advanceDispatched(t *testing.T, lifecycle *Lifecycle) {
	t.Helper()
	for _, phase := range []Phase{PhaseDispatched} {
		if err := lifecycle.Advance(phase); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBudgetBoundaryOrdersJournalBeforeDispatchAndReconcile(t *testing.T) {
	now := time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)
	request := boundaryRequest(now)
	calls := []string{}
	materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now), calls: calls}
	journal := &boundaryJournal{calls: &materializer.calls}
	boundary := newBoundary(materializer, journal)
	lifecycle := newLifecycle(t)
	reservation, err := boundary.Reserve(context.Background(), lifecycle, request)
	if err != nil || !reservation.DispatchReady() {
		t.Fatalf("reserve = %#v, %v", reservation, err)
	}
	if current, _ := lifecycle.Current(); current != PhasePostgresJournaled {
		t.Fatalf("reserve phase = %s", current)
	}
	if !reflect.DeepEqual(materializer.calls, []string{"accept", "reservation:reservation-event-1"}) {
		t.Fatalf("reserve ordering = %#v", materializer.calls)
	}

	advanceDispatched(t, lifecycle)
	if err := boundary.Finalize(context.Background(), lifecycle, reservation, []budget.CompletionEvent{boundaryCompletion(request, now)}); err != nil {
		t.Fatalf("finalize = %v", err)
	}
	if got := materializer.calls; !reflect.DeepEqual(got, []string{"accept", "reservation:reservation-event-1", "completion:completion-event-1", "reconcile"}) {
		t.Fatalf("finalize ordering = %#v", got)
	}
	if current, _ := lifecycle.Current(); current != PhaseRedisReconciled {
		t.Fatalf("finalize phase = %s", current)
	}
}

func TestBudgetBoundaryJournalsEveryAcceptedEventInOrder(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	result := boundaryAcceptedResult(request, now)
	second := result.Events[0]
	second.EventID = "reservation-event-2"
	second.WindowID = "window-2"
	result.Events = append(result.Events, second)
	materializer := &boundaryMaterializer{result: result}
	journal := &boundaryJournal{calls: &materializer.calls}
	boundary := newBoundary(materializer, journal)
	reservation, err := boundary.Reserve(context.Background(), newLifecycle(t), request)
	if err != nil || !reservation.DispatchReady() {
		t.Fatalf("multi-event reserve = %#v, %v", reservation, err)
	}
	if !reflect.DeepEqual(materializer.calls, []string{"accept", "reservation:reservation-event-1", "reservation:reservation-event-2"}) {
		t.Fatalf("multi-event ordering = %#v", materializer.calls)
	}
}

func TestBudgetBoundaryPreflightsDuplicateReservationIDs(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	result := boundaryAcceptedResult(request, now)
	duplicate := result.Events[0]
	result.Events = append(result.Events, duplicate)
	materializer := &boundaryMaterializer{result: result}
	journal := &boundaryJournal{calls: &materializer.calls}
	boundary := newBoundary(materializer, journal)
	reservation, err := boundary.Reserve(context.Background(), newLifecycle(t), request)
	if !errors.Is(err, ErrJournalPending) || len(journal.reservations) != 0 || reservation.PostgresRecoveryRequired {
		t.Fatalf("duplicate reservation preflight = %#v, %v, journal=%#v", reservation, err, journal.reservations)
	}
}

func TestBudgetBoundaryMarksPartialPostgresJournalRecovery(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	result := boundaryAcceptedResult(request, now)
	second := result.Events[0]
	second.EventID = "reservation-event-2"
	second.WindowID = "window-2"
	result.Events = append(result.Events, second)
	materializer := &boundaryMaterializer{result: result}
	journal := &boundaryJournal{failReserve: 2, calls: &materializer.calls}
	boundary := newBoundary(materializer, journal)
	reservation, err := boundary.Reserve(context.Background(), newLifecycle(t), request)
	if !errors.Is(err, ErrJournalPending) || !reservation.PostgresRecoveryRequired || len(reservation.JournalRecords) != 1 || reservation.DispatchReady() {
		t.Fatalf("partial journal state = %#v, %v", reservation, err)
	}
}

func TestBudgetBoundaryDenialDoesNotJournal(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	calls := []string{}
	materializer := &boundaryMaterializer{result: ReserveResult{OperationID: request.OperationID, GenerationID: request.GenerationID}, calls: calls}
	journal := &boundaryJournal{calls: &materializer.calls}
	boundary := newBoundary(materializer, journal)
	reservation, err := boundary.Reserve(context.Background(), newLifecycle(t), request)
	if err != nil || reservation.DispatchReady() {
		t.Fatalf("denial = %#v, %v", reservation, err)
	}
	if !reflect.DeepEqual(materializer.calls, []string{"accept"}) {
		t.Fatalf("denial side effects = %#v", materializer.calls)
	}
}

func TestBudgetBoundaryStopsOnReservationJournalFailure(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	calls := []string{}
	materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now), calls: calls}
	journal := &boundaryJournal{failReserve: 1, calls: &materializer.calls}
	boundary := newBoundary(materializer, journal)
	lifecycle := newLifecycle(t)
	reservation, err := boundary.Reserve(context.Background(), lifecycle, request)
	if !errors.Is(err, ErrJournalPending) || reservation.DispatchReady() {
		t.Fatalf("journal failure = %#v, %v", reservation, err)
	}
	if len(materializer.calls) != 3 || materializer.calls[1] != "reservation:reservation-event-1" || materializer.calls[2] != "reconcile" {
		t.Fatalf("journal ordering = %#v", materializer.calls)
	}
	if _, ok := lifecycle.Current(); !ok {
		t.Fatal("lifecycle did not retain Redis acceptance phase")
	} else if current, _ := lifecycle.Current(); current != PhaseRedisAccepted {
		t.Fatalf("journal failure phase = %s", current)
	}
	journal.failReserve = 0
	if _, err := boundary.Reserve(context.Background(), lifecycle, request); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("reservation retry after cleanup = %v", err)
	}
}

func TestBudgetBoundaryCompletionJournalFailurePreventsReconcile(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now)}
	calls := &materializer.calls
	journal := &boundaryJournal{failComplete: 1, calls: calls}
	boundary := newBoundary(materializer, journal)
	lifecycle := newLifecycle(t)
	reservation, err := boundary.Reserve(context.Background(), lifecycle, request)
	if err != nil {
		t.Fatal(err)
	}
	advanceDispatched(t, lifecycle)
	err = boundary.Finalize(context.Background(), lifecycle, reservation, []budget.CompletionEvent{boundaryCompletion(request, now)})
	if !errors.Is(err, ErrJournalPending) {
		t.Fatalf("completion journal error = %v", err)
	}
	if current, _ := lifecycle.Current(); current != PhaseDispatched {
		t.Fatalf("completion journal phase = %s", current)
	}
	if len(materializer.calls) != 3 || materializer.calls[2] != "completion:completion-event-1" {
		t.Fatalf("reconcile was called after completion journal failure: %#v", materializer.calls)
	}
}

func TestBudgetBoundaryReconcileFailureIsRetryableAfterPostgres(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now), reconcileErr: errors.New("Redis unavailable")}
	journal := &boundaryJournal{calls: &materializer.calls}
	boundary := newBoundary(materializer, journal)
	lifecycle := newLifecycle(t)
	reservation, err := boundary.Reserve(context.Background(), lifecycle, request)
	if err != nil {
		t.Fatal(err)
	}
	advanceDispatched(t, lifecycle)
	err = boundary.Finalize(context.Background(), lifecycle, reservation, []budget.CompletionEvent{boundaryCompletion(request, now)})
	if !errors.Is(err, ErrReconcilePending) {
		t.Fatalf("reconcile error = %v", err)
	}
	if current, _ := lifecycle.Current(); current != PhasePostgresFinalized {
		t.Fatalf("reconcile failure phase = %s", current)
	}
	changed := boundaryCompletion(request, now)
	changed.EventID = "completion-event-changed"
	changed.AccountedIncreaseUSD = pricing.MustUSD("0.06")
	changed.ActualCostUSD = ptr(pricing.MustUSD("0.06"))
	if err := boundary.Finalize(context.Background(), lifecycle, reservation, []budget.CompletionEvent{changed}); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("changed reconciliation batch error = %v", err)
	}
	materializer.reconcileErr = nil
	if err := boundary.Finalize(context.Background(), lifecycle, reservation, []budget.CompletionEvent{boundaryCompletion(request, now)}); err != nil {
		t.Fatalf("reconciliation retry = %v", err)
	}
	if len(journal.completions) != 1 {
		t.Fatalf("reconciliation retry duplicated completion journal: %#v", journal.completions)
	}
}

func TestBudgetBoundaryReportsReleaseCleanupPending(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now), reconcileErr: errors.New("Redis unavailable")}
	journal := &boundaryJournal{failReserve: 1, calls: &materializer.calls}
	boundary := newBoundary(materializer, journal)
	reservation, err := boundary.Reserve(context.Background(), newLifecycle(t), request)
	if !errors.Is(err, ErrJournalPending) || !reservation.ReleasePending || len(reservation.ReleaseEvents) != 1 || reservation.DispatchReady() {
		t.Fatalf("release cleanup result = %#v, %v", reservation, err)
	}
}

func TestBudgetBoundaryRejectsCompletionIdentityMismatch(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now)}
	journal := &boundaryJournal{calls: &materializer.calls}
	boundary := newBoundary(materializer, journal)
	lifecycle := newLifecycle(t)
	reservation, err := boundary.Reserve(context.Background(), lifecycle, request)
	if err != nil {
		t.Fatal(err)
	}
	advanceDispatched(t, lifecycle)
	completion := boundaryCompletion(request, now)
	completion.OperationID = "other-operation"
	if err := boundary.Finalize(context.Background(), lifecycle, reservation, []budget.CompletionEvent{completion}); err == nil {
		t.Fatal("identity-mismatched completion accepted")
	}
	if len(journal.completions) != 0 || len(materializer.calls) != 2 {
		t.Fatalf("mismatched completion side effects: journal=%#v calls=%#v", journal.completions, materializer.calls)
	}
}

func TestBudgetBoundaryPreflightsDuplicateCompletionIDs(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now)}
	journal := &boundaryJournal{calls: &materializer.calls}
	boundary := newBoundary(materializer, journal)
	lifecycle := newLifecycle(t)
	reservation, err := boundary.Reserve(context.Background(), lifecycle, request)
	if err != nil {
		t.Fatal(err)
	}
	advanceDispatched(t, lifecycle)
	completion := boundaryCompletion(request, now)
	if err := boundary.Finalize(context.Background(), lifecycle, reservation, []budget.CompletionEvent{completion, completion}); !errors.Is(err, ErrBudgetBoundaryInvalid) {
		t.Fatalf("duplicate completion accepted: %v", err)
	}
	if len(journal.completions) != 0 {
		t.Fatalf("duplicate completion was partially journaled: %#v", journal.completions)
	}
}

func TestBudgetBoundaryRejectsTypedNilPorts(t *testing.T) {
	var materializer *boundaryMaterializer
	var journal *boundaryJournal
	boundary := newBoundary(materializer, journal)
	if err := boundary.Validate(); !errors.Is(err, ErrBudgetBoundaryInvalid) {
		t.Fatalf("typed nil ports accepted: %v", err)
	}
}

func TestBudgetBoundaryRejectsWrongAcceptanceIdentityAndPhase(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	result := boundaryAcceptedResult(request, now)
	result.GenerationID = "other-generation"
	materializer := &boundaryMaterializer{result: result}
	journal := &boundaryJournal{calls: &materializer.calls}
	boundary := newBoundary(materializer, journal)
	if _, err := boundary.Reserve(context.Background(), newLifecycle(t), request); err == nil {
		t.Fatal("acceptance from another generation accepted")
	}
	// A reload must use a fresh lifecycle; a boundary cannot apply a second
	// acceptance to a lifecycle that already crossed the Redis handoff.
	materializer.result = boundaryAcceptedResult(request, now)
	lifecycle := newLifecycle(t)
	if _, err := boundary.Reserve(context.Background(), lifecycle, request); err != nil {
		t.Fatal(err)
	}
	if _, err := boundary.Reserve(context.Background(), lifecycle, request); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("second acceptance phase error = %v", err)
	}
}

func TestBudgetBoundaryBindsIncarnationAcrossReservationRetry(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now)}
	journal := &boundaryJournal{failReserve: 1, calls: &materializer.calls}
	boundary := newBoundary(materializer, journal)
	lifecycle := newLifecycle(t)
	if _, err := boundary.Reserve(context.Background(), lifecycle, request); !errors.Is(err, ErrJournalPending) {
		t.Fatalf("initial journal failure = %v", err)
	}
	journal.failReserve = 0
	materializer.result.IncarnationID = "different-incarnation"
	if _, err := boundary.Reserve(context.Background(), lifecycle, request); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("reservation retry after cleanup = %v", err)
	}
}
