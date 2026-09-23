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
)

type boundaryMaterializer struct {
	result       ReserveResult
	acceptErr    error
	claimErr     error
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

func (m *boundaryMaterializer) Claim(_ context.Context, request ClaimRequest) (ClaimReceipt, error) {
	m.calls = append(m.calls, "claim")
	return ClaimReceipt{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: request.IncarnationID}, m.claimErr
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

func newBoundary(materializer BudgetLeaser) BudgetBoundary {
	return BudgetBoundary{
		Identity: StateIdentity{
			Postgres:     PostgresIdentity{Database: "llmtw", Schema: "worker", TablePrefix: "prod_"},
			Redis:        RedisIdentity{KeyPrefix: "llmtw", HashTag: "admission"},
			ConfigDigest: sha256.Sum256([]byte("snapshot")),
		},
		Materializer: materializer,
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

func TestBudgetBoundaryOrdersClaimBeforeDispatchAndReconcile(t *testing.T) {
	now := time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)
	request := boundaryRequest(now)
	calls := []string{}
	materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now), calls: calls}

	boundary := newBoundary(materializer)
	lifecycle := newLifecycle(t)
	reservation, err := boundary.Reserve(context.Background(), lifecycle, request)
	if err != nil || reservation.DispatchReady() {
		t.Fatalf("reserve = %#v, %v", reservation, err)
	}
	if current, _ := lifecycle.Current(); current != PhaseRedisAccepted {
		t.Fatalf("reserve phase = %s", current)
	}
	if !reflect.DeepEqual(materializer.calls, []string{"accept"}) {
		t.Fatalf("reserve ordering = %#v", materializer.calls)
	}

	if _, err := boundary.Claim(context.Background(), lifecycle, &reservation); err != nil {
		t.Fatal(err)
	}
	advanceDispatched(t, lifecycle)
	if err := boundary.Finalize(context.Background(), lifecycle, reservation, []budget.CompletionEvent{boundaryCompletion(request, now)}); err != nil {
		t.Fatalf("finalize = %v", err)
	}
	if got := materializer.calls; !reflect.DeepEqual(got, []string{"accept", "claim", "reconcile"}) {
		t.Fatalf("finalize ordering = %#v", got)
	}
	if current, _ := lifecycle.Current(); current != PhaseRedisReconciled {
		t.Fatalf("finalize phase = %s", current)
	}
}

func TestBudgetBoundaryPreflightsDuplicateReservationIDs(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	result := boundaryAcceptedResult(request, now)
	duplicate := result.Events[0]
	result.Events = append(result.Events, duplicate)
	materializer := &boundaryMaterializer{result: result}

	boundary := newBoundary(materializer)
	reservation, err := boundary.Reserve(context.Background(), newLifecycle(t), request)
	if !errors.Is(err, ErrBudgetBoundaryInvalid) || reservation.DispatchReady() {
		t.Fatalf("duplicate reservation: %v", err)
	}
}

func TestBudgetBoundaryWaitDoesNotClaim(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	calls := []string{}
	materializer := &boundaryMaterializer{result: ReserveResult{OperationID: request.OperationID, GenerationID: request.GenerationID}, calls: calls}

	boundary := newBoundary(materializer)
	reservation, err := boundary.Reserve(context.Background(), newLifecycle(t), request)
	if err != nil || reservation.DispatchReady() {
		t.Fatalf("denial = %#v, %v", reservation, err)
	}
	if !reflect.DeepEqual(materializer.calls, []string{"accept"}) {
		t.Fatalf("denial side effects = %#v", materializer.calls)
	}
}

func TestBudgetBoundaryReconcileFailureIsRetryableAfterFinalization(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now), reconcileErr: errors.New("Redis unavailable")}

	boundary := newBoundary(materializer)
	lifecycle := newLifecycle(t)
	reservation, err := boundary.Reserve(context.Background(), lifecycle, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boundary.Claim(context.Background(), lifecycle, &reservation); err != nil {
		t.Fatal(err)
	}
	advanceDispatched(t, lifecycle)
	err = boundary.Finalize(context.Background(), lifecycle, reservation, []budget.CompletionEvent{boundaryCompletion(request, now)})
	if !errors.Is(err, ErrReconcilePending) {
		t.Fatalf("reconcile error = %v", err)
	}
	if current, _ := lifecycle.Current(); current != PhaseResultFinalized {
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
}

func TestBudgetBoundaryRejectsCompletionIdentityMismatch(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now)}

	boundary := newBoundary(materializer)
	lifecycle := newLifecycle(t)
	reservation, err := boundary.Reserve(context.Background(), lifecycle, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boundary.Claim(context.Background(), lifecycle, &reservation); err != nil {
		t.Fatal(err)
	}
	advanceDispatched(t, lifecycle)
	completion := boundaryCompletion(request, now)
	completion.OperationID = "other-operation"
	if err := boundary.Finalize(context.Background(), lifecycle, reservation, []budget.CompletionEvent{completion}); err == nil {
		t.Fatal("identity-mismatched completion accepted")
	}
}

func TestBudgetBoundaryPreflightsDuplicateCompletionIDs(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now)}

	boundary := newBoundary(materializer)
	lifecycle := newLifecycle(t)
	reservation, err := boundary.Reserve(context.Background(), lifecycle, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boundary.Claim(context.Background(), lifecycle, &reservation); err != nil {
		t.Fatal(err)
	}
	advanceDispatched(t, lifecycle)
	completion := boundaryCompletion(request, now)
	if err := boundary.Finalize(context.Background(), lifecycle, reservation, []budget.CompletionEvent{completion, completion}); !errors.Is(err, ErrBudgetBoundaryInvalid) {
		t.Fatalf("duplicate completion accepted: %v", err)
	}
}

func TestBudgetBoundaryRequiresOneCompletionPerReservedWindowBucket(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	result := boundaryAcceptedResult(request, now)
	secondReservation := result.Events[0]
	secondReservation.EventID = "reservation-event-2"
	secondReservation.WindowID = "window-2"
	result.Events = append(result.Events, secondReservation)

	tests := []struct {
		name   string
		events func() []budget.CompletionEvent
	}{
		{
			name: "missing reserved window",
			events: func() []budget.CompletionEvent {
				return []budget.CompletionEvent{boundaryCompletion(request, now)}
			},
		},
		{
			name: "two completions for one reserved window",
			events: func() []budget.CompletionEvent {
				first := boundaryCompletion(request, now)
				second := first
				second.EventID = "completion-event-2"
				return []budget.CompletionEvent{first, second}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			materializer := &boundaryMaterializer{result: result}

			boundary := newBoundary(materializer)
			lifecycle := newLifecycle(t)
			reservation, err := boundary.Reserve(context.Background(), lifecycle, request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := boundary.Claim(context.Background(), lifecycle, &reservation); err != nil {
				t.Fatal(err)
			}
			advanceDispatched(t, lifecycle)

			err = boundary.Finalize(context.Background(), lifecycle, reservation, test.events())
			if !errors.Is(err, ErrBudgetBoundaryInvalid) {
				t.Fatalf("incomplete completion coverage accepted: %v", err)
			}
			if got := materializer.calls; !reflect.DeepEqual(got, []string{"accept", "claim"}) {
				t.Fatalf("invalid completion batch caused a side effect: %#v", got)
			}
			if current, _ := lifecycle.Current(); current != PhaseDispatched {
				t.Fatalf("invalid completion batch phase = %s", current)
			}
		})
	}
}

func TestBudgetBoundaryDistinguishesBucketTimesOutsideUnixNanoRange(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	result := boundaryAcceptedResult(request, now)
	firstBucket := time.Unix(-30610224000, 0).UTC()
	secondBucket := time.Unix(firstBucket.Unix()+18446744073, 709551616).UTC()
	if !firstBucket.Before(secondBucket) || firstBucket.UnixNano() != secondBucket.UnixNano() {
		t.Fatalf("test timestamps do not exercise a UnixNano collision: %s, %s", firstBucket, secondBucket)
	}
	result.Events[0].BucketStart = firstBucket
	secondReservation := result.Events[0]
	secondReservation.EventID = "reservation-event-2"
	secondReservation.BucketStart = secondBucket
	result.Events = append(result.Events, secondReservation)

	materializer := &boundaryMaterializer{result: result}

	boundary := newBoundary(materializer)
	lifecycle := newLifecycle(t)
	reservation, err := boundary.Reserve(context.Background(), lifecycle, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boundary.Claim(context.Background(), lifecycle, &reservation); err != nil {
		t.Fatal(err)
	}
	advanceDispatched(t, lifecycle)
	firstCompletion := boundaryCompletion(request, now)
	firstCompletion.BucketStart = firstBucket
	secondCompletion := firstCompletion
	secondCompletion.EventID = "completion-event-2"
	secondCompletion.BucketStart = secondBucket

	if err := boundary.Finalize(context.Background(), lifecycle, reservation, []budget.CompletionEvent{firstCompletion, secondCompletion}); err != nil {
		t.Fatalf("distinct out-of-range buckets rejected: %v", err)
	}
}

func TestBudgetBoundaryRejectsTypedNilPorts(t *testing.T) {
	var materializer *boundaryMaterializer

	boundary := newBoundary(materializer)
	if err := boundary.Validate(); !errors.Is(err, ErrBudgetBoundaryInvalid) {
		t.Fatalf("typed nil ports accepted: %v", err)
	}
}

func TestBudgetBoundaryClaimFailurePreventsDispatchWithoutSQL(t *testing.T) {
	now := time.Now().UTC()
	request := boundaryRequest(now)
	for _, claimError := range []error{ErrLeaseExpired, ErrAlreadyClaimed, errors.New("Redis unavailable")} {
		t.Run(claimError.Error(), func(t *testing.T) {
			materializer := &boundaryMaterializer{result: boundaryAcceptedResult(request, now), claimErr: claimError}
			boundary := newBoundary(materializer)
			boundary.Identity.Postgres = PostgresIdentity{}
			lifecycle := newLifecycle(t)
			reserved, err := boundary.Reserve(context.Background(), lifecycle, request)
			if err != nil {
				t.Fatal(err)
			}
			if reserved.DispatchReady() {
				t.Fatal("unclaimed lease permits dispatch")
			}
			if _, err := boundary.Claim(context.Background(), lifecycle, &reserved); !errors.Is(err, claimError) {
				t.Fatal(err)
			}
			if reserved.DispatchReady() {
				t.Fatal("failed claim permits dispatch")
			}
			if err := lifecycle.Advance(PhaseDispatched); !errors.Is(err, ErrClaimRequired) {
				t.Fatalf("dispatch phase after failed claim: %v", err)
			}
		})
	}
}
