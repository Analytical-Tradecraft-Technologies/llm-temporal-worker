package redis

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

func recoveryFixture(t *testing.T, quotedAt time.Time) (BudgetKeySpace, BudgetRecoveryOperation) {
	t.Helper()
	space, err := NewBudgetKeySpace(KeyOptions{Prefix: "test", HashTag: "budget", KeySecret: []byte(strings.Repeat("s", 32))})
	if err != nil {
		t.Fatal(err)
	}
	request := durable.ReserveRequest{OperationID: "op", GenerationID: "gen", ExpiresAt: quotedAt.Add(durable.BudgetStartLease), Reservations: []admission.WindowReservation{{
		PolicyID: "tenant", WindowID: "day", Bucket: quotedAt.UnixNano() / int64(time.Hour), BucketNanos: int64(time.Hour), DurationNanos: int64(24 * time.Hour),
		AmountUSD: pricing.MustUSD("0.5"), LimitUSD: pricing.MustUSD("10"),
	}}}
	reservations, err := canonicalDurableReservations(request.OperationID, request.GenerationID, request.Reservations, request.ExpiresAt, quotedAt)
	if err != nil {
		t.Fatal(err)
	}
	bucketStart, _ := strconv.ParseInt(reservations[0].BucketStart, 10, 64)
	result := durable.ReserveResult{OperationID: "op", Accepted: true, GenerationID: "gen", IncarnationID: "inc", Events: []budget.ReservationEvent{{
		EventID: reservations[0].EventID, GenerationID: "gen", OperationID: "op", WindowID: "day", BucketStart: time.Unix(0, bucketStart).UTC(),
		ReservationRevision: 1, AmountUSD: pricing.MustUSD("0.5"), OccurredAt: quotedAt,
	}}}
	started := quotedAt.Add(3 * time.Hour) // waited for capacity before acceptance
	return space, BudgetRecoveryOperation{Request: request, Reservation: result, Claimed: true, StartedAt: started, StartBy: started.Add(durable.BudgetStartLease + BudgetRecoveryClockSkew)}
}

func TestRecoveredRecordKeepsIdempotencyIdentity(t *testing.T) {
	quotedAt := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	space, operation := recoveryFixture(t, quotedAt)
	record, keys, err := space.recoveredRecord(operation)
	if err != nil {
		t.Fatal(err)
	}
	reservations, _ := canonicalDurableReservations("op", "gen", operation.Request.Reservations, operation.Request.ExpiresAt, quotedAt)
	fingerprint, _ := durableRequestFingerprint(operation.Request, reservations)
	if record.Fingerprint != fingerprint || keys[0] != space.space.durableBudgetOperationKey("gen", "op") || len(keys) != 3 {
		t.Fatalf("restored identity differs from acceptance: %#v %v", record, keys)
	}
	got := record.Reservations[0]
	if got.ReservedNano != "500000000" || got.AccountedNano != "0" || got.Status != "reserved" || !record.Claimed {
		t.Fatalf("claimed reservation = %#v", got)
	}
	// Without a recorded acceptance expiry, spend counts at least until the end
	// of the bucket containing the latest possible acceptance plus the window.
	accepted := operation.StartedAt.Add(BudgetRecoveryClockSkew).Truncate(time.Hour).Add(time.Hour + 24*time.Hour)
	if got.WindowExpiresMillis < accepted.UnixMilli() {
		t.Fatalf("fallback expiry %d shortens spend retention before %d", got.WindowExpiresMillis, accepted.UnixMilli())
	}
	recorded := quotedAt.Add(40 * time.Hour)
	operation.Reservation.WindowExpiries = map[string]time.Time{got.EventID: recorded}
	record, _, err = space.recoveredRecord(operation)
	if err != nil || record.Reservations[0].WindowExpiresMillis != recorded.UnixMilli() {
		t.Fatalf("recorded expiry not restored exactly: %v", err)
	}
}

func TestRecoveredRecordAppliesDurableSettlement(t *testing.T) {
	quotedAt := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	space, operation := recoveryFixture(t, quotedAt)
	reserved := operation.Reservation.Events[0]
	settle := func(kind budget.JournalEventKind) *durable.ReconcileRequest {
		event := budget.CompletionEvent{EventID: "settle", GenerationID: "gen", OperationID: "op", WindowID: "day", BucketStart: reserved.BucketStart, ReservationRevision: 2,
			Kind: kind, ReservedDecreaseUSD: reserved.AmountUSD, AccountedDecreaseUSD: pricing.MustUSD("0"), OccurredAt: quotedAt}
		if kind == budget.JournalFinalizeExact {
			cost := pricing.MustUSD("0.2")
			event.CostStatus, event.ActualCostUSD, event.AccountedIncreaseUSD = budget.CostExact, &cost, cost
		} else {
			event.CostStatus, event.UnknownReasonCode, event.AccountedIncreaseUSD = budget.CostUnknown, "provider_cost_unknown", reserved.AmountUSD
		}
		return &durable.ReconcileRequest{OperationID: "op", GenerationID: "gen", IncarnationID: "inc", Events: []budget.CompletionEvent{event}}
	}
	for kind, want := range map[budget.JournalEventKind]recoveredReservation{
		budget.JournalFinalizeExact:   {ReservedNano: "0", AccountedNano: "200000000", ReservationRevision: 2, Status: "finalized"},
		budget.JournalFinalizeUnknown: {ReservedNano: "0", AccountedNano: "500000000", ReservationRevision: 2, Status: "ambiguous"},
	} {
		operation.Settlement = settle(kind)
		record, _, err := space.recoveredRecord(operation)
		if err != nil {
			t.Fatal(err)
		}
		got := record.Reservations[0]
		if got.ReservedNano != want.ReservedNano || got.AccountedNano != want.AccountedNano || got.ReservationRevision != want.ReservationRevision || got.Status != want.Status {
			t.Fatalf("%s restored as %#v", kind, got)
		}
		// The settlement event fingerprint is the duplicate-settlement fence.
		if record.Events["settle"] != durableEventFingerprint(operation.Settlement.Events[0]) {
			t.Fatalf("%s settlement fence missing", kind)
		}
	}
	operation.Settlement.IncarnationID = "other"
	if _, _, err := space.recoveredRecord(operation); !errors.Is(err, ErrBudgetRecoveryConflict) {
		t.Fatalf("foreign settlement = %v", err)
	}
	operation.Settlement = settle(budget.JournalFinalizeExact)
	operation.Claimed = false
	if _, _, err := space.recoveredRecord(operation); !errors.Is(err, ErrBudgetRecoveryConflict) {
		t.Fatalf("unclaimed settlement = %v", err)
	}
}

func TestRecoveredWindowExpiryRejectsUnrepresentableBounds(t *testing.T) {
	value := durableReservation{BucketNanos: "3600000000000", DurationNanos: "86400000000000", WindowExpiresMillis: 1}
	if _, err := recoveredWindowExpiry(value, time.Time{}, time.Unix(1<<50, 0)); !errors.Is(err, ErrBudgetRecoveryConflict) {
		t.Fatalf("overflowing start = %v", err)
	}
	value.BucketNanos = "1"
	if _, err := recoveredWindowExpiry(value, time.Time{}, time.Unix(1700000000, 0)); !errors.Is(err, ErrBudgetRecoveryConflict) {
		t.Fatalf("sub-millisecond bucket = %v", err)
	}
	if _, err := recoveredWindowExpiry(durableReservation{WindowExpiresMillis: 1}, time.UnixMilli(maxRecoveryInteger+1), time.Now()); !errors.Is(err, ErrBudgetRecoveryConflict) {
		t.Fatalf("unrepresentable recorded expiry = %v", err)
	}
}
