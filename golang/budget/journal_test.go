package budget

import (
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

// testReservationEvent reserves amount USD in the shared test window.
func testReservationEvent(amount string) ReservationEvent {
	return ReservationEvent{
		EventID: "event", GenerationID: "generation", OperationID: "operation", WindowID: "window",
		BucketStart: time.Unix(100, 0).UTC(), OccurredAt: time.Unix(101, 0).UTC(),
		AmountUSD: pricing.MustUSD(amount),
	}
}

func TestReservationEventValidation(t *testing.T) {
	event := testReservationEvent("0.000000001")
	testassert.NoError(t, event.Validate(), "valid reservation rejected")
	event.AmountUSD = pricing.MustUSD("0")
	if err := event.Validate(); err == nil {
		t.Fatal("zero reservation accepted")
	}
}

func TestCompletionEventEnforcesAccountingVocabulary(t *testing.T) {
	base := CompletionEvent{
		EventID: "event", GenerationID: "generation", OperationID: "operation", WindowID: "window",
		BucketStart: time.Unix(100, 0).UTC(), OccurredAt: time.Unix(101, 0).UTC(),
		Kind: JournalFinalizeUnknown, ReservedDecreaseUSD: pricing.MustUSD("1"),
		AccountedIncreaseUSD: pricing.MustUSD("1"), CostStatus: CostUnknown, UnknownReasonCode: "provider_unpriced",
	}
	testassert.NoError(t, base.Validate(), "valid unknown finalization rejected")
	base.AccountedIncreaseUSD = pricing.MustUSD("0.9")
	if err := base.Validate(); err == nil {
		t.Fatal("inconsistent finalization accepted")
	}
	base.AccountedIncreaseUSD = pricing.MustUSD("1")
	base.UnknownReasonCode = "provider timeout"
	if err := base.Validate(); err == nil {
		t.Fatal("unsafe reason code accepted")
	}
}

// exactReleaseEvent releases a one-dollar reservation at an exact zero cost.
func exactReleaseEvent() CompletionEvent {
	return CompletionEvent{
		EventID: "event", GenerationID: "generation", OperationID: "operation", WindowID: "window",
		BucketStart: time.Unix(100, 0).UTC(), OccurredAt: time.Unix(101, 0).UTC(),
		Kind: JournalRelease, ReservedDecreaseUSD: pricing.MustUSD("1"),
		ActualCostUSD: ptrUSD(pricing.MustUSD("0")), CostStatus: CostExact,
	}
}

func TestCompletionEventSupportsExactReleaseAndAmbiguousRetention(t *testing.T) {
	release := exactReleaseEvent()
	testassert.NoError(t, release.Validate(), "valid release rejected")
	retain := release
	retain.Kind = JournalRetainAmbiguous
	retain.ReservedDecreaseUSD = pricing.MustUSD("0")
	retain.ActualCostUSD = nil
	retain.CostStatus = CostUnknown
	retain.UnknownReasonCode = "dispatch_ambiguous"
	testassert.NoError(t, retain.Validate(), "valid retention rejected")
	resolveZero := retain
	resolveZero.Kind = JournalResolveUnknownExact
	resolveZero.ReservedDecreaseUSD = pricing.MustUSD("1")
	resolveZero.AccountedIncreaseUSD = pricing.MustUSD("0")
	resolveZero.ActualCostUSD = ptrUSD(pricing.MustUSD("0"))
	resolveZero.CostStatus = CostExact
	resolveZero.UnknownReasonCode = ""
	testassert.NoError(t, resolveZero.Validate(), "valid zero-cost resolution rejected")
}

func TestCompletionEventRejectsUnknownReasonForKnownCost(t *testing.T) {
	event := CompletionEvent{
		EventID: "event", GenerationID: "generation", OperationID: "operation", WindowID: "window",
		BucketStart: time.Unix(1, 0), OccurredAt: time.Unix(2, 0), Kind: JournalFinalizeExact,
		ReservedDecreaseUSD: pricing.MustUSD("1"), AccountedIncreaseUSD: pricing.MustUSD("1"),
		ActualCostUSD: ptrUSD(pricing.MustUSD("1")), CostStatus: CostExact, UnknownReasonCode: "provider_timeout",
	}
	if err := event.Validate(); err == nil {
		t.Fatal("known-cost completion accepted an unknown reason code")
	}
}

func ptrUSD(value pricing.USD) *pricing.USD { return &value }

func TestJournalEventsRejectBadSharedFields(t *testing.T) {
	reservation := testReservationEvent("1")
	completion := exactReleaseEvent()
	for _, test := range []struct {
		name        string
		reservation func(*ReservationEvent)
		completion  func(*CompletionEvent)
		want        string
	}{
		{"missing window", func(e *ReservationEvent) { e.WindowID = "" }, func(e *CompletionEvent) { e.WindowID = "" }, "window_id"},
		{"missing timestamp", func(e *ReservationEvent) { e.OccurredAt = time.Time{} }, func(e *CompletionEvent) { e.BucketStart = time.Time{} }, "timestamps are required"},
		{"negative revision", func(e *ReservationEvent) { e.ReservationRevision = -1 }, func(e *CompletionEvent) { e.ReservationRevision = -1 }, "reservation revision must be non-negative"},
	} {
		r := reservation
		test.reservation(&r)
		if err := r.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("%s: reservation error = %v", test.name, err)
		}
		c := completion
		test.completion(&c)
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("%s: completion error = %v", test.name, err)
		}
	}
	r := reservation
	r.OccurredAt = time.Time{}
	if err := r.Validate(); err == nil || err.Error() != "budget reservation timestamps are required" {
		t.Fatalf("reservation timestamp error = %v", err)
	}
	c := completion
	c.OccurredAt = time.Time{}
	if err := c.Validate(); err == nil || err.Error() != "budget completion timestamps are required" {
		t.Fatalf("completion timestamp error = %v", err)
	}
}
