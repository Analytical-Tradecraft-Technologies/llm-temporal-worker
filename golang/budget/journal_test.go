package budget

import (
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func TestReservationEventValidation(t *testing.T) {
	event := ReservationEvent{
		EventID: "event", GenerationID: "generation", OperationID: "operation", WindowID: "window",
		BucketStart: time.Unix(100, 0).UTC(), OccurredAt: time.Unix(101, 0).UTC(),
		AmountUSD: pricing.MustUSD("0.000000001"),
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("valid reservation rejected: %v", err)
	}
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
	if err := base.Validate(); err != nil {
		t.Fatalf("valid unknown finalization rejected: %v", err)
	}
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

func TestCompletionEventSupportsExactReleaseAndAmbiguousRetention(t *testing.T) {
	release := CompletionEvent{
		EventID: "event", GenerationID: "generation", OperationID: "operation", WindowID: "window",
		BucketStart: time.Unix(100, 0).UTC(), OccurredAt: time.Unix(101, 0).UTC(),
		Kind: JournalRelease, ReservedDecreaseUSD: pricing.MustUSD("1"),
		ActualCostUSD: ptrUSD(pricing.MustUSD("0")), CostStatus: CostExact,
	}
	if err := release.Validate(); err != nil {
		t.Fatalf("valid release rejected: %v", err)
	}
	retain := release
	retain.Kind = JournalRetainAmbiguous
	retain.ReservedDecreaseUSD = pricing.MustUSD("0")
	retain.ActualCostUSD = nil
	retain.CostStatus = CostUnknown
	retain.UnknownReasonCode = "dispatch_ambiguous"
	if err := retain.Validate(); err != nil {
		t.Fatalf("valid retention rejected: %v", err)
	}
	resolveZero := retain
	resolveZero.Kind = JournalResolveUnknownExact
	resolveZero.ReservedDecreaseUSD = pricing.MustUSD("1")
	resolveZero.AccountedIncreaseUSD = pricing.MustUSD("0")
	resolveZero.ActualCostUSD = ptrUSD(pricing.MustUSD("0"))
	resolveZero.CostStatus = CostExact
	resolveZero.UnknownReasonCode = ""
	if err := resolveZero.Validate(); err != nil {
		t.Fatalf("valid zero-cost resolution rejected: %v", err)
	}
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
