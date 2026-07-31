package engine

import (
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestAggregateReservationsNormalizesExactAmountAcrossUnionWindows(t *testing.T) {
	expected := pricing.MustUSD("0.20")
	candidates := []quotedCandidate{
		{reservations: []admission.WindowReservation{{PolicyID: "policy-a", WindowID: "window-a", Amount: 200_000, AmountUSD: pricing.MustUSD("0.20")}}},
		{reservations: []admission.WindowReservation{{PolicyID: "policy-b", WindowID: "window-b", Amount: 200_000, AmountUSD: pricing.MustUSD("0.10")}}},
	}
	reservations := aggregateReservations(candidates, expected)
	if len(reservations) != 2 {
		t.Fatalf("aggregateReservations() returned %d reservations, want 2", len(reservations))
	}
	for _, reservation := range reservations {
		if reservation.AmountUSD.Cmp(expected) != 0 {
			t.Fatalf("reservation %s/%s exact amount = %s, want %s", reservation.PolicyID, reservation.WindowID, reservation.AmountUSD.String(), expected.String())
		}
	}
}
