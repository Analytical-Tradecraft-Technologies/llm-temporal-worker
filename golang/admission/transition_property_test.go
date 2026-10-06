package admission

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func TestTransitionAndDispatchInvariants(t *testing.T) {
	states := []OperationState{StateReserved, StateDispatching, StateCompleted, StateDefiniteFailed, StateAmbiguous, StateCanceled}
	allowed := map[OperationState]map[OperationState]bool{
		StateReserved:       {StateDispatching: true, StateDefiniteFailed: true, StateCanceled: true, StateAmbiguous: true},
		StateDispatching:    {StateCompleted: true, StateDefiniteFailed: true, StateAmbiguous: true, StateCanceled: true, StateReserved: true},
		StateCompleted:      {StateCompleted: true},
		StateDefiniteFailed: {StateDefiniteFailed: true},
		StateAmbiguous:      {StateAmbiguous: true},
		StateCanceled:       {StateCanceled: true},
	}
	for _, from := range states {
		for _, to := range states {
			want := allowed[from][to]
			if got := ValidateTransition(from, to) == nil; got != want {
				t.Errorf("transition %s -> %s allowed=%v, want %v", from, to, got, want)
			}
		}
	}

	for _, certainty := range []DispatchCertainty{NotDispatched, Rejected, Accepted, Ambiguous} {
		if err := ValidateOutcome(AttemptOutcome{Certainty: certainty, Incurred: pricing.RedisSafeLimit}); err != nil {
			t.Errorf("dispatch certainty %q rejected: %v", certainty, err)
		}
	}
	for _, certainty := range []DispatchCertainty{"", "unknown", "provider_default"} {
		if err := ValidateOutcome(AttemptOutcome{Certainty: certainty, Incurred: 0}); err == nil {
			t.Errorf("invalid dispatch certainty %q accepted", certainty)
		}
	}
}
