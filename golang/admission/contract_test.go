package admission

import (
	"encoding/hex"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func TestOperationStateTerminality(t *testing.T) {
	for _, test := range []struct {
		state    OperationState
		terminal bool
	}{
		{StateReserved, false},
		{StateDispatching, false},
		{StateCompleted, true},
		{StateDefiniteFailed, true},
		{StateAmbiguous, true},
		{StateCanceled, true},
		{"unknown", false},
	} {
		if got := test.state.Terminal(); got != test.terminal {
			t.Errorf("%q.Terminal() = %v, want %v", test.state, got, test.terminal)
		}
	}
}

func TestValidateTransitionMatchesLifecycleMatrix(t *testing.T) {
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
			wantAllowed := allowed[from][to]
			err := ValidateTransition(from, to)
			if (err == nil) != wantAllowed {
				t.Errorf("ValidateTransition(%q, %q) error = %v, allowed = %v", from, to, err, wantAllowed)
			}
		}
	}
	if err := ValidateTransition("unknown", StateReserved); err == nil {
		t.Fatal("unknown source state accepted")
	}
}

func TestValidateOutcomeAcceptsCertaintiesAndSafeCosts(t *testing.T) {
	for _, certainty := range []DispatchCertainty{NotDispatched, Rejected, Accepted, Ambiguous} {
		for _, incurred := range []pricing.MicroUSD{0, 1, pricing.RedisSafeLimit} {
			if err := ValidateOutcome(AttemptOutcome{Certainty: certainty, Incurred: incurred}); err != nil {
				t.Errorf("ValidateOutcome(%q, %d) = %v", certainty, incurred, err)
			}
		}
	}
	for _, outcome := range []AttemptOutcome{
		{Certainty: "unknown", Incurred: 0},
		{Certainty: Accepted, Incurred: -1},
		{Certainty: Accepted, Incurred: pricing.RedisSafeLimit + 1},
	} {
		if err := ValidateOutcome(outcome); err == nil {
			t.Fatalf("invalid outcome accepted: %#v", outcome)
		}
	}
}

func TestOperationCloneCopiesMutableFields(t *testing.T) {
	original := Operation{
		ID:           "operation",
		Reservations: []WindowReservation{{PolicyID: "policy", WindowID: "window", Amount: 4}},
		ResultRef:    &state.BlobRef{Digest: [32]byte{1}, Size: 4, Media: "application/octet-stream"},
	}
	clone := original.Clone()

	clone.Reservations[0].Amount = 9
	clone.ResultRef.Media = "text/plain"
	if original.Reservations[0].Amount != 4 || original.ResultRef.Media != "application/octet-stream" {
		t.Fatalf("clone mutation changed original: %#v", original)
	}

	original.Reservations[0].Amount = 2
	original.ResultRef.Media = "application/json"
	if clone.Reservations[0].Amount != 9 || clone.ResultRef.Media != "text/plain" {
		t.Fatalf("original mutation changed clone: %#v", clone)
	}

	withoutRef := (Operation{ID: "without-ref"}).Clone()
	if withoutRef.ResultRef != nil {
		t.Fatal("nil result reference was materialized")
	}
}

func TestDigestIsDeterministicAndInputSensitive(t *testing.T) {
	first := Digest([]byte("request"))
	if first != Digest([]byte("request")) {
		t.Fatal("digest is not deterministic")
	}
	if first == Digest([]byte("different")) {
		t.Fatal("digest does not distinguish inputs")
	}
	if got := hex.EncodeToString(first[:]); got != "1f58b9145b24d108d7ac38887338b3ea3229833b9c1e418250343f907bfd1047" {
		t.Fatalf("digest = %s, want stable SHA-256 encoding", got)
	}
}
