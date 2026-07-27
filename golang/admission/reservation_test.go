package admission

import (
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestValidateReservationEnvelope(t *testing.T) {
	valid := WindowReservation{PolicyID: "policy", WindowID: "window", Bucket: 7, Amount: 5}
	for _, test := range []struct {
		name         string
		reservations []WindowReservation
		expected     pricing.MicroUSD
		wantErr      bool
	}{
		{name: "matching vector", reservations: []WindowReservation{valid}, expected: 5},
		{name: "zero without windows", expected: 0},
		{name: "nonzero without windows", expected: 5, wantErr: true},
		{name: "mismatched amount", reservations: []WindowReservation{{PolicyID: "policy", WindowID: "window", Bucket: 7, Amount: 4}}, expected: 5, wantErr: true},
		{name: "duplicate identity", reservations: []WindowReservation{valid, valid}, expected: 5, wantErr: true},
		{name: "different buckets", reservations: []WindowReservation{valid, {PolicyID: "policy", WindowID: "window", Bucket: 8, Amount: 5}}, expected: 5},
		{name: "invalid scalar", reservations: []WindowReservation{valid}, expected: -1, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateReservationEnvelope(test.reservations, test.expected); (err != nil) != test.wantErr {
				t.Fatalf("ValidateReservationEnvelope() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}
