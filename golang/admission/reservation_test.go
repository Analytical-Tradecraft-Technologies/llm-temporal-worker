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
		{name: "nonzero without windows", expected: 5},
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

func TestValidateExactUSDReservationEnvelope(t *testing.T) {
	base := WindowReservation{
		PolicyID: "policy", WindowID: "window", Bucket: 7, Amount: 2, Limit: 2,
		AmountUSD: pricing.MustUSD("0.000001000000000001"), LimitUSD: pricing.MustUSD("0.000002"),
	}
	for _, test := range []struct {
		name        string
		reservation WindowReservation
		expected    pricing.MicroUSD
		expectedUSD pricing.USD
		wantErr     bool
	}{
		{name: "matching exact projection", reservation: base, expected: 2, expectedUSD: base.AmountUSD},
		{name: "conservative over-reservation", reservation: func() WindowReservation { value := base; value.Amount = 3; return value }(), expected: 3, expectedUSD: base.AmountUSD},
		{name: "scalar under-reserves exact", reservation: base, expected: 1, expectedUSD: base.AmountUSD, wantErr: true},
		{name: "vector amount diverges", reservation: func() WindowReservation { value := base; value.AmountUSD = pricing.MustUSD("0.000001"); return value }(), expected: 2, expectedUSD: base.AmountUSD, wantErr: true},
		{name: "vector exact amount without scalar", reservation: base, expected: 2, wantErr: true},
		{name: "limit over-admits exact", reservation: func() WindowReservation { value := base; value.Limit = 3; return value }(), expected: 2, expectedUSD: base.AmountUSD, wantErr: true},
		{name: "legacy projection remains supported", reservation: WindowReservation{Amount: 2, Limit: 3}, expected: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateExactUSDReservationEnvelope([]WindowReservation{test.reservation}, test.expected, test.expectedUSD)
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateExactUSDReservationEnvelope() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestValidateExactUSDReservationEnvelopeRejectsUnrepresentableValues(t *testing.T) {
	tooLarge := pricing.MustUSD("9007199254.740991000001")
	reservation := WindowReservation{Amount: 1, Limit: 1, AmountUSD: tooLarge, LimitUSD: pricing.MustUSD("1")}
	if err := ValidateExactUSDReservationEnvelope([]WindowReservation{reservation}, 1, tooLarge); err == nil {
		t.Fatal("accepted an exact USD amount outside the Redis compatibility range")
	}
}
