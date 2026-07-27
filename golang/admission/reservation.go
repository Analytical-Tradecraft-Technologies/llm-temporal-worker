package admission

import (
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

// ValidateReservationEnvelope verifies the relationship between the scalar
// reservation and the per-window reservation vector. Admission stores use the
// scalar as the request-wide estimate while budget buckets are mutated from
// the vector, so accepting divergent values could under-reserve or debit the
// same bucket more than once.
//
// A reservation identity is the policy, window, and bucket tuple. Duplicate
// identities are rejected rather than implicitly aggregated because callers
// must provide one unambiguous mutation for each bucket.
func ValidateReservationEnvelope(reservations []WindowReservation, expected pricing.MicroUSD) error {
	if expected < 0 || !expected.Valid() {
		return fmt.Errorf("invalid scalar reservation amount")
	}
	if len(reservations) == 0 {
		if expected != 0 {
			return fmt.Errorf("scalar reservation %d has no window reservations", expected)
		}
		return nil
	}
	type identity struct {
		policy string
		window string
		bucket int64
	}
	seen := make(map[identity]struct{}, len(reservations))
	for index, reservation := range reservations {
		if reservation.Amount != expected {
			return fmt.Errorf("reservation %d amount %d does not match scalar reservation %d", index, reservation.Amount, expected)
		}
		key := identity{policy: reservation.PolicyID, window: reservation.WindowID, bucket: reservation.Bucket}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate reservation identity at index %d", index)
		}
		seen[key] = struct{}{}
	}
	return nil
}
