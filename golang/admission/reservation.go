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

// ValidateExactUSDReservationEnvelope validates the optional exact USD
// projection that accompanies the legacy MicroUSD admission envelope. Exact
// values are authoritative in durable stores, while Redis and the memory
// compatibility store mutate integer MicroUSD buckets. The compatibility
// amount may conservatively over-reserve an exact charge, but it must never
// under-reserve it or admit more than an exact limit permits.
//
// Callers that have no exact projection may continue using
// ValidateReservationEnvelope. Once any exact value is supplied, malformed or
// divergent fields fail closed before a compatibility-store mutation.
func ValidateExactUSDReservationEnvelope(reservations []WindowReservation, expected pricing.MicroUSD, expectedUSD pricing.USD) error {
	if err := ValidateReservationEnvelope(reservations, expected); err != nil {
		return err
	}
	if err := expectedUSD.Validate(); err != nil {
		return fmt.Errorf("invalid scalar USD reservation: %w", err)
	}
	hasExact := !expectedUSD.IsZero()
	for _, reservation := range reservations {
		if !reservation.AmountUSD.IsZero() || !reservation.LimitUSD.IsZero() {
			hasExact = true
			break
		}
	}
	if !hasExact {
		return nil
	}
	if !expectedUSD.IsZero() {
		required, err := pricing.CeilMicroFromUSD(expectedUSD)
		if err != nil {
			return fmt.Errorf("scalar USD reservation cannot cross Redis boundary: %w", err)
		}
		if expected < required {
			return fmt.Errorf("scalar MicroUSD reservation %d under-reserves exact USD reservation %s (requires at least %d)", expected, expectedUSD.String(), required)
		}
	}
	for index, reservation := range reservations {
		if err := reservation.AmountUSD.Validate(); err != nil {
			return fmt.Errorf("reservation %d has invalid USD amount: %w", index, err)
		}
		if err := reservation.LimitUSD.Validate(); err != nil {
			return fmt.Errorf("reservation %d has invalid USD limit: %w", index, err)
		}
		if !expectedUSD.IsZero() {
			if reservation.AmountUSD.IsZero() || reservation.AmountUSD.Cmp(expectedUSD) != 0 {
				return fmt.Errorf("reservation %d USD amount %s does not match scalar USD reservation %s", index, reservation.AmountUSD.String(), expectedUSD.String())
			}
			required, err := pricing.CeilMicroFromUSD(reservation.AmountUSD)
			if err != nil {
				return fmt.Errorf("reservation %d USD amount cannot cross Redis boundary: %w", index, err)
			}
			if reservation.Amount < required {
				return fmt.Errorf("reservation %d MicroUSD amount %d under-reserves exact USD amount %s (requires at least %d)", index, reservation.Amount, reservation.AmountUSD.String(), required)
			}
		} else if !reservation.AmountUSD.IsZero() {
			return fmt.Errorf("reservation %d supplies an exact USD amount without a scalar USD reservation", index)
		}
		if !reservation.LimitUSD.IsZero() {
			limit, err := pricing.MicroFromUSD(reservation.LimitUSD)
			if err != nil {
				return fmt.Errorf("reservation %d USD limit cannot cross Redis boundary: %w", index, err)
			}
			// A positive exact limit smaller than one micro-dollar still needs
			// one integer unit to remain usable at the Redis boundary.
			if limit == 0 {
				limit = 1
			}
			if reservation.Limit > limit {
				return fmt.Errorf("reservation %d MicroUSD limit %d exceeds exact USD limit %s (maximum %d)", index, reservation.Limit, reservation.LimitUSD.String(), limit)
			}
		}
	}
	return nil
}
