package cache

import (
	"fmt"
	"math"
	"time"
)

const (
	// DefaultMaximumAge is the largest age accepted by the provider-neutral
	// policy. Deployments may use a shorter bound, but should not widen this
	// protocol limit without changing the public contract.
	DefaultMaximumAge = 365 * 24 * time.Hour
)

// Policy is an explicit opt-in to the exact-response cache. A zero Policy is
// not a valid enabled policy; callers represent omission with a nil *Policy.
// Variant is a cache discriminator and is never sent to a provider.
type Policy struct {
	MaxAge  time.Duration
	Variant int32
}

// NewPolicy builds a policy from the wire representation. Keeping conversion
// here prevents integer-second multiplication from overflowing a duration and
// gives all callers the same positive/bounded checks.
func NewPolicy(maxAgeSeconds int64, variant int32) (Policy, error) {
	if maxAgeSeconds <= 0 {
		return Policy{}, fmt.Errorf("cache max age must be positive")
	}
	if maxAgeSeconds > int64(DefaultMaximumAge/time.Second) {
		return Policy{}, fmt.Errorf("cache max age must not exceed %s", DefaultMaximumAge)
	}
	if variant < 0 {
		return Policy{}, fmt.Errorf("cache variant must not be negative")
	}
	return Policy{MaxAge: time.Duration(maxAgeSeconds) * time.Second, Variant: variant}, nil
}

// Validate checks an enabled policy after settings inheritance has been
// materialized. An unknown effective temperature is intentionally treated as
// unsafe for positive variants: provider defaults are not a reproducible
// semantic contract. Compact is domain-separated and only supports variant 0.
func (policy Policy) Validate(operation OperationKind, effectiveTemperature *float64) error {
	return policy.ValidateWithMaximum(operation, effectiveTemperature, DefaultMaximumAge)
}

// ValidateWithMaximum applies an operator's shorter maximum age without
// weakening the package-level protocol bound.
func (policy Policy) ValidateWithMaximum(operation OperationKind, effectiveTemperature *float64, maximumAge time.Duration) error {
	if operation != OperationGenerate && operation != OperationCompact {
		return fmt.Errorf("unsupported cache operation %q", operation)
	}
	if maximumAge <= 0 || maximumAge > DefaultMaximumAge {
		return fmt.Errorf("cache maximum age must be between 1ns and %s", DefaultMaximumAge)
	}
	if policy.MaxAge <= 0 || policy.MaxAge > maximumAge {
		return fmt.Errorf("cache max age must be between 1ns and %s", maximumAge)
	}
	if policy.Variant < 0 {
		return fmt.Errorf("cache variant must not be negative")
	}
	if operation == OperationCompact && policy.Variant != 0 {
		return fmt.Errorf("compact cache variant must be zero")
	}
	if effectiveTemperature == nil {
		if policy.Variant != 0 {
			return fmt.Errorf("cache variant must be zero when effective temperature is unknown")
		}
		return nil
	}
	if math.IsNaN(*effectiveTemperature) || math.IsInf(*effectiveTemperature, 0) || *effectiveTemperature < 0 {
		return fmt.Errorf("effective temperature is invalid")
	}
	if *effectiveTemperature == 0 && policy.Variant != 0 {
		return fmt.Errorf("temperature zero requires cache variant zero")
	}
	return nil
}

// ValidateOptional validates an omitted-or-present cache policy. Omission is
// the only disabled state and deliberately performs no cache read or fill.
func ValidateOptional(policy *Policy, operation OperationKind, effectiveTemperature *float64) error {
	if policy == nil {
		return nil
	}
	return policy.Validate(operation, effectiveTemperature)
}

// MaxAgeSeconds returns the exact wire-sized age after validation. It is
// useful when forwarding a policy to a v1 contract and never silently rounds a
// sub-second duration.
func (policy Policy) MaxAgeSeconds() (int64, error) {
	if policy.MaxAge <= 0 || policy.MaxAge%time.Second != 0 {
		return 0, fmt.Errorf("cache max age must be a positive whole number of seconds")
	}
	if policy.MaxAge > DefaultMaximumAge {
		return 0, fmt.Errorf("cache max age must not exceed %s", DefaultMaximumAge)
	}
	return int64(policy.MaxAge / time.Second), nil
}
