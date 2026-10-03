package pricing

import (
	"fmt"
	"math/big"
	"strconv"
)

// NanoUSD is the integer amount used by the Redis budget materialization.
//
// The authoritative money value remains USD at 10^-18 precision. NanoUSD is
// deliberately a separate type so callers cannot accidentally use the
// materialized value as an exact ledger amount. Redis Functions operate on
// integers only, and therefore every value must fit in the IEEE-754 safe
// integer range.
type NanoUSD int64

const (
	// NanoUSDMaterializationVersion identifies the rounding contract persisted
	// in a Redis generation manifest.
	NanoUSDMaterializationVersion = "nano_usd_v1"

	// NanoUSDSafeLimit is the largest integer that Redis can represent without
	// losing precision in Lua numeric operations.
	NanoUSDSafeLimit NanoUSD = 1<<53 - 1
)

var nanoUSDScaleFactor = big.NewInt(1_000_000_000)

// Valid reports whether the value can be passed to Redis safely.
func (value NanoUSD) Valid() bool {
	return value >= 0 && value <= NanoUSDSafeLimit
}

// Int64 returns the checked integer representation used in Redis arguments.
func (value NanoUSD) Int64() int64 { return int64(value) }

func (value NanoUSD) String() string { return strconv.FormatInt(int64(value), 10) }

// Add performs checked non-negative arithmetic within Redis's safe range.
func (value NanoUSD) Add(other NanoUSD) (NanoUSD, error) {
	if !value.Valid() || !other.Valid() || other > NanoUSDSafeLimit-value {
		return 0, fmt.Errorf("nanoUSD addition exceeds Redis safe integer range")
	}
	return value + other, nil
}

// Sub performs checked non-negative arithmetic. A negative materialized
// balance is always an error; callers must not clamp it to zero silently.
func (value NanoUSD) Sub(other NanoUSD) (NanoUSD, error) {
	if !value.Valid() || !other.Valid() || other > value {
		return 0, fmt.Errorf("nanoUSD subtraction would be negative")
	}
	return value - other, nil
}

// USDFromNano converts an integer materialization back to exact USD. This is
// only a display/reconciliation conversion; it does not recover any fraction
// discarded by a prior limit floor or charge ceiling.
func USDFromNano(value NanoUSD) (USD, error) {
	if !value.Valid() {
		return USD{}, fmt.Errorf("nanoUSD value is outside the Redis-safe range")
	}
	units := new(big.Int).Mul(big.NewInt(int64(value)), nanoUSDScaleFactor)
	return USD{units: units}, nil
}

// FloorNanoUSD materializes a non-negative exact USD limit conservatively.
// Limits are rounded down so Redis cannot authorize more than the exact
// configured limit. Values above NanoUSDSafeLimit are rejected.
func FloorNanoUSD(usd USD) (NanoUSD, error) {
	if err := usd.valid(); err != nil {
		return 0, err
	}
	units := usdUnits(usd)
	if err := validateNanoSource(units); err != nil {
		return 0, err
	}
	units.Quo(units, nanoUSDScaleFactor)
	return nanoFromBig(units)
}

// CeilNanoUSD materializes a non-negative exact USD charge conservatively.
// Positive fractional nano-dollars are rounded up so Redis cannot
// under-account an exact USD charge. Values above NanoUSDSafeLimit are
// rejected instead of overflowing or wrapping.
func CeilNanoUSD(usd USD) (NanoUSD, error) {
	if err := usd.valid(); err != nil {
		return 0, err
	}
	units := usdUnits(usd)
	if err := validateNanoSource(units); err != nil {
		return 0, err
	}
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(units, nanoUSDScaleFactor, remainder)
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return nanoFromBig(quotient)
}

func usdUnits(usd USD) *big.Int {
	if usd.units == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(usd.units)
}

func validateNanoSource(units *big.Int) error {
	max := new(big.Int).Mul(big.NewInt(int64(NanoUSDSafeLimit)), nanoUSDScaleFactor)
	if units.Cmp(max) > 0 {
		return fmt.Errorf("USD value exceeds the nanoUSD Redis safe limit")
	}
	return nil
}

func nanoFromBig(value *big.Int) (NanoUSD, error) {
	if value.Sign() < 0 || !value.IsInt64() {
		return 0, fmt.Errorf("nanoUSD result overflows int64")
	}
	result := NanoUSD(value.Int64())
	if !result.Valid() {
		return 0, fmt.Errorf("nanoUSD result exceeds Redis safe integer range")
	}
	return result, nil
}
