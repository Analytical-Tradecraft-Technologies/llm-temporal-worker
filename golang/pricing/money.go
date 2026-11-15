package pricing

import (
	"fmt"
	"math"
	"math/big"
)

// MicroUSD is an exact integer number of one-millionth US dollars.
type MicroUSD int64

// MicroUSD is retained only at the Redis/materialization boundary while the
// public pricing, budget, and response contracts migrate to USD. New callers
// should use USD and the explicit conversion helpers below.

const RedisSafeLimit MicroUSD = 1<<53 - 1

func (money MicroUSD) Valid() bool { return money >= 0 && money <= RedisSafeLimit }

func (money MicroUSD) Add(other MicroUSD) (MicroUSD, error) {
	if money < 0 || other < 0 || money > RedisSafeLimit || other > RedisSafeLimit || other > RedisSafeLimit-money {
		return 0, fmt.Errorf("microUSD addition overflows safe range")
	}
	return money + other, nil
}

func (money MicroUSD) Sub(other MicroUSD) (MicroUSD, error) {
	if money < 0 || other < 0 || money > RedisSafeLimit || other > RedisSafeLimit || other > money {
		return 0, fmt.Errorf("microUSD subtraction would be negative")
	}
	return money - other, nil
}

func (money MicroUSD) Int64() int64 { return int64(money) }

// USDFromMicro converts a legacy Redis-safe amount without going through a
// float. It is intentionally explicit so an adapter cannot be mistaken for
// the authoritative money representation.
func USDFromMicro(money MicroUSD) (USD, error) {
	if !money.Valid() {
		return USD{}, fmt.Errorf("microUSD value is outside the Redis-safe range")
	}
	units := new(big.Int).Mul(big.NewInt(int64(money)), usdScaleFactor)
	units.Quo(units, big.NewInt(1_000_000))
	return USD{units: units}, nil
}

// MicroFromUSD rounds a USD value down to microUSD for a Redis compatibility
// boundary. It must not be used for pricing or admission decisions.
func MicroFromUSD(usd USD) (MicroUSD, error) {
	if err := usd.valid(); err != nil {
		return 0, err
	}
	units := usdUnits(usd)
	units.Quo(units, big.NewInt(1_000_000_000_000))
	if !units.IsInt64() {
		return 0, fmt.Errorf("USD value overflows microUSD compatibility range")
	}
	result := MicroUSD(units.Int64())
	if !result.Valid() {
		return 0, fmt.Errorf("USD value exceeds Redis-safe microUSD range")
	}
	return result, nil
}

// CeilMicroFromUSD materializes a non-negative exact USD amount at the Redis
// integer boundary without undercharging. Any positive fractional microUSD is
// rounded up, and a result above RedisSafeLimit is rejected.
func CeilMicroFromUSD(usd USD) (MicroUSD, error) {
	if err := usd.valid(); err != nil {
		return 0, err
	}
	units := usdUnits(usd)
	divisor := big.NewInt(1_000_000_000_000)
	remainder := new(big.Int)
	units.QuoRem(units, divisor, remainder)
	if remainder.Sign() > 0 {
		units.Add(units, big.NewInt(1))
	}
	if !units.IsInt64() {
		return 0, fmt.Errorf("USD value overflows microUSD compatibility range")
	}
	result := MicroUSD(units.Int64())
	if !result.Valid() {
		return 0, fmt.Errorf("USD value exceeds Redis-safe microUSD range")
	}
	return result, nil
}

func checkedInt64(value int64) error {
	if value < 0 || value > math.MaxInt64 {
		return fmt.Errorf("value is outside non-negative int64 range")
	}
	return nil
}
