package pricing

import (
	"math/big"
	"strings"
	"testing"
)

func TestDecimalCeilingAndOverflowInvariants(t *testing.T) {
	tests := []struct {
		price         string
		units         int64
		unitsPerPrice int64
	}{
		{price: "0", units: 0, unitsPerPrice: 1},
		{price: "0.000001", units: 1, unitsPerPrice: 1_000_000},
		{price: "0.0000011", units: 1, unitsPerPrice: 1},
		{price: "12.34", units: 100, unitsPerPrice: 1_000_000},
		{price: "0.0000000001", units: 3, unitsPerPrice: 1},
		{price: "9007199254.740992", units: 1, unitsPerPrice: 1},
	}

	for _, test := range tests {
		t.Run(test.price, func(t *testing.T) {
			assertCeilMatchesOracle(t, test.price, mustDecimalUSD(t, test.price), test.units, test.unitsPerPrice)
		})
	}
}

// assertCeilMatchesOracle checks CeilMicroUSD(price) against the exact
// rational ceiling of value, requiring an error when that ceiling exceeds the
// Redis-safe range.
func assertCeilMatchesOracle(t *testing.T, value string, price DecimalUSD, units, unitsPerPrice int64) {
	t.Helper()
	want := decimalCeilingOracle(t, value, units, unitsPerPrice)
	got, err := CeilMicroUSD(price, units, unitsPerPrice)
	if want.Cmp(big.NewInt(int64(RedisSafeLimit))) > 0 {
		if err == nil {
			t.Fatalf("overflow result = %d, want error", got)
		}
		return
	}
	if err != nil || got.Int64() != want.Int64() {
		t.Fatalf("CeilMicroUSD(%q, %d, %d) = %d, %v; want exact ceiling %d", value, units, unitsPerPrice, got, err, want)
	}
}

func decimalCeilingOracle(t *testing.T, value string, units, unitsPerPrice int64) *big.Int {
	t.Helper()
	parts := strings.SplitN(value, ".", 2)
	digits := strings.ReplaceAll(value, ".", "")
	numerator, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		t.Fatalf("oracle cannot parse %q", value)
	}
	numerator.Mul(numerator, big.NewInt(units))
	numerator.Mul(numerator, big.NewInt(1_000_000))
	scale := 0
	if len(parts) == 2 {
		scale = len(parts[1])
	}
	denominator := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	denominator.Mul(denominator, big.NewInt(unitsPerPrice))
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, denominator, remainder)
	if remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient
}
