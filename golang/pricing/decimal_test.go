package pricing

import (
	"math/big"
	"testing"
)

func TestCeilMicroUSDExact(t *testing.T) {
	tests := []struct {
		price string
		units int64
		want  MicroUSD
	}{
		{"0", 1, 0},
		{"0.000001", 1, 1},
		{"0.0000011", 1, 2},
		{"0.01", 1_000_000, 10_000_000_000},
		{"0.0000000001", 3, 1},
	}
	for _, test := range tests {
		price, err := ParseDecimalUSD(test.price)
		if err != nil {
			t.Fatal(err)
		}
		got, err := CeilMicroUSD(price, test.units, 1)
		if err != nil || got != test.want {
			t.Fatalf("%s * %d = %d, want %d (err=%v)", test.price, test.units, got, test.want, err)
		}
	}
}

func TestParseDecimalRejectsFloatLikeValues(t *testing.T) {
	for _, value := range []string{"", "-1", "+1", "1e-3", "1.", ".1", "a"} {
		if _, err := ParseDecimalUSD(value); err == nil {
			t.Fatalf("%q unexpectedly accepted", value)
		}
	}
}

func TestParseDecimalEnforcesNumericBoundsAndCanonicalizesOutput(t *testing.T) {
	for _, value := range []string{
		"99999999999999999999.999999999999999999",
		"99999999999999999999.000000000000000000",
		"0.000000000000000001",
	} {
		if _, err := ParseDecimalUSD(value); err != nil {
			t.Fatalf("ParseDecimalUSD(%q) rejected an in-range value: %v", value, err)
		}
	}
	for _, value := range []string{
		"100000000000000000000",
		"100000000000000000000.000000000000000000",
		"99999999999999999999.9999999999999999991",
	} {
		if _, err := ParseDecimalUSD(value); err == nil {
			t.Fatalf("ParseDecimalUSD(%q) accepted a value outside NUMERIC(38,18)", value)
		}
	}

	price := MustDecimalUSD("001.2300")
	if got, want := price.String(), "001.2300"; got != want {
		t.Fatalf("DecimalUSD.String() = %q, want source spelling %q", got, want)
	}
	if got, want := price.CanonicalString(), "1.23"; got != want {
		t.Fatalf("DecimalUSD.CanonicalString() = %q, want %q", got, want)
	}
	encoded, err := price.MarshalJSON()
	if err != nil || string(encoded) != `"1.23"` {
		t.Fatalf("DecimalUSD.MarshalJSON() = %s, %v; want %q", encoded, err, `"1.23"`)
	}
}

func TestMicroUSDCheckedArithmetic(t *testing.T) {
	if got, err := MicroUSD(4).Add(5); err != nil || got != 9 {
		t.Fatalf("add = %d %v", got, err)
	}
	if _, err := MicroUSD(4).Sub(5); err == nil {
		t.Fatal("negative subtraction accepted")
	}
	if _, err := RedisSafeLimit.Add(1); err == nil {
		t.Fatal("Redis unsafe sum accepted")
	}
}

func TestCeilMatchesRationalOracle(t *testing.T) {
	price := MustDecimalUSD("0.00012345")
	got, err := CeilMicroUSD(price, 12345, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	numerator := new(big.Int).Mul(big.NewInt(12345), big.NewInt(12345))
	denominator := big.NewInt(100_000_000_000_000)
	numerator.Mul(numerator, big.NewInt(1_000_000))
	want := new(big.Int).Quo(numerator, denominator)
	if new(big.Int).Mod(numerator, denominator).Sign() != 0 {
		want.Add(want, big.NewInt(1))
	}
	if got.Int64() != want.Int64() {
		t.Fatalf("oracle = %d, got %d", want, got)
	}
}
