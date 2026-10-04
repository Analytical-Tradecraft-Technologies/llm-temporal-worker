package pricing

import (
	"encoding/json"
	"math/big"
	"testing"
)

func TestUSDParseCanonicalPrecisionAndBounds(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"0", "0.000000000000000000"},
		{"0.000000000000000001", "0.000000000000000001"},
		{"1", "1.000000000000000000"},
		{"10.5", "10.500000000000000000"},
		{"99999999999999999999.999999999999999999", "99999999999999999999.999999999999999999"},
	}
	for _, test := range tests {
		got, err := ParseUSD(test.input)
		if err != nil {
			t.Fatalf("ParseUSD(%q): %v", test.input, err)
		}
		if got.String() != test.want {
			t.Errorf("ParseUSD(%q).String() = %q, want %q", test.input, got.String(), test.want)
		}
		encoded, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("MarshalJSON(%q): %v", test.input, err)
		}
		var roundTrip USD
		if err := json.Unmarshal(encoded, &roundTrip); err != nil {
			t.Fatalf("UnmarshalJSON(%q): %v", test.input, err)
		}
		if roundTrip.Cmp(got) != 0 {
			t.Errorf("JSON round trip %q changed value", test.input)
		}
	}
	for _, input := range []string{
		"",
		".",
		"1.",
		"-1",
		"+1",
		" 1",
		"1.0000000000000000001",
		"100000000000000000000.000000000000000000",
		"99999999999999999999.9999999999999999999",
		"not-a-number",
	} {
		if _, err := ParseUSD(input); err == nil {
			t.Errorf("ParseUSD(%q) accepted invalid or overflowing value", input)
		}
	}
}

func TestUSDParseExponentFormExactly(t *testing.T) {
	for _, test := range []struct {
		input string
		want  string
	}{
		{"1.23e-7", "0.000000123000000000"},
		{"1.23E-7", "0.000000123000000000"},
		{"1e2", "100.000000000000000000"},
		{"0e999", "0.000000000000000000"},
	} {
		got, err := ParseUSD(test.input)
		if err != nil || got.String() != test.want {
			t.Fatalf("ParseUSD(%q) = %s, %v; want %s", test.input, got.String(), err, test.want)
		}
	}
	for _, input := range []string{"1e-19", "1.2e999", "1e", "1e+", "1efoo"} {
		if _, err := ParseUSD(input); err == nil {
			t.Errorf("ParseUSD(%q) accepted an inexact or malformed exponent", input)
		}
	}
}

func TestUSDCheckedArithmeticAndRatio(t *testing.T) {
	one := MustUSD("1")
	tenth := MustUSD("0.1")
	if got, err := one.Add(tenth); err != nil || got.String() != "1.100000000000000000" {
		t.Fatalf("USD.Add = %s, %v", got.String(), err)
	}
	if got, err := one.Sub(tenth); err != nil || got.String() != "0.900000000000000000" {
		t.Fatalf("USD.Sub = %s, %v", got.String(), err)
	}
	if _, err := tenth.Sub(one); err == nil {
		t.Fatal("USD.Sub permitted a negative result")
	}
	if got, err := MustUSD("0.000000000000000001").MulRatio(big.NewInt(3), big.NewInt(2)); err != nil || got.String() != "0.000000000000000002" {
		t.Fatalf("USD.MulRatio = %s, %v", got.String(), err)
	}
	if got, err := CeilUSD(MustDecimalUSD("0.00000123"), 1, 1); err != nil || got.String() != "0.000001230000000000" {
		t.Fatalf("CeilUSD preserved precision = %s, %v", got.String(), err)
	}
}

func TestUSDUnknownIsDistinctFromKnownZero(t *testing.T) {
	knownFree := MustUSD("0")
	var unknown *USD
	if unknown != nil || !knownFree.IsZero() {
		t.Fatal("unknown and known-free USD states were not distinct")
	}
}

func TestUSDRejectsExtremeExponentsWithoutOverflow(t *testing.T) {
	for _, input := range []string{"1e9223372036854775790", "1e9223372036854775807", "1e-9223372036854775808", "1.1e9223372036854775790", "1e2048", "1e-2048"} {
		if got, err := ParseUSD(input); err == nil {
			t.Errorf("ParseUSD(%q) unexpectedly returned %s", input, got.String())
		}
	}
}
