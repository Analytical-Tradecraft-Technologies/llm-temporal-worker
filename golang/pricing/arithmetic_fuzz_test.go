package pricing

import "testing"

func FuzzParseDecimalAndCeil(f *testing.F) {
	f.Add("0.000001", int64(1))
	f.Add("0.0000011", int64(1))
	f.Add("12.34", int64(100))
	f.Fuzz(func(t *testing.T, value string, units int64) {
		if len(value) > 64 || units < 0 || units > 1_000_000_000 {
			return
		}
		price, err := ParseDecimalUSD(value)
		if err != nil || price.scale > 18 {
			return
		}
		assertCeilMatchesOracle(t, value, price, units, 1_000_000)
	})
}
