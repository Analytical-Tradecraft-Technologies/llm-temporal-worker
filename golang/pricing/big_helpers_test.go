package pricing

import (
	"math/big"
	"testing"
)

func TestCeilQuoRoundsUpOnlyARemainder(t *testing.T) {
	for _, test := range []struct{ numerator, denominator, want int64 }{
		{0, 3, 0}, {6, 3, 2}, {7, 3, 3}, {1, 1_000_000, 1},
	} {
		if got := ceilQuo(big.NewInt(test.numerator), big.NewInt(test.denominator)); got.Int64() != test.want {
			t.Errorf("ceilQuo(%d, %d) = %s, want %d", test.numerator, test.denominator, got, test.want)
		}
	}
}

func TestCheckedUSDRejectsOutOfRangeUnits(t *testing.T) {
	if got, err := checkedUSD(big.NewInt(5)); err != nil || got.Cmp(USD{units: big.NewInt(5)}) != 0 {
		t.Fatalf("checkedUSD(5) = %v, %v", got, err)
	}
	if _, err := checkedUSD(new(big.Int).Add(maxUSDUnits(), big.NewInt(1))); err == nil {
		t.Fatal("checkedUSD accepted units beyond NUMERIC(38,18)")
	}
	if got := (USD{}).unitsOrZero(); got.Sign() != 0 {
		t.Fatalf("zero USD units = %s", got)
	}
}
