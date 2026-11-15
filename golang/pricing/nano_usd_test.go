package pricing

import (
	"math/big"
	"testing"
)

func TestNanoUSDMaterializationRoundsInTheConservativeDirection(t *testing.T) {
	tests := []struct {
		name  string
		usd   string
		floor NanoUSD
		ceil  NanoUSD
	}{
		{name: "known zero", usd: "0", floor: 0, ceil: 0},
		{name: "one nano", usd: "0.000000001", floor: 1, ceil: 1},
		{name: "fractional nano", usd: "0.000000001000000001", floor: 1, ceil: 2},
		{name: "safe limit", usd: "9007199.254740991", floor: NanoUSDSafeLimit, ceil: NanoUSDSafeLimit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			usd := MustUSD(test.usd)
			floor, err := FloorNanoUSD(usd)
			if err != nil || floor != test.floor {
				t.Fatalf("FloorNanoUSD(%s) = %d, %v; want %d", test.usd, floor, err, test.floor)
			}
			ceil, err := CeilNanoUSD(usd)
			if err != nil || ceil != test.ceil {
				t.Fatalf("CeilNanoUSD(%s) = %d, %v; want %d", test.usd, ceil, err, test.ceil)
			}
			if ceil < floor {
				t.Fatalf("ceil %d is below floor %d", ceil, floor)
			}
		})
	}
}

func TestNanoUSDMaterializationRejectsUnsafeAmounts(t *testing.T) {
	for _, value := range []string{
		"9007199.254740992",
		"9007199.254740991000000001",
		"99999999999999999999.999999999999999999",
	} {
		usd := MustUSD(value)
		if _, err := FloorNanoUSD(usd); err == nil {
			t.Errorf("FloorNanoUSD(%s) accepted an unsafe value", value)
		}
		if _, err := CeilNanoUSD(usd); err == nil {
			t.Errorf("CeilNanoUSD(%s) accepted an unsafe value", value)
		}
	}
}

func TestNanoUSDMaterializationPreservesConservativeInvariants(t *testing.T) {
	const maxNano = int64(NanoUSDSafeLimit)
	values := []*big.Int{
		big.NewInt(0),
		big.NewInt(1),
		new(big.Int).Sub(new(big.Int).Mul(big.NewInt(maxNano), nanoUSDScaleFactor), big.NewInt(1)),
		new(big.Int).Mul(big.NewInt(maxNano), nanoUSDScaleFactor),
	}
	state := uint64(0x9e3779b97f4a7c15)
	for index := 0; index < 10_000; index++ {
		state ^= state << 7
		state ^= state >> 9
		state ^= state << 8
		nano := state % uint64(maxNano)
		fraction := state % 1_000_000_000
		units := new(big.Int).Mul(new(big.Int).SetUint64(nano), nanoUSDScaleFactor)
		values = append(values, units.Add(units, new(big.Int).SetUint64(fraction)))
	}
	for _, units := range values {
		usd := USD{units: units}
		floor, err := FloorNanoUSD(usd)
		if err != nil {
			t.Fatalf("FloorNanoUSD(%s): %v", units, err)
		}
		ceil, err := CeilNanoUSD(usd)
		if err != nil {
			t.Fatalf("CeilNanoUSD(%s): %v", units, err)
		}
		floorUnits := new(big.Int).Mul(big.NewInt(int64(floor)), nanoUSDScaleFactor)
		ceilUnits := new(big.Int).Mul(big.NewInt(int64(ceil)), nanoUSDScaleFactor)
		if floorUnits.Cmp(units) > 0 {
			t.Fatalf("floor overcharged units: %s > %s", floorUnits, units)
		}
		if ceilUnits.Cmp(units) < 0 {
			t.Fatalf("ceil undercharged units: %s < %s", ceilUnits, units)
		}
	}
}

func TestNanoUSDCheckedArithmeticAndExactDisplayConversion(t *testing.T) {
	if got, err := NanoUSD(2).Add(3); err != nil || got != 5 {
		t.Fatalf("NanoUSD.Add = %d, %v", got, err)
	}
	if _, err := NanoUSDSafeLimit.Add(1); err == nil {
		t.Fatal("NanoUSD.Add accepted a safe-integer overflow")
	}
	if _, err := NanoUSD(2).Sub(3); err == nil {
		t.Fatal("NanoUSD.Sub accepted a negative result")
	}
	value := assertUSDResult(t, "USDFromNano(123)", "0.000000123000000000")(USDFromNano(123))
	if got, err := CeilNanoUSD(value); err != nil || got != 123 {
		t.Fatalf("CeilNanoUSD(USDFromNano(123)) = %d, %v", got, err)
	}
}

func FuzzNanoUSDMaterialization(f *testing.F) {
	for _, seed := range []string{"0", "0.000000000000000001", "1", "9007199.254740991"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		usd, err := ParseUSD(input)
		if err != nil {
			return
		}
		floor, floorErr := FloorNanoUSD(usd)
		ceil, ceilErr := CeilNanoUSD(usd)
		if (floorErr == nil) != (ceilErr == nil) {
			t.Fatalf("rounding methods disagree on acceptance for %q: floor=%v ceil=%v", input, floorErr, ceilErr)
		}
		if floorErr != nil {
			return
		}
		if floor > ceil || !floor.Valid() || !ceil.Valid() {
			t.Fatalf("invalid rounding order for %q: floor=%d ceil=%d", input, floor, ceil)
		}
	})
}
