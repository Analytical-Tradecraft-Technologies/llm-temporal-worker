package pricing

import "testing"

func TestUnitPricesInvalidPriceNamesTheInvalidComponent(t *testing.T) {
	valid := UnitPrices{InputPerMillion: MustDecimalUSD("1"), PerRequest: MustDecimalUSD("0.01")}
	if name, err := valid.invalidPrice(); err != nil || name != "" {
		t.Fatalf("valid prices = %q, %v", name, err)
	}
	invalid := valid
	invalid.CacheWritePerMillion = DecimalUSD{scale: -1}
	if name, err := invalid.invalidPrice(); err == nil || name != "cache_write" {
		t.Fatalf("invalid cache_write price = %q, %v", name, err)
	}
}
