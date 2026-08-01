package pricing

import "fmt"

func CostFromUsage(entry Entry, usage Usage) (Cost, error) {
	components := []struct {
		component     PriceComponent
		price         DecimalUSD
		units         int64
		unitsPerPrice int64
		name          string
	}{
		{PriceComponentInput, entry.Prices.InputPerMillion, usage.InputTokens, 1_000_000, "input"},
		{PriceComponentOutput, entry.Prices.OutputPerMillion, usage.OutputTokens, 1_000_000, "output"},
		{PriceComponentCacheRead, entry.Prices.CacheReadPerMillion, usage.CacheReadTokens, 1_000_000, "cache_read"},
		{PriceComponentCacheWrite, entry.Prices.CacheWritePerMillion, usage.CacheWriteTokens, 1_000_000, "cache_write"},
		{PriceComponentReasoning, entry.Prices.ReasoningPerMillion, usage.ReasoningTokens, 1_000_000, "reasoning"},
		// PerRequest is an absolute USD charge for one invocation, not a
		// per-million-unit price. Keep its denominator at one so a catalog
		// value such as 0.10 is charged as ten cents.
		{PriceComponentPerRequest, entry.Prices.PerRequest, 1, 1, "per_request"},
	}
	totalUSD := MustUSD("0")
	legacyTotal := MicroUSD(0)
	for _, component := range components {
		if component.units < 0 {
			return Cost{}, fmt.Errorf("usage %s is negative", component.name)
		}
		if component.units > 0 && entry.ComponentUnknown(component.component) {
			return Cost{}, fmt.Errorf("usage %s has no known USD catalog price", component.name)
		}
		value, err := CeilUSD(component.price, component.units, component.unitsPerPrice)
		if err != nil {
			return Cost{}, fmt.Errorf("usage %s: %w", component.name, err)
		}
		totalUSD, err = totalUSD.Add(value)
		if err != nil {
			return Cost{}, fmt.Errorf("usage %s USD total: %w", component.name, err)
		}
		legacy, err := CeilMicroUSD(component.price, component.units, component.unitsPerPrice)
		if err != nil {
			return Cost{}, fmt.Errorf("usage %s microUSD compatibility conversion: %w", component.name, err)
		}
		legacyTotal, err = legacyTotal.Add(legacy)
		if err != nil {
			return Cost{}, fmt.Errorf("usage %s microUSD compatibility total: %w", component.name, err)
		}
	}
	return Cost{USD: totalUSD, MicroUSD: legacyTotal, Method: CostCatalogUsage, CatalogVersion: entry.Version}, nil
}
