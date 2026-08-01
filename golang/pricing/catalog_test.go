package pricing

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCatalogResolveExactEntryAndReload(t *testing.T) {
	entry := Entry{Provider: "openai", Family: "responses", EndpointID: "prod", Model: "gpt", ProviderTier: "default", Version: "entry-v1", EffectiveFrom: time.Unix(1, 0), Prices: UnitPrices{InputPerMillion: MustDecimalUSD("1")}}
	catalog, err := CompileUSD("catalog-v1", []Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	resolver := NewResolver(catalog)
	quote, err := resolver.Resolve(Query{Provider: "openai", Family: "responses", EndpointID: "prod", Model: "gpt", ProviderTier: "default", At: time.Unix(2, 0)})
	if err != nil || quote.Entry.Version != "entry-v1" {
		t.Fatalf("quote = %#v %v", quote, err)
	}
	if _, err := resolver.Resolve(Query{Provider: "openai", Family: "responses", EndpointID: "prod", Model: "other", ProviderTier: "default", At: time.Unix(2, 0)}); !errors.Is(err, ErrNoActivePrice) {
		t.Fatalf("unknown price error = %v, want ErrNoActivePrice", err)
	}
	price, err := CostFromUsage(entry, Usage{InputTokens: 1_000_000})
	if err != nil || price.MicroUSD != 1_000_000 {
		t.Fatalf("cost = %#v %v", price, err)
	}
}

func TestCatalogReloadRejectsUnverifiedReplacementAndRetainsSnapshot(t *testing.T) {
	entry := Entry{Provider: "openai", Family: "responses", EndpointID: "prod", Region: "global", Model: "gpt", ProviderTier: "standard", Version: "catalog-v1", Prices: UnitPrices{InputPerMillion: MustDecimalUSD("1")}}
	valid, err := CompileUSD("catalog-v1", []Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	resolver := NewResolver(valid)
	// Publishing owns the entry slice: mutating the source value after reload
	// must not mutate the resolver's active snapshot.
	valid.Entries[0].Prices.InputPerMillion = MustDecimalUSD("9")
	quote, err := resolver.Resolve(Query{Provider: entry.Provider, Family: entry.Family, EndpointID: entry.EndpointID, Region: entry.Region, Model: entry.Model, ProviderTier: entry.ProviderTier, At: time.Now()})
	if err != nil || quote.Entry.Prices.InputPerMillion.String() != "1" {
		t.Fatalf("resolver retained caller-owned catalog storage: quote=%#v err=%v", quote, err)
	}

	changedEntry := entry
	changedEntry.Prices.InputPerMillion = MustDecimalUSD("2")
	invalid := valid
	invalid.Entries = []Entry{changedEntry}
	if err := resolver.ReloadValidated(invalid); err == nil {
		t.Fatal("ReloadValidated accepted a replacement with a stale compiled digest")
	}
	quote, err = resolver.Resolve(Query{Provider: entry.Provider, Family: entry.Family, EndpointID: entry.EndpointID, Region: entry.Region, Model: entry.Model, ProviderTier: entry.ProviderTier, At: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if got := quote.Entry.Prices.InputPerMillion.String(); got != "1" {
		t.Fatalf("failed replacement changed active price to %q", got)
	}

	// The compatibility method has the same fail-closed behavior even when a
	// caller cannot consume an error return.
	resolver.Reload(invalid)
	quote, err = resolver.Resolve(Query{Provider: entry.Provider, Family: entry.Family, EndpointID: entry.EndpointID, Region: entry.Region, Model: entry.Model, ProviderTier: entry.ProviderTier, At: time.Now()})
	if err != nil || quote.Entry.Prices.InputPerMillion.String() != "1" {
		t.Fatalf("Reload compatibility path replaced the valid snapshot: quote=%#v err=%v", quote, err)
	}
}

func TestCatalogValidateRequiresCompiledDigest(t *testing.T) {
	entry := Entry{Provider: "openai", Family: "responses", EndpointID: "prod", Model: "gpt", ProviderTier: "standard", Prices: UnitPrices{InputPerMillion: MustDecimalUSD("1")}}
	withoutDigest := Catalog{Version: "catalog-v1", Entries: []Entry{entry}}
	if err := withoutDigest.Validate(); err == nil {
		t.Fatal("Validate accepted a catalog without a compiled digest")
	}
}

func TestCostFromUsageRejectsUnknownCatalogComponent(t *testing.T) {
	entry := Entry{
		Prices:            UnitPrices{InputPerMillion: MustDecimalUSD("1")},
		UnknownComponents: []PriceComponent{PriceComponentInput},
	}
	if _, err := CostFromUsage(entry, Usage{InputTokens: 1}); err == nil {
		t.Fatal("CostFromUsage accepted an omitted input price as known zero")
	}
	if _, err := CostFromUsage(entry, Usage{}); err != nil {
		t.Fatalf("zero usage should not require an unknown input price: %v", err)
	}
}

func TestCompileUSDRejectsInvalidUnknownComponent(t *testing.T) {
	entry := Entry{Provider: "openai", Family: "responses", EndpointID: "prod", Model: "gpt", ProviderTier: "standard", UnknownComponents: []PriceComponent{"future"}}
	if _, err := CompileUSD("catalog-v1", []Entry{entry}); err == nil {
		t.Fatal("CompileUSD accepted an unknown price component")
	}
}

func TestCompileUSDRejectsUnknownComponentWithNonZeroPrice(t *testing.T) {
	tests := []struct {
		name      string
		component PriceComponent
		prices    UnitPrices
	}{
		{name: "input", component: PriceComponentInput, prices: UnitPrices{InputPerMillion: MustDecimalUSD("1")}},
		{name: "output", component: PriceComponentOutput, prices: UnitPrices{OutputPerMillion: MustDecimalUSD("1")}},
		{name: "cache_read", component: PriceComponentCacheRead, prices: UnitPrices{CacheReadPerMillion: MustDecimalUSD("1")}},
		{name: "cache_write", component: PriceComponentCacheWrite, prices: UnitPrices{CacheWritePerMillion: MustDecimalUSD("1")}},
		{name: "reasoning", component: PriceComponentReasoning, prices: UnitPrices{ReasoningPerMillion: MustDecimalUSD("1")}},
		{name: "per_request", component: PriceComponentPerRequest, prices: UnitPrices{PerRequest: MustDecimalUSD("1")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entry := Entry{
				Provider:          "openai",
				Family:            "responses",
				EndpointID:        "prod",
				Model:             "gpt",
				ProviderTier:      "standard",
				Prices:            test.prices,
				UnknownComponents: []PriceComponent{test.component},
			}
			_, err := CompileUSD("catalog-v1", []Entry{entry})
			if err == nil {
				t.Fatal("CompileUSD accepted an unknown component with a non-zero price")
			}
			if !strings.Contains(err.Error(), "unknown") || !strings.Contains(err.Error(), "non-zero") {
				t.Fatalf("CompileUSD error = %q, want unknown/non-zero context", err)
			}
		})
	}
}

func TestCompileUSDRejectsDuplicatePricingIdentity(t *testing.T) {
	from := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	base := Entry{
		Provider:       "openai",
		Family:         "openai_responses",
		EndpointID:     "openai-production",
		Region:         "global",
		Model:          "gpt-example",
		ProviderTier:   "standard",
		EffectiveFrom:  from,
		EffectiveUntil: from.Add(time.Hour),
		Prices:         UnitPrices{InputPerMillion: MustDecimalUSD("1")},
	}
	duplicate := base
	// Different end times and prices must not make two entries with the same
	// effective start look like distinct resolvable identities.
	duplicate.EffectiveUntil = from.Add(2 * time.Hour)
	duplicate.Prices.InputPerMillion = MustDecimalUSD("2")
	if _, err := CompileUSD("prices-v1", []Entry{base, duplicate}); err == nil {
		t.Fatal("CompileUSD accepted duplicate pricing identities")
	}
}
