package pricing

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"strings"
	"testing"
	"time"
)

func TestCatalogResolveExactEntryAndReload(t *testing.T) {
	entry := gptEntry()
	entry.ProviderTier, entry.Version, entry.EffectiveFrom = "default", "entry-v1", time.Unix(1, 0)
	catalog := mustCompileUSD(t, "catalog-v1", []Entry{entry})
	resolver := NewResolver(catalog)
	quote, err := resolver.Resolve(entryQuery(entry, time.Unix(2, 0)))
	if err != nil || quote.Entry.Version != "entry-v1" {
		t.Fatalf("quote = %#v %v", quote, err)
	}
	testassert.ErrorIs(t, testassert.Err(resolver.Resolve(Query{Provider: "openai", Family: "responses", EndpointID: "prod", Model: "other", ProviderTier: "default", At: time.Unix(2, 0)})), ErrNoActivePrice, "unknown price")
	price, err := CostFromUsage(entry, Usage{InputTokens: 1_000_000})
	if err != nil || price.MicroUSD != 1_000_000 {
		t.Fatalf("cost = %#v %v", price, err)
	}
}

func TestCatalogReloadRejectsUnverifiedReplacementAndRetainsSnapshot(t *testing.T) {
	entry := gptEntry()
	entry.Region, entry.Version = "global", "catalog-v1"
	valid := mustCompileUSD(t, "catalog-v1", []Entry{entry})
	resolver := NewResolver(valid)
	// Publishing owns the entry slice: mutating the source value after reload
	// must not mutate the resolver's active snapshot.
	valid.Entries[0].Prices.InputPerMillion = MustDecimalUSD("9")
	quote, err := resolver.Resolve(entryQuery(entry, time.Now()))
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
	if got := mustResolve(t, resolver, entryQuery(entry, time.Now())).Entry.Prices.InputPerMillion.String(); got != "1" {
		t.Fatalf("failed replacement changed active price to %q", got)
	}

	// The compatibility method has the same fail-closed behavior even when a
	// caller cannot consume an error return.
	resolver.Reload(invalid)
	quote, err = resolver.Resolve(entryQuery(entry, time.Now()))
	if err != nil || quote.Entry.Prices.InputPerMillion.String() != "1" {
		t.Fatalf("Reload compatibility path replaced the valid snapshot: quote=%#v err=%v", quote, err)
	}
}

func TestCatalogValidateRequiresCompiledDigest(t *testing.T) {
	entry := gptEntry()
	withoutDigest := Catalog{Version: "catalog-v1", Entries: []Entry{entry}}
	if err := withoutDigest.Validate(); err == nil {
		t.Fatal("Validate accepted a catalog without a compiled digest")
	}
}

func TestPriceResolverSnapshotIsDetachedAndSurvivesReload(t *testing.T) {
	entry := gptEntry()
	entry.UnknownComponents = []PriceComponent{PriceComponentOutput}
	catalog := mustCompileUSD(t, "v1", []Entry{entry})
	resolver := NewResolver(catalog)
	pinned := resolver.Snapshot()
	testassert.NoError(t, pinned.Validate())
	changed := resolver.Snapshot()
	changed.Entries[0].Prices.InputPerMillion = MustDecimalUSD("2")
	changed.Entries[0].UnknownComponents[0] = PriceComponentCacheRead
	replacement := mustCompileUSD(t, "v2", changed.Entries)
	testassert.NoError(t, resolver.ReloadValidated(replacement))
	query := entryQuery(entry, time.Unix(2, 0))
	old, err := pinned.Resolve(query)
	if err != nil || old.Entry.Prices.InputPerMillion.String() != "1" || !old.Entry.ComponentUnknown(PriceComponentOutput) {
		t.Fatalf("snapshot changed after reload: %#v, %v", old, err)
	}
	current, err := resolver.Resolve(query)
	if err != nil || current.Entry.Prices.InputPerMillion.String() != "2" || current.Entry.ComponentUnknown(PriceComponentOutput) {
		t.Fatalf("snapshot mutated or prevented resolver reload: %#v, %v", current, err)
	}
	var missing *PriceResolver
	if missing.Snapshot().Digest != ([32]byte{}) || new(PriceResolver).Snapshot().Digest != ([32]byte{}) {
		t.Fatal("uninitialized resolver returned a usable snapshot")
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
	testassert.NoError(t, testassert.Err(CostFromUsage(entry, Usage{})), "zero usage should not require an unknown input price")
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
			entry := gptEntry()
			entry.Prices, entry.UnknownComponents = test.prices, []PriceComponent{test.component}
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
	base := exampleEntry()
	base.EffectiveFrom, base.EffectiveUntil = from, from.Add(time.Hour)
	base.Prices = UnitPrices{InputPerMillion: MustDecimalUSD("1")}
	duplicate := base
	// Different end times and prices must not make two entries with the same
	// effective start look like distinct resolvable identities.
	duplicate.EffectiveUntil = from.Add(2 * time.Hour)
	duplicate.Prices.InputPerMillion = MustDecimalUSD("2")
	if _, err := CompileUSD("prices-v1", []Entry{base, duplicate}); err == nil {
		t.Fatal("CompileUSD accepted duplicate pricing identities")
	}
}

func TestCompileUSDPriceIntervals(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	base := gptEntry()
	base.EffectiveFrom = start
	for _, test := range []struct {
		name                                           string
		firstFrom, firstUntil, secondFrom, secondUntil time.Time
		overlap                                        bool
	}{
		{"open ended", start, time.Time{}, start.Add(time.Hour), time.Time{}, true},
		{"bounded overlap", start, start.Add(2 * time.Hour), start.Add(time.Hour), start.Add(3 * time.Hour), true},
		{"nested", start, start.Add(3 * time.Hour), start.Add(time.Hour), start.Add(2 * time.Hour), true},
		{"unbounded past", time.Time{}, start.Add(time.Hour), start, time.Time{}, true},
		{"adjacent", start, start.Add(time.Hour), start.Add(time.Hour), time.Time{}, false},
		{"gap", start, start.Add(time.Hour), start.Add(2 * time.Hour), time.Time{}, false},
		{"adjacent unbounded", time.Time{}, start, start, time.Time{}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, b := base, base
			a.EffectiveFrom, a.EffectiveUntil = test.firstFrom, test.firstUntil
			b.EffectiveFrom, b.EffectiveUntil = test.secondFrom, test.secondUntil
			b.Prices.InputPerMillion = MustDecimalUSD("10")
			for _, entries := range [][]Entry{{a, b}, {b, a}} {
				catalog, err := CompileUSD("v1", entries)
				if (err != nil) != test.overlap {
					t.Fatalf("overlap=%v, error=%v", test.overlap, err)
				}
				if err == nil {
					quote, err := catalog.Resolve(entryQuery(base, b.EffectiveFrom))
					if err != nil || quote.Entry.Prices.InputPerMillion.String() != "10" {
						t.Fatalf("boundary quote=%+v err=%v", quote, err)
					}
				}
			}
		})
	}
	for _, change := range []func(*Entry){
		func(e *Entry) { e.Provider = "other" }, func(e *Entry) { e.Family = "other" }, func(e *Entry) { e.EndpointID = "other" }, func(e *Entry) { e.Region = "other" }, func(e *Entry) { e.Model = "other" }, func(e *Entry) { e.ProviderTier = "other" },
	} {
		other := base
		change(&other)
		testassert.NoError(t, testassert.Err(CompileUSD("v1", []Entry{base, other})), "distinct route rejected")
	}
}
