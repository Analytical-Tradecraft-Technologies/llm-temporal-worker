package pricing

import (
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	yaml "go.yaml.in/yaml/v4"
)

func TestPricingCatalogResolveAndCostSmoke(t *testing.T) {
	from := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	entry := Entry{
		Provider:       "openai",
		Family:         "responses",
		EndpointID:     "production",
		Region:         "global",
		Model:          "gpt-test",
		ProviderTier:   "standard",
		EffectiveFrom:  from,
		EffectiveUntil: from.Add(time.Hour),
		Prices: UnitPrices{
			InputPerMillion:      MustDecimalUSD("0.000001"),
			OutputPerMillion:     MustDecimalUSD("0.000002"),
			CacheReadPerMillion:  MustDecimalUSD("0.000003"),
			CacheWritePerMillion: MustDecimalUSD("0.000004"),
			ReasoningPerMillion:  MustDecimalUSD("0.000005"),
			PerRequest:           MustDecimalUSD("0.10"),
		},
		Version: "entry-v1",
	}
	catalog := mustCompileUSD(t, "catalog-v1", []Entry{entry})

	resolver := NewResolver(catalog)
	quote := mustResolve(t, resolver, entryQuery(entry, from.Add(30*time.Minute)))
	if quote.CatalogVersion != "catalog-v1" || quote.CatalogDigest == "" || quote.Entry.Version != "entry-v1" {
		t.Fatalf("quote = %#v", quote)
	}

	cost, err := CostFromUsage(quote.Entry, Usage{
		InputTokens:      1_000_000,
		OutputTokens:     1_000_000,
		CacheReadTokens:  1_000_000,
		CacheWriteTokens: 1_000_000,
		ReasoningTokens:  1_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cost.USD.String() != "0.100015000000000000" || cost.MicroUSD != 100015 || cost.Method != CostCatalogUsage || cost.CatalogVersion != "entry-v1" {
		t.Fatalf("cost = %#v", cost)
	}

	// The replacement keeps the identity but drops the effective interval.
	next := entry
	next.EffectiveFrom, next.EffectiveUntil = time.Time{}, time.Time{}
	next.Prices, next.Version = UnitPrices{InputPerMillion: MustDecimalUSD("2")}, "entry-v2"
	resolver.Reload(mustCompileUSD(t, "catalog-v2", []Entry{next}))
	quote, err = resolver.Resolve(entryQuery(entry, from.Add(30*time.Minute)))
	if err != nil || quote.CatalogVersion != "catalog-v2" || quote.Entry.Version != "entry-v2" {
		t.Fatalf("reloaded quote = %#v, %v", quote, err)
	}
}

func TestCostFromUsageKeepsTokenPricesPerMillion(t *testing.T) {
	entry := Entry{Prices: UnitPrices{
		InputPerMillion: MustDecimalUSD("2"),
		PerRequest:      MustDecimalUSD("0.10"),
	}}
	assertCostUSD(t, "token plus per-request", entry, Usage{InputTokens: 500_000}, "1.100000000000000000")
}

func TestPricingCatalogValidationAndResolutionBoundaries(t *testing.T) {
	valid := Entry{
		Provider:     "provider",
		Family:       "family",
		EndpointID:   "endpoint",
		Region:       "region",
		Model:        "model",
		ProviderTier: "tier",
		Prices:       UnitPrices{InputPerMillion: MustDecimalUSD("1")},
	}
	from := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)

	for name, test := range map[string]struct {
		compile func() (Catalog, error)
	}{
		"USD requires version": {
			compile: func() (Catalog, error) { return CompileUSD("", []Entry{valid}) },
		},
		"USD requires complete identity": {
			compile: func() (Catalog, error) {
				incomplete := valid
				incomplete.Model = ""
				return CompileUSD("v1", []Entry{incomplete})
			},
		},
		"USD rejects empty interval": {
			compile: func() (Catalog, error) {
				invalid := valid
				invalid.EffectiveFrom, invalid.EffectiveUntil = from, from
				return CompileUSD("v1", []Entry{invalid})
			},
		},
		"USD rejects invalid price": {
			compile: func() (Catalog, error) {
				invalid := valid
				invalid.Prices.InputPerMillion = DecimalUSD{numerator: *big.NewInt(-1)}
				return CompileUSD("v1", []Entry{invalid})
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := test.compile(); err == nil {
				t.Fatal("invalid catalog unexpectedly compiled")
			}
		})
	}

	valid.EffectiveFrom, valid.EffectiveUntil = from, from.Add(time.Hour)
	catalog := mustCompileUSD(t, "v1", []Entry{valid})
	if _, err := catalog.Resolve(entryQuery(valid, from.Add(time.Hour))); !errors.Is(err, ErrNoActivePrice) {
		t.Fatalf("price at exclusive end = %v", err)
	}
	if _, err := catalog.Resolve(entryQuery(valid, from)); err != nil {
		t.Fatalf("price at inclusive start = %v", err)
	}

	var nilResolver *PriceResolver
	if _, err := nilResolver.Resolve(Query{}); err == nil {
		t.Fatal("nil resolver unexpectedly resolved a quote")
	}
	nilResolver.Reload(catalog)
	if _, err := catalog.Resolve(Query{Provider: "missing"}); !errors.Is(err, ErrNoActivePrice) {
		t.Fatalf("missing price error = %v", err)
	}
}

func TestLegacyMoneyConversionsUseExplicitFloorAndSafeBounds(t *testing.T) {
	assertUSDResult(t, "USDFromMicro(1)", "0.000001000000000000")(USDFromMicro(1))

	for _, test := range []struct {
		name    string
		convert func(USD) (MicroUSD, error)
		usd     string
		want    MicroUSD
	}{
		{"MicroFromUSD", MicroFromUSD, "0.000001999999999999", 1},
		{"MicroFromUSD", MicroFromUSD, "0.000000999999999999", 0},
		{"MicroFromUSD", MicroFromUSD, "9007199254.740991000000000000", RedisSafeLimit},
		{"CeilMicroFromUSD", CeilMicroFromUSD, "0.000001000000000001", 2},
		{"CeilMicroFromUSD", CeilMicroFromUSD, "0.000000000000000001", 1},
		{"CeilMicroFromUSD", CeilMicroFromUSD, "9007199254.740991", RedisSafeLimit},
	} {
		got, err := test.convert(MustUSD(test.usd))
		if err != nil || got != test.want {
			t.Errorf("%s(%s) = %d, %v; want %d", test.name, test.usd, got, err, test.want)
		}
	}
	if _, err := USDFromMicro(-1); err == nil {
		t.Fatal("negative microUSD converted successfully")
	}
	if _, err := USDFromMicro(RedisSafeLimit + 1); err == nil {
		t.Fatal("unsafe microUSD converted successfully")
	}
	if _, err := MicroFromUSD(MustUSD("9007199254.740992000000000000")); err == nil {
		t.Fatal("USD above Redis-safe compatibility range converted successfully")
	}
	if _, err := CeilMicroFromUSD(MustUSD("9007199254.740991000001000000")); err == nil {
		t.Fatal("CeilMicroFromUSD permitted a value above the Redis-safe ceiling")
	}
}

func TestUSDBoundariesValidateYAMLAndUnderflow(t *testing.T) {
	var document struct {
		Amount USD `yaml:"amount"`
	}
	if err := yaml.Unmarshal([]byte("amount: \"1.25\"\n"), &document); err != nil {
		t.Fatal(err)
	}
	if document.Amount.String() != "1.250000000000000000" {
		t.Fatalf("YAML amount = %s", document.Amount.String())
	}
	if err := yaml.Unmarshal([]byte("amount: 1.25\n"), &document); err == nil {
		t.Fatal("unquoted YAML money unexpectedly accepted")
	}
	if err := MustUSD("1").Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (USD{units: big.NewInt(-1)}).Validate(); err == nil {
		t.Fatal("negative USD unexpectedly validated")
	}
	if got := MustUSD("0.1").SubOrZero(MustUSD("1")); !got.IsZero() || got.String() != "0.000000000000000000" {
		t.Fatalf("SubOrZero underflow = %s", got.String())
	}
}

func TestCostFromUsageRejectsNegativeUsage(t *testing.T) {
	entry := Entry{Prices: UnitPrices{InputPerMillion: MustDecimalUSD("1")}}
	if _, err := CostFromUsage(entry, Usage{InputTokens: -1}); err == nil {
		t.Fatal("negative usage unexpectedly charged")
	}
}

func TestCostFromUsageRejectsMicroUSDComponentOverflow(t *testing.T) {
	entry := Entry{Prices: UnitPrices{InputPerMillion: MustDecimalUSD("1")}}
	_, err := CostFromUsage(entry, Usage{InputTokens: math.MaxInt64})
	if err == nil {
		t.Fatal("CostFromUsage silently discarded an input component above the Redis-safe range")
	}
	if !strings.Contains(err.Error(), "usage input microUSD compatibility conversion") {
		t.Fatalf("overflow error = %v, want component context", err)
	}
}

func TestCostFromUsageRejectsMicroUSDAggregateOverflow(t *testing.T) {
	entry := Entry{Prices: UnitPrices{
		InputPerMillion:  MustDecimalUSD("9007199254.740991"),
		OutputPerMillion: MustDecimalUSD("9007199254.740991"),
	}}
	_, err := CostFromUsage(entry, Usage{InputTokens: 1_000_000, OutputTokens: 1_000_000})
	if err == nil {
		t.Fatal("CostFromUsage silently discarded a checked microUSD aggregate overflow")
	}
	if !strings.Contains(err.Error(), "usage output microUSD compatibility total") {
		t.Fatalf("aggregate overflow error = %v, want total context", err)
	}
}

func TestCostFromUsagePreservesSubMicroUSDInExactCost(t *testing.T) {
	entry := Entry{Prices: UnitPrices{PerRequest: MustDecimalUSD("0.000000000000000001")}}
	cost, err := CostFromUsage(entry, Usage{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cost.USD.String(), "0.000000000000000001"; got != want {
		t.Fatalf("exact sub-micro cost = %s, want %s", got, want)
	}
	if got, want := cost.MicroUSD, MicroUSD(1); got != want {
		t.Fatalf("compatibility sub-micro cost = %d, want %d", got, want)
	}
}
