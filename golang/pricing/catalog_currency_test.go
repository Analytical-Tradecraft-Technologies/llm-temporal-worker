package pricing

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"testing"
	"time"
)

func TestCompileUSDHasNoCurrencyInCanonicalCatalog(t *testing.T) {
	entry := exampleEntry()
	entry.Prices = UnitPrices{InputPerMillion: mustDecimalUSD(t, "1.25")}
	first := mustCompileUSD(t, "prices-v1", []Entry{entry})
	second := mustCompileUSD(t, "prices-v1", []Entry{entry})
	if first.Digest != second.Digest {
		t.Fatalf("same USD catalog produced different digests: %s != %s", first.DigestHex(), second.DigestHex())
	}
	if first.Version != "prices-v1" || len(first.Entries) != 1 {
		t.Fatalf("compiled USD catalog = %#v", first)
	}
}

func TestCompileUSDCanonicalizesEquivalentDecimalFormatting(t *testing.T) {
	base := exampleEntry()
	firstEntry := base
	firstEntry.Prices.InputPerMillion = mustDecimalUSD(t, "1.23")
	secondEntry := base
	secondEntry.Prices.InputPerMillion = mustDecimalUSD(t, "001.2300")
	first := mustCompileUSD(t, "prices-v1", []Entry{firstEntry})
	second := mustCompileUSD(t, "prices-v1", []Entry{secondEntry})
	if first.Digest != second.Digest {
		t.Fatalf("equivalent USD formatting produced different digests: %s != %s", first.DigestHex(), second.DigestHex())
	}
}

func TestCompileUSDRejectsIncompleteEntry(t *testing.T) {
	if _, err := CompileUSD("prices-v1", []Entry{{Provider: "openai"}}); err == nil {
		t.Fatal("CompileUSD() accepted an incomplete entry")
	}
}

// exampleEntry is the pricing identity of a standard-tier OpenAI Responses
// model on the "openai-production" endpoint, with no prices.
func exampleEntry() Entry {
	return Entry{Provider: "openai", Family: "openai_responses", EndpointID: "openai-production", Region: "global", Model: "gpt-example", ProviderTier: "standard"}
}

// gptEntry prices the standard-tier "gpt" model on the "prod" OpenAI
// Responses endpoint at $1 per million input tokens.
func gptEntry() Entry {
	return Entry{Provider: "openai", Family: "responses", EndpointID: "prod", Model: "gpt", ProviderTier: "standard", Prices: UnitPrices{InputPerMillion: MustDecimalUSD("1")}}
}

// entryQuery queries entry's exact pricing identity at time at.
func entryQuery(entry Entry, at time.Time) Query {
	return Query{Provider: entry.Provider, Family: entry.Family, EndpointID: entry.EndpointID, Region: entry.Region, Model: entry.Model, ProviderTier: entry.ProviderTier, At: at}
}

// mustResolve resolves query, failing the test on an error.
func mustResolve(tb testing.TB, resolver interface{ Resolve(Query) (Quote, error) }, query Query) Quote {
	tb.Helper()
	quote, err := resolver.Resolve(query)
	if err != nil {
		tb.Fatalf("Resolve(%+v): %v", query, err)
	}
	return quote
}

// assertCostUSD checks that usage priced by entry costs exactly want USD.
func assertCostUSD(tb testing.TB, what string, entry Entry, usage Usage, want string) {
	tb.Helper()
	cost, err := CostFromUsage(entry, usage)
	if err != nil || cost.USD.Cmp(MustUSD(want)) != 0 {
		tb.Fatalf("%s cost = %v, %v; want %s", what, cost.USD, err, want)
	}
}

// assertUSDResult returns a check that a (USD, error) result is exactly want
// with no error; what names the call. The check returns the value.
func assertUSDResult(tb testing.TB, what, want string) func(USD, error) USD {
	return func(got USD, err error) USD {
		tb.Helper()
		if err != nil || got.String() != want {
			tb.Fatalf("%s = %s, %v; want %s", what, got.String(), err, want)
		}
		return got
	}
}

func mustCompileUSD(t *testing.T, version string, entries []Entry) Catalog {
	t.Helper()
	catalog, err := CompileUSD(version, entries)
	testassert.NoError(t, err)
	return catalog
}

func mustDecimalUSD(t *testing.T, value string) DecimalUSD {
	t.Helper()
	parsed, err := ParseDecimalUSD(value)
	testassert.NoError(t, err)
	return parsed
}
