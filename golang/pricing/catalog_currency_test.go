package pricing

import "testing"

func TestCompileUSDHasNoCurrencyInCanonicalCatalog(t *testing.T) {
	entry := Entry{
		Provider:     "openai",
		Family:       "openai_responses",
		EndpointID:   "openai-production",
		Region:       "global",
		Model:        "gpt-example",
		ProviderTier: "standard",
		Prices:       UnitPrices{InputPerMillion: mustDecimalUSD(t, "1.25")},
	}
	first, err := CompileUSD("prices-v1", []Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompileUSD("prices-v1", []Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest {
		t.Fatalf("same USD catalog produced different digests: %s != %s", first.DigestHex(), second.DigestHex())
	}
	if first.Version != "prices-v1" || len(first.Entries) != 1 {
		t.Fatalf("compiled USD catalog = %#v", first)
	}
}

func TestCompileUSDCanonicalizesEquivalentDecimalFormatting(t *testing.T) {
	base := Entry{
		Provider:     "openai",
		Family:       "openai_responses",
		EndpointID:   "openai-production",
		Region:       "global",
		Model:        "gpt-example",
		ProviderTier: "standard",
	}
	firstEntry := base
	firstEntry.Prices.InputPerMillion = mustDecimalUSD(t, "1.23")
	secondEntry := base
	secondEntry.Prices.InputPerMillion = mustDecimalUSD(t, "001.2300")
	first, err := CompileUSD("prices-v1", []Entry{firstEntry})
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompileUSD("prices-v1", []Entry{secondEntry})
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest {
		t.Fatalf("equivalent USD formatting produced different digests: %s != %s", first.DigestHex(), second.DigestHex())
	}
}

func TestCompileUSDRejectsIncompleteEntry(t *testing.T) {
	if _, err := CompileUSD("prices-v1", []Entry{{Provider: "openai"}}); err == nil {
		t.Fatal("CompileUSD() accepted an incomplete entry")
	}
}

func mustDecimalUSD(t *testing.T, value string) DecimalUSD {
	t.Helper()
	parsed, err := ParseDecimalUSD(value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
