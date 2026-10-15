package pricing

import (
	"encoding/json"
	"strings"
	"testing"
)

// gpt54DefaultEntry is OpenAI's direct gpt-5.4 standard price: 2x input and
// 1.5x output from 272,000 prompt tokens.
func gpt54DefaultEntry() Entry {
	return Entry{Provider: "openai", Family: "openai_responses", EndpointID: "openai", Model: "gpt-5.4", ProviderTier: "default",
		Prices: UnitPrices{InputPerMillion: MustDecimalUSD("2.5"), OutputPerMillion: MustDecimalUSD("15"), CacheReadPerMillion: MustDecimalUSD("0.25"),
			CacheWritePerMillion: MustDecimalUSD("0"), ReasoningPerMillion: MustDecimalUSD("0"), PerRequest: MustDecimalUSD("0")},
		LongPrompt: &LongPromptPrices{MinPromptTokens: 272_000, Prices: UnitPrices{InputPerMillion: MustDecimalUSD("5"), OutputPerMillion: MustDecimalUSD("22.5"),
			CacheReadPerMillion: MustDecimalUSD("0.5"), CacheWritePerMillion: MustDecimalUSD("0"), ReasoningPerMillion: MustDecimalUSD("0"), PerRequest: MustDecimalUSD("0")}},
	}
}

func TestCostFromUsageSettlesLongPromptAtOverridePrice(t *testing.T) {
	entry := gpt54DefaultEntry()
	for name, test := range map[string]struct {
		usage Usage
		want  string
	}{
		// 292,025 prompt tokens: 280,000 x $5 + 12,025 x $0.50 + 16,000 x $22.50 per million.
		"above threshold": {Usage{InputTokens: 280_000, CacheReadTokens: 12_025, OutputTokens: 16_000}, "1.7660125"},
		// Cache reads count toward the threshold.
		"cache read reaches threshold": {Usage{InputTokens: 260_000, CacheReadTokens: 12_000, OutputTokens: 1_000}, "1.3285"},
		"below threshold":              {Usage{InputTokens: 271_999, OutputTokens: 1_000}, "0.6949975"},
	} {
		t.Run(name, func(t *testing.T) {
			cost, err := CostFromUsage(entry, test.usage)
			if err != nil {
				t.Fatal(err)
			}
			if cost.USD.Cmp(MustUSD(test.want)) != 0 {
				t.Fatalf("cost = %s, want %s", cost.USD.String(), test.want)
			}
		})
	}
}

func TestLongPromptPriceIsDetachedFromCatalogCopies(t *testing.T) {
	source := gpt54DefaultEntry()
	catalog, err := CompileUSD("v", []Entry{source})
	if err != nil {
		t.Fatal(err)
	}
	source.LongPrompt.MinPromptTokens = 1
	resolver := NewResolver(catalog)
	query := Query{Provider: "openai", Family: "openai_responses", EndpointID: "openai", Model: "gpt-5.4", ProviderTier: "default"}
	quote, err := resolver.Resolve(query)
	if err != nil {
		t.Fatal(err)
	}
	quote.Entry.LongPrompt.MinPromptTokens = 2
	resolver.Snapshot().Entries[0].LongPrompt.MinPromptTokens = 3
	again, err := resolver.Resolve(query)
	if err != nil {
		t.Fatal(err)
	}
	if got := again.Entry.LongPrompt.MinPromptTokens; got != 272_000 {
		t.Fatalf("long-prompt threshold = %d after mutating copies, want 272000", got)
	}
}

func TestLongPromptPriceIsValidatedAndOmittedWhenUnset(t *testing.T) {
	entry := gpt54DefaultEntry()
	if _, err := CompileUSD("v", []Entry{entry}); err != nil {
		t.Fatal(err)
	}
	invalid := gpt54DefaultEntry()
	invalid.LongPrompt.MinPromptTokens = 0
	if _, err := CompileUSD("v", []Entry{invalid}); err == nil {
		t.Fatal("a non-positive long-prompt threshold compiled")
	}
	if _, err := CostFromUsage(invalid, Usage{InputTokens: 1}); err == nil {
		t.Fatal("a non-positive long-prompt threshold priced usage")
	}
	// Entries without a long-prompt price keep their canonical encoding, and
	// therefore their catalog digest.
	entry.LongPrompt = nil
	encoded, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "LongPrompt") {
		t.Fatalf("encoded entry %s names an unset long-prompt price", encoded)
	}
}
