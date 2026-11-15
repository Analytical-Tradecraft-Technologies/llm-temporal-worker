package pricing

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// gpt54DefaultEntry is OpenAI's direct gpt-5.4 standard price: 2x input and
// 1.5x output over 272,000 prompt tokens.
func gpt54DefaultEntry() Entry {
	return Entry{Provider: "openai", Family: "openai_responses", EndpointID: "openai", Model: "gpt-5.4", ProviderTier: "default",
		Prices:     tokenPrices("2.5", "15", "0.25", "0"),
		LongPrompt: &LongPromptPrices{MinPromptTokens: 272_000, Prices: tokenPrices("5", "22.5", "0.5", "0")},
	}
}

// tokenPrices returns per-million token prices with no reasoning or
// per-request charge.
func tokenPrices(input, output, cacheRead, cacheWrite string) UnitPrices {
	return UnitPrices{InputPerMillion: MustDecimalUSD(input), OutputPerMillion: MustDecimalUSD(output), CacheReadPerMillion: MustDecimalUSD(cacheRead),
		CacheWritePerMillion: MustDecimalUSD(cacheWrite), ReasoningPerMillion: MustDecimalUSD("0"), PerRequest: MustDecimalUSD("0")}
}

// usageCost is one usage and the USD cost it must settle at.
type usageCost struct {
	usage Usage
	want  string
}

func assertUsageCosts(t *testing.T, entry Entry, tests map[string]usageCost) {
	t.Helper()
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assertCostUSD(t, name, entry, test.usage, test.want)
		})
	}
}

func TestCostFromUsageSettlesLongPromptAtOverridePrice(t *testing.T) {
	assertUsageCosts(t, gpt54DefaultEntry(), map[string]usageCost{
		// 292,025 prompt tokens: 280,000 x $5 + 12,025 x $0.50 + 16,000 x $22.50 per million.
		"above threshold": {Usage{InputTokens: 280_000, CacheReadTokens: 12_025, OutputTokens: 16_000}, "1.7660125"},
		// Cache reads count toward the threshold.
		"cache read exceeds threshold": {Usage{InputTokens: 260_000, CacheReadTokens: 12_001, OutputTokens: 1_000}, "1.3285005"},
		// The threshold is exclusive: OpenAI bills prompts over 272,000 tokens
		// at the long-prompt price.
		"at threshold":    {Usage{InputTokens: 260_000, CacheReadTokens: 12_000, OutputTokens: 1_000}, "0.668"},
		"below threshold": {Usage{InputTokens: 271_999, OutputTokens: 1_000}, "0.6949975"},
	})
}

// Anthropic bills a Claude Haiku 5.5 prompt (input, cache reads and cache
// writes) over 100,000 tokens at 5x every price.
func TestCostFromUsageSettlesHaiku55LongPrompt(t *testing.T) {
	entry := Entry{Provider: "anthropic", Family: "anthropic_messages", EndpointID: "anthropic", Model: "claude-haiku-5-5", ProviderTier: "standard_only",
		Prices:     tokenPrices("0.1", "0.5", "0.01", "0.125"),
		LongPrompt: &LongPromptPrices{MinPromptTokens: 100_000, Prices: tokenPrices("0.5", "2.5", "0.05", "0.625")},
	}
	assertUsageCosts(t, entry, map[string]usageCost{
		// 144,000 x $0.50 + 4,000 x $2.50 per million.
		"over threshold": {Usage{InputTokens: 144_000, OutputTokens: 4_000}, "0.082"},
		// 40,000 x $0.50 + 50,000 x $0.05 + 10,001 x $0.625 + 1,000 x $2.50.
		"cache reads and writes exceed threshold": {Usage{InputTokens: 40_000, CacheReadTokens: 50_000, CacheWriteTokens: 10_001, OutputTokens: 1_000}, "0.031250625"},
		// 40,000 x $0.10 + 50,000 x $0.01 + 10,000 x $0.125 + 1,000 x $0.50.
		"at threshold": {Usage{InputTokens: 40_000, CacheReadTokens: 50_000, CacheWriteTokens: 10_000, OutputTokens: 1_000}, "0.00625"},
	})
}

func TestLongPromptPriceIsDetachedFromCatalogCopies(t *testing.T) {
	source := gpt54DefaultEntry()
	catalog := mustCompileUSD(t, "v", []Entry{source})
	source.LongPrompt.MinPromptTokens = 1
	resolver := NewResolver(catalog)
	query := entryQuery(gpt54DefaultEntry(), time.Time{})
	quote := mustResolve(t, resolver, query)
	quote.Entry.LongPrompt.MinPromptTokens = 2
	resolver.Snapshot().Entries[0].LongPrompt.MinPromptTokens = 3
	if got := mustResolve(t, resolver, query).Entry.LongPrompt.MinPromptTokens; got != 272_000 {
		t.Fatalf("long-prompt threshold = %d after mutating copies, want 272000", got)
	}
}

func TestLongPromptPriceIsValidatedAndOmittedWhenUnset(t *testing.T) {
	entry := gpt54DefaultEntry()
	mustCompileUSD(t, "v", []Entry{entry})
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
