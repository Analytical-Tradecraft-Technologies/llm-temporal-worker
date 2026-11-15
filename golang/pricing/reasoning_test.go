package pricing

import "testing"

func TestInclusiveReasoningCannotBeChargedTwice(t *testing.T) {
	for _, family := range []string{"openai_chat", "openai_responses", "anthropic_messages", "bedrock_messages", "bedrock_converse"} {
		entry := Entry{Provider: "provider", Family: family, EndpointID: "endpoint", Model: "model", ProviderTier: "default", Prices: UnitPrices{OutputPerMillion: MustDecimalUSD("2"), ReasoningPerMillion: MustDecimalUSD("3")}}
		if _, err := CompileUSD("v1", []Entry{entry}); err == nil {
			t.Fatalf("%s accepted overlapping prices", family)
		}
		if _, err := CostFromUsage(entry, Usage{OutputTokens: 100, ReasoningTokens: 60}); err == nil {
			t.Fatalf("%s charged overlapping usage", family)
		}
		entry.Prices.ReasoningPerMillion = MustDecimalUSD("0")
		mustCompileUSD(t, "v1", []Entry{entry})
		assertCostUSD(t, family+" inclusive output", entry, Usage{OutputTokens: 100, ReasoningTokens: 60}, "0.0002")
	}
}

// Messages output_tokens already includes thinking tokens, so an 800-token
// thinking share of 1000 output tokens bills once at the output price.
func TestMessagesThinkingIsBilledOnceThroughOutput(t *testing.T) {
	for _, family := range []string{"anthropic_messages", "bedrock_messages"} {
		entry := Entry{Provider: "provider", Family: family, EndpointID: "endpoint", Model: "model", ProviderTier: "default", Prices: UnitPrices{OutputPerMillion: MustDecimalUSD("15"), ReasoningPerMillion: MustDecimalUSD("15")}}
		if _, err := CompileUSD("v1", []Entry{entry}); err == nil {
			t.Fatalf("%s accepted a reasoning price", family)
		}
		entry.Prices.ReasoningPerMillion = MustDecimalUSD("0")
		assertCostUSD(t, family, entry, Usage{OutputTokens: 1000, ReasoningTokens: 800}, "0.015")
	}
}

func TestSeparateReasoningPricesRemainSupported(t *testing.T) {
	entry := Entry{Family: "separate_usage", Prices: UnitPrices{OutputPerMillion: MustDecimalUSD("2"), ReasoningPerMillion: MustDecimalUSD("3")}}
	assertCostUSD(t, "separate", entry, Usage{OutputTokens: 40, ReasoningTokens: 60}, "0.00026")
}
