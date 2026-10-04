package pricing

import "testing"

func TestInclusiveReasoningCannotBeChargedTwice(t *testing.T) {
	for _, family := range []string{"openai_chat", "openai_responses"} {
		entry := Entry{Provider: "provider", Family: family, EndpointID: "endpoint", Model: "model", ProviderTier: "default", Prices: UnitPrices{OutputPerMillion: MustDecimalUSD("2"), ReasoningPerMillion: MustDecimalUSD("3")}}
		if _, err := CompileUSD("v1", []Entry{entry}); err == nil {
			t.Fatalf("%s accepted overlapping prices", family)
		}
		if _, err := CostFromUsage(entry, Usage{OutputTokens: 100, ReasoningTokens: 60}); err == nil {
			t.Fatalf("%s charged overlapping usage", family)
		}
		entry.Prices.ReasoningPerMillion = MustDecimalUSD("0")
		if _, err := CompileUSD("v1", []Entry{entry}); err != nil {
			t.Fatal(err)
		}
		cost, err := CostFromUsage(entry, Usage{OutputTokens: 100, ReasoningTokens: 60})
		if err != nil || cost.USD.Cmp(MustUSD("0.0002")) != 0 {
			t.Fatalf("inclusive output cost = %v, %v", cost.USD, err)
		}
	}
}

func TestSeparateReasoningPricesRemainSupported(t *testing.T) {
	entry := Entry{Family: "separate_usage", Prices: UnitPrices{OutputPerMillion: MustDecimalUSD("2"), ReasoningPerMillion: MustDecimalUSD("3")}}
	cost, err := CostFromUsage(entry, Usage{OutputTokens: 40, ReasoningTokens: 60})
	if err != nil || cost.USD.Cmp(MustUSD("0.00026")) != 0 {
		t.Fatalf("separate cost = %v, %v", cost.USD, err)
	}
}
