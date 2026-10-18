package engine

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func TestPriceCeilingTakesTheLargerOfBaseAndLongPromptPrices(t *testing.T) {
	if PriceCeiling(nil) != nil {
		t.Fatal("an unpriced call has a ceiling")
	}
	entry := &pricing.Entry{Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("2.5"), OutputPerMillion: pricing.MustDecimalUSD("15"), PerRequest: pricing.MustDecimalUSD("0")},
		LongPrompt: &pricing.LongPromptPrices{MinPromptTokens: 272000, Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("5"), OutputPerMillion: pricing.MustDecimalUSD("10"), PerRequest: pricing.MustDecimalUSD("0")}}}
	got := *PriceCeiling(entry)
	if want := (provider.PriceCeiling{PromptPerMillion: "5", CompletionPerMillion: "15", PerRequest: "0"}); got != want {
		t.Fatalf("ceiling = %+v, want %+v", got, want)
	}
}
