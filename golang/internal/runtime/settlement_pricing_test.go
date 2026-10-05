package runtime

import (
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

func settlementPricingPlan(unpriced bool) cloudstate.BudgetPlan {
	return cloudstate.BudgetPlan{Unpriced: unpriced, Quote: pricing.Quote{Entry: pricing.Entry{Version: "prices-v1", Prices: pricing.UnitPrices{
		InputPerMillion: pricing.MustDecimalUSD("1"), OutputPerMillion: pricing.MustDecimalUSD("2"), CacheReadPerMillion: pricing.MustDecimalUSD("0"),
		CacheWritePerMillion: pricing.MustDecimalUSD("0"), ReasoningPerMillion: pricing.MustDecimalUSD("0"), PerRequest: pricing.MustDecimalUSD("0.0000000001"),
	}}}}
}

func TestPriceExecutionResponseTreatsMissingUsageAsUnknownCost(t *testing.T) {
	response := llm.Response{Status: llm.ResponseStatusCompleted, Output: []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "answer"}}}}}
	priceExecutionResponse(settlementPricingPlan(false), &response)
	if response.Cost.Status != llm.CostStatusUnknown || response.Cost.ActualCostUSD != nil {
		t.Fatalf("missing usage cost = %#v, want unknown", response.Cost)
	}

	priced := llm.Response{Status: llm.ResponseStatusCompleted, Usage: llm.Usage{InputTokens: 10, OutputTokens: 5}}
	priceExecutionResponse(settlementPricingPlan(false), &priced)
	if priced.Cost.Status != llm.CostStatusKnown || priced.Cost.ActualCostUSD == nil {
		t.Fatalf("reported usage cost = %#v, want known", priced.Cost)
	}
}

func TestPriceExecutionResponseDoesNotPromoteProviderCostOnUnpricedPlans(t *testing.T) {
	reported := pricing.MustUSD("0.5")
	response := llm.Response{Status: llm.ResponseStatusCompleted, Usage: llm.Usage{InputTokens: 10, OutputTokens: 5}, Cost: llm.Cost{ActualCostUSD: &reported}}
	priceExecutionResponse(settlementPricingPlan(true), &response)
	if response.Cost.Status != llm.CostStatusUnknown || response.Cost.ActualCostUSD != nil {
		t.Fatalf("unpriced provider cost = %#v, want unknown", response.Cost)
	}

	response = llm.Response{Status: llm.ResponseStatusCompleted, Usage: llm.Usage{InputTokens: 10, OutputTokens: 5}, Cost: llm.Cost{ActualCostUSD: &reported}}
	priceExecutionResponse(settlementPricingPlan(false), &response)
	if response.Cost.Status != llm.CostStatusKnown || response.Cost.Method != string(pricing.CostProviderReported) {
		t.Fatalf("priced provider cost = %#v, want provider_reported", response.Cost)
	}
}
