package runtime

import (
	"encoding/json"
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

func TestHostedToolsSettlement(t *testing.T) {
	for _, tc := range []struct {
		name     string
		raw      map[string]json.RawMessage
		reported *pricing.USD
		unknown  bool
		total    string
	}{
		{name: "search fee", raw: map[string]json.RawMessage{"web_search_calls": json.RawMessage("2")}, total: "0.020020001"},
		{name: "execution duration unknown", raw: map[string]json.RawMessage{"hosted_execution_used": json.RawMessage("true")}, unknown: true},
		{name: "provider total already includes tools", raw: map[string]json.RawMessage{"web_search_calls": json.RawMessage("2")}, reported: func() *pricing.USD { v := pricing.MustUSD("0.5"); return &v }(), total: "0.5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := llm.Response{Usage: llm.Usage{InputTokens: 10, OutputTokens: 5, ProviderRaw: tc.raw}, Cost: llm.Cost{ActualCostUSD: tc.reported}}
			priceExecutionResponse(settlementPricingPlan(false), &response)
			if tc.unknown {
				if response.Cost.Status != llm.CostStatusUnknown || response.Cost.ActualCostUSD != nil {
					t.Fatalf("cost = %#v", response.Cost)
				}
				return
			}
			want := pricing.MustUSD(tc.total)
			if response.Cost.Status != llm.CostStatusKnown || response.Cost.ActualCostUSD == nil || *response.Cost.ActualCostUSD != want {
				t.Fatalf("cost = %#v, want %s", response.Cost, tc.total)
			}
		})
	}
}
