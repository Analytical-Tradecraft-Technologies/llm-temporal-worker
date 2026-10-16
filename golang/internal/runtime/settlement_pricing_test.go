package runtime

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
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
		{name: "search fee", raw: map[string]json.RawMessage{"web_search_calls": json.RawMessage("2")}, total: "0.0200200001"},
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
			if response.Cost.Status != llm.CostStatusKnown || response.Cost.ActualCostUSD == nil || response.Cost.ActualCostUSD.Cmp(want) != 0 {
				t.Fatalf("cost = %#v, want %s", response.Cost, tc.total)
			}
		})
	}
}

func TestPublishedUnknownCostReason(t *testing.T) {
	partial := settlementPricingPlan(false)
	partial.Quote.Entry.UnknownComponents = []pricing.PriceComponent{pricing.PriceComponentOutput}
	usage := llm.Usage{InputTokens: 10, OutputTokens: 5}
	for _, tc := range []struct {
		name     string
		plan     cloudstate.BudgetPlan
		response llm.Response
		want     string
	}{
		{name: "unpriced route", plan: settlementPricingPlan(true), response: llm.Response{Usage: usage}, want: "catalog_incomplete"},
		{name: "unpriceable usage", plan: partial, response: llm.Response{Usage: usage}, want: "catalog_incomplete"},
		{name: "missing usage", plan: settlementPricingPlan(false), response: llm.Response{}, want: "provider_did_not_report_cost"},
		{name: "missing tool charge", plan: settlementPricingPlan(false), response: llm.Response{Usage: llm.Usage{InputTokens: 10, ProviderRaw: map[string]json.RawMessage{"hosted_execution_used": json.RawMessage("true")}}}, want: "provider_did_not_report_cost"},
		{name: "unusable provider cost", plan: settlementPricingPlan(false), response: llm.Response{Usage: usage, Cost: llm.Cost{Method: "openrouter_reported"}}, want: "provider_did_not_report_cost"},
		{name: "adapter reason kept", plan: settlementPricingPlan(false), response: llm.Response{Usage: usage, Cost: llm.Cost{Status: llm.CostStatusUnknown, UnknownReason: llm.CostUnknownCatalogIncomplete}}, want: "catalog_incomplete"},
		{name: "stored without reason", plan: settlementPricingPlan(false), response: llm.Response{Usage: usage, Cost: llm.Cost{Status: llm.CostStatusUnknown}}, want: "provider_did_not_report_cost"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			priceExecutionResponse(tc.plan, &tc.response)
			published := publicationCost(tc.response.Cost)
			if published.Status != "unknown" || published.UnknownReason != tc.want || published.Method != "" {
				t.Fatalf("published cost = %#v, want unknown %s", published, tc.want)
			}
		})
	}

	priced := llm.Response{Usage: usage, Cost: llm.Cost{UnknownReason: llm.CostUnknownCatalogIncomplete}}
	priceExecutionResponse(settlementPricingPlan(false), &priced)
	if priced.Cost.Status != llm.CostStatusKnown || priced.Cost.UnknownReason != "" {
		t.Fatalf("known cost kept an unknown reason: %#v", priced.Cost)
	}
}

func TestCostUnknownReasonRoundTripsAndIsOptional(t *testing.T) {
	encoded, err := json.Marshal(llm.Cost{Status: llm.CostStatusUnknown, UnknownReason: llm.CostUnknownCatalogIncomplete})
	if err != nil {
		t.Fatal(err)
	}
	var decoded llm.Cost
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.UnknownReason != llm.CostUnknownCatalogIncomplete {
		t.Fatalf("decoded = %#v, %v from %s", decoded, err, encoded)
	}
	encoded, err = json.Marshal(llm.Cost{Status: llm.CostStatusUnknown})
	if err != nil || strings.Contains(string(encoded), "unknown_reason") {
		t.Fatalf("cost without a reason encoded as %s, %v", encoded, err)
	}
	var stored llm.Cost
	if err := json.Unmarshal([]byte(`{"cost_status":"unknown","method":"","catalog_version":"prices-v1","reserved_cost_usd":"0.5","actual_cost_usd":null}`), &stored); err != nil {
		t.Fatal(err)
	}
	if got := publicationCost(stored); got.Status != "unknown" || got.UnknownReason != "provider_did_not_report_cost" {
		t.Fatalf("stored record published as %#v", got)
	}
}
