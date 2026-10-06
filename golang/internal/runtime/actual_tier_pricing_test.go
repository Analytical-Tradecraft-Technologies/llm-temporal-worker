package runtime

import (
	"context"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

// A response the provider served at another class than attempted is priced
// with that class's entry captured in the plan (#765).
func TestPriceExecutionResponseUsesTheClassTheProviderReported(t *testing.T) {
	entry := func(tier, output string) pricing.Entry {
		return pricing.Entry{Provider: "openai", Family: "openai_responses", EndpointID: "endpoint", Region: "region", Model: "provider-model", ProviderTier: tier, Version: "price/v1",
			Prices: pricing.UnitPrices{OutputPerMillion: pricing.MustDecimalUSD(output)}}
	}
	plan := cloudstate.BudgetPlan{Mode: cloudstate.BudgetFree, AttemptedClass: llm.ServiceClassPriority, Quote: pricing.Quote{Entry: entry("priority", "10")},
		ClassEntries: map[llm.ServiceClass]pricing.Entry{llm.ServiceClassStandard: entry("default", "1")}}
	standard := llm.ServiceClassStandard
	for name, test := range map[string]struct {
		actual *llm.ServiceClass
		want   string
	}{
		"served at standard": {actual: &standard, want: "0.00001"},
		"no reported class":  {actual: nil, want: "0.0001"},
	} {
		response := &llm.Response{Usage: llm.Usage{OutputTokens: 10}, Service: llm.ServiceFacts{Actual: test.actual}}
		priceExecutionResponse(plan, response)
		if response.Cost.ActualCostUSD == nil || response.Cost.ActualCostUSD.Cmp(pricing.MustUSD(test.want)) != 0 {
			t.Fatalf("%s: cost = %v, want %s", name, response.Cost.ActualCostUSD, test.want)
		}
	}
}

// Budget planning captures, at the quote time, the price of every other class
// the route offers, so finalization can price the class actually served.
func TestBudgetPlanningCapturesThePricesOfTheRoutesOtherClasses(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	model := f.source.value.Routes.Models["alias"]
	model.Routes[0].Classes = []llm.ServiceClass{llm.ServiceClassStandard, llm.ServiceClassPriority}
	model.Routes[0].ProviderTiers = map[llm.ServiceClass]string{llm.ServiceClassStandard: "default", llm.ServiceClassPriority: "priority"}
	f.source.value.Routes.Models["alias"] = model
	priority := f.entry
	priority.ProviderTier, priority.Prices.OutputPerMillion = "priority", pricing.MustDecimalUSD("20")
	f.prices(t, []pricing.Entry{f.entry, priority})
	planned, err := f.planning(t).Generate(context.Background(), f.generate, f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	captured, ok := planned.ClassEntries[llm.ServiceClassPriority]
	if !ok || captured.ProviderTier != "priority" || len(planned.ClassEntries) != 1 {
		t.Fatalf("class entries = %#v, want only the priority entry", planned.ClassEntries)
	}
}
