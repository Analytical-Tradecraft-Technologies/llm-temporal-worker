package engine

import (
	"context"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

// A priority request the provider served at standard is priced at standard
// (#765): priority output $10/M, standard $1/M, 10 output tokens.
func TestGeneratePricesTheServiceClassTheProviderReported(t *testing.T) {
	response := successfulResponse()
	response.Usage = llm.Usage{OutputTokens: 10}
	standard := llm.ServiceClassStandard
	response.Service = llm.ServiceFacts{Actual: &standard, ProviderValue: "standard-tier"}
	harness := newHarness(t, &fakeAdapter{name: "actual-tier", response: response})
	static := harness.engine.dependencies.Snapshots.(StaticSnapshot)
	entries := []pricing.Entry{}
	for _, tier := range []struct {
		name  string
		price string
	}{{"economy-tier", "1"}, {"standard-tier", "1"}, {"priority-tier", "10"}} {
		entries = append(entries, pricing.Entry{Provider: "provider-1", Family: string(provider.FamilyOpenAIResponses), EndpointID: "endpoint-1", Region: "us-east-1", Model: "provider-model", ProviderTier: tier.name, Version: "prices-1", Prices: pricing.UnitPrices{OutputPerMillion: pricing.MustDecimalUSD(tier.price)}})
	}
	catalog, err := pricing.CompileUSD("prices-1", entries)
	if err != nil {
		t.Fatal(err)
	}
	static.Value.Prices = pricing.NewResolver(catalog)
	harness.engine.dependencies.Snapshots = static
	request := baseRequest("actual-tier")
	request.ServiceClass = llm.ServiceClassPriority
	result, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Cost.ActualCostUSD == nil || result.Cost.ActualCostUSD.Cmp(pricing.MustUSD("0.00001")) != 0 {
		t.Fatalf("actual cost = %v, want 0.00001 at the standard rate", result.Cost.ActualCostUSD)
	}
}

// A served-class entry that cannot price the usage (standard marks output
// unknown) does not discard the paid response: it is priced at the quoted
// priority entry instead.
func TestGenerateFallsBackToTheQuotedEntryWhenTheServedClassCannotPrice(t *testing.T) {
	response := successfulResponse()
	response.Usage = llm.Usage{OutputTokens: 10}
	standard := llm.ServiceClassStandard
	response.Service = llm.ServiceFacts{Actual: &standard, ProviderValue: "standard-tier"}
	harness := newHarness(t, &fakeAdapter{name: "actual-tier-partial", response: response})
	static := harness.engine.dependencies.Snapshots.(StaticSnapshot)
	entry := func(tier, price string) pricing.Entry {
		return pricing.Entry{Provider: "provider-1", Family: string(provider.FamilyOpenAIResponses), EndpointID: "endpoint-1", Region: "us-east-1", Model: "provider-model", ProviderTier: tier, Version: "prices-1", Prices: pricing.UnitPrices{OutputPerMillion: pricing.MustDecimalUSD(price)}}
	}
	partial := entry("standard-tier", "0")
	partial.UnknownComponents = []pricing.PriceComponent{pricing.PriceComponentOutput}
	catalog, err := pricing.CompileUSD("prices-1", []pricing.Entry{entry("economy-tier", "1"), partial, entry("priority-tier", "10")})
	if err != nil {
		t.Fatal(err)
	}
	static.Value.Prices = pricing.NewResolver(catalog)
	harness.engine.dependencies.Snapshots = static
	request := baseRequest("actual-tier-partial")
	request.ServiceClass = llm.ServiceClassPriority
	result, err := harness.engine.Generate(context.Background(), request)
	if err != nil {
		t.Fatalf("generate = %v, want the paid response priced at the quoted entry", err)
	}
	if result.Cost.ActualCostUSD == nil || result.Cost.ActualCostUSD.Cmp(pricing.MustUSD("0.0001")) != 0 {
		t.Fatalf("actual cost = %v, want 0.0001 at the quoted priority rate", result.Cost.ActualCostUSD)
	}
}
