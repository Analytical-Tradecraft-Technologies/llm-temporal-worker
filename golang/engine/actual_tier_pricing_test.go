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
	result, err := generatePriorityServedAtStandard(t, "actual-tier", outputPriceEntry("economy-tier", "1"), outputPriceEntry("standard-tier", "1"), outputPriceEntry("priority-tier", "10"))
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
	partial := outputPriceEntry("standard-tier", "0")
	partial.UnknownComponents = []pricing.PriceComponent{pricing.PriceComponentOutput}
	result, err := generatePriorityServedAtStandard(t, "actual-tier-partial", outputPriceEntry("economy-tier", "1"), partial, outputPriceEntry("priority-tier", "10"))
	if err != nil {
		t.Fatalf("generate = %v, want the paid response priced at the quoted entry", err)
	}
	if result.Cost.ActualCostUSD == nil || result.Cost.ActualCostUSD.Cmp(pricing.MustUSD("0.0001")) != 0 {
		t.Fatalf("actual cost = %v, want 0.0001 at the quoted priority rate", result.Cost.ActualCostUSD)
	}
}

// generatePriorityServedAtStandard generates a priority request, keyed by
// name, against a catalog of entries while the provider reports that it served
// 10 output tokens at the standard tier.
func generatePriorityServedAtStandard(t *testing.T, name string, entries ...pricing.Entry) (llm.Response, error) {
	t.Helper()
	response := successfulResponse()
	response.Usage = llm.Usage{OutputTokens: 10}
	standard := llm.ServiceClassStandard
	response.Service = llm.ServiceFacts{Actual: &standard, ProviderValue: "standard-tier"}
	harness := newHarness(t, &fakeAdapter{name: name, response: response})
	harness.updateSnapshot(func(snapshot *Snapshot) { snapshot.Prices = pricing.NewResolver(testPriceCatalog(t, entries...)) })
	request := baseRequest(name)
	request.ServiceClass = llm.ServiceClassPriority
	return harness.engine.Generate(context.Background(), request)
}

// outputPriceEntry prices only output tokens for tier.
func outputPriceEntry(tier, outputPerMillion string) pricing.Entry {
	return pricing.Entry{Provider: "provider-1", Family: string(provider.FamilyOpenAIResponses), EndpointID: "endpoint-1", Region: "us-east-1", Model: "provider-model", ProviderTier: tier, Version: "prices-1", Prices: pricing.UnitPrices{OutputPerMillion: pricing.MustDecimalUSD(outputPerMillion)}}
}
