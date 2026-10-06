package engine

import (
	"context"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
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
