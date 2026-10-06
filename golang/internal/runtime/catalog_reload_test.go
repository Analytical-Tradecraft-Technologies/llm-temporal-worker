package runtime

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/catalog"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func TestCatalogReloadRejectsInvalidReplacementWithoutMutatingPriorSnapshot(t *testing.T) {
	prior, err := mergePricingCatalogs(catalog.Bundle{Pricing: map[string]catalog.PricingCatalog{
		"prices": compiledPriceCatalog(t, "prices", "prices-v1", []pricing.Entry{testPriceEntry("endpoint-a", "model-a", "standard")}),
	}}, "config-v1", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mergePricingCatalogs(catalog.Bundle{Pricing: map[string]catalog.PricingCatalog{
		"prices": compiledPriceCatalog(t, "prices", "prices-v2", nil),
	}}, "config-v2", false); err == nil {
		t.Fatal("invalid replacement unexpectedly succeeded")
	}
	if prior.Version != "runtime-prices/config-v1" || len(prior.Entries) != 1 || prior.Entries[0].Model != "model-a" {
		t.Fatalf("prior snapshot was mutated: %#v", prior)
	}
}
