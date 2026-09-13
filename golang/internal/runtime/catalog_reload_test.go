package runtime

import (
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/internal/catalog"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestCatalogReloadRejectsInvalidReplacementWithoutMutatingPriorSnapshot(t *testing.T) {
	prior, err := MergePricingCatalogs(catalog.Bundle{Pricing: map[string]catalog.PricingCatalog{
		"prices": compiledPriceCatalog(t, "prices", "prices-v1", []pricing.Entry{testPriceEntry("endpoint-a", "model-a", "standard")}),
	}}, "config-v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := MergePricingCatalogs(catalog.Bundle{Pricing: map[string]catalog.PricingCatalog{
		"prices": compiledPriceCatalog(t, "prices", "prices-v2", nil),
	}}, "config-v2"); err == nil {
		t.Fatal("invalid replacement unexpectedly succeeded")
	}
	if prior.Version != "runtime-prices:config-v1" || len(prior.Entries) != 1 || prior.Entries[0].Model != "model-a" {
		t.Fatalf("prior snapshot was mutated: %#v", prior)
	}
}
