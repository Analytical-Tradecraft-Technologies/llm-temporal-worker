package postgres

import (
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestPricingCatalogPublicationLockKeyIsStableAndNamespaceScoped(t *testing.T) {
	namespace, err := NewNamespace("llm_worker", "llm_worker", "")
	if err != nil {
		t.Fatal(err)
	}
	key := pricingCatalogPublicationLockKey(namespace)
	if key != pricingCatalogPublicationLockKey(namespace) {
		t.Fatal("pricing catalog publication lock key is not stable")
	}
	otherNamespace, err := NewNamespace("llm_worker", "other_worker", "")
	if err != nil {
		t.Fatal(err)
	}
	if key == pricingCatalogPublicationLockKey(otherNamespace) {
		t.Fatal("different namespaces share a pricing catalog publication lock key")
	}
	otherPrefix, err := NewNamespace("llm_worker", "llm_worker", "tenant_")
	if err != nil {
		t.Fatal(err)
	}
	if key == pricingCatalogPublicationLockKey(otherPrefix) {
		t.Fatal("different table prefixes share a pricing catalog publication lock key")
	}
}

func TestEntryRowPreservesExactUSDAndZero(t *testing.T) {
	entry := pricing.Entry{
		Provider: "openai", Family: "openai_responses", EndpointID: "prod", Region: "global", Model: "gpt", ProviderTier: "standard",
		Prices: pricing.UnitPrices{
			InputPerMillion: pricing.MustDecimalUSD("0"), OutputPerMillion: pricing.MustDecimalUSD("10.125000"),
			CacheReadPerMillion: pricing.MustDecimalUSD("0.000001"), CacheWritePerMillion: pricing.MustDecimalUSD("0.000002"),
			ReasoningPerMillion: pricing.MustDecimalUSD("0.000003"), PerRequest: pricing.MustDecimalUSD("0.10"),
		},
		Version: "formula-v1", EffectiveFrom: time.Unix(10, 0).UTC(),
	}
	row, err := entryRow(entry, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if row.status != "exact" || len(row.unknown) != 0 || row.reason != nil {
		t.Fatalf("row status = %#v, want exact without unknowns", row)
	}
	want := []string{"0.000000000000000000", "10.125000000000000000", "0.000001000000000000", "0.000002000000000000", "0.000003000000000000", "0.100000000000000000"}
	for i, value := range row.prices {
		if value == nil || *value != want[i] {
			t.Fatalf("price[%d] = %v, want %q", i, value, want[i])
		}
	}
}

func TestEntryRowMapsPartialAndUnknownComponents(t *testing.T) {
	entry := pricing.Entry{
		Provider: "openai", Family: "openai_responses", EndpointID: "prod", Region: "global", Model: "gpt", ProviderTier: "standard",
		Prices:            pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("1")},
		UnknownComponents: []pricing.PriceComponent{pricing.PriceComponentOutput, pricing.PriceComponentPerRequest},
	}
	row, err := entryRow(entry, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if row.status != "partial" || len(row.unknown) != 2 || row.prices[1] != nil || row.prices[5] != nil || row.prices[0] == nil {
		t.Fatalf("partial row = %#v", row)
	}
	if row.reason == nil || *row.reason != priceUnknownReason {
		t.Fatalf("partial reason = %v, want %q", row.reason, priceUnknownReason)
	}
	all := []pricing.PriceComponent{pricing.PriceComponentInput, pricing.PriceComponentOutput, pricing.PriceComponentCacheRead, pricing.PriceComponentCacheWrite, pricing.PriceComponentReasoning, pricing.PriceComponentPerRequest}
	entry.UnknownComponents = all
	row, err = entryRow(entry, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if row.status != "unknown" || len(row.unknown) != len(all) {
		t.Fatalf("unknown row = %#v", row)
	}
	for i, value := range row.prices {
		if value != nil {
			t.Fatalf("unknown price[%d] = %q, want NULL", i, *value)
		}
	}
}

func TestEntryRowRejectsUnsupportedAndOutOfRangePrices(t *testing.T) {
	entry := pricing.Entry{UnknownComponents: []pricing.PriceComponent{"future"}}
	if _, err := entryRow(entry, time.Now()); err == nil {
		t.Fatal("unsupported unknown component unexpectedly accepted")
	}
	entry.UnknownComponents = nil
	if _, err := pricing.ParseDecimalUSD("0.1234567890123456789"); err == nil {
		t.Fatal("more than 18 fractional digits unexpectedly accepted")
	}
}

func TestComponentCodeMappingIsClosed(t *testing.T) {
	for _, test := range []struct {
		component pricing.PriceComponent
		code      string
	}{
		{pricing.PriceComponentInput, "input_tokens"}, {pricing.PriceComponentOutput, "output_tokens"},
		{pricing.PriceComponentCacheRead, "cache_read_tokens"}, {pricing.PriceComponentCacheWrite, "cache_write_tokens"},
		{pricing.PriceComponentReasoning, "reasoning_tokens"}, {pricing.PriceComponentPerRequest, "request"},
	} {
		code, ok := componentCode(test.component)
		if !ok || code != test.code {
			t.Fatalf("componentCode(%q) = %q, %v; want %q, true", test.component, code, ok, test.code)
		}
	}
	if _, ok := componentCode("future"); ok {
		t.Fatal("future component unexpectedly mapped")
	}
	if component, ok := componentForCode("output_tokens"); !ok || component != pricing.PriceComponentOutput {
		t.Fatalf("componentForCode(output_tokens) = %q, %v", component, ok)
	}
	if _, ok := componentForCode("future"); ok {
		t.Fatal("future component code unexpectedly mapped")
	}
}

func TestValidatePriceStatusIsClosed(t *testing.T) {
	for _, status := range []string{"exact", "partial", "unknown"} {
		if err := validatePriceStatus(status); err != nil {
			t.Fatalf("validatePriceStatus(%q) = %v", status, err)
		}
	}
	if err := validatePriceStatus("tampered"); err == nil {
		t.Fatal("unsupported persisted price status unexpectedly accepted")
	}
}

func TestValidatePersistableCatalogRejectsDigestLoss(t *testing.T) {
	base := pricing.Entry{Provider: "openai", Family: "openai_responses", EndpointID: "prod", Region: "global", Model: "gpt", ProviderTier: "standard", Version: "catalog-v1", EffectiveFrom: time.Unix(10, 0).UTC(), Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("1")}}
	catalog, err := pricing.CompileUSD("catalog-v1", []pricing.Entry{base})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validatePersistableCatalog(catalog); err != nil {
		t.Fatalf("round-trippable catalog rejected: %v", err)
	}
	base.Version = "entry-v1"
	withEntryVersion, err := pricing.CompileUSD("catalog-v1", []pricing.Entry{base})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validatePersistableCatalog(withEntryVersion); err == nil {
		t.Fatal("entry version mismatch unexpectedly accepted")
	}
	base.Version = "catalog-v1"
	base.Provenance = "operator-verified"
	withProvenance, err := pricing.CompileUSD("catalog-v1", []pricing.Entry{base})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validatePersistableCatalog(withProvenance); err == nil {
		t.Fatal("unrepresentable provenance unexpectedly accepted")
	}
}

func TestValidatePersistableCatalogCanonicalizesUTCIntervals(t *testing.T) {
	location := time.FixedZone("offset", 5*60*60)
	entry := pricing.Entry{
		Provider: "openai", Family: "openai_responses", EndpointID: "prod", Region: "global", Model: "gpt", ProviderTier: "standard",
		Version: "catalog-v1", EffectiveFrom: time.Date(2026, 7, 22, 12, 0, 0, 0, location), EffectiveUntil: time.Date(2026, 7, 23, 12, 0, 0, 0, location),
		Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("1")},
	}
	catalog, err := pricing.CompileUSD("catalog-v1", []pricing.Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := validatePersistableCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !canonical.Entries[0].EffectiveFrom.Equal(entry.EffectiveFrom) || canonical.Entries[0].EffectiveFrom.Location() != time.UTC {
		t.Fatalf("effective_from = %v, want UTC equivalent", canonical.Entries[0].EffectiveFrom)
	}
	if !canonical.Entries[0].EffectiveUntil.Equal(entry.EffectiveUntil) || canonical.Entries[0].EffectiveUntil.Location() != time.UTC {
		t.Fatalf("effective_until = %v, want UTC equivalent", canonical.Entries[0].EffectiveUntil)
	}
}
