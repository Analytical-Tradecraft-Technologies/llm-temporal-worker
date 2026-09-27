package runtime

import (
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/catalog"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestRoutePriceIdentityUsesEndpointIdentity(t *testing.T) {
	when := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
	endpoint := config.EndpointConfig{
		Family:       "openai_responses",
		Region:       "australiaeast",
		PriceCatalog: "prices",
		ServiceClasses: map[llm.ServiceClass]config.TierConfig{
			llm.ServiceClassStandard: {ProviderValue: "standard"},
		},
	}
	bundle := catalog.Bundle{Pricing: map[string]catalog.PricingCatalog{
		"prices": {Version: "prices-v1", Catalog: pricing.Catalog{
			Version: "prices-v1",
			Entries: []pricing.Entry{
				{Provider: "wrong-provider", Family: "openai_responses", EndpointID: "other-endpoint", Region: "australiaeast", Model: "model", ProviderTier: "standard", Version: "wrong"},
				{Provider: "right-provider", Family: "openai_responses", EndpointID: "target-endpoint", Region: "australiaeast", Model: "model", ProviderTier: "standard", Version: "right"},
			},
		}},
	}}

	providerName, region, version, available, err := routePriceIdentity(bundle, "target-endpoint", endpoint, "model", []llm.ServiceClass{llm.ServiceClassStandard}, 0, when)
	if err != nil {
		t.Fatalf("routePriceIdentity() error = %v", err)
	}
	if providerName != "right-provider" || region != "australiaeast" || version != "right" || !available {
		t.Fatalf("identity = (%q, %q, %q, %t), want target endpoint quote", providerName, region, version, available)
	}
}

func TestRoutePriceIdentityRejectsMissingEndpointQuote(t *testing.T) {
	endpoint := config.EndpointConfig{
		Family:       "openai_responses",
		PriceCatalog: "prices",
		ServiceClasses: map[llm.ServiceClass]config.TierConfig{
			llm.ServiceClassPriority: {ProviderValue: "priority"},
		},
	}
	bundle := catalog.Bundle{Pricing: map[string]catalog.PricingCatalog{
		"prices": {Catalog: pricing.Catalog{Version: "prices-v1", Entries: []pricing.Entry{{
			Provider: "provider", Family: "openai_responses", EndpointID: "other", Model: "model", ProviderTier: "priority",
		}}}},
	}}
	_, _, _, _, err := routePriceIdentity(bundle, "target", endpoint, "model", []llm.ServiceClass{llm.ServiceClassPriority}, 0, time.Now())
	if err == nil {
		t.Fatal("routePriceIdentity() succeeded without endpoint-specific quote")
	}
	if got := err.Error(); got == "" || !strings.Contains(got, "no price entry") {
		t.Fatalf("error = %q, want missing quote", got)
	}
}

func TestRoutePriceIdentityUsesVerifiedIdentityWithoutCurrentQuote(t *testing.T) {
	when := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
	endpoint := config.EndpointConfig{
		Family:       "openai_responses",
		Region:       "australiaeast",
		PriceCatalog: "prices",
		ServiceClasses: map[llm.ServiceClass]config.TierConfig{
			llm.ServiceClassPriority: {ProviderValue: "priority"},
		},
	}
	stale := pricing.Entry{Provider: "verified-provider", Family: "openai_responses", EndpointID: "target-endpoint", Region: "australiaeast", Model: "model", ProviderTier: "priority", Version: "old-v1", EffectiveUntil: when.Add(-time.Second)}
	bundle := catalog.Bundle{Pricing: map[string]catalog.PricingCatalog{
		"prices": {Version: "prices-v1", Catalog: pricing.Catalog{Version: "prices-v1", Entries: []pricing.Entry{stale}}},
	}}

	providerName, region, version, available, err := routePriceIdentity(bundle, "target-endpoint", endpoint, "model", []llm.ServiceClass{llm.ServiceClassPriority}, 0, when)
	if err != nil {
		t.Fatalf("routePriceIdentity() error = %v", err)
	}
	if providerName != "verified-provider" || region != "australiaeast" || version != "" || available {
		t.Fatalf("identity = (%q, %q, %q, %t), want verified unpriced endpoint identity", providerName, region, version, available)
	}
}
