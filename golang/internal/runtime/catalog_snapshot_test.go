package runtime

import (
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/catalog"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
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

	providerName, region, version, available, err := routePriceIdentity(bundle, "target-endpoint", endpoint, "model", []llm.ServiceClass{llm.ServiceClassStandard}, when)
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
	_, _, _, _, err := routePriceIdentity(bundle, "target", endpoint, "model", []llm.ServiceClass{llm.ServiceClassPriority}, time.Now())
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

	providerName, region, version, available, err := routePriceIdentity(bundle, "target-endpoint", endpoint, "model", []llm.ServiceClass{llm.ServiceClassPriority}, when)
	if err != nil {
		t.Fatalf("routePriceIdentity() error = %v", err)
	}
	if providerName != "verified-provider" || region != "australiaeast" || version != "" || available {
		t.Fatalf("identity = (%q, %q, %q, %t), want verified unpriced endpoint identity", providerName, region, version, available)
	}
}

func TestRoutePricedWindowsCoverEveryClassAcrossIntervalBoundaries(t *testing.T) {
	loaded := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
	start := loaded.Add(time.Hour)
	end := loaded.Add(48 * time.Hour)
	endpoint := config.EndpointConfig{Family: "openai_responses", Region: "australiaeast", PriceCatalog: "prices", ServiceClasses: map[llm.ServiceClass]config.TierConfig{
		llm.ServiceClassStandard: {ProviderValue: "default"},
		llm.ServiceClassPriority: {ProviderValue: "priority"},
	}}
	entry := func(tier string, from, until time.Time) pricing.Entry {
		return pricing.Entry{Provider: "verified-provider", Family: "openai_responses", EndpointID: "target", Region: "australiaeast", Model: "model", ProviderTier: tier, Version: "v1", EffectiveFrom: from, EffectiveUntil: until}
	}
	classes := []llm.ServiceClass{llm.ServiceClassStandard, llm.ServiceClassPriority}
	// Standard is priced from load onward; priority only from start to end.
	entries := []pricing.Entry{entry("default", time.Time{}, time.Time{}), entry("priority", start, end)}
	windows := routePricedWindows(entries, "target", endpoint, "model", classes, "verified-provider", "australiaeast")
	if len(windows) != 1 || !windows[0].From.Equal(start) || !windows[0].Until.Equal(end) {
		t.Fatalf("windows = %+v, want [%s, %s)", windows, start, end)
	}
	route := routing.Route{PriceAvailable: false, PricedWindows: windows}
	for at, want := range map[time.Time]bool{loaded: false, start: true, end.Add(-time.Second): true, end: false} {
		if got := route.PriceAvailableAt(at); got != want {
			t.Fatalf("PriceAvailableAt(%s) = %t, want %t", at, got, want)
		}
	}
	// Back-to-back intervals merge into one window; an always-priced route
	// gets one unbounded window.
	entries = []pricing.Entry{entry("default", time.Time{}, time.Time{}), entry("priority", time.Time{}, start), entry("priority", start, time.Time{})}
	if windows := routePricedWindows(entries, "target", endpoint, "model", classes, "verified-provider", "australiaeast"); len(windows) != 1 || !windows[0].From.IsZero() || !windows[0].Until.IsZero() {
		t.Fatalf("contiguous windows = %+v, want one unbounded window", windows)
	}
	if windows := routePricedWindows(entries[:1], "target", endpoint, "model", classes, "verified-provider", "australiaeast"); windows == nil || len(windows) != 0 {
		t.Fatalf("never fully priced windows = %+v, want empty and non-nil", windows)
	}
}

func TestRoutePricedWindowsIgnoreEntriesForAnotherProviderOrRegion(t *testing.T) {
	transition := time.Date(2026, time.July, 15, 0, 0, 0, 0, time.UTC)
	endpoint := config.EndpointConfig{Family: "openai_responses", PriceCatalog: "prices", ServiceClasses: map[llm.ServiceClass]config.TierConfig{llm.ServiceClassStandard: {ProviderValue: "default"}}}
	entry := func(providerName, region string, from, until time.Time) pricing.Entry {
		return pricing.Entry{Provider: providerName, Family: "openai_responses", EndpointID: "target", Region: region, Model: "model", ProviderTier: "default", Version: "v1", EffectiveFrom: from, EffectiveUntil: until}
	}
	// The endpoint moves from provider A in one region to provider B in
	// another; a route bound to A is priced only until the transition.
	entries := []pricing.Entry{entry("provider-a", "region-a", time.Time{}, transition), entry("provider-b", "region-b", transition, time.Time{})}
	windows := routePricedWindows(entries, "target", endpoint, "model", []llm.ServiceClass{llm.ServiceClassStandard}, "provider-a", "region-a")
	if len(windows) != 1 || !windows[0].From.IsZero() || !windows[0].Until.Equal(transition) {
		t.Fatalf("windows = %+v, want only provider A's interval ending at %s", windows, transition)
	}
}
