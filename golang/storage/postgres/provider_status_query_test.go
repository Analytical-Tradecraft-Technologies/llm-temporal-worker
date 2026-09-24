package postgres

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/control"
)

func TestProviderStatusListOptionsNormalizeDefaultsAndBounds(t *testing.T) {
	options := ProviderStatusListOptions{ConfigDigest: sha256.Sum256([]byte("config"))}
	if err := options.Normalize(); err != nil {
		t.Fatal(err)
	}
	if options.Limit != DefaultProviderStatusPageSize {
		t.Fatalf("default page size = %d, want %d", options.Limit, DefaultProviderStatusPageSize)
	}

	for _, test := range []struct {
		name    string
		options ProviderStatusListOptions
	}{
		{name: "missing digest", options: ProviderStatusListOptions{}},
		{name: "oversized page", options: ProviderStatusListOptions{ConfigDigest: options.ConfigDigest, Limit: MaxProviderStatusPageSize + 1}},
		{name: "invalid availability", options: ProviderStatusListOptions{ConfigDigest: options.ConfigDigest, Availability: control.Availability("future")}},
		{name: "unsafe provider", options: ProviderStatusListOptions{ConfigDigest: options.ConfigDigest, Provider: " provider"}},
		{name: "unsafe route key", options: ProviderStatusListOptions{ConfigDigest: options.ConfigDigest, AfterRouteID: strings.Repeat("r", 257)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.options.Normalize(); err == nil {
				t.Fatal("invalid options were accepted")
			}
		})
	}
}

func TestProviderStatusProjectionEnumsAreClosed(t *testing.T) {
	for _, value := range []control.Availability{
		control.AvailabilityAvailable, control.AvailabilityDegraded,
		control.AvailabilityUnavailable, control.AvailabilityUnknown,
	} {
		if !validProviderStatusAvailability(value) {
			t.Fatalf("availability %q rejected", value)
		}
	}
	if validProviderStatusAvailability(control.Availability("future")) {
		t.Fatal("unknown availability accepted")
	}
	if validProviderStatusCredit(control.CreditState("future")) {
		t.Fatal("unknown credit state accepted")
	}
	if validProviderStatusBilling(control.BillingState("future")) {
		t.Fatal("unknown billing state accepted")
	}
	if validProviderStatusCircuit(control.CircuitState("future")) {
		t.Fatal("unknown circuit state accepted")
	}
}
