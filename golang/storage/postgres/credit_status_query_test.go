package postgres

import (
	"crypto/sha256"
	"strings"
	"testing"
)

func TestCreditStatusListOptionsNormalizeDefaultsAndBounds(t *testing.T) {
	options := CreditStatusListOptions{ConfigDigest: sha256.Sum256([]byte("config"))}
	if err := options.Normalize(); err != nil {
		t.Fatal(err)
	}
	if options.Limit != DefaultCreditStatusPageSize {
		t.Fatalf("default page size = %d, want %d", options.Limit, DefaultCreditStatusPageSize)
	}
	for _, test := range []struct {
		name    string
		options CreditStatusListOptions
	}{
		{name: "missing digest", options: CreditStatusListOptions{}},
		{name: "oversized page", options: CreditStatusListOptions{ConfigDigest: options.ConfigDigest, Limit: MaxCreditStatusPageSize + 1}},
		{name: "unsafe provider", options: CreditStatusListOptions{ConfigDigest: options.ConfigDigest, Provider: " provider"}},
		{name: "unsafe continuation", options: CreditStatusListOptions{ConfigDigest: options.ConfigDigest, AfterEndpointKey: strings.Repeat("e", 257)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.options.Normalize(); err == nil {
				t.Fatal("invalid options were accepted")
			}
		})
	}
}

func TestCreditStatusListQueryBindsIncidentEvidenceToProjectionEpoch(t *testing.T) {
	query := creditStatusListQuery("routes", "events")
	for _, expected := range []string{
		"e.config_digest = r.config_digest",
		"e.config_epoch = r.config_epoch",
		"e.route_id = r.route_id",
		"e.observed_at <= $4",
		"r.observed_at <= $4",
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("credit status query missing %q: %s", expected, query)
		}
	}
}
