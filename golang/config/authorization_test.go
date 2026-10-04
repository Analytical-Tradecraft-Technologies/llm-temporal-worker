package config_test

import (
	"context"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
)

func TestAuthorizationPolicyValidation(t *testing.T) {
	valid := func() config.AuthorizationConfig {
		return config.AuthorizationConfig{Mode: config.AuthorizationTrustedTemporal, AllowedScopes: []config.AuthorizedScope{{Tenant: "tenant", Project: "project"}}}
	}
	for name, edit := range map[string]func(*config.AuthorizationConfig){
		"missing mode":    func(p *config.AuthorizationConfig) { p.Mode = "" },
		"unknown mode":    func(p *config.AuthorizationConfig) { p.Mode = "allow_all" },
		"empty scopes":    func(p *config.AuthorizationConfig) { p.AllowedScopes = nil },
		"empty tenant":    func(p *config.AuthorizationConfig) { p.AllowedScopes[0].Tenant = "" },
		"empty project":   func(p *config.AuthorizationConfig) { p.AllowedScopes[0].Project = "" },
		"tenant wildcard": func(p *config.AuthorizationConfig) { p.AllowedScopes[0].Tenant = "*" },
		"project pattern": func(p *config.AuthorizationConfig) { p.AllowedScopes[0].Project = "proj?ct" },
		"space":           func(p *config.AuthorizationConfig) { p.AllowedScopes[0].Tenant = " tenant" },
		"control":         func(p *config.AuthorizationConfig) { p.AllowedScopes[0].Project = "project\x00" },
		"unicode space":   func(p *config.AuthorizationConfig) { p.AllowedScopes[0].Tenant = "tenant\u2003" },
		"invalid utf8":    func(p *config.AuthorizationConfig) { p.AllowedScopes[0].Tenant = "tenant\xff" },
		"oversize":        func(p *config.AuthorizationConfig) { p.AllowedScopes[0].Tenant = strings.Repeat("x", 257) },
		"duplicate":       func(p *config.AuthorizationConfig) { p.AllowedScopes = append(p.AllowedScopes, p.AllowedScopes[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			policy := valid()
			edit(&policy)
			if err := policy.Validate(); err == nil {
				t.Fatal("accepted invalid authorization policy")
			}
		})
	}
	policy := valid()
	policy.AllowedScopes = append(policy.AllowedScopes, config.AuthorizedScope{Tenant: "租户", Project: "project:two"})
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorizationPolicyStrictLoadAndSnapshotIsolation(t *testing.T) {
	data := exampleYAML(t)
	snapshot, err := config.Compile(context.Background(), data, nil)
	if err != nil {
		t.Fatal(err)
	}
	copy := snapshot.Config()
	if copy.Authorization == nil {
		t.Fatal("missing example authorization")
	}
	copy.Authorization.AllowedScopes[0].Tenant = "modified"
	if snapshot.Config().Authorization.AllowedScopes[0].Tenant != "acme" {
		t.Fatal("mutable authorization snapshot")
	}
	next, err := config.Compile(context.Background(), []byte(strings.Replace(string(data), "project: invoice-processing", "project: other", 1)), nil)
	if err != nil || next.Digest() == snapshot.Digest() {
		t.Fatalf("policy omitted from digest: %v", err)
	}
	for _, replacement := range []string{"mode: allow_all", "mode: trusted_temporal\n  unknown: true", "mode: trusted_temporal\n  mode: trusted_temporal"} {
		if _, err := config.Load([]byte(strings.Replace(string(data), "mode: trusted_temporal", replacement, 1))); err == nil {
			t.Fatal("accepted invalid strict policy")
		}
	}
	// Embeddings may deliberately supply their own scope resolver; omission
	// must not silently enable the trusted Temporal policy.
	embedded, err := config.Compile(context.Background(), data, config.ReferenceResolverFunc(func(_ context.Context, c *config.Config) error { c.Authorization = nil; return nil }))
	if err != nil || embedded.Config().Authorization != nil {
		t.Fatalf("defaulted caller trust: %v", err)
	}
}
