package config_test

import (
	"os"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
)

func withoutOpenRouterUpstreamOverrides(t *testing.T) {
	t.Helper()
	for _, name := range []string{config.EnvOpenRouterCountries, config.EnvOpenRouterRequireNativePrecision, config.EnvOpenRouterRequireZeroRetention, config.EnvOpenRouterDenyPromptTraining} {
		value, present := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if present {
				_ = os.Setenv(name, value)
			}
		})
	}
}

func TestOpenRouterUpstreamDefaultsToCore5WithNoFilters(t *testing.T) {
	withoutOpenRouterUpstreamOverrides(t)
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.OpenRouterUpstream != nil {
		t.Fatalf("unset upstream settings = %+v, want omitted so the digest is unchanged", loaded.OpenRouterUpstream)
	}
	if got := loaded.OpenRouterUpstreamSettings(); got != (config.OpenRouterUpstreamConfig{Countries: "Core5"}) {
		t.Fatalf("effective upstream settings = %+v", got)
	}
}

func TestOpenRouterUpstreamEnvironmentOverrides(t *testing.T) {
	withoutOpenRouterUpstreamOverrides(t)
	t.Setenv(config.EnvOpenRouterCountries, "Core2")
	t.Setenv(config.EnvOpenRouterRequireNativePrecision, "true")
	t.Setenv(config.EnvOpenRouterRequireZeroRetention, "false")
	t.Setenv(config.EnvOpenRouterDenyPromptTraining, "1")
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	want := config.OpenRouterUpstreamConfig{Countries: "Core2", RequireNativePrecision: true, DenyPromptTraining: true}
	if got := loaded.OpenRouterUpstreamSettings(); got != want {
		t.Fatalf("upstream settings = %+v, want %+v", got, want)
	}
	snapshot, err := config.Compile(t.Context(), exampleYAML(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(snapshot.Canonical()), `"openrouter_upstream":{"countries":"Core2","deny_prompt_training":true,"require_native_precision":true}`) {
		t.Fatalf("effective config omitted the upstream settings: %s", snapshot.Canonical())
	}
}

func TestOpenRouterUpstreamRejectsInvalidValues(t *testing.T) {
	for name, value := range map[string]string{
		config.EnvOpenRouterCountries:              "Core9",
		config.EnvOpenRouterRequireNativePrecision: "yes",
		config.EnvOpenRouterRequireZeroRetention:   "",
		config.EnvOpenRouterDenyPromptTraining:     "on",
	} {
		t.Run(name, func(t *testing.T) {
			withoutOpenRouterUpstreamOverrides(t)
			t.Setenv(name, value)
			if _, err := config.Load(exampleYAML(t)); err == nil {
				t.Fatalf("%s=%q accepted", name, value)
			}
		})
	}
	withoutOpenRouterUpstreamOverrides(t)
	t.Setenv(config.EnvOpenRouterCountries, "")
	if _, err := config.Load(exampleYAML(t)); err == nil {
		t.Fatal("an empty country list was accepted")
	}
}
