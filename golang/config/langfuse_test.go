package config_test

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"testing"
)

func TestLangfuseEndpointDefaultsAndOverrides(t *testing.T) {
	direct := config.EndpointConfig{Family: "openai_chat"}
	router := config.EndpointConfig{Family: "openai_chat", Extensions: map[string]map[string]any{"openrouter": {}}}
	if !direct.LangfuseEnabled() || router.LangfuseEnabled() {
		t.Fatal("direct must default on and OpenRouter off")
	}
	on, off := true, false
	router.Langfuse = &config.LangfuseEndpointConfig{Enabled: &on}
	direct.Langfuse = &config.LangfuseEndpointConfig{Enabled: &off}
	if !router.LangfuseEnabled() || direct.LangfuseEnabled() {
		t.Fatal("explicit overrides must win")
	}
}

func TestLangfuseCredentialsRequirePair(t *testing.T) {
	c, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	c.Langfuse = &config.LangfuseConfig{BaseURL: "https://cloud.langfuse.com", PublicKey: config.SecretRef{Kind: "env", Name: "LANGFUSE_PUBLIC_KEY"}}
	if c.Validate() == nil {
		t.Fatal("missing secret key accepted")
	}
	c.Langfuse.SecretKey = config.SecretRef{Kind: "env", Name: "LANGFUSE_SECRET_KEY"}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Langfuse.BaseURL = "https://user:password@cloud.langfuse.com"
	if c.Validate() == nil {
		t.Fatal("embedded credentials accepted")
	}
}

func TestLangfuseMaxInFlightDefaultsAndLeavesModelSlots(t *testing.T) {
	c, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	c.Langfuse = &config.LangfuseConfig{BaseURL: "https://cloud.langfuse.com", PublicKey: config.SecretRef{Kind: "env", Name: "LANGFUSE_PUBLIC_KEY"}, SecretKey: config.SecretRef{Kind: "env", Name: "LANGFUSE_SECRET_KEY"}}
	if err := c.Validate(); err != nil || c.Langfuse.EffectiveMaxInFlight(c.Temporal.Worker.MaxConcurrentActivities) != config.DefaultLangfuseMaxInFlight {
		t.Fatalf("omitted limit: %v, %d", err, c.Langfuse.EffectiveMaxInFlight(c.Temporal.Worker.MaxConcurrentActivities))
	}
	slots := c.Temporal.Worker.MaxConcurrentActivities
	c.Langfuse.MaxInFlight = slots - 1
	if err := c.Validate(); err != nil || c.Langfuse.EffectiveMaxInFlight(slots) != slots-1 {
		t.Fatalf("limit below the Activity slots: %v", err)
	}
	for _, invalid := range []int{-1, slots, slots + 1} {
		c.Langfuse.MaxInFlight = invalid
		if c.Validate() == nil {
			t.Fatalf("max_in_flight %d accepted with %d Activity slots", invalid, slots)
		}
	}
	// The omitted default also leaves a model slot on a small worker.
	c.Langfuse.MaxInFlight = 0
	for slots, want := range map[int]int{1: 1, 2: 1, 3: 2, 16: 2} {
		if got := c.Langfuse.EffectiveMaxInFlight(slots); got != want {
			t.Fatalf("default with %d Activity slots = %d, want %d", slots, got, want)
		}
	}
}
