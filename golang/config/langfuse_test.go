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
