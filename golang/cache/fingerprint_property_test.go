package cache

import (
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestFingerprintRouteIdentityFieldsNeverShare(t *testing.T) {
	base := testInput()
	baseFingerprint, err := Compute([]byte("secret"), base)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		edit func(*Input)
	}{
		{"provider", func(input *Input) { input.Route.Provider = "azure-openai" }},
		{"endpoint", func(input *Input) { input.Route.Endpoint = "https://azure.test" }},
		{"account", func(input *Input) { input.Route.Account = "acct-b" }},
		{"region", func(input *Input) { input.Route.Region = "eu-west-1" }},
		{"model", func(input *Input) { input.Route.Model = "different-display-model" }},
		{"revision", func(input *Input) { input.Route.Revision = "gpt-2026-02" }},
		{"compiler", func(input *Input) { input.Route.Compiler = "openai-chat/v1" }},
		{"config", func(input *Input) { input.Config = "config-b" }},
		{"capability", func(input *Input) { input.CapabilityLowering = "cap/v2" }},
		{"epoch", func(input *Input) { input.Epoch = "epoch-2" }},
		{"conversation", func(input *Input) { input.Conversation = "sha256:other" }},
		{"cache maximum age", func(input *Input) { input.Policy.MaxAge = 10 * time.Minute }},
		{"cache variant", func(input *Input) { input.Policy.Variant++ }},
		{"service class", func(input *Input) { input.Request.ServiceClass = llm.ServiceClassPriority }},
		{"service fallbacks", func(input *Input) { input.Request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassEconomy} }},
		{"tool policy", func(input *Input) { input.Request.ToolPolicy = llm.ToolPolicy{Mode: llm.ToolChoiceRequired} }},
		{"output", func(input *Input) {
			input.Request.Input = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "different"}}}}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			input := base
			test.edit(&input)
			fingerprint, err := Compute([]byte("secret"), input)
			if err != nil {
				t.Fatal(err)
			}
			if fingerprint == baseFingerprint {
				t.Fatalf("%s did not change route-isolated fingerprint", test.name)
			}
		})
	}
}
