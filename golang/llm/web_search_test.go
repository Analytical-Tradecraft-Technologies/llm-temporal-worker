package llm

import "testing"

func TestWebSearchTransportSupport(t *testing.T) {
	for _, model := range []string{"openai/gpt-test", "anthropic/claude-test", "google/gemini-test", "x-ai/grok-test", "deepseek/deepseek-test", "qwen/qwen-test", "z-ai/glm-test", "other/model"} {
		if !SupportsWebSearch("openrouter", "openai_chat", model) {
			t.Errorf("OpenRouter server search rejected for %s", model)
		}
	}
	for _, test := range []struct {
		provider string
		family   string
		want     bool
	}{
		{"openai", "openai_responses", true},
		{"anthropic", "anthropic_messages", true},
		{"openai", "openai_chat", false},
		{"other", "openai_chat", false},
		{"openrouter", "openai_responses", false},
		{"openrouter", "anthropic_messages", false},
		{"openrouter", "unknown", false},
	} {
		if got := SupportsWebSearch(test.provider, test.family, "other/model"); got != test.want {
			t.Errorf("%s/%s: got %t, want %t", test.provider, test.family, got, test.want)
		}
	}
}
