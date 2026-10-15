// Package thinkingwire checks the cross-field rules Anthropic-format Messages
// APIs place on manual extended thinking.
//
// Each request control is lowered independently, so every field can be valid
// on its own while the combination is one the provider documents as
// rejected. The check runs on the final wire document, after defaults and
// profile extensions, so it sees exactly what would be sent.
// https://platform.claude.com/docs/en/build-with-claude/extended-thinking
package thinkingwire

import (
	"encoding/json"
	"fmt"
)

// Validate rejects a manual thinking request (thinking.type "enabled") whose
// budget, sampling or tool choice the provider would refuse. Adaptive and
// disabled thinking are unaffected. provider names the API in errors.
//
// The documented exception that lets a budget exceed max_tokens needs the
// interleaved-thinking beta, which neither Messages adapter can enable, so
// the budget must always be below max_tokens here.
func Validate(wire map[string]any, provider string) error {
	thinking, ok := wire["thinking"].(map[string]any)
	if !ok || thinking["type"] != "enabled" {
		return nil
	}
	budget, budgetOK := number(thinking["budget_tokens"])
	maxTokens, maxOK := number(wire["max_tokens"])
	if budgetOK && maxOK && budget >= maxTokens {
		return fmt.Errorf("%s thinking token_budget %v must be less than max_tokens %v", provider, budget, maxTokens)
	}
	if choice, ok := wire["tool_choice"].(map[string]any); ok {
		if kind := choice["type"]; kind == "any" || kind == "tool" {
			return fmt.Errorf("%s thinking with a token budget does not support forced tool choice %q; use auto or none", provider, kind)
		}
	}
	// Temperature and top_k may not be modified with thinking; top_p may only
	// be narrowed to between 0.95 and 1.
	if value, exists := wire["temperature"]; exists {
		if temperature, ok := number(value); !ok || temperature != 1 {
			return fmt.Errorf("%s thinking with a token budget does not support temperature %v", provider, value)
		}
	}
	if _, exists := wire["top_k"]; exists {
		return fmt.Errorf("%s thinking with a token budget does not support top_k", provider)
	}
	if value, exists := wire["top_p"]; exists {
		if topP, ok := number(value); !ok || topP < 0.95 || topP > 1 {
			return fmt.Errorf("%s thinking with a token budget requires top_p between 0.95 and 1, got %v", provider, value)
		}
	}
	return nil
}

// number converts the numeric forms a wire document holds: semantic fields
// lowered from Go values and extension fields decoded from JSON.
func number(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}
