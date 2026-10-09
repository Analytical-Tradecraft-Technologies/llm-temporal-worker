package anthropicmessages

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

const haiku55Model = "claude-haiku-5-5"

// Haiku 5.5 uses adaptive thinking and does not offer Priority Tier. Reject
// incompatible semantic settings rather than silently changing their meaning.
// https://platform.claude.com/docs/en/models/haiku-5-5/migration-guide
func validateHaiku55Request(request llm.Request) error {
	if request.Model != haiku55Model {
		return nil
	}
	if request.ServiceClass == llm.ServiceClassPriority {
		return unsupportedServiceError("Haiku 5.5 does not support Priority Tier")
	}
	if reasoning := request.Reasoning; reasoning != nil && (reasoning.Mode == llm.ReasoningModeEnabled || reasoning.TokenBudget != nil) {
		return unsupportedError(provider.FeatureReasoning, "Haiku 5.5 supports adaptive thinking without a token budget")
	}
	return nil
}

// Validate the final document too: an allowed profile extension can supply or
// replace these fields, including combining them with semantic sampling fields.
func validateHaiku55Wire(model string, wire map[string]any) error {
	if model != haiku55Model {
		return nil
	}
	unsupported := func(message string) error {
		return provider.NewError(provider.CodeUnsupportedCapability, provider.PhaseCompile, provider.DispatchNotDispatched, provider.RetryNever, "Haiku 5.5 "+message)
	}
	temperature, hasTemperature := wire["temperature"]
	topP, hasTopP := wire["top_p"]
	if hasTemperature && hasTopP {
		return unsupported("does not support combining temperature and top_p")
	}
	if hasTemperature && temperature != float64(1) {
		return unsupported("only supports temperature=1")
	}
	if hasTopP && topP != float64(0.99) {
		return unsupported("only supports top_p=0.99")
	}
	if _, hasTopK := wire["top_k"]; hasTopK {
		return unsupported("does not support top_k")
	}
	if value, exists := wire["thinking"]; exists {
		thinking, ok := value.(map[string]any)
		if !ok {
			return unsupported("only supports adaptive or disabled thinking")
		}
		if _, hasBudget := thinking["budget_tokens"]; hasBudget {
			return unsupported("does not support a thinking token budget")
		}
		if thinking["type"] != "adaptive" && thinking["type"] != "disabled" {
			return unsupported("only supports adaptive or disabled thinking")
		}
	}
	return nil
}
