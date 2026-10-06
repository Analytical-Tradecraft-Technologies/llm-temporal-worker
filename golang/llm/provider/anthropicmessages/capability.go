package anthropicmessages

import (
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func validateQuery(query provider.CapabilityQuery, endpointID string) error {
	if query.Family != "" && query.Family != provider.FamilyAnthropicMessages {
		return fmt.Errorf("anthropic messages: capability family %q does not match %q", query.Family, provider.FamilyAnthropicMessages)
	}
	if query.EndpointID != "" && query.EndpointID != endpointID {
		return fmt.Errorf("anthropic messages: capability endpoint %q does not match %q", query.EndpointID, endpointID)
	}
	return nil
}

func requiredFeatures(request llm.Request) []provider.Feature {
	features := []provider.Feature{provider.FeatureText, provider.FeatureUsage}
	for _, instruction := range request.Instructions {
		for _, part := range instruction.Content {
			features = append(features, partFeature(part))
		}
	}
	for _, item := range request.Input {
		switch value := item.(type) {
		case llm.Message:
			for _, part := range value.Content {
				features = append(features, partFeature(part))
			}
		case llm.ToolCall, llm.ToolResult:
			features = append(features, provider.FeatureToolCall)
		case llm.ProviderState:
			features = append(features, provider.FeatureContinuation, provider.FeatureReasoning)
		}
	}
	if len(request.Tools) > 0 || request.ToolPolicy.Mode != "" {
		features = append(features, provider.FeatureToolCall)
	}
	if request.Output != nil && request.Output.Format.Kind != "" && request.Output.Format.Kind != llm.OutputKindText {
		features = append(features, provider.FeatureStructuredOutput)
	}
	if request.Reasoning != nil {
		features = append(features, provider.FeatureReasoning)
	}
	if request.Continuation != nil {
		features = append(features, provider.FeatureContinuation)
	}
	return uniqueFeatures(features)
}

func partFeature(part llm.Part) provider.Feature {
	switch part.PartKind() {
	case llm.PartKindImage:
		return provider.FeatureImage
	case llm.PartKindDocument:
		return provider.FeatureDocument
	case llm.PartKindProviderState:
		return provider.FeatureContinuation
	default:
		return provider.FeatureText
	}
}

func uniqueFeatures(features []provider.Feature) []provider.Feature {
	seen := make(map[provider.Feature]struct{}, len(features))
	result := make([]provider.Feature, 0, len(features))
	for _, feature := range features {
		if feature == "" {
			continue
		}
		if _, ok := seen[feature]; ok {
			continue
		}
		seen[feature] = struct{}{}
		result = append(result, feature)
	}
	return result
}
