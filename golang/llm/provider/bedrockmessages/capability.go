package bedrockmessages

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

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
	return provider.UniqueFeatures(features)
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
