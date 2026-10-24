package openairesponses

import (
	"context"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

const (
	adapterName              = "openai.responses"
	defaultCapabilityVersion = "openai-responses/v1"
)

// capabilities is intentionally explicit. A route may only claim a feature
// after the adapter has a lossless lowering/lifting path for that feature.
func capabilities(version string) provider.CapabilitySet {
	return provider.CapabilitySet{
		Version: version,
		Features: map[provider.Feature]provider.Capability{
			provider.FeatureText:             {State: provider.CapabilityNative},
			provider.FeatureImage:            {State: provider.CapabilityNative},
			provider.FeatureDocument:         {State: provider.CapabilityNative},
			provider.FeatureToolCall:         {State: provider.CapabilityNative},
			provider.FeatureStructuredOutput: {State: provider.CapabilityNative},
			provider.FeatureReasoning:        {State: provider.CapabilityNative},
			provider.FeatureContinuation:     {State: provider.CapabilityNative},
			provider.FeatureStreaming: {
				State:  provider.CapabilityUnsupported,
				Reason: "the adapter has an SSE decoder but no typed stream port or official SDK stream dispatch",
			},
			provider.FeatureUsage: {State: provider.CapabilityNative},
		},
	}
}

// Capabilities is kept as a small helper so tests and future route discovery
// can inspect the exact adapter claims without constructing an SDK request.
func (adapter *Adapter) capabilities(ctx context.Context, query provider.CapabilityQuery) (provider.CapabilitySet, error) {
	if err := ctx.Err(); err != nil {
		return provider.CapabilitySet{}, err
	}
	if err := provider.ValidateCapabilityQuery(query, "openai responses", provider.FamilyOpenAIResponses, adapter.endpointID); err != nil {
		return provider.CapabilitySet{}, err
	}
	result := capabilities(adapter.capabilityVersion)
	if adapter.storageDenied {
		result.Features[provider.FeatureContinuation] = provider.Capability{State: provider.CapabilityUnsupported, Reason: "endpoint forbids provider storage"}
	}
	return result, nil
}
