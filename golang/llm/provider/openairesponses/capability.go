package openairesponses

import (
	"context"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
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
			provider.FeatureContinuation: {
				State:  provider.CapabilityUnsupported,
				Reason: "provider-hosted response identifiers are prohibited; replay the canonical transcript",
			},
			provider.FeatureStreaming: {
				State:  provider.CapabilityUnsupported,
				Reason: "the adapter has an SSE decoder but no typed stream port or official SDK stream dispatch",
			},
			provider.FeatureUsage: {State: provider.CapabilityNative},
		},
	}
}

func validateQuery(query provider.CapabilityQuery, endpointID string) error {
	if query.Family != "" && query.Family != provider.FamilyOpenAIResponses {
		return fmt.Errorf("openai responses: capability family %q does not match %q", query.Family, provider.FamilyOpenAIResponses)
	}
	if query.EndpointID != "" && query.EndpointID != endpointID {
		return fmt.Errorf("openai responses: capability endpoint %q does not match %q", query.EndpointID, endpointID)
	}
	return nil
}

// Capabilities is kept as a small helper so tests and future route discovery
// can inspect the exact adapter claims without constructing an SDK request.
func (adapter *Adapter) capabilities(ctx context.Context, query provider.CapabilityQuery) (provider.CapabilitySet, error) {
	if err := ctx.Err(); err != nil {
		return provider.CapabilitySet{}, err
	}
	if err := validateQuery(query, adapter.endpointID); err != nil {
		return provider.CapabilitySet{}, err
	}
	return capabilities(adapter.capabilityVersion), nil
}
