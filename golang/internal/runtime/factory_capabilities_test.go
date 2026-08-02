package runtime

import (
	"reflect"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

func TestEndpointCapabilitiesRejectsSameVersionFeatureConflicts(t *testing.T) {
	tests := []struct {
		name  string
		left  provider.Capability
		right provider.Capability
	}{
		{
			name:  "state",
			left:  provider.Capability{State: provider.CapabilityNative},
			right: provider.Capability{State: provider.CapabilityUnsupported},
		},
		{
			name:  "transform",
			left:  provider.Capability{State: provider.CapabilityEmulated, Transform: "json-v1"},
			right: provider.Capability{State: provider.CapabilityEmulated, Transform: "json-v2"},
		},
		{
			name:  "reason",
			left:  provider.Capability{State: provider.CapabilityUnknown, Reason: "model-specific"},
			right: provider.Capability{State: provider.CapabilityUnknown, Reason: "region-specific"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := endpointCapabilities(capabilityConflictSnapshot(test.left, test.right), "bedrock")
			if err == nil || !strings.Contains(err.Error(), "conflicting capability declaration") {
				t.Fatalf("endpointCapabilities() error = %v, want a feature declaration conflict", err)
			}
		})
	}
}

func TestEndpointCapabilitiesAcceptsEquivalentFeatureMaps(t *testing.T) {
	features := map[routing.Feature]routing.Capability{
		routing.FeatureText: {
			State: routing.CapabilityNative,
		},
		routing.FeatureToolCall: {
			State:     routing.CapabilityEmulated,
			Transform: "json-tool-v1",
			Reason:    "provider schema transform",
		},
	}
	snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
		"model-a": {Routes: []routing.Route{{EndpointID: "bedrock", Capabilities: routing.CapabilitySet{Version: "bedrock/v1", Features: cloneRoutingFeatures(features)}}}},
		"model-b": {Routes: []routing.Route{{EndpointID: "bedrock", Capabilities: routing.CapabilitySet{Version: "bedrock/v1", Features: cloneRoutingFeatures(features)}}}},
	}}}
	got, err := endpointCapabilities(snapshot, "bedrock")
	if err != nil {
		t.Fatalf("endpointCapabilities() error = %v, want equivalent declarations accepted", err)
	}
	want := provider.CapabilitySet{Version: "bedrock/v1", Features: map[provider.Feature]provider.Capability{
		provider.FeatureText: {
			State: provider.CapabilityNative,
		},
		provider.FeatureToolCall: {
			State:     provider.CapabilityEmulated,
			Transform: "json-tool-v1",
			Reason:    "provider schema transform",
		},
		provider.FeatureImage:            {State: provider.CapabilityUnknown, Reason: "catalog did not declare this capability"},
		provider.FeatureDocument:         {State: provider.CapabilityUnknown, Reason: "catalog did not declare this capability"},
		provider.FeatureStructuredOutput: {State: provider.CapabilityUnknown, Reason: "catalog did not declare this capability"},
		provider.FeatureReasoning:        {State: provider.CapabilityUnknown, Reason: "catalog did not declare this capability"},
		provider.FeatureContinuation:     {State: provider.CapabilityUnknown, Reason: "catalog did not declare this capability"},
		provider.FeatureStreaming:        {State: provider.CapabilityUnknown, Reason: "catalog did not declare this capability"},
		provider.FeatureUsage:            {State: provider.CapabilityUnknown, Reason: "catalog did not declare this capability"},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("endpointCapabilities() = %#v, want %#v", got, want)
	}
}

func capabilityConflictSnapshot(left, right provider.Capability) engine.Snapshot {
	return engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
		"model-a": {Routes: []routing.Route{{EndpointID: "bedrock", Capabilities: routing.CapabilitySet{Version: "bedrock/v1", Features: map[routing.Feature]routing.Capability{routing.FeatureText: routing.Capability{State: routing.CapabilityState(left.State), Transform: left.Transform, Reason: left.Reason}}}}}},
		"model-b": {Routes: []routing.Route{{EndpointID: "bedrock", Capabilities: routing.CapabilitySet{Version: "bedrock/v1", Features: map[routing.Feature]routing.Capability{routing.FeatureText: routing.Capability{State: routing.CapabilityState(right.State), Transform: right.Transform, Reason: right.Reason}}}}}},
	}}}
}

func cloneRoutingFeatures(features map[routing.Feature]routing.Capability) map[routing.Feature]routing.Capability {
	clone := make(map[routing.Feature]routing.Capability, len(features))
	for feature, capability := range features {
		clone[feature] = capability
	}
	return clone
}
