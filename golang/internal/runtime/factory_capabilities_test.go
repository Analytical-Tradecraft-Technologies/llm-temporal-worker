package runtime

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
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
		provider.FeatureUsage:            {State: provider.CapabilityNative, Reason: "adapter lifts provider usage"},
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

func TestCatalogDerivedChatAdapterCompilesTextAndImageRequests(t *testing.T) {
	value, bundle := testRouteInputs(t)
	profile := bundle.Capabilities["profile-a"]
	profile.Set.Features[provider.FeatureImage] = provider.Capability{State: provider.CapabilityNative}
	bundle.Capabilities["profile-a"] = profile
	routes, err := compileRoutes(value, bundle, time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	route := routes.Models["logical-model"].Routes[0]
	if route.ProviderFeatures[string(provider.FeatureImage)].State != routing.CapabilityNative {
		t.Fatalf("route provider features = %#v, want catalog image capability", route.ProviderFeatures)
	}
	if _, leaked := route.Capabilities.Features[routing.Feature("image")]; leaked {
		t.Fatal("provider-only image capability leaked into routing")
	}

	route.EndpointID = "azure-chat"
	snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{"model": {Routes: []routing.Route{route}}}}}
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver:       secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) { return []byte("test-key"), nil }),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) { return engine.Snapshot{}, nil }),
		HTTPClient:     &http.Client{},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := factory.buildAdapter(context.Background(), azureOpenAIChatConfig(config.AuthConfig{Kind: "header_env", Name: "AZURE_OPENAI_API_KEY"}), snapshot, "azure-chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, strict := range []bool{true, false} {
		_, err = adapter.Compile(context.Background(), provider.CompileInput{
			Request: llm.Request{OperationKey: "catalog-capabilities", Model: "chat-deployment", ServiceClass: llm.ServiceClassStandard, Input: []llm.Item{
				llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
					llm.TextPart{Text: "describe"},
					llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png"},
				}},
			}},
			Query:  provider.CapabilityQuery{EndpointID: "azure-chat", Family: provider.FamilyOpenAIChat, Model: "chat-deployment"},
			Strict: strict,
		})
		if err != nil {
			t.Fatalf("Compile(strict=%t) error = %v", strict, err)
		}
	}
}
