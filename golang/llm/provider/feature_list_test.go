package provider_test

import (
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestAllFeaturesListsEveryFeatureInCanonicalOrder(t *testing.T) {
	want := []provider.Feature{
		provider.FeatureText,
		provider.FeatureImage,
		provider.FeatureDocument,
		provider.FeatureToolCall,
		provider.FeatureStructuredOutput,
		provider.FeatureReasoning,
		provider.FeatureContinuation,
		provider.FeatureStreaming,
		provider.FeatureUsage,
	}
	got := provider.AllFeatures()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AllFeatures = %v, want %v", got, want)
	}
	got[0] = "changed"
	if provider.AllFeatures()[0] != provider.FeatureText {
		t.Fatal("AllFeatures returned a shared slice")
	}
}

func TestUnsupportedStreamingKeepsDeclaredReason(t *testing.T) {
	got := provider.UnsupportedStreaming(provider.Capability{State: provider.CapabilityNative, Transform: "sse", Reason: "profile reason"})
	if got != (provider.Capability{State: provider.CapabilityUnsupported, Reason: "profile reason"}) {
		t.Fatalf("UnsupportedStreaming = %+v", got)
	}
	got = provider.UnsupportedStreaming(provider.Capability{State: provider.CapabilityEmulated})
	if got != (provider.Capability{State: provider.CapabilityUnsupported, Reason: "adapter does not implement OpenStream"}) {
		t.Fatalf("UnsupportedStreaming = %+v", got)
	}
}
