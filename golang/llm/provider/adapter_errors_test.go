package provider

import (
	"context"
	"reflect"
	"testing"
)

func TestUniqueFeaturesKeepsFirstSeenOrder(t *testing.T) {
	got := UniqueFeatures([]Feature{FeatureText, "", FeatureImage, FeatureText, FeatureUsage, FeatureImage})
	want := []Feature{FeatureText, FeatureImage, FeatureUsage}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("UniqueFeatures = %v, want %v", got, want)
	}
}

func TestValidateCapabilityQuery(t *testing.T) {
	if err := ValidateCapabilityQuery(CapabilityQuery{}, "openai chat", FamilyOpenAIChat, "endpoint"); err != nil {
		t.Fatalf("empty query rejected: %v", err)
	}
	err := ValidateCapabilityQuery(CapabilityQuery{Family: FamilyAnthropicMessages}, "openai chat", FamilyOpenAIChat, "endpoint")
	if err == nil || err.Error() != `openai chat: capability family "anthropic_messages" does not match "openai_chat"` {
		t.Fatalf("family mismatch error = %v", err)
	}
	err = ValidateCapabilityQuery(CapabilityQuery{EndpointID: "other"}, "openai chat", FamilyOpenAIChat, "endpoint")
	if err == nil || err.Error() != `openai chat: capability endpoint "other" does not match "endpoint"` {
		t.Fatalf("endpoint mismatch error = %v", err)
	}
}

func TestContextErrorsAreNotDispatched(t *testing.T) {
	for _, mapped := range []*Error{
		NewCompileContextError(context.Canceled),
		NewCompileContextError(context.DeadlineExceeded),
		NewDispatchContextError(context.Canceled),
		NewDispatchContextError(context.DeadlineExceeded),
	} {
		if mapped.Dispatch != DispatchNotDispatched || mapped.Retry != RetryNever {
			t.Fatalf("context error = %#v", mapped)
		}
	}
	if code := NewDispatchContextError(context.Canceled).Code; code != CodeCanceled {
		t.Fatalf("canceled code = %s", code)
	}
	if code := NewDispatchContextError(context.DeadlineExceeded).Code; code != CodeDeadlineExceeded {
		t.Fatalf("deadline code = %s", code)
	}
}
