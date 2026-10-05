package provider

import (
	"fmt"
	"strings"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

type Feature string

const (
	FeatureText             Feature = "text"
	FeatureImage            Feature = "image"
	FeatureDocument         Feature = "document"
	FeatureToolCall         Feature = "tool_call"
	FeatureStructuredOutput Feature = "structured_output"
	FeatureReasoning        Feature = "reasoning"
	FeatureContinuation     Feature = "continuation"
	FeatureStreaming        Feature = "streaming"
	FeatureUsage            Feature = "usage"
)

type CapabilityState string

const (
	CapabilityNative      CapabilityState = "native"
	CapabilityEmulated    CapabilityState = "emulated"
	CapabilityUnsupported CapabilityState = "unsupported"
	CapabilityUnknown     CapabilityState = "unknown"
)

func (state CapabilityState) Valid() bool {
	switch state {
	case CapabilityNative, CapabilityEmulated, CapabilityUnsupported, CapabilityUnknown:
		return true
	default:
		return false
	}
}

// ToolResultErrorTransform names the reviewed emulation used by endpoint
// families whose wire contract has no tool-result error field (OpenAI
// Responses function_call_output and Chat Completions tool messages). A tool
// result with is_error=true is sent as ordinary tool output text that starts
// with ToolResultErrorPrefix, followed by the unchanged result content. The
// prefix is a constant so compiled bodies stay deterministic.
const (
	ToolResultErrorTransform = "tool_result_error_text_prefix/v1"
	ToolResultErrorPrefix    = llm.ToolResultErrorTextPrefix
)

// ReservedToolResultPrefix returns the call ID of the first successful tool
// result whose text output already starts with ToolResultErrorPrefix. Such a
// result would be indistinguishable on the wire from a failed one, so strict
// compilation rejects it to keep the transform injective.
func ReservedToolResultPrefix(items []llm.Item) (string, bool) {
	for _, item := range items {
		result, ok := item.(llm.ToolResult)
		if !ok || result.IsError {
			continue
		}
		var output strings.Builder
		for _, part := range result.Content {
			switch value := part.(type) {
			case llm.TextPart:
				output.WriteString(value.Text)
			case llm.JSONPart:
				output.Write(value.Value)
			}
			if output.Len() >= len(ToolResultErrorPrefix) {
				break
			}
		}
		if strings.HasPrefix(output.String(), ToolResultErrorPrefix) {
			return result.CallID, true
		}
	}
	return "", false
}

type Capability struct {
	State     CapabilityState
	Transform string
	Reason    string
}

type CapabilityQuery struct {
	EndpointID   string
	Family       Family
	Model        string
	ServiceClass llm.ServiceClass
}

type CapabilitySet struct {
	Version  string
	Features map[Feature]Capability
}

func (set CapabilitySet) Resolve(feature Feature, strict bool) (Capability, error) {
	capability, ok := set.Features[feature]
	if !ok {
		capability = Capability{State: CapabilityUnknown, Reason: "no verified capability"}
	}
	if !capability.State.Valid() {
		return capability, fmt.Errorf("capability %q has invalid state %q", feature, capability.State)
	}
	if strict && (capability.State == CapabilityUnsupported || capability.State == CapabilityUnknown) {
		return capability, fmt.Errorf("capability %q is %s", feature, capability.State)
	}
	return capability, nil
}

func (set CapabilitySet) Supports(feature Feature, strict bool) bool {
	capability, err := set.Resolve(feature, strict)
	if err != nil {
		return false
	}
	return capability.State == CapabilityNative || capability.State == CapabilityEmulated
}
