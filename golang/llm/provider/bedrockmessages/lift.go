package bedrockmessages

import (
	"encoding/json"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/anthropicwire"
)

// contentStateBlocks are the response content blocks kept as opaque provider
// state.
var contentStateBlocks = []string{"thinking", "redacted_thinking"}

func (profile Profile) liftResponse(call provider.Call, response *anthropic.Message, requestID string) (llm.Response, error) {
	return profile.liftResponseWithTier(call, response, requestID, "")
}

// liftResponseWithTier lifts a response whose service tier may have been
// reported outside the Anthropic body. InvokeModel carries it in a response
// header; a tier in usage still wins when the body has one.
func (profile Profile) liftResponseWithTier(call provider.Call, response *anthropic.Message, requestID, headerTier string) (llm.Response, error) {
	if response == nil {
		return llm.Response{}, provider.NewInvalidResponseError(call, requestID, "provider returned an empty response")
	}
	providerTier := string(response.Usage.ServiceTier)
	if providerTier == "" {
		providerTier = headerTier
	}
	actual := profile.actualClass(providerTier)
	output, states, hasToolCalls, err := anthropicwire.LiftContent(response.Content, "bedrock", contentStateBlocks)
	if err != nil {
		return llm.Response{}, provider.NewInvalidResponseErrorWithID(call, requestID, response.ID, err.Error())
	}
	hasRefusal := string(response.StopReason) == "refusal" || response.StopDetails.Category != ""
	status, err := liftStatus(response.StopReason, hasToolCalls, hasRefusal)
	if err != nil {
		return llm.Response{}, provider.NewInvalidResponseErrorWithID(call, requestID, response.ID, err.Error())
	}
	if err := anthropicwire.ValidateFinalJSON(call, output, status, hasToolCalls, hasRefusal); err != nil {
		return llm.Response{}, provider.NewInvalidResponseErrorWithID(call, requestID, response.ID, err.Error())
	}
	return llm.Response{
		APIVersion:   llm.APIVersion,
		OperationKey: call.OperationKey,
		Status:       status,
		Output:       output,
		Route: llm.RouteFacts{
			EndpointID: call.EndpointID, APIFamily: string(provider.FamilyBedrockMessages),
			RequestedModel: call.Model, ResolvedModel: string(response.Model),
		},
		Service: llm.ServiceFacts{
			Requested: call.ServiceClass, Attempted: call.ServiceClass, Actual: actual,
			ProviderValue: providerTier, FallbackIndex: 0,
		},
		Usage: llm.Usage{
			InputTokens: response.Usage.InputTokens, OutputTokens: response.Usage.OutputTokens,
			ReasoningTokens: response.Usage.OutputTokensDetails.ThinkingTokens,
			CacheReadTokens: response.Usage.CacheReadInputTokens, CacheWriteTokens: response.Usage.CacheCreationInputTokens,
			ProviderRaw: rawUsageFacts(response),
		},
		Provider: llm.ProviderFacts{
			ResponseID: response.ID, RequestID: requestID, FinishReason: string(response.StopReason), Raw: rawResponseFacts(response),
		},
		Continuation: continuationForResponse(call, response, states),
		Diagnostics:  liftDiagnostics(call),
	}, nil
}

// liftDiagnostics reports what a best-effort compilation left out of the
// provider request.
func liftDiagnostics(call provider.Call) []llm.Diagnostic {
	if !call.Metadata.ExtensionsDropped {
		return nil
	}
	return []llm.Diagnostic{{Code: "extensions_dropped", Severity: llm.DiagnosticWarning, Path: "/extensions",
		Message: "Bedrock Messages has no extension allowlist; the request's extensions were not sent to the provider"}}
}

func liftStatus(stopReason anthropic.StopReason, hasToolCalls, hasRefusal bool) (llm.ResponseStatus, error) {
	switch stopReason {
	case anthropic.StopReasonEndTurn, anthropic.StopReasonStopSequence, anthropic.StopReasonPauseTurn:
		if hasRefusal {
			return llm.ResponseStatusRefused, nil
		}
		if hasToolCalls {
			return llm.ResponseStatusToolCalls, nil
		}
		return llm.ResponseStatusCompleted, nil
	case anthropic.StopReasonToolUse:
		if !hasToolCalls {
			return "", fmt.Errorf("provider stop reason tool_use did not contain a tool call")
		}
		return llm.ResponseStatusToolCalls, nil
	case anthropic.StopReasonMaxTokens, anthropic.StopReasonModelContextWindowExceeded:
		// Context-window exhaustion is a length truncation: keep the partial
		// output and usage rather than discarding a paid response.
		return llm.ResponseStatusLength, nil
	case anthropic.StopReasonRefusal:
		return llm.ResponseStatusRefused, nil
	default:
		return "", fmt.Errorf("provider returned unknown stop reason %q", stopReason)
	}
}

func rawResponseFacts(response *anthropic.Message) map[string]json.RawMessage {
	result := map[string]json.RawMessage{}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(response.RawJSON()), &fields); err != nil {
		return result
	}
	for _, key := range []string{"stop_sequence", "container", "inference_geo", "stop_details"} {
		if raw, ok := fields[key]; ok {
			result[key] = append(json.RawMessage(nil), raw...)
		}
	}
	return result
}

func rawUsageFacts(response *anthropic.Message) map[string]json.RawMessage {
	result := map[string]json.RawMessage{}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(response.Usage.RawJSON()), &fields); err != nil {
		for key, value := range map[string]any{
			"service_tier":                response.Usage.ServiceTier,
			"cache_creation_input_tokens": response.Usage.CacheCreationInputTokens,
			"cache_read_input_tokens":     response.Usage.CacheReadInputTokens,
			"thinking_tokens":             response.Usage.OutputTokensDetails.ThinkingTokens,
		} {
			encoded, _ := json.Marshal(value)
			result[key] = encoded
		}
		return result
	}
	for key, raw := range fields {
		result[key] = append(json.RawMessage(nil), raw...)
	}
	return result
}

func continuationForResponse(call provider.Call, response *anthropic.Message, states []llm.ProviderState) *llm.Continuation {
	if response.ID == "" || len(states) == 0 {
		return nil
	}
	return &llm.Continuation{Handle: "bedrock-messages:" + response.ID, EndpointID: call.EndpointID, Model: string(response.Model), Pinned: true, ProviderStates: append([]llm.ProviderState(nil), states...)}
}
