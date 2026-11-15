package bedrockmessages

import (
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
	output, states, status, err := anthropicwire.LiftMessage(call, requestID, response, "bedrock", contentStateBlocks, liftStatus)
	if err != nil {
		return llm.Response{}, err
	}
	return llm.Response{
		APIVersion:   llm.APIVersion,
		OperationKey: call.OperationKey,
		Status:       status,
		Output:       output,
		Route:        call.RouteFacts(provider.FamilyBedrockMessages, string(response.Model)),
		Service:      call.ServiceFacts(actual, providerTier),
		Usage:        anthropicwire.Usage(response),
		Provider: llm.ProviderFacts{
			ResponseID: response.ID, RequestID: requestID, FinishReason: string(response.StopReason), Raw: anthropicwire.RawResponseFacts(response),
		},
		Continuation: anthropicwire.Continuation(call, response, states, "bedrock-messages:"),
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
