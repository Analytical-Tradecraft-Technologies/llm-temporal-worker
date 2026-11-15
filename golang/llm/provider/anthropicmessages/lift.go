package anthropicmessages

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
var contentStateBlocks = []string{"thinking", "redacted_thinking", "server_tool_use", "web_search_tool_result", "web_fetch_tool_result", "code_execution_tool_result", "bash_code_execution_tool_result", "text_editor_code_execution_tool_result"}

func (profile Profile) liftResponse(call provider.Call, response *anthropic.Message, requestID string) (llm.Response, error) {
	if response == nil {
		return llm.Response{}, provider.NewInvalidResponseError(call, requestID, "provider returned an empty response")
	}
	actual := profile.actualClass(responseTier(response))
	output, states, hasToolCalls, err := anthropicwire.LiftContent(response.Content, "anthropic", contentStateBlocks)
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

	providerRaw := rawResponseFacts(response)
	var rawEnvelope map[string]json.RawMessage
	if json.Unmarshal([]byte(response.RawJSON()), &rawEnvelope) == nil {
		if raw := rawEnvelope["container"]; len(raw) > 0 {
			providerRaw["container"] = raw
			var container struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(raw, &container) == nil && container.ID != "" {
				state := llm.ProviderState{Provider: "anthropic", EndpointFamily: "messages", MediaType: "application/vnd.anthropic.container+json", Opaque: raw}
				output = append(output, state)
				states = append(states, state)
			}
		}
	}
	providerFacts := llm.ProviderFacts{
		ResponseID:   response.ID,
		RequestID:    requestID,
		FinishReason: string(response.StopReason),
		Raw:          providerRaw,
	}
	usage := llm.Usage{
		InputTokens:      response.Usage.InputTokens,
		OutputTokens:     response.Usage.OutputTokens,
		ReasoningTokens:  response.Usage.OutputTokensDetails.ThinkingTokens,
		CacheReadTokens:  response.Usage.CacheReadInputTokens,
		CacheWriteTokens: response.Usage.CacheCreationInputTokens,
		ProviderRaw:      rawUsageFacts(response),
	}
	if usage.ProviderRaw == nil {
		usage.ProviderRaw = make(map[string]json.RawMessage)
	}
	if call.Metadata.CodeExecution {
		usage.ProviderRaw["hosted_execution"] = json.RawMessage("true")
		for _, block := range response.Content {
			if block.Type == "server_tool_use" && (block.Name == "code_execution" || block.Name == "bash_code_execution" || block.Name == "text_editor_code_execution") {
				usage.ProviderRaw["hosted_execution_used"] = json.RawMessage("true")
			}
		}
	}
	if call.Metadata.WebSearch {
		usage.ProviderRaw["web_search_calls"] = webSearchCallCount(response)
	}
	output = append(output, llm.WebSearchReferences([]byte(response.RawJSON()))...)
	service := llm.ServiceFacts{
		Requested:     call.ServiceClass,
		Attempted:     call.ServiceClass,
		Actual:        actual,
		ProviderValue: responseTier(response),
		FallbackIndex: 0,
	}
	result := llm.Response{
		APIVersion:   llm.APIVersion,
		OperationKey: call.OperationKey,
		Status:       status,
		Output:       output,
		Route: llm.RouteFacts{
			EndpointID:     call.EndpointID,
			APIFamily:      string(provider.FamilyAnthropicMessages),
			RequestedModel: call.Model,
			ResolvedModel:  string(response.Model),
		},
		Service:      service,
		Usage:        usage,
		Provider:     providerFacts,
		Continuation: continuationForResponse(call, response, states),
	}
	if _, err := llm.HostedToolCharge(usage, result.Route.ResolvedModel); err != nil {
		result.Cost.Status = llm.CostStatusUnknown
	}
	if priceModifiedUsage(usage.ProviderRaw, call.Metadata.ProviderTier) {
		result.Cost.Status, result.Cost.UnknownReason = llm.CostStatusUnknown, llm.CostUnknownCatalogIncomplete
	}
	return result, nil
}

// priceModifiedUsage reports whether the provider billed the response with a
// request-level multiplier the catalog entry does not model: US-only inference
// (usage.inference_geo "us", 1.1x on Claude 4.6 and later) or fast mode
// (usage.speed "fast", 2x) on a call that did not attempt the fast tier, whose
// own catalog entry carries fast-mode prices. A workspace default_inference_geo
// applies without any request field, so only the reported usage settles the
// question. Such a response is kept at the reservation rather than settled at
// base prices.
func priceModifiedUsage(raw map[string]json.RawMessage, attemptedTier string) bool {
	modifiers := map[string]string{"inference_geo": "global"}
	if attemptedTier != FastTier {
		modifiers["speed"] = "standard"
	}
	for key, base := range modifiers {
		value, present := raw[key]
		if !present || string(value) == "null" {
			continue
		}
		var reported string
		if json.Unmarshal(value, &reported) != nil || reported != base {
			return true
		}
	}
	return false
}

func webSearchCallCount(response *anthropic.Message) json.RawMessage {
	unknown := json.RawMessage("null")
	var usage map[string]json.RawMessage
	if json.Unmarshal([]byte(response.Usage.RawJSON()), &usage) != nil || usage == nil {
		return unknown
	}
	if raw, present := usage["server_tool_use"]; present {
		var server map[string]json.RawMessage
		if json.Unmarshal(raw, &server) != nil || server == nil {
			return unknown
		}
		if raw, present := server["web_search_requests"]; present {
			var count *int64
			if json.Unmarshal(raw, &count) != nil || count == nil || *count < 0 {
				return unknown
			}
			encoded, _ := json.Marshal(*count)
			return encoded
		}
	}
	// Anthropic can omit search usage when the available tool was not used.
	// Search blocks without a counter are incomplete billing evidence, so
	// keep their charge unknown instead of counting attempts as billed uses.
	if !response.JSON.Content.Valid() {
		return unknown
	}
	for _, block := range response.Content {
		if block.Type == "web_search_tool_result" || (block.Type == "server_tool_use" && block.Name == "web_search") {
			return unknown
		}
	}
	return json.RawMessage("0")
}

func liftStatus(stopReason anthropic.StopReason, hasToolCalls, hasRefusal bool) (llm.ResponseStatus, error) {
	switch stopReason {
	case anthropic.StopReasonPauseTurn:
		return llm.ResponseStatusPaused, nil
	case anthropic.StopReasonEndTurn, anthropic.StopReasonStopSequence:
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

// responseTier names the provider tier that served the response. Fast mode is
// reported as usage.speed "fast" beside usage.service_tier "standard", so it
// is named FastTier to price and classify it apart from standard.
func responseTier(response *anthropic.Message) string {
	var usage struct {
		Speed string `json:"speed"`
	}
	if json.Unmarshal([]byte(response.Usage.RawJSON()), &usage) == nil && usage.Speed == "fast" {
		return FastTier
	}
	return string(response.Usage.ServiceTier)
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
		// Constructed SDK responses do not carry RawJSON; retain the typed fields
		// that are useful for reconciliation instead.
		for key, value := range map[string]any{
			"service_tier":                string(response.Usage.ServiceTier),
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
	return &llm.Continuation{
		Handle:         "anthropic-messages:" + response.ID,
		EndpointID:     call.EndpointID,
		Model:          string(response.Model),
		Pinned:         true,
		ProviderStates: append([]llm.ProviderState(nil), states...),
	}
}
