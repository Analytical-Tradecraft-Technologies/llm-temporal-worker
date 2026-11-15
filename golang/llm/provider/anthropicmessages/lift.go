package anthropicmessages

import (
	"encoding/json"

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
	output, states, status, err := anthropicwire.LiftMessage(call, requestID, response, "anthropic", contentStateBlocks, true)
	if err != nil {
		return llm.Response{}, err
	}

	providerRaw := anthropicwire.RawResponseFacts(response)
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
	usage := anthropicwire.Usage(response)
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
	result := llm.Response{
		APIVersion:   llm.APIVersion,
		OperationKey: call.OperationKey,
		Status:       status,
		Output:       output,
		Route:        call.RouteFacts(provider.FamilyAnthropicMessages, string(response.Model)),
		Service:      call.ServiceFacts(actual, responseTier(response)),
		Usage:        usage,
		Provider:     providerFacts,
		Continuation: anthropicwire.Continuation(call, response, states, "anthropic-messages:"),
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
