package anthropicmessages

import (
	"encoding/json"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/anthropicschema"
)

func (profile Profile) liftResponse(call provider.Call, response *anthropic.Message, requestID string) (llm.Response, error) {
	if response == nil {
		return llm.Response{}, invalidResponseError(call, requestID, "provider returned an empty response")
	}
	actual, err := profile.actualClass(string(response.Usage.ServiceTier))
	if err != nil {
		mapped := invalidResponseError(call, requestID, err.Error())
		mapped.Provider.ResponseID = response.ID
		return llm.Response{}, mapped
	}
	output, states, hasToolCalls, hasRefusal, err := liftContent(response.Content)
	if err != nil {
		mapped := invalidResponseError(call, requestID, err.Error())
		mapped.Provider.ResponseID = response.ID
		return llm.Response{}, mapped
	}
	if string(response.StopReason) == "refusal" {
		hasRefusal = true
	}
	if response.StopDetails.Category != "" {
		hasRefusal = true
	}
	status, err := liftStatus(response.StopReason, hasToolCalls, hasRefusal)
	if err != nil {
		mapped := invalidResponseError(call, requestID, err.Error())
		mapped.Provider.ResponseID = response.ID
		return llm.Response{}, mapped
	}
	if err := validateFinalJSON(call, output, status, hasToolCalls, hasRefusal); err != nil {
		mapped := invalidResponseError(call, requestID, err.Error())
		mapped.Provider.ResponseID = response.ID
		return llm.Response{}, mapped
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
		usage.ProviderRaw["web_search_calls"] = json.RawMessage("null")
		var envelope struct {
			Usage struct {
				ServerToolUse *struct {
					WebSearchRequests *int64 `json:"web_search_requests"`
				} `json:"server_tool_use"`
			} `json:"usage"`
		}
		if json.Unmarshal([]byte(response.RawJSON()), &envelope) == nil && envelope.Usage.ServerToolUse != nil && envelope.Usage.ServerToolUse.WebSearchRequests != nil {
			usage.ProviderRaw["web_search_calls"], _ = json.Marshal(*envelope.Usage.ServerToolUse.WebSearchRequests)
		}
	}
	output = append(output, llm.WebSearchReferences([]byte(response.RawJSON()))...)
	service := llm.ServiceFacts{
		Requested:     call.ServiceClass,
		Attempted:     call.ServiceClass,
		Actual:        actual,
		ProviderValue: string(response.Usage.ServiceTier),
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
	return result, nil
}

// validateFinalJSON enforces the caller's json_schema output locally. The
// provider only saw a lowered schema, so constraints moved into descriptions
// are checked here before the bytes enter Temporal history.
func validateFinalJSON(call provider.Call, output []llm.Item, status llm.ResponseStatus, hasToolCalls, hasRefusal bool) error {
	// Incomplete text is retained for callers and accounting, not validated
	// as a promised complete JSON document.
	if status != llm.ResponseStatusCompleted || hasToolCalls || hasRefusal || len(call.OutputSchema) == 0 {
		return nil
	}
	return anthropicschema.Validate(call.OutputSchema, output)
}

func liftContent(blocks []anthropic.ContentBlockUnion) ([]llm.Item, []llm.ProviderState, bool, bool, error) {
	output := make([]llm.Item, 0, len(blocks))
	states := make([]llm.ProviderState, 0)
	hasToolCalls := false
	hasRefusal := false
	for index, block := range blocks {
		switch block.Type {
		case "text":
			text := block.Text
			if len(output) > 0 {
				if message, ok := output[len(output)-1].(llm.Message); ok && message.Actor == llm.ActorModel {
					message.Content = append(message.Content, llm.TextPart{Text: text})
					output[len(output)-1] = message
					continue
				}
			}
			output = append(output, llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: text}}})
		case "thinking", "redacted_thinking", "server_tool_use", "web_search_tool_result", "web_fetch_tool_result", "code_execution_tool_result", "bash_code_execution_tool_result", "text_editor_code_execution_tool_result":
			raw, err := contentBlockRaw(block)
			if err != nil {
				return nil, nil, false, false, fmt.Errorf("content block %d: %w", index, err)
			}
			state := llm.ProviderState{
				Provider:       "anthropic",
				EndpointFamily: "messages",
				MediaType:      "application/vnd.anthropic.content-block+json",
				Opaque:         raw,
			}
			states = append(states, state)
			output = append(output, state)
		case "tool_use":
			if block.ID == "" || block.Name == "" {
				return nil, nil, false, false, fmt.Errorf("content block %d tool_use is missing id or name", index)
			}
			arguments := append(json.RawMessage(nil), block.Input...)
			if len(arguments) == 0 {
				arguments = json.RawMessage(`{}`)
			}
			if !json.Valid(arguments) {
				return nil, nil, false, false, fmt.Errorf("content block %d tool_use input is invalid JSON", index)
			}
			// encoding/json accepts duplicate keys but a normalized tool call
			// does not. Reject them here as a classified invalid response.
			if _, err := llm.CanonicalJSON(arguments); err != nil {
				return nil, nil, false, false, fmt.Errorf("content block %d tool_use input is invalid JSON", index)
			}
			hasToolCalls = true
			output = append(output, llm.ToolCall{ID: block.ID, Name: block.Name, Arguments: arguments})
		default:
			return nil, nil, false, false, fmt.Errorf("content block %d has unsupported type %q", index, block.Type)
		}
	}
	return output, states, hasToolCalls, hasRefusal, nil
}

func contentBlockRaw(block anthropic.ContentBlockUnion) ([]byte, error) {
	raw := []byte(block.RawJSON())
	if len(raw) == 0 {
		var err error
		raw, err = json.Marshal(block)
		if err != nil {
			return nil, err
		}
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("provider state is not valid JSON")
	}
	return append([]byte(nil), raw...), nil
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

func invalidResponseError(call provider.Call, requestID, message string) *provider.Error {
	mapped := provider.NewError(provider.CodeProviderInvalidResponse, provider.PhaseLift, provider.DispatchAccepted, provider.RetryNever, message)
	mapped.Provider.RequestID = requestID
	mapped.OperationID = call.OperationKey
	return mapped
}
