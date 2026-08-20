package anthropicmessages

import (
	"encoding/json"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
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

	providerRaw := rawResponseFacts(response)
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
	return result, nil
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
		case "thinking", "redacted_thinking":
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
	case anthropic.StopReasonMaxTokens:
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
