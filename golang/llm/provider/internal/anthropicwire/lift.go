package anthropicwire

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/anthropicschema"
)

// LiftContent lifts response content blocks in order: text into model
// messages, blocks whose type is one of opaqueTypes into content-block
// provider state for stateProvider, and tool_use blocks into tool calls. It
// returns the output, the provider states among it, and whether any block was
// a tool call. Any other block type is an invalid response.
func LiftContent(blocks []anthropic.ContentBlockUnion, stateProvider string, opaqueTypes []string) ([]llm.Item, []llm.ProviderState, bool, error) {
	output := make([]llm.Item, 0, len(blocks))
	states := make([]llm.ProviderState, 0)
	hasToolCalls := false
	for index, block := range blocks {
		switch {
		case block.Type == "text":
			if len(output) > 0 {
				if message, ok := output[len(output)-1].(llm.Message); ok && message.Actor == llm.ActorModel {
					message.Content = append(message.Content, llm.TextPart{Text: block.Text})
					output[len(output)-1] = message
					continue
				}
			}
			output = append(output, llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: block.Text}}})
		case slices.Contains(opaqueTypes, block.Type):
			raw, err := contentBlockRaw(block)
			if err != nil {
				return nil, nil, false, fmt.Errorf("content block %d: %w", index, err)
			}
			state := llm.ProviderState{Provider: stateProvider, EndpointFamily: "messages", MediaType: "application/vnd.anthropic.content-block+json", Opaque: raw}
			states = append(states, state)
			output = append(output, state)
		case block.Type == "tool_use":
			if block.ID == "" || block.Name == "" {
				return nil, nil, false, fmt.Errorf("content block %d tool_use is missing id or name", index)
			}
			arguments := append(json.RawMessage(nil), block.Input...)
			if len(arguments) == 0 {
				arguments = json.RawMessage(`{}`)
			}
			if !json.Valid(arguments) {
				return nil, nil, false, fmt.Errorf("content block %d tool_use input is invalid JSON", index)
			}
			// encoding/json accepts duplicate keys but a normalized tool call
			// does not, and arguments nested deeper than
			// llm.MaxToolArgumentsDepth could not be saved. Reject both here as
			// a classified invalid response.
			if _, err := llm.CanonicalToolArguments(arguments); err != nil {
				return nil, nil, false, fmt.Errorf("content block %d tool_use input is invalid JSON", index)
			}
			hasToolCalls = true
			output = append(output, llm.ToolCall{ID: block.ID, Name: block.Name, Arguments: arguments})
		default:
			return nil, nil, false, fmt.Errorf("content block %d has unsupported type %q", index, block.Type)
		}
	}
	return output, states, hasToolCalls, nil
}

// contentBlockRaw returns a copy of the block's raw JSON, or its encoding
// when the SDK kept none.
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

// ValidateFinalJSON enforces the caller's json_schema output locally. The
// provider only saw a lowered schema, so constraints moved into descriptions
// are checked here before the bytes enter Temporal history.
func ValidateFinalJSON(call provider.Call, output []llm.Item, status llm.ResponseStatus, hasToolCalls, hasRefusal bool) error {
	// Incomplete text is retained for callers and accounting, not validated
	// as a promised complete JSON document.
	if status != llm.ResponseStatusCompleted || hasToolCalls || hasRefusal || len(call.OutputSchema) == 0 {
		return nil
	}
	return anthropicschema.Validate(call.OutputSchema, output)
}

// LiftMessage lifts the content of response as LiftContent does, derives its
// status with liftStatus and checks the final JSON output. A failure is an
// invalid response carrying requestID and the response ID.
func LiftMessage(call provider.Call, requestID string, response *anthropic.Message, stateProvider string, opaqueTypes []string, liftStatus func(anthropic.StopReason, bool, bool) (llm.ResponseStatus, error)) ([]llm.Item, []llm.ProviderState, llm.ResponseStatus, error) {
	invalid := func(err error) ([]llm.Item, []llm.ProviderState, llm.ResponseStatus, error) {
		return nil, nil, "", provider.NewInvalidResponseErrorWithID(call, requestID, response.ID, err.Error())
	}
	output, states, hasToolCalls, err := LiftContent(response.Content, stateProvider, opaqueTypes)
	if err != nil {
		return invalid(err)
	}
	hasRefusal := string(response.StopReason) == "refusal" || response.StopDetails.Category != ""
	status, err := liftStatus(response.StopReason, hasToolCalls, hasRefusal)
	if err != nil {
		return invalid(err)
	}
	if err := ValidateFinalJSON(call, output, status, hasToolCalls, hasRefusal); err != nil {
		return invalid(err)
	}
	return output, states, status, nil
}

// Usage returns the token usage of response, with its raw usage facts.
func Usage(response *anthropic.Message) llm.Usage {
	return llm.Usage{
		InputTokens:      response.Usage.InputTokens,
		OutputTokens:     response.Usage.OutputTokens,
		ReasoningTokens:  response.Usage.OutputTokensDetails.ThinkingTokens,
		CacheReadTokens:  response.Usage.CacheReadInputTokens,
		CacheWriteTokens: response.Usage.CacheCreationInputTokens,
		ProviderRaw:      rawUsageFacts(response),
	}
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

// RawResponseFacts copies the response-level facts kept for reconciliation
// out of the raw response body.
func RawResponseFacts(response *anthropic.Message) map[string]json.RawMessage {
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

// Continuation returns the pinned continuation of response, whose handle is
// handlePrefix followed by the response ID, or nil when the response has no
// ID or no provider states.
func Continuation(call provider.Call, response *anthropic.Message, states []llm.ProviderState, handlePrefix string) *llm.Continuation {
	if response.ID == "" || len(states) == 0 {
		return nil
	}
	return &llm.Continuation{
		Handle:         handlePrefix + response.ID,
		EndpointID:     call.EndpointID,
		Model:          string(response.Model),
		Pinned:         true,
		ProviderStates: append([]llm.ProviderState(nil), states...),
	}
}
