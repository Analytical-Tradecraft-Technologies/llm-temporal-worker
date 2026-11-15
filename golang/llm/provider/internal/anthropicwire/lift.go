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
			output = append(output, llm.ModelText(block.Text))
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
