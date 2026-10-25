package anthropicmessages

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/schemaorder"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/packages/param"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/anthropicwire"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/thinkingwire"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/toolcallid"
)

func lowerRequest(request llm.Request, profile Profile, serviceTier string) (anthropic.MessageNewParams, error) {
	return lowerRequestWithStrict(request, profile, serviceTier, false)
}

func lowerRequestWithStrict(request llm.Request, profile Profile, serviceTier string, strict bool) (anthropic.MessageNewParams, error) {
	if err := validateHaiku55Request(request); err != nil {
		return anthropic.MessageNewParams{}, err
	}
	if strict && anthropicwire.HasMixedInstructionLevels(request.Instructions) {
		return anthropic.MessageNewParams{}, provider.NewStrictPortabilityError("instruction hierarchy cannot be preserved by Anthropic Messages in strict portability mode")
	}
	messages := make([]any, 0, len(request.Input))
	var containerID string
	if request.Continuation != nil {
		for _, state := range request.Continuation.ProviderStates {
			if isContainerState(state) {
				id, err := containerStateID(state)
				if err != nil {
					return anthropic.MessageNewParams{}, err
				}
				containerID = id
			}
		}
	}
	if err := appendContinuationStates(&messages, request.Continuation, profile, ""); err != nil {
		return anthropic.MessageNewParams{}, err
	}
	for index, item := range request.Input {
		if value, ok := item.(llm.ProviderState); ok && isContainerState(value) {
			id, err := containerStateID(value)
			if err != nil {
				return anthropic.MessageNewParams{}, err
			}
			containerID = id
			continue
		}
		// A reference is an output annotation (for example a citation) that
		// a replayed transcript still carries. It has no wire form, so it is
		// left out instead of failing every later turn.
		if _, annotation := item.(llm.Reference); annotation {
			continue
		}
		message, err := lowerItem(item)
		if err != nil {
			return anthropic.MessageNewParams{}, fmt.Errorf("input item %d: %w", index, err)
		}
		if anthropicwire.EmptyModelMessage(item) {
			continue
		}
		messages = append(messages, message)
	}

	maxTokens := profile.DefaultMaxTokens
	if maxTokens == 0 {
		maxTokens = defaultMaxTokens
	}
	requestMap := map[string]any{
		"model":      request.Model,
		"max_tokens": maxTokens,
		"messages":   messages,
	}
	if request.Output != nil {
		if request.Output.MaxTokens != nil {
			if *request.Output.MaxTokens < 0 {
				return anthropic.MessageNewParams{}, fmt.Errorf("output max_tokens must not be negative")
			}
			requestMap["max_tokens"] = *request.Output.MaxTokens
		}
		if err := wire.LowerOutput(*request.Output, requestMap, strict); err != nil {
			return anthropic.MessageNewParams{}, err
		}
	}
	if len(request.Instructions) > 0 {
		system := make([]any, 0)
		for index, instruction := range request.Instructions {
			blocks, err := lowerInstruction(instruction)
			if err != nil {
				return anthropic.MessageNewParams{}, fmt.Errorf("instruction %d: %w", index, err)
			}
			system = append(system, blocks...)
		}
		requestMap["system"] = system
	}
	if serviceTier == FastTier {
		requestMap["service_tier"] = "standard_only"
		requestMap["speed"] = "fast"
	} else if serviceTier != "" {
		requestMap["service_tier"] = serviceTier
	}
	if request.Sampling != nil {
		if err := wire.LowerSampling(*request.Sampling, requestMap); err != nil {
			return anthropic.MessageNewParams{}, err
		}
	}
	if request.Reasoning != nil {
		thinking, err := wire.LowerReasoning(*request.Reasoning, strict)
		if err != nil {
			return anthropic.MessageNewParams{}, err
		}
		if thinking != nil {
			requestMap["thinking"] = thinking
		}
		if effort := anthropicwire.ReasoningOutputEffort(*request.Reasoning); effort != "" {
			outputConfig, _ := requestMap["output_config"].(map[string]any)
			if outputConfig == nil {
				outputConfig = map[string]any{}
				requestMap["output_config"] = outputConfig
			}
			outputConfig["effort"] = effort
		}
	}
	if containerID != "" {
		requestMap["container"] = containerID
	}
	if len(request.Tools) > 0 || request.WebSearch || request.WebFetch || request.CodeExecution {
		tools, err := lowerTools(request.Tools)
		if err != nil {
			return anthropic.MessageNewParams{}, err
		}
		if request.WebSearch {
			tools = append(tools, map[string]any{"type": "web_search_20250305", "name": "web_search", "max_uses": llm.MaxWebSearchCalls})
		}
		if request.WebFetch {
			tools = append(tools, map[string]any{"type": "web_fetch_20250910", "name": "web_fetch", "max_uses": llm.MaxWebSearchCalls, "max_content_tokens": 10000, "citations": map[string]any{"enabled": true}})
		}
		if request.CodeExecution {
			tools = append(tools, map[string]any{"type": "code_execution_20250825", "name": "code_execution"})
		}
		requestMap["tools"] = tools
		choice, err := anthropicwire.LowerToolPolicy(request.ToolPolicy)
		if err != nil {
			return anthropic.MessageNewParams{}, err
		}
		if choice != nil {
			requestMap["tool_choice"] = choice
		}
	} else if request.ToolPolicy.Mode != "" && request.ToolPolicy.Mode != llm.ToolChoiceAuto && request.ToolPolicy.Mode != llm.ToolChoiceNone {
		return anthropic.MessageNewParams{}, fmt.Errorf("tool policy %q requires at least one tool", request.ToolPolicy.Mode)
	}
	if err := lowerExtensions(profile, request.Extensions, requestMap); err != nil {
		return anthropic.MessageNewParams{}, err
	}
	if err := validateHaiku55Wire(request.Model, requestMap); err != nil {
		return anthropic.MessageNewParams{}, err
	}
	if err := thinkingwire.Validate(requestMap, "Anthropic Messages"); err != nil {
		return anthropic.MessageNewParams{}, err
	}

	encoded, err := json.Marshal(requestMap)
	if err != nil {
		return anthropic.MessageNewParams{}, err
	}
	var params anthropic.MessageNewParams
	if err := json.Unmarshal(encoded, &params); err != nil {
		return anthropic.MessageNewParams{}, fmt.Errorf("anthropic messages parameter union: %w", err)
	}
	// Keep the exact validated wire document as the SDK override. This retains
	// opaque thinking signatures and redacted blocks byte-for-byte while the
	// concrete typed fields above remain available to callers and tests.
	param.SetJSON(encoded, &params)
	return params, nil
}

func lowerInstruction(instruction llm.Instruction) ([]any, error) {
	parts := instruction.Content
	if instruction.Kind == llm.InstructionKindText || (instruction.Kind == "" && len(parts) == 0) {
		parts = []llm.Part{llm.TextPart{Text: instruction.Text}}
	}
	return lowerInstructionParts(parts)
}

func lowerInstructionParts(parts []llm.Part) ([]any, error) {
	blocks := make([]any, 0, len(parts))
	for index, part := range parts {
		switch value := part.(type) {
		case llm.TextPart:
			blocks = append(blocks, map[string]any{"type": "text", "text": value.Text})
		case llm.JSONPart:
			if !json.Valid(value.Value) {
				return nil, fmt.Errorf("instruction part %d JSON is invalid", index)
			}
			blocks = append(blocks, map[string]any{"type": "text", "text": string(value.Value)})
		default:
			return nil, fmt.Errorf("instruction part %d kind %q is not supported by Anthropic system blocks", index, part.PartKind())
		}
	}
	return blocks, nil
}

func lowerItem(item llm.Item) (map[string]any, error) {
	switch value := item.(type) {
	case llm.Message:
		role := "user"
		if value.Actor == llm.ActorModel {
			role = "assistant"
		}
		content, err := lowerParts(value.Content)
		if err != nil {
			return nil, err
		}
		return map[string]any{"role": role, "content": content}, nil
	case llm.ToolCall:
		if value.ID == "" || value.Name == "" {
			return nil, fmt.Errorf("tool call requires ID and name")
		}
		if !json.Valid(value.Arguments) {
			return nil, fmt.Errorf("tool call %q arguments are invalid JSON", value.ID)
		}
		// Preserve numeric literals in historical tool arguments.
		input := json.RawMessage(value.Arguments)
		return map[string]any{
			"role": "assistant",
			"content": []any{map[string]any{
				"type":  "tool_use",
				"id":    toolcallid.Portable(value.ID),
				"name":  value.Name,
				"input": input,
			}},
		}, nil
	case llm.ToolResult:
		if value.CallID == "" {
			return nil, fmt.Errorf("tool result requires call ID")
		}
		content, err := lowerToolResultContent(value.Content)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"role": "user",
			"content": []any{map[string]any{
				"type":        "tool_result",
				"tool_use_id": toolcallid.Portable(value.CallID),
				"content":     content,
				"is_error":    value.IsError,
			}},
		}, nil
	case llm.ProviderState:
		block, err := providerStateRaw(value, "")
		if err != nil {
			return nil, err
		}
		return map[string]any{"role": "assistant", "content": []any{block}}, nil
	case llm.Reference:
		return nil, fmt.Errorf("reference input is not accepted by Anthropic Messages")
	default:
		return nil, fmt.Errorf("unsupported input item %T", item)
	}
}

func lowerImage(value llm.ImagePart) (map[string]any, error) {
	if value.Blob != nil {
		return nil, fmt.Errorf("image blob-backed media is not available to adapter")
	}
	if value.URL == "" && len(value.Bytes) == 0 {
		return nil, fmt.Errorf("image requires URL or bytes")
	}
	if value.Detail != "" {
		return nil, fmt.Errorf("image detail %q is not supported by Anthropic Messages", value.Detail)
	}
	switch strings.ToLower(value.MediaType) {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
	default:
		return nil, fmt.Errorf("image media type %q is not supported by Anthropic Messages", value.MediaType)
	}
	source := map[string]any{}
	if value.URL != "" {
		source["type"] = "url"
		source["url"] = value.URL
	} else {
		source["type"] = "base64"
		source["media_type"] = value.MediaType
		source["data"] = base64.StdEncoding.EncodeToString(value.Bytes)
	}
	result := map[string]any{"type": "image", "source": source}
	return result, nil
}

func lowerDocument(value llm.DocumentPart) (map[string]any, error) {
	if value.Blob != nil {
		return nil, fmt.Errorf("document blob-backed media is not available to adapter")
	}
	if value.URL == "" && len(value.Bytes) == 0 {
		return nil, fmt.Errorf("document requires URL or bytes")
	}
	mediaType := strings.ToLower(value.MediaType)
	result := map[string]any{"type": "document"}
	if value.URL != "" {
		if mediaType != "application/pdf" {
			return nil, fmt.Errorf("document URL media type %q is not supported by Anthropic Messages", value.MediaType)
		}
		result["source"] = map[string]any{"type": "url", "url": value.URL}
	} else if mediaType == "application/pdf" {
		result["source"] = map[string]any{
			"type":       "base64",
			"media_type": "application/pdf",
			"data":       base64.StdEncoding.EncodeToString(value.Bytes),
		}
	} else if mediaType == "text/plain" {
		result["source"] = map[string]any{
			"type":       "text",
			"media_type": value.MediaType,
			"data":       string(value.Bytes),
		}
	} else {
		return nil, fmt.Errorf("document media type %q is not supported by Anthropic Messages", value.MediaType)
	}
	if value.Title != "" {
		result["title"] = value.Title
	}
	return result, nil
}

func lowerToolResultContent(parts []llm.Part) ([]any, error) {
	content := make([]any, 0, len(parts))
	for index, part := range parts {
		lowered, err := lowerPart(part)
		if err != nil {
			return nil, fmt.Errorf("tool result part %d: %w", index, err)
		}
		content = append(content, lowered)
	}
	return content, nil
}

func lowerTools(tools []llm.Tool) ([]any, error) {
	result := make([]any, 0, len(tools))
	for index, tool := range tools {
		if len(tool.OutputSchema) > 0 {
			// This API has no function-tool output schema slot. Reject in both
			// portability modes rather than silently dropping the contract.
			return nil, provider.NewStrictPortabilityError("function tool output schema is not representable by Anthropic Messages")
		}
		if tool.Kind != "" && tool.Kind != llm.ToolKindFunction {
			return nil, fmt.Errorf("tool %d kind %q is not supported by Anthropic Messages", index, tool.Kind)
		}
		var schema map[string]any
		if err := schemaorder.Decode(tool.InputSchema, &schema); err != nil {
			return nil, fmt.Errorf("tool %q input schema: %w", tool.Name, err)
		}
		if schema == nil {
			return nil, fmt.Errorf("tool %q input schema must be an object", tool.Name)
		}
		// Strict tool use only accepts a closed schema subset (for example
		// additionalProperties: false and no minLength), which the worker's
		// schema validator does not enforce. Forcing it would reject ordinary
		// tool definitions, so tools are sent non-strict like Responses.
		result = append(result, map[string]any{
			"name":         tool.Name,
			"description":  tool.Description,
			"input_schema": schema,
		})
	}
	return result, nil
}

func lowerExtensions(profile Profile, extensions map[string]json.RawMessage, target map[string]any) error {
	// NewProfile rejects allowlists with colliding wire fields; a profile that
	// bypassed it must still never let map iteration choose the value.
	written := make(map[string]struct{})
	for namespace, raw := range extensions {
		spec, ok := profile.AllowedExtensions[namespace]
		if !ok {
			return fmt.Errorf("extension namespace %q is not supported by Anthropic profile %q", namespace, profile.ID)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return fmt.Errorf("extension %q must be an object: %w", namespace, err)
		}
		for name, value := range fields {
			wire, ok := spec.Fields[name]
			if !ok {
				return fmt.Errorf("extension %q field %q is not allowed", namespace, name)
			}
			if wire == "" {
				wire = name
			}
			if requestOwnedWireField(wire) {
				return fmt.Errorf("extension %q field %q cannot override %q", namespace, name, wire)
			}
			var decoded any
			if err := json.Unmarshal(value, &decoded); err != nil {
				return fmt.Errorf("extension %q field %q: %w", namespace, name, err)
			}
			if _, duplicate := written[wire]; duplicate {
				return fmt.Errorf("extension %q field %q maps to wire field %q already set by another extension field", namespace, name, wire)
			}
			written[wire] = struct{}{}
			target[wire] = decoded
		}
	}
	return nil
}

func appendContinuationStates(messages *[]any, continuation *llm.Continuation, profile Profile, _ string) error {
	if continuation == nil {
		return nil
	}
	if continuation.Pinned {
		if continuation.Model != "" && profile.ExpectedModel != "" && continuation.Model != profile.ExpectedModel {
			return fmt.Errorf("continuation model %q is not pinned profile model %q", continuation.Model, profile.ExpectedModel)
		}
	}
	if continuation.Handle != "" && !strings.HasPrefix(continuation.Handle, "anthropic-messages:") {
		return fmt.Errorf("continuation handle %q is not an Anthropic Messages handle", continuation.Handle)
	}
	if len(continuation.ProviderStates) == 0 {
		return fmt.Errorf("Anthropic Messages continuation has no replayable provider state")
	}
	for index, state := range continuation.ProviderStates {
		if isContainerState(state) {
			if _, err := containerStateID(state); err != nil {
				return err
			}
			continue
		}
		block, err := providerStateRaw(state, fmt.Sprintf("continuation provider state %d", index))
		if err != nil {
			return err
		}
		*messages = append(*messages, map[string]any{"role": "assistant", "content": []any{block}})
	}
	return nil
}

func providerStateRaw(state llm.ProviderState, prefix string) (json.RawMessage, error) {
	if state.Provider != "anthropic" || state.EndpointFamily != "messages" {
		if prefix == "" {
			prefix = "provider state"
		}
		return nil, fmt.Errorf("%s is pinned to %s/%s, not anthropic/messages", prefix, state.Provider, state.EndpointFamily)
	}
	return rawContentBlock(state.Opaque)
}

func providerStatePartRaw(state llm.ProviderStatePart) (json.RawMessage, error) {
	if state.Provider != "anthropic" || state.EndpointFamily != "messages" {
		return nil, fmt.Errorf("provider state part is pinned to %s/%s, not anthropic/messages", state.Provider, state.EndpointFamily)
	}
	return rawContentBlock(state.Opaque)
}

func rawContentBlock(raw []byte) (json.RawMessage, error) {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil, fmt.Errorf("provider state must contain valid JSON")
	}
	var block map[string]any
	if err := json.Unmarshal(raw, &block); err != nil {
		return nil, fmt.Errorf("provider state: %w", err)
	}
	typeValue, _ := block["type"].(string)
	if !replayableBlockType(typeValue) {
		return nil, fmt.Errorf("provider state content block type %q is not replayable", typeValue)
	}
	return append(json.RawMessage(nil), raw...), nil
}

func replayableBlockType(kind string) bool {
	switch kind {
	case "thinking", "redacted_thinking", "server_tool_use", "web_search_tool_result", "web_fetch_tool_result", "code_execution_tool_result", "bash_code_execution_tool_result", "text_editor_code_execution_tool_result":
		return true
	}
	return false
}

func isContainerState(state llm.ProviderState) bool {
	return state.Provider == "anthropic" && state.EndpointFamily == "messages" && state.MediaType == "application/vnd.anthropic.container+json"
}
func containerStateID(state llm.ProviderState) (string, error) {
	var container struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(state.Opaque, &container) != nil || container.ID == "" {
		return "", fmt.Errorf("invalid container provider state")
	}
	return container.ID, nil
}

// wire names this endpoint in the shared Anthropic Messages lowering errors.
var wire = anthropicwire.Anthropic

func lowerParts(parts []llm.Part) ([]any, error) {
	return wire.LowerParts(parts, partLowering())
}

func lowerPart(part llm.Part) (any, error) {
	return wire.LowerPart(part, partLowering())
}

// partLowering supplies this endpoint's media and provider-state lowering.
func partLowering() anthropicwire.PartLowering {
	return anthropicwire.PartLowering{Image: lowerImage, Document: lowerDocument, ProviderState: providerStatePartRaw}
}
