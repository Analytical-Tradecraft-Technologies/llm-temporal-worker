package bedrockmessages

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

// lowerRequest builds the Anthropic body InvokeModel forwards to the model.
// The Bedrock service tier is not part of that body: InvokeModel takes it as
// the X-Amzn-Bedrock-Service-Tier request header, which Invoke sets from the
// call metadata.
func lowerRequest(request llm.Request, profile Profile) (anthropic.MessageNewParams, error) {
	return lowerRequestWithStrict(request, profile, false)
}

func lowerRequestWithStrict(request llm.Request, profile Profile, strict bool) (anthropic.MessageNewParams, error) {
	// Anthropic's hosted tools are not offered on Amazon Bedrock, and an
	// adapter never drops a request parameter it cannot honour; the planner
	// also keeps such requests off Bedrock routes (#1506).
	if hosted := hostedToolFlag(request); hosted != "" {
		return anthropic.MessageNewParams{}, provider.NewUnsupportedCapabilityError(hosted + " is not supported by the Bedrock Messages adapter")
	}
	// The profile has no extension allowlist, so no namespace can be carried
	// to the wire. Strict mode rejects them; best-effort mode leaves them out
	// and Compile records the drop so the lift reports it as a diagnostic.
	if strict && len(request.Extensions) > 0 {
		return anthropic.MessageNewParams{}, provider.NewStrictPortabilityError("extensions are not supported by the Bedrock Messages adapter in strict portability mode")
	}
	if strict && anthropicwire.HasMixedInstructionLevels(request.Instructions) {
		return anthropic.MessageNewParams{}, provider.NewStrictPortabilityError("instruction hierarchy cannot be preserved by Bedrock Messages in strict portability mode")
	}
	messages := make([]any, 0, len(request.Input))
	if err := appendContinuationStates(&messages, request.Continuation, profile); err != nil {
		return anthropic.MessageNewParams{}, err
	}
	for index, item := range request.Input {
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
	target := map[string]any{"model": request.Model, "max_tokens": maxTokens, "messages": messages}
	if request.Output != nil {
		if request.Output.MaxTokens != nil {
			if *request.Output.MaxTokens < 0 {
				return anthropic.MessageNewParams{}, fmt.Errorf("output max_tokens must not be negative")
			}
			target["max_tokens"] = *request.Output.MaxTokens
		}
		if err := wire.LowerOutput(*request.Output, target, strict); err != nil {
			return anthropic.MessageNewParams{}, err
		}
	}
	if len(request.Instructions) > 0 {
		system := make([]any, 0)
		for index, instruction := range request.Instructions {
			parts := instruction.Content
			if instruction.Kind == llm.InstructionKindText || (instruction.Kind == "" && len(parts) == 0) {
				parts = []llm.Part{llm.TextPart{Text: instruction.Text}}
			}
			for partIndex, part := range parts {
				text, ok := part.(llm.TextPart)
				if !ok {
					if jsonPart, jsonOK := part.(llm.JSONPart); jsonOK && json.Valid(jsonPart.Value) {
						text = llm.TextPart{Text: string(jsonPart.Value)}
						ok = true
					}
				}
				if !ok {
					return anthropic.MessageNewParams{}, fmt.Errorf("instruction %d part %d kind %q is not supported by Bedrock system blocks", index, partIndex, part.PartKind())
				}
				system = append(system, map[string]any{"type": "text", "text": text.Text})
			}
		}
		target["system"] = system
	}
	if request.Sampling != nil {
		if err := wire.LowerSampling(*request.Sampling, target); err != nil {
			return anthropic.MessageNewParams{}, err
		}
	}
	if request.Reasoning != nil {
		thinking, err := wire.LowerReasoning(*request.Reasoning, strict)
		if err != nil {
			return anthropic.MessageNewParams{}, err
		}
		if thinking != nil {
			target["thinking"] = thinking
		}
		if effort := anthropicwire.ReasoningOutputEffort(request.Reasoning.Effort); effort != "" {
			outputConfig, _ := target["output_config"].(map[string]any)
			if outputConfig == nil {
				outputConfig = map[string]any{}
				target["output_config"] = outputConfig
			}
			outputConfig["effort"] = effort
		}
	}
	if len(request.Tools) > 0 {
		tools, err := lowerTools(request.Tools)
		if err != nil {
			return anthropic.MessageNewParams{}, err
		}
		target["tools"] = tools
		choice, err := anthropicwire.LowerToolPolicy(request.ToolPolicy)
		if err != nil {
			return anthropic.MessageNewParams{}, err
		}
		if choice != nil {
			target["tool_choice"] = choice
		}
	} else if request.ToolPolicy.Mode != "" && request.ToolPolicy.Mode != llm.ToolChoiceAuto && request.ToolPolicy.Mode != llm.ToolChoiceNone {
		return anthropic.MessageNewParams{}, fmt.Errorf("tool policy %q requires at least one tool", request.ToolPolicy.Mode)
	}
	if err := thinkingwire.Validate(target, "Bedrock Messages"); err != nil {
		return anthropic.MessageNewParams{}, err
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		return anthropic.MessageNewParams{}, err
	}
	var params anthropic.MessageNewParams
	if err := json.Unmarshal(encoded, &params); err != nil {
		return anthropic.MessageNewParams{}, fmt.Errorf("bedrock messages parameter union: %w", err)
	}
	param.SetJSON(encoded, &params)
	return params, nil
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
		if value.ID == "" || value.Name == "" || !json.Valid(value.Arguments) {
			return nil, fmt.Errorf("tool call requires ID, name, and valid JSON arguments")
		}
		// Preserve numeric literals in historical tool arguments.
		input := json.RawMessage(value.Arguments)
		return map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": toolcallid.Portable(value.ID), "name": value.Name, "input": input}}}, nil
	case llm.ToolResult:
		if value.CallID == "" {
			return nil, fmt.Errorf("tool result requires call ID")
		}
		content, err := lowerParts(value.Content)
		if err != nil {
			return nil, err
		}
		return map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": toolcallid.Portable(value.CallID), "content": content, "is_error": value.IsError}}}, nil
	case llm.ProviderState:
		raw, err := providerStateRaw(value)
		if err != nil {
			return nil, err
		}
		return map[string]any{"role": "assistant", "content": []any{raw}}, nil
	case llm.Reference:
		return nil, fmt.Errorf("reference input is not accepted by Bedrock Messages")
	default:
		return nil, fmt.Errorf("unsupported input item %T", item)
	}
}

// unsupportedMediaError marks media this route cannot carry, so Compile
// reports an unsupported capability rather than an invalid request.
type unsupportedMediaError struct {
	feature provider.Feature
	message string
}

func (err *unsupportedMediaError) Error() string { return err.message }

func lowerImage(value llm.ImagePart) (map[string]any, error) {
	// Claude on Amazon Bedrock does not support URL sources, and the worker
	// does not fetch URLs on a caller's behalf.
	if value.URL != "" {
		return nil, &unsupportedMediaError{feature: provider.FeatureImage, message: "image URL sources are not supported by Bedrock Messages"}
	}
	if value.Blob != nil || len(value.Bytes) == 0 {
		return nil, fmt.Errorf("image requires bytes and cannot use blob-backed media")
	}
	if value.Detail != "" {
		return nil, fmt.Errorf("image detail %q is not supported by Bedrock Messages", value.Detail)
	}
	switch strings.ToLower(value.MediaType) {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
	default:
		return nil, fmt.Errorf("image media type %q is not supported by Bedrock Messages", value.MediaType)
	}
	source := map[string]any{"type": "base64", "media_type": value.MediaType, "data": base64.StdEncoding.EncodeToString(value.Bytes)}
	return map[string]any{"type": "image", "source": source}, nil
}

func lowerDocument(value llm.DocumentPart) (map[string]any, error) {
	if value.URL != "" {
		return nil, &unsupportedMediaError{feature: provider.FeatureDocument, message: "document URL sources are not supported by Bedrock Messages"}
	}
	if value.Blob != nil || len(value.Bytes) == 0 {
		return nil, fmt.Errorf("document requires bytes and cannot use blob-backed media")
	}
	mediaType := strings.ToLower(value.MediaType)
	result := map[string]any{"type": "document"}
	switch mediaType {
	case "application/pdf":
		result["source"] = map[string]any{"type": "base64", "media_type": mediaType, "data": base64.StdEncoding.EncodeToString(value.Bytes)}
	case "text/plain":
		result["source"] = map[string]any{"type": "text", "media_type": value.MediaType, "data": string(value.Bytes)}
	default:
		return nil, fmt.Errorf("document media type %q is not supported by Bedrock Messages", value.MediaType)
	}
	if value.Title != "" {
		result["title"] = value.Title
	}
	return result, nil
}

func lowerTools(tools []llm.Tool) ([]any, error) {
	result := make([]any, 0, len(tools))
	for index, tool := range tools {
		if len(tool.OutputSchema) > 0 {
			// This API has no function-tool output schema slot. Reject in both
			// portability modes rather than silently dropping the contract.
			return nil, provider.NewStrictPortabilityError("function tool output schema is not representable by Bedrock Messages")
		}
		if tool.Kind != "" && tool.Kind != llm.ToolKindFunction {
			return nil, fmt.Errorf("tool %d kind %q is not supported by Bedrock Messages", index, tool.Kind)
		}
		var schema map[string]any
		if err := schemaorder.Decode(tool.InputSchema, &schema); err != nil || schema == nil {
			return nil, fmt.Errorf("tool %q input schema must be an object", tool.Name)
		}
		// Strict tool use only accepts a closed schema subset the worker does
		// not enforce, so tools are sent non-strict like Responses.
		result = append(result, map[string]any{"name": tool.Name, "description": tool.Description, "input_schema": schema})
	}
	return result, nil
}

func appendContinuationStates(messages *[]any, continuation *llm.Continuation, profile Profile) error {
	if continuation == nil {
		return nil
	}
	if continuation.Pinned && continuation.Model != "" && profile.ExpectedModel != "" && continuation.Model != profile.ExpectedModel {
		return fmt.Errorf("continuation model %q is not pinned profile model %q", continuation.Model, profile.ExpectedModel)
	}
	if continuation.Handle != "" && !strings.HasPrefix(continuation.Handle, "bedrock-messages:") {
		return fmt.Errorf("continuation handle %q is not a Bedrock Messages handle", continuation.Handle)
	}
	if len(continuation.ProviderStates) == 0 {
		return fmt.Errorf("Bedrock Messages continuation has no replayable provider state")
	}
	for index, state := range continuation.ProviderStates {
		raw, err := providerStateRaw(state)
		if err != nil {
			return fmt.Errorf("continuation provider state %d: %w", index, err)
		}
		*messages = append(*messages, map[string]any{"role": "assistant", "content": []any{raw}})
	}
	return nil
}

func providerStateRaw(state llm.ProviderState) (json.RawMessage, error) {
	if state.Provider != "bedrock" || state.EndpointFamily != "messages" {
		return nil, fmt.Errorf("provider state is pinned to %s/%s, not bedrock/messages", state.Provider, state.EndpointFamily)
	}
	return rawContentBlock(state.Opaque)
}

func providerStatePartRaw(state llm.ProviderStatePart) (json.RawMessage, error) {
	if state.Provider != "bedrock" || state.EndpointFamily != "messages" {
		return nil, fmt.Errorf("provider state part is pinned to %s/%s, not bedrock/messages", state.Provider, state.EndpointFamily)
	}
	return rawContentBlock(state.Opaque)
}

func rawContentBlock(raw []byte) (json.RawMessage, error) {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil, fmt.Errorf("provider state must contain valid JSON")
	}
	var block map[string]any
	if err := json.Unmarshal(raw, &block); err != nil {
		return nil, err
	}
	if kind, _ := block["type"].(string); kind != "thinking" && kind != "redacted_thinking" {
		return nil, fmt.Errorf("provider state content block type %q is not replayable", kind)
	}
	return append(json.RawMessage(nil), raw...), nil
}

// wire names this endpoint in the shared Anthropic Messages lowering errors.
var wire = anthropicwire.Bedrock

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

// hostedToolFlag names the first provider-hosted tool the request asks for.
func hostedToolFlag(request llm.Request) string {
	switch {
	case request.WebSearch:
		return "web_search"
	case request.WebFetch:
		return "web_fetch"
	case request.CodeExecution:
		return "code_execution"
	}
	return ""
}
