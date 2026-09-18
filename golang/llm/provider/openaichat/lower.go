package openaichat

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	openai "github.com/openai/openai-go/v3"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func lowerRequest(request llm.Request, profile Profile, serviceTier string) (openai.ChatCompletionNewParams, error) {
	messages := make([]any, 0, len(request.Instructions)+len(request.Input))
	toolCalls := make(map[string]struct{})
	for index, instruction := range request.Instructions {
		message, err := lowerInstruction(instruction)
		if err != nil {
			return openai.ChatCompletionNewParams{}, fmt.Errorf("instruction %d: %w", index, err)
		}
		messages = append(messages, message)
	}
	for index, item := range request.Input {
		if err := appendInputItem(&messages, item, toolCalls); err != nil {
			return openai.ChatCompletionNewParams{}, fmt.Errorf("input item %d: %w", index, err)
		}
	}
	requestMap := map[string]any{
		"model":    request.Model,
		"messages": messages,
	}
	for wire, raw := range profile.WireDefaults {
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return openai.ChatCompletionNewParams{}, fmt.Errorf("wire default %q: %w", wire, err)
		}
		requestMap[wire] = value
	}
	if serviceTier != "" && !profile.WireShape.OmitServiceTier {
		requestMap["service_tier"] = serviceTier
	}
	if request.Output != nil {
		if err := lowerOutput(*request.Output, profile.WireShape, requestMap); err != nil {
			return openai.ChatCompletionNewParams{}, err
		}
	}
	if request.Sampling != nil {
		if err := lowerSampling(*request.Sampling, requestMap); err != nil {
			return openai.ChatCompletionNewParams{}, err
		}
	}
	if request.Reasoning != nil {
		if err := lowerReasoning(*request.Reasoning, requestMap); err != nil {
			return openai.ChatCompletionNewParams{}, err
		}
	}
	if len(request.Tools) > 0 {
		tools, err := lowerTools(request.Tools)
		if err != nil {
			return openai.ChatCompletionNewParams{}, err
		}
		requestMap["tools"] = tools
	}
	policy, err := lowerToolPolicy(request.ToolPolicy, len(request.Tools) > 0)
	if err != nil {
		return openai.ChatCompletionNewParams{}, err
	}
	if policy != nil {
		requestMap["tool_choice"] = policy
	}
	if len(request.Tools) > 0 || request.ToolPolicy.Mode != "" {
		if profile.WireShape.ParallelToolCalls == DefaultFalseFieldUnsupported {
			if request.ToolPolicy.Parallel {
				return openai.ChatCompletionNewParams{}, fmt.Errorf("profile %q does not support parallel_tool_calls", profile.ID)
			}
		} else {
			requestMap["parallel_tool_calls"] = request.ToolPolicy.Parallel
		}
	}
	if request.Continuation != nil {
		return openai.ChatCompletionNewParams{}, fmt.Errorf("continuation is not representable by Chat Completions")
	}
	if err := lowerExtensions(profile, request.Extensions, requestMap); err != nil {
		return openai.ChatCompletionNewParams{}, err
	}
	if configured, ok := requestMap["store"]; ok {
		store, valid := configured.(bool)
		if !valid {
			return openai.ChatCompletionNewParams{}, fmt.Errorf("chat store policy must be boolean")
		}
		if store {
			return openai.ChatCompletionNewParams{}, fmt.Errorf("provider-hosted Chat response storage is prohibited")
		}
	}
	if profile.WireShape.Store == DefaultFalseFieldUnsupported {
		delete(requestMap, "store")
	} else {
		requestMap["store"] = false
	}
	encoded, err := json.Marshal(requestMap)
	if err != nil {
		return openai.ChatCompletionNewParams{}, err
	}
	var params openai.ChatCompletionNewParams
	if err := json.Unmarshal(encoded, &params); err != nil {
		return openai.ChatCompletionNewParams{}, fmt.Errorf("openai chat parameter union: %w", err)
	}
	// The official parameter union intentionally ignores unknown compatible
	// fields. Preserve profile-owned fields (such as OpenRouter's provider
	// routing object or Exa's extra_body) through its audited extension escape
	// hatch instead of silently dropping them.
	knownBytes, err := json.Marshal(params)
	if err != nil {
		return openai.ChatCompletionNewParams{}, fmt.Errorf("openai chat known fields: %w", err)
	}
	var known map[string]json.RawMessage
	if err := json.Unmarshal(knownBytes, &known); err != nil {
		return openai.ChatCompletionNewParams{}, fmt.Errorf("openai chat known fields: %w", err)
	}
	extras := make(map[string]any)
	for key, value := range requestMap {
		if _, ok := known[key]; ok {
			continue
		}
		var decoded any
		rawValue, err := json.Marshal(value)
		if err != nil {
			return openai.ChatCompletionNewParams{}, fmt.Errorf("wire field %q: %w", key, err)
		}
		if err := json.Unmarshal(rawValue, &decoded); err != nil {
			return openai.ChatCompletionNewParams{}, fmt.Errorf("wire field %q: %w", key, err)
		}
		extras[key] = decoded
	}
	if len(extras) > 0 {
		params.SetExtraFields(extras)
	}
	return params, nil
}

func lowerInstruction(instruction llm.Instruction) (map[string]any, error) {
	role := "developer"
	if instruction.Level == llm.InstructionLevelPolicy {
		role = "system"
	}
	parts := instruction.Content
	if instruction.Kind == llm.InstructionKindText || (instruction.Kind == "" && len(parts) == 0) {
		parts = []llm.Part{llm.TextPart{Text: instruction.Text}}
	}
	content, err := lowerInstructionParts(parts)
	if err != nil {
		return nil, err
	}
	return map[string]any{"role": role, "content": content}, nil
}

func lowerInstructionParts(parts []llm.Part) ([]any, error) {
	content := make([]any, 0, len(parts))
	for index, part := range parts {
		switch value := part.(type) {
		case llm.TextPart:
			content = append(content, map[string]any{"type": "text", "text": value.Text})
		case llm.JSONPart:
			if !json.Valid(value.Value) {
				return nil, fmt.Errorf("instruction part %d JSON is invalid", index)
			}
			content = append(content, map[string]any{"type": "text", "text": string(value.Value)})
		default:
			return nil, fmt.Errorf("instruction part %d kind %q is not supported by system/developer messages", index, part.PartKind())
		}
	}
	return content, nil
}

func appendInputItem(messages *[]any, item llm.Item, toolCalls map[string]struct{}) error {
	switch value := item.(type) {
	case llm.Message:
		message, err := lowerMessage(value)
		if err != nil {
			return err
		}
		*messages = append(*messages, message)
		return nil
	case llm.ToolCall:
		if value.ID == "" || value.Name == "" {
			return fmt.Errorf("tool call requires ID and name")
		}
		if !json.Valid(value.Arguments) {
			return fmt.Errorf("tool call %q arguments are invalid JSON", value.ID)
		}
		if _, exists := toolCalls[value.ID]; exists {
			return fmt.Errorf("tool call ID %q is duplicated", value.ID)
		}
		if len(*messages) > 0 {
			if last, ok := (*messages)[len(*messages)-1].(map[string]any); ok && last["role"] == "assistant" {
				calls, _ := last["tool_calls"].([]any)
				calls = append(calls, map[string]any{
					"id":   value.ID,
					"type": "function",
					"function": map[string]any{
						"name":      value.Name,
						"arguments": string(value.Arguments),
					},
				})
				last["tool_calls"] = calls
				toolCalls[value.ID] = struct{}{}
				return nil
			}
		}
		*messages = append(*messages, map[string]any{
			"role": "assistant",
			"tool_calls": []any{map[string]any{
				"id":   value.ID,
				"type": "function",
				"function": map[string]any{
					"name":      value.Name,
					"arguments": string(value.Arguments),
				},
			}},
		})
		toolCalls[value.ID] = struct{}{}
		return nil
	case llm.ToolResult:
		if value.CallID == "" {
			return fmt.Errorf("tool result requires call ID")
		}
		if _, ok := toolCalls[value.CallID]; !ok {
			return fmt.Errorf("tool result %q has no preceding tool call", value.CallID)
		}
		if value.IsError {
			return fmt.Errorf("tool result %q error state cannot be represented by Chat Completions", value.CallID)
		}
		content, err := lowerToolResultContent(value.Content)
		if err != nil {
			return err
		}
		*messages = append(*messages, map[string]any{
			"role":         "tool",
			"tool_call_id": value.CallID,
			"content":      content,
		})
		return nil
	case llm.ProviderState, llm.Reference:
		return fmt.Errorf("item kind %q is not accepted as Chat Completions input", item.ItemKind())
	default:
		return fmt.Errorf("unsupported input item %T", item)
	}
}

func lowerMessage(message llm.Message) (map[string]any, error) {
	role := "user"
	if message.Actor == llm.ActorModel {
		role = "assistant"
	}
	content, err := lowerParts(message.Content)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"role": role}
	if role == "assistant" && len(content) == 0 {
		return result, nil
	}
	result["content"] = content
	return result, nil
}

func lowerParts(parts []llm.Part) ([]any, error) {
	content := make([]any, 0, len(parts))
	for index, part := range parts {
		lowered, err := lowerPart(part)
		if err != nil {
			return nil, fmt.Errorf("part %d: %w", index, err)
		}
		content = append(content, lowered)
	}
	return content, nil
}

func lowerPart(part llm.Part) (map[string]any, error) {
	switch value := part.(type) {
	case llm.TextPart:
		return map[string]any{"type": "text", "text": value.Text}, nil
	case llm.JSONPart:
		if !json.Valid(value.Value) {
			return nil, fmt.Errorf("JSON part is invalid")
		}
		return map[string]any{"type": "text", "text": string(value.Value)}, nil
	case llm.ImagePart:
		url, err := mediaURL(value.URL, value.Bytes, value.MediaType)
		if err != nil {
			return nil, fmt.Errorf("image: %w", err)
		}
		detail := value.Detail
		if detail == "" {
			detail = "auto"
		}
		return map[string]any{"type": "image_url", "image_url": map[string]any{"url": url, "detail": detail}}, nil
	case llm.DocumentPart:
		return nil, fmt.Errorf("document parts are not representable by Chat Completions")
	case llm.RefusalPart, llm.ProviderStatePart:
		return nil, fmt.Errorf("part kind %q is not accepted as Chat Completions input", part.PartKind())
	default:
		return nil, fmt.Errorf("unsupported part %T", part)
	}
}

func mediaURL(rawURL string, data []byte, mediaType string) (string, error) {
	if rawURL != "" {
		return rawURL, nil
	}
	if len(data) == 0 {
		return "", fmt.Errorf("blob-backed media is not available to adapter")
	}
	return "data:" + mediaType + ";base64," + base64.StdEncoding.EncodeToString(data), nil
}

func lowerToolResultContent(content []llm.Part) (string, error) {
	var builder strings.Builder
	for index, part := range content {
		switch value := part.(type) {
		case llm.TextPart:
			builder.WriteString(value.Text)
		case llm.JSONPart:
			if !json.Valid(value.Value) {
				return "", fmt.Errorf("tool result part %d is invalid JSON", index)
			}
			builder.Write(value.Value)
		default:
			return "", fmt.Errorf("tool result part %d kind %q cannot be represented as text", index, part.PartKind())
		}
	}
	return builder.String(), nil
}

func lowerTools(tools []llm.Tool) ([]any, error) {
	result := make([]any, 0, len(tools))
	for index, tool := range tools {
		if tool.Kind != "" && tool.Kind != llm.ToolKindFunction {
			return nil, fmt.Errorf("tool %d kind %q is not supported by Chat Completions", index, tool.Kind)
		}
		var schema map[string]any
		if err := json.Unmarshal(tool.InputSchema, &schema); err != nil {
			return nil, fmt.Errorf("tool %q input schema: %w", tool.Name, err)
		}
		// OutputSchema is the caller-owned contract for validating a tool
		// result. Chat Completions has no wire slot for it, and the provider
		// does not execute or validate caller-owned tools, so it remains in the
		// durable request without being sent to the provider.
		function := map[string]any{
			"name":        tool.Name,
			"description": tool.Description,
			"parameters":  schema,
		}
		result = append(result, map[string]any{"type": "function", "function": function})
	}
	return result, nil
}

func lowerToolPolicy(policy llm.ToolPolicy, hasTools bool) (any, error) {
	mode := policy.Mode
	if mode == "" {
		mode = llm.ToolChoiceAuto
	}
	if mode == llm.ToolChoiceNone {
		return "none", nil
	}
	if !hasTools && mode == llm.ToolChoiceAuto {
		return nil, nil
	}
	switch mode {
	case llm.ToolChoiceAuto:
		return "auto", nil
	case llm.ToolChoiceRequired:
		return "required", nil
	case llm.ToolChoiceNamed:
		if policy.Name == "" {
			return nil, fmt.Errorf("named tool policy requires a name")
		}
		return map[string]any{"type": "function", "function": map[string]any{"name": policy.Name}}, nil
	default:
		return nil, fmt.Errorf("tool policy mode %q is invalid", mode)
	}
}

func lowerOutput(output llm.OutputSpec, shape WireShape, target map[string]any) error {
	if output.MaxTokens != nil {
		if *output.MaxTokens < 0 {
			return fmt.Errorf("output max_tokens must not be negative")
		}
		switch shape.OutputTokenLimitField {
		case "", OutputTokenLimitFieldMaxCompletionTokens:
			target[string(OutputTokenLimitFieldMaxCompletionTokens)] = *output.MaxTokens
		case OutputTokenLimitFieldMaxTokens:
			target[string(OutputTokenLimitFieldMaxTokens)] = *output.MaxTokens
		default:
			return fmt.Errorf("output token limit field %q is unsupported", shape.OutputTokenLimitField)
		}
	}
	switch output.Format.Kind {
	case "", llm.OutputKindText:
		return nil
	case llm.OutputKindJSON:
		target["response_format"] = map[string]any{"type": "json_object"}
		return nil
	case llm.OutputKindJSONSchema:
		var schema map[string]any
		if err := json.Unmarshal(output.Format.Schema, &schema); err != nil {
			return fmt.Errorf("output schema: %w", err)
		}
		jsonSchema := map[string]any{
			"name":   output.Format.Name,
			"schema": schema,
			"strict": output.Format.Strict,
		}
		if output.Format.Description != "" {
			jsonSchema["description"] = output.Format.Description
		}
		target["response_format"] = map[string]any{"type": "json_schema", "json_schema": jsonSchema}
		return nil
	default:
		return fmt.Errorf("output format %q is not supported", output.Format.Kind)
	}
}

func lowerSampling(sampling llm.SamplingSpec, target map[string]any) error {
	if sampling.TopK != nil {
		return fmt.Errorf("sampling top_k is not supported by Chat Completions")
	}
	if sampling.Temperature != nil {
		target["temperature"] = *sampling.Temperature
	}
	if sampling.TopP != nil {
		target["top_p"] = *sampling.TopP
	}
	if sampling.Seed != nil {
		target["seed"] = *sampling.Seed
	}
	if sampling.PresencePenalty != nil {
		target["presence_penalty"] = *sampling.PresencePenalty
	}
	if sampling.FrequencyPenalty != nil {
		target["frequency_penalty"] = *sampling.FrequencyPenalty
	}
	if len(sampling.StopSequences) == 1 {
		target["stop"] = sampling.StopSequences[0]
	} else if len(sampling.StopSequences) > 1 {
		target["stop"] = sampling.StopSequences
	}
	return nil
}

func lowerReasoning(reasoning llm.ReasoningSpec, target map[string]any) error {
	switch reasoning.Mode {
	case "", llm.ReasoningModeProviderDefault, llm.ReasoningModeAdaptive, llm.ReasoningModeEnabled, llm.ReasoningModeDisabled:
	default:
		return fmt.Errorf("reasoning mode %q is not supported", reasoning.Mode)
	}
	if reasoning.TokenBudget != nil {
		return fmt.Errorf("reasoning token_budget is not supported by Chat Completions")
	}
	if reasoning.Summary != "" && reasoning.Summary != llm.ReasoningSummaryProviderDefault {
		return fmt.Errorf("reasoning summary %q is not supported by Chat Completions", reasoning.Summary)
	}
	effort := reasoning.Effort
	if reasoning.Mode == llm.ReasoningModeDisabled || reasoning.Summary == llm.ReasoningSummaryNone {
		effort = llm.ReasoningEffortMinimal
	}
	switch effort {
	case "", llm.ReasoningEffortProviderDefault:
	case llm.ReasoningEffortMinimal, llm.ReasoningEffortLow, llm.ReasoningEffortMedium, llm.ReasoningEffortHigh:
		target["reasoning_effort"] = string(effort)
	case llm.ReasoningEffortMaximum:
		target["reasoning_effort"] = "max"
	default:
		return fmt.Errorf("reasoning effort %q is not supported", effort)
	}
	return nil
}

func lowerExtensions(profile Profile, extensions map[string]json.RawMessage, target map[string]any) error {
	if len(extensions) == 0 {
		return nil
	}
	for namespace, raw := range extensions {
		spec, ok := profile.AllowedExtensions[namespace]
		if !ok {
			return fmt.Errorf("extension namespace %q is not supported by profile %q", namespace, profile.ID)
		}
		fields, err := extensionObject(raw)
		if err != nil {
			return fmt.Errorf("extension %q: %w", namespace, err)
		}
		for field, value := range fields {
			wire, ok := spec.Fields[field]
			if !ok {
				return fmt.Errorf("extension %q field %q is not allowed by profile %q", namespace, field, profile.ID)
			}
			if wire == "" {
				wire = field
			}
			if wire == "model" || wire == "messages" || wire == "service_tier" {
				return fmt.Errorf("extension %q field %q cannot override %q", namespace, field, wire)
			}
			if _, reserved := profile.ReservedWireFields[wire]; reserved {
				return fmt.Errorf("extension %q field %q cannot override reserved wire field %q", namespace, field, wire)
			}
			var decoded any
			if err := json.Unmarshal(value, &decoded); err != nil {
				return fmt.Errorf("extension %q field %q: %w", namespace, field, err)
			}
			target[wire] = decoded
		}
	}
	return nil
}
