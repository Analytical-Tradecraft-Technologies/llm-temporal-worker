package bedrockmessages

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/packages/param"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func lowerRequest(request llm.Request, profile Profile, serviceTier string) (anthropic.MessageNewParams, error) {
	return lowerRequestWithStrict(request, profile, serviceTier, false)
}

func lowerRequestWithStrict(request llm.Request, profile Profile, serviceTier string, strict bool) (anthropic.MessageNewParams, error) {
	if strict && hasMixedInstructionLevels(request.Instructions) {
		return anthropic.MessageNewParams{}, fmt.Errorf("instruction hierarchy cannot be preserved by Bedrock Messages in strict portability mode")
	}
	messages := make([]any, 0, len(request.Input)+1)
	if err := appendContinuationStates(&messages, request.Continuation, profile); err != nil {
		return anthropic.MessageNewParams{}, err
	}
	for index, item := range request.Input {
		message, err := lowerItem(item)
		if err != nil {
			return anthropic.MessageNewParams{}, fmt.Errorf("input item %d: %w", index, err)
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
		if err := lowerOutput(*request.Output, target); err != nil {
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
	if serviceTier != "" {
		target["service_tier"] = serviceTier
	}
	if request.Sampling != nil {
		if err := lowerSampling(*request.Sampling, target); err != nil {
			return anthropic.MessageNewParams{}, err
		}
	}
	if request.Reasoning != nil {
		thinking, err := lowerReasoning(*request.Reasoning)
		if err != nil {
			return anthropic.MessageNewParams{}, err
		}
		if thinking != nil {
			target["thinking"] = thinking
		}
		if effort := reasoningOutputEffort(request.Reasoning.Effort); effort != "" {
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
		choice, err := lowerToolPolicy(request.ToolPolicy)
		if err != nil {
			return anthropic.MessageNewParams{}, err
		}
		if choice != nil {
			target["tool_choice"] = choice
		}
	} else if request.ToolPolicy.Mode != "" && request.ToolPolicy.Mode != llm.ToolChoiceAuto {
		return anthropic.MessageNewParams{}, fmt.Errorf("tool policy %q requires at least one tool", request.ToolPolicy.Mode)
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

func hasMixedInstructionLevels(instructions []llm.Instruction) bool {
	application := false
	policy := false
	for _, instruction := range instructions {
		if instruction.Level == llm.InstructionLevelPolicy {
			policy = true
		} else {
			application = true
		}
	}
	return application && policy
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
		var input any
		if err := json.Unmarshal(value.Arguments, &input); err != nil {
			return nil, err
		}
		return map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": value.ID, "name": value.Name, "input": input}}}, nil
	case llm.ToolResult:
		if value.CallID == "" {
			return nil, fmt.Errorf("tool result requires call ID")
		}
		content, err := lowerParts(value.Content)
		if err != nil {
			return nil, err
		}
		return map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": value.CallID, "content": content, "is_error": value.IsError}}}, nil
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

func lowerParts(parts []llm.Part) ([]any, error) {
	content := make([]any, 0, len(parts))
	for index, part := range parts {
		value, err := lowerPart(part)
		if err != nil {
			return nil, fmt.Errorf("part %d: %w", index, err)
		}
		content = append(content, value)
	}
	return content, nil
}

func lowerPart(part llm.Part) (any, error) {
	switch value := part.(type) {
	case llm.TextPart:
		return map[string]any{"type": "text", "text": value.Text}, nil
	case llm.JSONPart:
		if !json.Valid(value.Value) {
			return nil, fmt.Errorf("JSON part is invalid")
		}
		return map[string]any{"type": "text", "text": string(value.Value)}, nil
	case llm.ImagePart:
		return lowerImage(value)
	case llm.DocumentPart:
		return lowerDocument(value)
	case llm.ProviderStatePart:
		return providerStatePartRaw(value)
	case llm.RefusalPart:
		return nil, fmt.Errorf("refusal part is not accepted as Bedrock input")
	default:
		return nil, fmt.Errorf("part kind %q is not supported by Bedrock Messages", part.PartKind())
	}
}

func lowerImage(value llm.ImagePart) (map[string]any, error) {
	if value.Blob != nil || (value.URL == "" && len(value.Bytes) == 0) {
		return nil, fmt.Errorf("image requires URL or bytes and cannot use blob-backed media")
	}
	if value.Detail != "" {
		return nil, fmt.Errorf("image detail %q is not supported by Bedrock Messages", value.Detail)
	}
	switch strings.ToLower(value.MediaType) {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
	default:
		return nil, fmt.Errorf("image media type %q is not supported by Bedrock Messages", value.MediaType)
	}
	source := map[string]any{}
	if value.URL != "" {
		source["type"], source["url"] = "url", value.URL
	} else {
		source["type"], source["media_type"], source["data"] = "base64", value.MediaType, base64.StdEncoding.EncodeToString(value.Bytes)
	}
	return map[string]any{"type": "image", "source": source}, nil
}

func lowerDocument(value llm.DocumentPart) (map[string]any, error) {
	if value.Blob != nil || (value.URL == "" && len(value.Bytes) == 0) {
		return nil, fmt.Errorf("document requires URL or bytes and cannot use blob-backed media")
	}
	mediaType := strings.ToLower(value.MediaType)
	result := map[string]any{"type": "document"}
	if value.URL != "" {
		if mediaType != "application/pdf" {
			return nil, fmt.Errorf("document URL media type %q is not supported by Bedrock Messages", value.MediaType)
		}
		result["source"] = map[string]any{"type": "url", "url": value.URL}
	} else {
		switch mediaType {
		case "application/pdf":
			result["source"] = map[string]any{"type": "base64", "media_type": mediaType, "data": base64.StdEncoding.EncodeToString(value.Bytes)}
		case "text/plain":
			result["source"] = map[string]any{"type": "text", "media_type": value.MediaType, "data": string(value.Bytes)}
		default:
			return nil, fmt.Errorf("document media type %q is not supported by Bedrock Messages", value.MediaType)
		}
	}
	if value.Title != "" {
		result["title"] = value.Title
	}
	return result, nil
}

func lowerTools(tools []llm.Tool) ([]any, error) {
	result := make([]any, 0, len(tools))
	for index, tool := range tools {
		if tool.Kind != "" && tool.Kind != llm.ToolKindFunction {
			return nil, fmt.Errorf("tool %d kind %q is not supported by Bedrock Messages", index, tool.Kind)
		}
		var schema map[string]any
		if err := json.Unmarshal(tool.InputSchema, &schema); err != nil || schema == nil {
			return nil, fmt.Errorf("tool %q input schema must be an object", tool.Name)
		}
		result = append(result, map[string]any{"name": tool.Name, "description": tool.Description, "input_schema": schema, "strict": true})
	}
	return result, nil
}

func lowerToolPolicy(policy llm.ToolPolicy) (map[string]any, error) {
	mode := policy.Mode
	if mode == "" {
		mode = llm.ToolChoiceAuto
	}
	choice := map[string]any{}
	switch mode {
	case llm.ToolChoiceNone:
		choice["type"] = "none"
	case llm.ToolChoiceAuto:
		choice["type"] = "auto"
	case llm.ToolChoiceRequired:
		choice["type"] = "any"
	case llm.ToolChoiceNamed:
		if policy.Name == "" {
			return nil, fmt.Errorf("named tool policy requires a name")
		}
		choice["type"], choice["name"] = "tool", policy.Name
	default:
		return nil, fmt.Errorf("tool policy mode %q is invalid", mode)
	}
	if mode != llm.ToolChoiceNone {
		choice["disable_parallel_tool_use"] = !policy.Parallel
	}
	return choice, nil
}

func lowerOutput(output llm.OutputSpec, target map[string]any) error {
	switch output.Format.Kind {
	case "", llm.OutputKindText:
		return nil
	case llm.OutputKindJSON:
		target["output_config"] = map[string]any{"format": map[string]any{"type": "json_schema", "schema": map[string]any{"type": "object"}}}
		return nil
	case llm.OutputKindJSONSchema:
		var schema map[string]any
		if err := json.Unmarshal(output.Format.Schema, &schema); err != nil || schema == nil {
			return fmt.Errorf("output schema must be an object")
		}
		target["output_config"] = map[string]any{"format": map[string]any{"type": "json_schema", "schema": schema}}
		return nil
	default:
		return fmt.Errorf("output format %q is not supported by Bedrock Messages", output.Format.Kind)
	}
}

func lowerSampling(sampling llm.SamplingSpec, target map[string]any) error {
	if sampling.Seed != nil || sampling.PresencePenalty != nil || sampling.FrequencyPenalty != nil {
		return fmt.Errorf("sampling field is not supported by Bedrock Messages")
	}
	if sampling.Temperature != nil {
		target["temperature"] = *sampling.Temperature
	}
	if sampling.TopP != nil {
		target["top_p"] = *sampling.TopP
	}
	if sampling.TopK != nil {
		target["top_k"] = *sampling.TopK
	}
	if sampling.StopSequences != nil {
		target["stop_sequences"] = sampling.StopSequences
	}
	return nil
}

func lowerReasoning(reasoning llm.ReasoningSpec) (map[string]any, error) {
	mode := reasoning.Mode
	if mode == "" {
		mode = llm.ReasoningModeProviderDefault
	}
	summary := reasoning.Summary
	if summary == "" {
		summary = llm.ReasoningSummaryProviderDefault
	}
	if summary != llm.ReasoningSummaryProviderDefault && summary != llm.ReasoningSummaryNone {
		return nil, fmt.Errorf("reasoning summary %q is not supported by Bedrock Messages", summary)
	}
	if reasoning.Effort != "" && reasoning.Effort != llm.ReasoningEffortProviderDefault && mode != llm.ReasoningModeAdaptive {
		return nil, fmt.Errorf("reasoning effort %q requires adaptive Bedrock thinking", reasoning.Effort)
	}
	if mode == llm.ReasoningModeProviderDefault {
		if reasoning.TokenBudget == nil && reasoning.Effort == "" && summary == llm.ReasoningSummaryProviderDefault {
			return nil, nil
		}
		if reasoning.TokenBudget != nil {
			mode = llm.ReasoningModeEnabled
		} else {
			mode = llm.ReasoningModeAdaptive
		}
	}
	if mode == llm.ReasoningModeDisabled {
		return map[string]any{"type": "disabled"}, nil
	}
	if mode != llm.ReasoningModeAdaptive && mode != llm.ReasoningModeEnabled {
		return nil, fmt.Errorf("reasoning mode %q is not supported by Bedrock Messages", reasoning.Mode)
	}
	display := "summarized"
	if summary == llm.ReasoningSummaryNone {
		display = "omitted"
	}
	result := map[string]any{"type": string(mode)}
	if mode == llm.ReasoningModeEnabled {
		if reasoning.TokenBudget == nil || *reasoning.TokenBudget < 1024 {
			return nil, fmt.Errorf("Bedrock thinking token_budget must be at least 1024")
		}
		result["budget_tokens"] = *reasoning.TokenBudget
	}
	if summary != llm.ReasoningSummaryProviderDefault {
		result["display"] = display
	}
	return result, nil
}

func reasoningOutputEffort(effort llm.ReasoningEffort) string {
	switch effort {
	case llm.ReasoningEffortMinimal, llm.ReasoningEffortLow:
		return "low"
	case llm.ReasoningEffortMedium:
		return "medium"
	case llm.ReasoningEffortHigh:
		return "high"
	case llm.ReasoningEffortMaximum:
		return "max"
	default:
		return ""
	}
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
