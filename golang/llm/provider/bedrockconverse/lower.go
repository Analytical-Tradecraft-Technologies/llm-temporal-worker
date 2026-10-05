package bedrockconverse

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	smithydocumentjson "github.com/aws/smithy-go/document/json"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func lowerRequest(request llm.Request, profile Profile, serviceTier string, strict bool) (bedrockruntime.ConverseInput, error) {
	if strict && hasMixedInstructionLevels(request.Instructions) {
		return bedrockruntime.ConverseInput{}, fmt.Errorf("instruction hierarchy cannot be preserved by Bedrock Converse in strict portability mode")
	}
	if request.Output != nil && (request.Output.Format.Kind == llm.OutputKindJSON || request.Output.Format.Kind == llm.OutputKindJSONSchema) {
		return bedrockruntime.ConverseInput{}, fmt.Errorf("structured output is not implemented by the Bedrock Converse adapter")
	}
	// The generic Converse lowering sends no additionalModelRequestFields, so
	// it cannot carry reasoning controls or extension namespaces. Reject them
	// instead of silently dropping them.
	if len(request.Extensions) > 0 {
		return bedrockruntime.ConverseInput{}, fmt.Errorf("extensions are not supported by the Bedrock Converse adapter")
	}
	if reasoning := request.Reasoning; strict && reasoning != nil && !reasoningIsProviderDefault(*reasoning) {
		return bedrockruntime.ConverseInput{}, fmt.Errorf("reasoning controls are not implemented by the Bedrock Converse adapter in strict portability mode")
	}
	if sampling := request.Sampling; sampling != nil && (sampling.TopK != nil || sampling.Seed != nil || sampling.PresencePenalty != nil || sampling.FrequencyPenalty != nil) {
		return bedrockruntime.ConverseInput{}, fmt.Errorf("top_k, seed and penalty sampling controls are not implemented by the Bedrock Converse adapter")
	}
	input := bedrockruntime.ConverseInput{ModelId: stringPtr(request.Model), ServiceTier: &types.ServiceTier{Type: types.ServiceTierType(serviceTier)}}
	if err := lowerInstructions(request.Instructions, &input); err != nil {
		return bedrockruntime.ConverseInput{}, err
	}
	for index, item := range request.Input {
		message, err := lowerItem(item)
		if err != nil {
			return bedrockruntime.ConverseInput{}, fmt.Errorf("input item %d: %w", index, err)
		}
		// A model turn can be split into text and tool-call items internally.
		// Converse requires those blocks (and parallel tool results) together.
		last := len(input.Messages) - 1
		if last >= 0 && input.Messages[last].Role == message.Role {
			input.Messages[last].Content = append(input.Messages[last].Content, message.Content...)
		} else {
			input.Messages = append(input.Messages, message)
		}
	}
	if len(input.Messages) == 0 {
		return bedrockruntime.ConverseInput{}, fmt.Errorf("at least one input message is required")
	}
	maxTokens := profile.DefaultMaxTokens
	if request.Output != nil && request.Output.MaxTokens != nil {
		if *request.Output.MaxTokens < 0 {
			return bedrockruntime.ConverseInput{}, fmt.Errorf("output max_tokens must not be negative")
		}
		maxTokens = int32(*request.Output.MaxTokens)
	}
	if maxTokens > 0 || request.Sampling != nil {
		inference := &types.InferenceConfiguration{}
		if maxTokens > 0 {
			inference.MaxTokens = &maxTokens
		}
		if request.Sampling != nil {
			inference.StopSequences = append([]string(nil), request.Sampling.StopSequences...)
			if request.Sampling.Temperature != nil {
				value := float32(*request.Sampling.Temperature)
				inference.Temperature = &value
			}
			if request.Sampling.TopP != nil {
				value := float32(*request.Sampling.TopP)
				inference.TopP = &value
			}
		}
		input.InferenceConfig = inference
	}
	if len(request.Tools) > 0 {
		// Converse has no native none choice. Retaining definitions without a
		// choice defaults to auto, so refuse instead of permitting forbidden calls.
		if request.ToolPolicy.Mode == llm.ToolChoiceNone {
			return bedrockruntime.ConverseInput{}, fmt.Errorf("tool policy none with tool definitions is unsupported by Bedrock Converse")
		}
		tools, err := lowerTools(request.Tools)
		if err != nil {
			return bedrockruntime.ConverseInput{}, err
		}
		input.ToolConfig = &types.ToolConfiguration{Tools: tools}
		choice, err := lowerToolChoice(request.ToolPolicy)
		if err != nil {
			return bedrockruntime.ConverseInput{}, err
		}
		input.ToolConfig.ToolChoice = choice
	} else if request.ToolPolicy.Mode != "" && request.ToolPolicy.Mode != llm.ToolChoiceAuto && request.ToolPolicy.Mode != llm.ToolChoiceNone {
		return bedrockruntime.ConverseInput{}, fmt.Errorf("tool policy %q requires at least one tool", request.ToolPolicy.Mode)
	}
	return input, nil
}

func lowerInstructions(instructions []llm.Instruction, input *bedrockruntime.ConverseInput) error {
	for index, instruction := range instructions {
		parts := instruction.Content
		if instruction.Kind == llm.InstructionKindText || (instruction.Kind == "" && len(parts) == 0) {
			parts = []llm.Part{llm.TextPart{Text: instruction.Text}}
		}
		for partIndex, part := range parts {
			text, ok := part.(llm.TextPart)
			if !ok {
				if jsonPart, jsonOK := part.(llm.JSONPart); jsonOK && json.Valid(jsonPart.Value) {
					text, ok = llm.TextPart{Text: string(jsonPart.Value)}, true
				}
			}
			if !ok {
				return fmt.Errorf("instruction %d part %d kind %q is not supported by Bedrock Converse system blocks", index, partIndex, part.PartKind())
			}
			input.System = append(input.System, &types.SystemContentBlockMemberText{Value: text.Text})
		}
	}
	return nil
}

func lowerItem(item llm.Item) (types.Message, error) {
	switch value := item.(type) {
	case llm.Message:
		content, err := lowerParts(value.Content)
		if err != nil {
			return types.Message{}, err
		}
		role := types.ConversationRoleUser
		if value.Actor == llm.ActorModel {
			role = types.ConversationRoleAssistant
		}
		return types.Message{Role: role, Content: content}, nil
	case llm.ToolCall:
		if value.ID == "" || value.Name == "" || !json.Valid(value.Arguments) {
			return types.Message{}, fmt.Errorf("tool call requires ID, name, and valid JSON arguments")
		}
		input, err := decodeDocument(value.Arguments)
		if err != nil {
			return types.Message{}, err
		}
		return types.Message{Role: types.ConversationRoleAssistant, Content: []types.ContentBlock{
			&types.ContentBlockMemberToolUse{Value: types.ToolUseBlock{ToolUseId: stringPtr(value.ID), Name: stringPtr(value.Name), Input: document.NewLazyDocument(input)}},
		}}, nil
	case llm.ToolResult:
		if value.CallID == "" {
			return types.Message{}, fmt.Errorf("tool result requires call ID")
		}
		content, err := lowerToolResultParts(value.Content)
		if err != nil {
			return types.Message{}, err
		}
		status := types.ToolResultStatusSuccess
		if value.IsError {
			status = types.ToolResultStatusError
		}
		return types.Message{Role: types.ConversationRoleUser, Content: []types.ContentBlock{
			&types.ContentBlockMemberToolResult{Value: types.ToolResultBlock{ToolUseId: stringPtr(value.CallID), Content: content, Status: status}},
		}}, nil
	case llm.ProviderState:
		return types.Message{}, fmt.Errorf("provider state is not accepted by Bedrock Converse")
	case llm.Reference:
		return types.Message{}, fmt.Errorf("reference input is not accepted by Bedrock Converse")
	default:
		return types.Message{}, fmt.Errorf("unsupported input item %T", item)
	}
}

func lowerParts(parts []llm.Part) ([]types.ContentBlock, error) {
	content := make([]types.ContentBlock, 0, len(parts))
	for index, part := range parts {
		text, ok := part.(llm.TextPart)
		if !ok {
			if value, jsonOK := part.(llm.JSONPart); jsonOK && json.Valid(value.Value) {
				text, ok = llm.TextPart{Text: string(value.Value)}, true
			}
		}
		if !ok {
			return nil, fmt.Errorf("part %d kind %q is not supported by Bedrock Converse", index, part.PartKind())
		}
		content = append(content, &types.ContentBlockMemberText{Value: text.Text})
	}
	return content, nil
}

func lowerToolResultParts(parts []llm.Part) ([]types.ToolResultContentBlock, error) {
	content := make([]types.ToolResultContentBlock, 0, len(parts))
	for index, part := range parts {
		text, ok := part.(llm.TextPart)
		if !ok {
			if value, jsonOK := part.(llm.JSONPart); jsonOK && json.Valid(value.Value) {
				text, ok = llm.TextPart{Text: string(value.Value)}, true
			}
		}
		if !ok {
			return nil, fmt.Errorf("tool result part %d kind %q is not supported by Bedrock Converse", index, part.PartKind())
		}
		content = append(content, &types.ToolResultContentBlockMemberText{Value: text.Text})
	}
	return content, nil
}

func lowerTools(tools []llm.Tool) ([]types.Tool, error) {
	result := make([]types.Tool, 0, len(tools))
	for index, tool := range tools {
		if tool.Name == "" || !json.Valid(tool.InputSchema) {
			return nil, fmt.Errorf("tool %d requires a name and valid JSON input schema", index)
		}
		var schema any
		if err := json.Unmarshal(tool.InputSchema, &schema); err != nil {
			return nil, fmt.Errorf("tool %q input schema: %w", tool.Name, err)
		}
		var description *string
		if tool.Description != "" {
			description = stringPtr(tool.Description)
		}
		result = append(result, &types.ToolMemberToolSpec{Value: types.ToolSpecification{Name: stringPtr(tool.Name), Description: description, InputSchema: &types.ToolInputSchemaMemberJson{Value: document.NewLazyDocument(schema)}}})
	}
	return result, nil
}

func lowerToolChoice(policy llm.ToolPolicy) (types.ToolChoice, error) {
	switch policy.Mode {
	case "", llm.ToolChoiceAuto:
		return &types.ToolChoiceMemberAuto{Value: types.AutoToolChoice{}}, nil
	case llm.ToolChoiceNone:
		return nil, nil
	case llm.ToolChoiceRequired:
		return &types.ToolChoiceMemberAny{Value: types.AnyToolChoice{}}, nil
	case llm.ToolChoiceNamed:
		if policy.Name == "" {
			return nil, fmt.Errorf("named tool policy requires a name")
		}
		return &types.ToolChoiceMemberTool{Value: types.SpecificToolChoice{Name: stringPtr(policy.Name)}}, nil
	default:
		return nil, fmt.Errorf("unsupported tool policy %q", policy.Mode)
	}
}

func hasMixedInstructionLevels(instructions []llm.Instruction) bool {
	application, policy := false, false
	for _, instruction := range instructions {
		if instruction.Level == llm.InstructionLevelPolicy {
			policy = true
		} else {
			application = true
		}
	}
	return application && policy
}

func stringPtr(value string) *string { return &value }

// decodeDocument preserves JSON numeric literals through the SDK document
// serializer. json.Number alone is encoded as a string by Smithy's encoder;
// its decoder converts those values to document.Number, including nested data.
func decodeDocument(raw json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var input any
	if err := decoder.Decode(&input); err != nil {
		return nil, err
	}
	var value any
	if err := smithydocumentjson.NewDecoder().DecodeJSONInterface(input, &value); err != nil {
		return nil, err
	}
	return value, nil
}

// reasoningIsProviderDefault reports whether a reasoning spec asks for nothing
// beyond the provider's default behaviour.
func reasoningIsProviderDefault(reasoning llm.ReasoningSpec) bool {
	return (reasoning.Mode == "" || reasoning.Mode == llm.ReasoningModeProviderDefault) &&
		reasoning.Effort == "" && reasoning.TokenBudget == nil &&
		(reasoning.Summary == "" || reasoning.Summary == llm.ReasoningSummaryProviderDefault)
}
