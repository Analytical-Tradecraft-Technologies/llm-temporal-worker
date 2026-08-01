package bedrockconverse

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func lowerRequest(request llm.Request, profile Profile, serviceTier string, strict bool) (bedrockruntime.ConverseInput, error) {
	if strict && hasMixedInstructionLevels(request.Instructions) {
		return bedrockruntime.ConverseInput{}, fmt.Errorf("instruction hierarchy cannot be preserved by Bedrock Converse in strict portability mode")
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
		input.Messages = append(input.Messages, message)
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
		var input any
		if err := json.Unmarshal(value.Arguments, &input); err != nil {
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
		result = append(result, &types.ToolMemberToolSpec{Value: types.ToolSpecification{Name: stringPtr(tool.Name), Description: stringPtr(tool.Description), InputSchema: &types.ToolInputSchemaMemberJson{Value: document.NewLazyDocument(schema)}}})
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
