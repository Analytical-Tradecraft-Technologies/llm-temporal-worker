package bedrockconverse

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	llmschema "github.com/mfow/llm-temporal-worker/golang/llm/schema"
)

const jsonSchemaPromptTransform = "json_schema_prompt_v1"

// Emulation retains the complete schema for local validation, without sending
// constraints that the model's native output grammar cannot represent.
type emulatedJSONCall struct {
	params bedrockruntime.ConverseInput
	schema *llmschema.Schema
}

func (call emulatedJSONCall) MarshalJSON() ([]byte, error) {
	return json.Marshal(call.params)
}

func compileRequest(request llm.Request, profile Profile, serviceTier string, strict bool, capability provider.Capability) (any, error) {
	if request.Output == nil || request.Output.Format.Kind == "" || request.Output.Format.Kind == llm.OutputKindText {
		return lowerRequest(request, profile, serviceTier, strict)
	}
	if capability.State != provider.CapabilityEmulated || capability.Transform != jsonSchemaPromptTransform {
		return nil, fmt.Errorf("structured output requires the emulated %q transform", jsonSchemaPromptTransform)
	}
	document := request.Output.Format.Schema
	switch request.Output.Format.Kind {
	case llm.OutputKindJSON:
		document = json.RawMessage(`{"type":"object"}`)
	case llm.OutputKindJSONSchema:
	default:
		return nil, fmt.Errorf("unsupported output format %q", request.Output.Format.Kind)
	}
	schema, err := llmschema.Parse(document)
	if err != nil {
		return nil, fmt.Errorf("emulated output schema: %w", err)
	}
	instructions := append([]llm.Instruction(nil), request.Instructions...)
	level := llm.InstructionLevelApplication
	if len(instructions) > 0 {
		level = instructions[0].Level
	}
	request.Instructions = append(instructions, llm.Instruction{
		Kind:  llm.InstructionKindText,
		Level: level,
		Text:  "Return exactly one JSON value satisfying the following complete JSON Schema. Do not use Markdown fences or add prose outside the JSON.\n" + string(schema.Canonical()),
	})
	output := *request.Output
	output.Format = llm.OutputFormat{Kind: llm.OutputKindText}
	request.Output = &output
	params, err := lowerRequest(request, profile, serviceTier, strict)
	if err != nil {
		return nil, err
	}
	return emulatedJSONCall{params: params, schema: schema}, nil
}

func compiledParameters(value any) (bedrockruntime.ConverseInput, *llmschema.Schema, bool) {
	switch value := value.(type) {
	case bedrockruntime.ConverseInput:
		return value, nil, true
	case *bedrockruntime.ConverseInput:
		if value != nil {
			return *value, nil, true
		}
	case emulatedJSONCall:
		return value.params, value.schema, true
	case *emulatedJSONCall:
		if value != nil {
			return value.params, value.schema, true
		}
	}
	return bedrockruntime.ConverseInput{}, nil, false
}

func lowerRequest(request llm.Request, profile Profile, serviceTier string, strict bool) (bedrockruntime.ConverseInput, error) {
	if strict && hasMixedInstructionLevels(request.Instructions) {
		return bedrockruntime.ConverseInput{}, fmt.Errorf("instruction hierarchy cannot be preserved by Bedrock Converse in strict portability mode")
	}
	input := bedrockruntime.ConverseInput{ModelId: stringPtr(request.Model), ServiceTier: &types.ServiceTier{Type: types.ServiceTierType(serviceTier)}}
	if request.Output != nil && request.Output.Format.Kind != "" && request.Output.Format.Kind != llm.OutputKindText {
		return bedrockruntime.ConverseInput{}, fmt.Errorf("native structured output is not supported by Bedrock Converse")
	}
	if err := lowerReasoning(request, &input); err != nil {
		return bedrockruntime.ConverseInput{}, err
	}
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

func lowerReasoning(request llm.Request, input *bedrockruntime.ConverseInput) error {
	if request.Reasoning == nil {
		return nil
	}
	// Only known Nova 2 Lite identifiers can use this model-specific contract.
	switch request.Model {
	case "amazon.nova-2-lite-v1:0", "us.amazon.nova-2-lite-v1:0", "eu.amazon.nova-2-lite-v1:0", "apac.amazon.nova-2-lite-v1:0", "global.amazon.nova-2-lite-v1:0":
	default:
		return fmt.Errorf("reasoning is not supported for model %q by Bedrock Converse", request.Model)
	}
	reasoning := request.Reasoning
	if reasoning.TokenBudget != nil {
		return fmt.Errorf("Nova 2 Lite reasoning does not support token_budget")
	}
	switch reasoning.Summary {
	case "", llm.ReasoningSummaryProviderDefault, llm.ReasoningSummaryNone:
	default:
		return fmt.Errorf("Nova 2 Lite reasoning does not support summary %q", reasoning.Summary)
	}
	mode := reasoning.Mode
	switch mode {
	case "", llm.ReasoningModeProviderDefault:
		if reasoning.Effort == "" || reasoning.Effort == llm.ReasoningEffortProviderDefault {
			return nil
		}
		mode = llm.ReasoningModeEnabled
	case llm.ReasoningModeDisabled:
		if reasoning.Effort != "" && reasoning.Effort != llm.ReasoningEffortProviderDefault {
			return fmt.Errorf("disabled Nova 2 Lite reasoning cannot specify effort")
		}
		input.AdditionalModelRequestFields = document.NewLazyDocument(map[string]any{"reasoningConfig": map[string]any{"type": "disabled"}})
		return nil
	case llm.ReasoningModeEnabled:
	default:
		return fmt.Errorf("Nova 2 Lite reasoning does not support mode %q", mode)
	}
	switch reasoning.Effort {
	case llm.ReasoningEffortLow, llm.ReasoningEffortMedium, llm.ReasoningEffortHigh:
	default:
		return fmt.Errorf("enabled Nova 2 Lite reasoning requires low, medium, or high effort, got %q", reasoning.Effort)
	}
	if reasoning.Effort == llm.ReasoningEffortHigh && request.Sampling != nil &&
		(request.Sampling.Temperature != nil || request.Sampling.TopP != nil || request.Sampling.TopK != nil) {
		return fmt.Errorf("high Nova 2 Lite reasoning effort cannot use temperature, top_p, or top_k")
	}
	input.AdditionalModelRequestFields = document.NewLazyDocument(map[string]any{
		"reasoningConfig": map[string]any{"type": string(mode), "maxReasoningEffort": string(reasoning.Effort)},
	})
	return nil
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
