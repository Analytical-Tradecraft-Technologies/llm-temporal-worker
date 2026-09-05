package openairesponses

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/openai/openai-go/v3/responses"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func providerTier(class llm.ServiceClass) string {
	switch class {
	case llm.ServiceClassEconomy:
		return "flex"
	case llm.ServiceClassPriority:
		return "priority"
	default:
		return "default"
	}
}

func lowerRequest(request llm.Request, serviceClass llm.ServiceClass) (responses.ResponseNewParams, error) {
	instructionCount := len(request.Instructions)
	inputCount := len(request.Input)
	if instructionCount > math.MaxInt-inputCount {
		return responses.ResponseNewParams{}, fmt.Errorf("request input too large")
	}
	totalInputCapacity := instructionCount + inputCount
	input := make([]any, 0, totalInputCapacity)
	for _, instruction := range request.Instructions {
		item, err := lowerInstruction(instruction)
		if err != nil {
			return responses.ResponseNewParams{}, err
		}
		input = append(input, item)
	}
	for index, item := range request.Input {
		lowered, err := lowerItem(item)
		if err != nil {
			return responses.ResponseNewParams{}, fmt.Errorf("input item %d: %w", index, err)
		}
		input = append(input, lowered)
	}
	requestMap := map[string]any{
		"model":        request.Model,
		"input":        input,
		"service_tier": providerTier(serviceClass),
	}
	if request.Output != nil {
		output, err := lowerOutput(*request.Output)
		if err != nil {
			return responses.ResponseNewParams{}, err
		}
		for key, value := range output {
			requestMap[key] = value
		}
	}
	if request.Sampling != nil {
		if err := lowerSampling(requestMap, *request.Sampling); err != nil {
			return responses.ResponseNewParams{}, err
		}
	}
	if request.Reasoning != nil {
		reasoning, err := lowerReasoning(*request.Reasoning)
		if err != nil {
			return responses.ResponseNewParams{}, err
		}
		if reasoning != nil {
			requestMap["reasoning"] = reasoning
		}
	}
	if len(request.Tools) > 0 {
		tools, err := lowerTools(request.Tools)
		if err != nil {
			return responses.ResponseNewParams{}, err
		}
		requestMap["tools"] = tools
	}
	policy, err := lowerToolPolicy(request.ToolPolicy)
	if err != nil {
		return responses.ResponseNewParams{}, err
	}
	requestMap["tool_choice"] = policy.choice
	requestMap["parallel_tool_calls"] = policy.parallel
	continuation, err := lowerContinuation(request.Continuation)
	if err != nil {
		return responses.ResponseNewParams{}, err
	}
	if continuation != "" {
		requestMap["previous_response_id"] = continuation
	}
	if err := lowerExtensions(request.Extensions, requestMap); err != nil {
		return responses.ResponseNewParams{}, err
	}
	encoded, err := json.Marshal(requestMap)
	if err != nil {
		return responses.ResponseNewParams{}, err
	}
	var params responses.ResponseNewParams
	if err := json.Unmarshal(encoded, &params); err != nil {
		return responses.ResponseNewParams{}, fmt.Errorf("openai responses parameter union: %w", err)
	}
	if policy.named {
		params.ToolChoice = responses.ResponseNewParamsToolChoiceUnion{
			OfFunctionTool: &responses.ToolChoiceFunctionParam{Name: policy.name},
		}
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
	content, err := lowerParts(parts)
	if err != nil {
		return nil, err
	}
	return map[string]any{"type": "message", "role": role, "content": content}, nil
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
		return map[string]any{"type": "message", "role": role, "content": content}, nil
	case llm.ToolCall:
		if !json.Valid(value.Arguments) {
			return nil, fmt.Errorf("tool call %q arguments are invalid JSON", value.ID)
		}
		return map[string]any{
			"type":      "function_call",
			"id":        value.ID,
			"call_id":   value.ID,
			"name":      value.Name,
			"arguments": string(value.Arguments),
		}, nil
	case llm.ToolResult:
		if value.IsError {
			return nil, fmt.Errorf("tool result %q error state cannot be represented by Responses", value.CallID)
		}
		output, err := lowerToolResultContent(value.Content)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "function_call_output", "call_id": value.CallID, "output": output}, nil
	case llm.ProviderState, llm.Reference:
		return nil, fmt.Errorf("item kind %q is not accepted as Responses input", item.ItemKind())
	default:
		return nil, fmt.Errorf("unsupported input item %T", item)
	}
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
		return map[string]any{"type": "input_text", "text": value.Text}, nil
	case llm.JSONPart:
		if !json.Valid(value.Value) {
			return nil, fmt.Errorf("JSON part is invalid")
		}
		return map[string]any{"type": "input_text", "text": string(value.Value)}, nil
	case llm.ImagePart:
		url, err := mediaURL(value.URL, value.Bytes, value.MediaType)
		if err != nil {
			return nil, fmt.Errorf("image: %w", err)
		}
		detail := value.Detail
		if detail == "" {
			detail = "auto"
		}
		return map[string]any{"type": "input_image", "image_url": url, "detail": detail}, nil
	case llm.DocumentPart:
		url, data, err := mediaFile(value.URL, value.Bytes, value.MediaType)
		if err != nil {
			return nil, fmt.Errorf("document: %w", err)
		}
		file := map[string]any{"type": "input_file"}
		if url != "" {
			file["file_url"] = url
		} else {
			file["file_data"] = data
		}
		if value.Title != "" {
			file["filename"] = value.Title
		}
		return file, nil
	case llm.RefusalPart, llm.ProviderStatePart:
		return nil, fmt.Errorf("part kind %q is not accepted as Responses input", part.PartKind())
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

func mediaFile(rawURL string, data []byte, mediaType string) (string, string, error) {
	if rawURL != "" {
		return rawURL, "", nil
	}
	if len(data) == 0 {
		return "", "", fmt.Errorf("blob-backed media is not available to adapter")
	}
	return "", "data:" + mediaType + ";base64," + base64.StdEncoding.EncodeToString(data), nil
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
			return nil, fmt.Errorf("tool %d kind %q is not supported by Responses", index, tool.Kind)
		}
		var schema map[string]any
		if err := json.Unmarshal(tool.InputSchema, &schema); err != nil {
			return nil, fmt.Errorf("tool %q input schema: %w", tool.Name, err)
		}
		entry := map[string]any{"type": "function", "name": tool.Name, "description": tool.Description, "parameters": schema, "strict": false}
		if len(tool.OutputSchema) > 0 {
			var outputSchema map[string]any
			if err := json.Unmarshal(tool.OutputSchema, &outputSchema); err != nil {
				return nil, fmt.Errorf("tool %q output schema: %w", tool.Name, err)
			}
			entry["output_schema"] = outputSchema
		}
		result = append(result, entry)
	}
	return result, nil
}

type loweredToolPolicy struct {
	choice   any
	parallel bool
	name     string
	named    bool
}

func lowerToolPolicy(policy llm.ToolPolicy) (loweredToolPolicy, error) {
	mode := policy.Mode
	if mode == "" {
		mode = llm.ToolChoiceAuto
	}
	result := loweredToolPolicy{parallel: policy.Parallel}
	switch mode {
	case llm.ToolChoiceNone:
		result.choice = "none"
	case llm.ToolChoiceAuto:
		result.choice = "auto"
	case llm.ToolChoiceRequired:
		result.choice = "required"
	case llm.ToolChoiceNamed:
		if policy.Name == "" {
			return loweredToolPolicy{}, fmt.Errorf("named tool policy requires a name")
		}
		result.choice = map[string]any{"type": "function", "name": policy.Name}
		result.name = policy.Name
		result.named = true
	default:
		return loweredToolPolicy{}, fmt.Errorf("tool policy mode %q is invalid", mode)
	}
	return result, nil
}

func lowerOutput(output llm.OutputSpec) (map[string]any, error) {
	result := make(map[string]any)
	if output.MaxTokens != nil {
		if *output.MaxTokens < 0 {
			return nil, fmt.Errorf("output max_tokens must not be negative")
		}
		result["max_output_tokens"] = *output.MaxTokens
	}
	switch output.Format.Kind {
	case "", llm.OutputKindText:
	case llm.OutputKindJSON:
		result["text"] = map[string]any{"format": map[string]any{"type": "json_object"}}
	case llm.OutputKindJSONSchema:
		var schema map[string]any
		if err := json.Unmarshal(output.Format.Schema, &schema); err != nil {
			return nil, fmt.Errorf("output schema: %w", err)
		}
		format := map[string]any{"type": "json_schema", "name": output.Format.Name, "schema": schema, "strict": output.Format.Strict}
		if output.Format.Description != "" {
			format["description"] = output.Format.Description
		}
		result["text"] = map[string]any{"format": format}
	default:
		return nil, fmt.Errorf("output format %q is not supported", output.Format.Kind)
	}
	return result, nil
}

func lowerSampling(target map[string]any, sampling llm.SamplingSpec) error {
	if sampling.TopK != nil || sampling.Seed != nil || sampling.PresencePenalty != nil || sampling.FrequencyPenalty != nil || len(sampling.StopSequences) > 0 {
		return fmt.Errorf("sampling field is not supported by Responses")
	}
	if sampling.Temperature != nil {
		target["temperature"] = *sampling.Temperature
	}
	if sampling.TopP != nil {
		target["top_p"] = *sampling.TopP
	}
	return nil
}

func lowerReasoning(reasoning llm.ReasoningSpec) (map[string]any, error) {
	result := make(map[string]any)
	switch reasoning.Mode {
	case "", llm.ReasoningModeProviderDefault:
	case llm.ReasoningModeDisabled:
		result["effort"] = "none"
	case llm.ReasoningModeAdaptive, llm.ReasoningModeEnabled:
		result["mode"] = "standard"
	default:
		return nil, fmt.Errorf("reasoning mode %q is not supported", reasoning.Mode)
	}
	switch reasoning.Effort {
	case "", llm.ReasoningEffortProviderDefault:
	case llm.ReasoningEffortMinimal, llm.ReasoningEffortLow, llm.ReasoningEffortMedium, llm.ReasoningEffortHigh:
		result["effort"] = string(reasoning.Effort)
	case llm.ReasoningEffortMaximum:
		result["effort"] = "max"
	default:
		return nil, fmt.Errorf("reasoning effort %q is not supported", reasoning.Effort)
	}
	switch reasoning.Summary {
	case "", llm.ReasoningSummaryProviderDefault:
	case llm.ReasoningSummaryAuto, llm.ReasoningSummaryConcise, llm.ReasoningSummaryDetailed:
		result["summary"] = string(reasoning.Summary)
	case llm.ReasoningSummaryNone:
		result["effort"] = "none"
	default:
		return nil, fmt.Errorf("reasoning summary %q is not supported", reasoning.Summary)
	}
	if reasoning.TokenBudget != nil {
		return nil, fmt.Errorf("reasoning token_budget is not supported by Responses")
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func lowerContinuation(continuation *llm.Continuation) (string, error) {
	if continuation == nil {
		return "", nil
	}
	for _, state := range continuation.ProviderStates {
		if state.Provider == "openai" && state.EndpointFamily == "responses" && len(state.Opaque) > 0 {
			return string(state.Opaque), nil
		}
	}
	if strings.HasPrefix(continuation.Handle, "openai-responses:") {
		return strings.TrimPrefix(continuation.Handle, "openai-responses:"), nil
	}
	return "", fmt.Errorf("continuation does not contain an OpenAI Responses response ID")
}

func lowerExtensions(extensions map[string]json.RawMessage, target map[string]any) error {
	for namespace, raw := range extensions {
		if namespace != "openai.responses" {
			return fmt.Errorf("extension namespace %q is not supported by Responses", namespace)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return fmt.Errorf("extension %q must be an object: %w", namespace, err)
		}
		for name, value := range fields {
			switch name {
			case "include":
				var include []string
				if err := json.Unmarshal(value, &include); err != nil {
					return fmt.Errorf("extension include: %w", err)
				}
				target["include"] = include
			case "store", "background", "truncation":
				var decoded any
				if err := json.Unmarshal(value, &decoded); err != nil {
					return fmt.Errorf("extension %s: %w", name, err)
				}
				target[name] = decoded
			default:
				return fmt.Errorf("extension field %q is not supported by Responses", name)
			}
		}
	}
	return nil
}
