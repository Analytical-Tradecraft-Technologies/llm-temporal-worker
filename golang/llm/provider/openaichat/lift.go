package openaichat

import (
	"encoding/json"
	"fmt"

	openai "github.com/openai/openai-go/v3"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	usageutil "github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/usage"
	llmschema "github.com/mfow/llm-temporal-worker/golang/llm/schema"
)

func (profile Profile) liftResponse(call provider.Call, response *openai.ChatCompletion, requestID string) (llm.Response, error) {
	if response == nil {
		return llm.Response{}, invalidResponseError(call, requestID, "provider returned an empty response")
	}
	actual, err := profile.actualClass(string(response.ServiceTier))
	if err != nil {
		mapped := invalidResponseError(call, requestID, err.Error())
		mapped.Provider.ResponseID = response.ID
		return llm.Response{}, mapped
	}
	if len(response.Choices) == 0 {
		return llm.Response{}, invalidResponseError(call, requestID, "provider response contained no choices")
	}
	if len(response.Choices) > 1 {
		return llm.Response{}, invalidResponseError(call, requestID, "provider returned multiple choices")
	}
	choice := response.Choices[0]
	output, hasToolCalls, hasRefusal, err := liftChoice(choice)
	if err != nil {
		mapped := invalidResponseError(call, requestID, err.Error())
		mapped.Provider.ResponseID = response.ID
		return llm.Response{}, mapped
	}
	status, err := liftStatus(choice.FinishReason, hasToolCalls, hasRefusal)
	if err != nil {
		mapped := invalidResponseError(call, requestID, err.Error())
		mapped.Provider.ResponseID = response.ID
		return llm.Response{}, mapped
	}
	if err := validateFinalJSON(call, output, hasToolCalls, hasRefusal); err != nil {
		mapped := invalidResponseError(call, requestID, err.Error())
		mapped.Provider.ResponseID = response.ID
		return llm.Response{}, mapped
	}
	providerRaw := map[string]json.RawMessage{}
	if response.SystemFingerprint != "" {
		encoded, _ := json.Marshal(response.SystemFingerprint)
		providerRaw["system_fingerprint"] = encoded
	}
	if choice.Index >= 0 {
		encoded, _ := json.Marshal(choice.Index)
		providerRaw["choice_index"] = encoded
	}
	usage, err := liftUsage(response.Usage)
	if err != nil {
		mapped := invalidResponseError(call, requestID, err.Error())
		mapped.Provider.ResponseID = response.ID
		return llm.Response{}, mapped
	}
	providerFacts := llm.ProviderFacts{
		ResponseID:   response.ID,
		RequestID:    requestID,
		FinishReason: choice.FinishReason,
		Raw:          providerRaw,
	}
	service := llm.ServiceFacts{
		Requested:     call.ServiceClass,
		Attempted:     call.ServiceClass,
		Actual:        actual,
		ProviderValue: string(response.ServiceTier),
		FallbackIndex: 0,
	}
	result := llm.Response{
		APIVersion:   llm.APIVersion,
		OperationKey: call.OperationKey,
		Status:       status,
		Output:       output,
		Route: llm.RouteFacts{
			EndpointID:     call.EndpointID,
			APIFamily:      string(provider.FamilyOpenAIChat),
			RequestedModel: call.Model,
			ResolvedModel:  response.Model,
		},
		Service:  service,
		Usage:    usage,
		Provider: providerFacts,
	}
	if profile.ResponseAugment != nil {
		if err := profile.ResponseAugment(call, response, &result); err != nil {
			mapped := invalidResponseError(call, requestID, err.Error())
			mapped.Provider.ResponseID = response.ID
			return llm.Response{}, mapped
		}
	}
	return result, nil
}

// validateFinalJSON enforces the requested JSON response contract locally.
// Chat Completions only promises a string content field, so provider-side
// response_format validation is not sufficient for the semantic boundary.
// The compiled SDK parameters are the source of truth for the requested
// response format and remain inside this adapter package.
func validateFinalJSON(call provider.Call, output []llm.Item, hasToolCalls, hasRefusal bool) error {
	if hasToolCalls || hasRefusal || call.SDKParams == nil {
		return nil
	}
	params, ok := call.SDKParams.(openai.ChatCompletionNewParams)
	if !ok {
		if pointer, pointerOK := call.SDKParams.(*openai.ChatCompletionNewParams); pointerOK && pointer != nil {
			params = *pointer
			ok = true
		}
	}
	if !ok {
		return nil
	}
	wire, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("response format validation parameters: %w", err)
	}
	var envelope struct {
		ResponseFormat json.RawMessage `json:"response_format"`
	}
	if err := json.Unmarshal(wire, &envelope); err != nil || len(envelope.ResponseFormat) == 0 || string(envelope.ResponseFormat) == "null" {
		return nil
	}
	var format struct {
		Type       string `json:"type"`
		JSONSchema struct {
			Schema json.RawMessage `json:"schema"`
		} `json:"json_schema"`
	}
	if err := json.Unmarshal(envelope.ResponseFormat, &format); err != nil {
		return fmt.Errorf("response format validation: %w", err)
	}
	content, ok := firstModelText(output)
	if !ok {
		return fmt.Errorf("provider response did not contain JSON text content")
	}
	switch format.Type {
	case "json_object":
		if !json.Valid([]byte(content)) {
			return fmt.Errorf("provider JSON response is invalid")
		}
	case "json_schema":
		compiled, err := llmschema.Parse(format.JSONSchema.Schema)
		if err != nil {
			return fmt.Errorf("response schema validation setup: %w", err)
		}
		if err := compiled.Validate([]byte(content)); err != nil {
			return fmt.Errorf("provider JSON response does not satisfy schema: %w", err)
		}
	default:
		return fmt.Errorf("provider returned unsupported response format %q", format.Type)
	}
	return nil
}

func firstModelText(output []llm.Item) (string, bool) {
	for _, item := range output {
		message, ok := item.(llm.Message)
		if !ok || message.Actor != llm.ActorModel {
			continue
		}
		for _, part := range message.Content {
			if text, ok := part.(llm.TextPart); ok {
				return text.Text, true
			}
		}
	}
	return "", false
}

func liftChoice(choice openai.ChatCompletionChoice) ([]llm.Item, bool, bool, error) {
	message := choice.Message
	output := make([]llm.Item, 0, 1+len(message.ToolCalls))
	content := make([]llm.Part, 0, 2)
	if message.Content != "" {
		content = append(content, llm.TextPart{Text: message.Content})
	}
	hasRefusal := message.Refusal != ""
	if hasRefusal {
		content = append(content, llm.RefusalPart{Text: message.Refusal, ProviderCode: "openai.refusal"})
	}
	if message.Content != "" || hasRefusal || len(message.ToolCalls) == 0 {
		output = append(output, llm.Message{Actor: llm.ActorModel, Content: content})
	}
	hasToolCalls := false
	for index, union := range message.ToolCalls {
		if union.Type != "function" {
			return nil, false, false, fmt.Errorf("choice tool call %d has unsupported type %q", index, union.Type)
		}
		call := union.AsFunction()
		if call.ID == "" || call.Function.Name == "" {
			return nil, false, false, fmt.Errorf("choice tool call %d is missing ID or name", index)
		}
		if !json.Valid([]byte(call.Function.Arguments)) {
			return nil, false, false, fmt.Errorf("tool call %q arguments are invalid JSON", call.ID)
		}
		hasToolCalls = true
		output = append(output, llm.ToolCall{ID: call.ID, Name: call.Function.Name, Arguments: []byte(call.Function.Arguments)})
	}
	return output, hasToolCalls, hasRefusal, nil
}

func liftStatus(finishReason string, hasToolCalls, hasRefusal bool) (llm.ResponseStatus, error) {
	switch finishReason {
	case "stop":
		if hasRefusal {
			return llm.ResponseStatusRefused, nil
		}
		return llm.ResponseStatusCompleted, nil
	case "tool_calls", "function_call":
		if !hasToolCalls {
			return "", fmt.Errorf("finish reason %q did not contain a tool call", finishReason)
		}
		return llm.ResponseStatusToolCalls, nil
	case "length":
		return llm.ResponseStatusLength, nil
	case "content_filter":
		return llm.ResponseStatusContentFiltered, nil
	default:
		return "", fmt.Errorf("provider returned unknown finish reason %q", finishReason)
	}
}

func liftUsage(usage openai.CompletionUsage) (llm.Usage, error) {
	input, err := usageutil.OrdinaryInput(usage.PromptTokens, usage.PromptTokensDetails.CachedTokens, usage.PromptTokensDetails.CacheWriteTokens)
	if err != nil {
		return llm.Usage{}, err
	}
	result := llm.Usage{
		InputTokens:      input,
		OutputTokens:     usage.CompletionTokens,
		ReasoningTokens:  usage.CompletionTokensDetails.ReasoningTokens,
		CacheReadTokens:  usage.PromptTokensDetails.CachedTokens,
		CacheWriteTokens: usage.PromptTokensDetails.CacheWriteTokens,
	}
	result.ProviderRaw = make(map[string]json.RawMessage)
	if usage.PromptTokens > 0 {
		encodedInput, _ := json.Marshal(usage.PromptTokens)
		result.ProviderRaw["prompt_tokens"] = encodedInput
	}
	if usage.TotalTokens > 0 {
		encoded, _ := json.Marshal(usage.TotalTokens)
		result.ProviderRaw["total_tokens"] = encoded
	}
	return result, nil
}
