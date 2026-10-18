package openaichat

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	usageutil "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/usage"
	llmschema "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

func (profile Profile) liftResponse(call provider.Call, response *openai.ChatCompletion, requestID string) (llm.Response, error) {
	if response == nil {
		return llm.Response{}, invalidResponseError(call, requestID, "provider returned an empty response")
	}
	if profile.ResponseError != nil {
		if mapped := profile.ResponseError(call, response, requestID); mapped != nil {
			return llm.Response{}, mapped
		}
	}
	actual := profile.actualClass(string(response.ServiceTier))
	if len(response.Choices) == 0 {
		return llm.Response{}, invalidResponseError(call, requestID, "provider response contained no choices")
	}
	if len(response.Choices) > 1 {
		return llm.Response{}, invalidResponseError(call, requestID, "provider returned multiple choices")
	}
	choice := response.Choices[0]
	output, hasToolCalls, hasRefusal, err := liftChoice(choice)
	if err != nil {
		return llm.Response{}, liftFailure(call, requestID, response.ID, choice.FinishReason, err)
	}
	status, err := liftStatus(choice.FinishReason, hasToolCalls, hasRefusal)
	if err != nil {
		return llm.Response{}, liftFailure(call, requestID, response.ID, choice.FinishReason, err)
	}
	if err := validateFinalJSON(call, output, status, hasToolCalls, hasRefusal); err != nil {
		return llm.Response{}, liftFailure(call, requestID, response.ID, choice.FinishReason, err)
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
		return llm.Response{}, liftFailure(call, requestID, response.ID, choice.FinishReason, withLiftCause("usage_invalid", err))
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
			return llm.Response{}, liftFailure(call, requestID, response.ID, choice.FinishReason, withLiftCause("response_metadata_invalid", err))
		}
	}
	return result, nil
}

// validateFinalJSON enforces the requested JSON response contract locally.
// Chat Completions only promises a string content field, so provider-side
// response_format validation is not sufficient for the semantic boundary.
// The compiled SDK parameters are the source of truth for the requested
// response format and remain inside this adapter package.
func validateFinalJSON(call provider.Call, output []llm.Item, status llm.ResponseStatus, hasToolCalls, hasRefusal bool) error {
	// Incomplete text is retained for callers and accounting, not validated
	// as a promised complete JSON document. It remains ineligible for success caching.
	if status != llm.ResponseStatusCompleted || hasToolCalls || hasRefusal || call.SDKParams == nil {
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
		return withLiftCause("json_text_missing", fmt.Errorf("provider response did not contain JSON text content"))
	}
	switch format.Type {
	case "json_object":
		if !json.Valid([]byte(content)) {
			return withLiftCause(jsonContentShape(content), fmt.Errorf("provider JSON response is invalid"))
		}
	case "json_schema":
		compiled, err := llmschema.Parse(validationSchema(call, format.JSONSchema.Schema))
		if err != nil {
			return fmt.Errorf("response schema validation setup: %w", err)
		}
		if err := compiled.Validate([]byte(content)); err != nil {
			return withLiftCause(jsonContentShape(content), fmt.Errorf("provider JSON response does not satisfy schema: %w", err))
		}
	default:
		return fmt.Errorf("provider returned unsupported response format %q", format.Type)
	}
	return nil
}

// validationSchema prefers the caller's schema over the wire schema, which a
// strict request carries in a lowered, looser form.
func validationSchema(call provider.Call, wire json.RawMessage) json.RawMessage {
	if len(call.OutputSchema) > 0 {
		return call.OutputSchema
	}
	return wire
}

// outputSchema is the caller's json_schema output schema, kept on the call so
// the lift validates against it rather than the lowered wire schema.
func outputSchema(request llm.Request) json.RawMessage {
	if request.Output == nil || request.Output.Format.Kind != llm.OutputKindJSONSchema {
		return nil
	}
	return append(json.RawMessage(nil), request.Output.Format.Schema...)
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
			return nil, false, false, withLiftCause("tool_call_type_unsupported", fmt.Errorf("choice tool call %d has unsupported type %q", index, union.Type))
		}
		call := union.AsFunction()
		if call.ID == "" || call.Function.Name == "" {
			return nil, false, false, withLiftCause("tool_call_incomplete", fmt.Errorf("choice tool call %d is missing ID or name", index))
		}
		arguments := call.Function.Arguments
		// Some providers send an empty string for a zero-argument tool. That
		// is the empty object, not a malformed call. A length finish keeps the
		// cut-off handling below, since the call may have ended before any
		// argument was written.
		if strings.TrimSpace(arguments) == "" && choice.FinishReason != "length" {
			arguments = "{}"
		}
		// encoding/json accepts duplicate keys but a normalized tool call does
		// not, so arguments must also be canonicalizable, and within
		// llm.MaxToolArgumentsDepth so the call can be saved.
		if _, err := llm.CanonicalToolArguments([]byte(arguments)); err != nil {
			// The output limit can cut a call off mid-arguments. Keep the paid
			// response as a length truncation and drop only the incomplete call.
			if choice.FinishReason == "length" {
				continue
			}
			return nil, false, false, withLiftCause("tool_call_arguments_invalid", fmt.Errorf("tool call %q arguments are invalid JSON", call.ID))
		}
		hasToolCalls = true
		output = append(output, llm.ToolCall{ID: call.ID, Name: call.Function.Name, Arguments: []byte(arguments)})
	}
	return output, hasToolCalls, hasRefusal, nil
}

func liftStatus(finishReason string, hasToolCalls, hasRefusal bool) (llm.ResponseStatus, error) {
	switch finishReason {
	// OpenRouter documents finish_reason as nullable. A response that names no
	// reason is classified by its content, exactly like a normal stop.
	case "stop", "":
		if hasRefusal {
			return llm.ResponseStatusRefused, nil
		}
		if hasToolCalls {
			return llm.ResponseStatusToolCalls, nil
		}
		return llm.ResponseStatusCompleted, nil
	case "tool_calls", "function_call":
		if !hasToolCalls {
			return "", withLiftCause("finish_reason_without_tool_call", fmt.Errorf("finish reason %q did not contain a tool call", finishReason))
		}
		return llm.ResponseStatusToolCalls, nil
	case "length":
		return llm.ResponseStatusLength, nil
	case "content_filter":
		return llm.ResponseStatusContentFiltered, nil
	default:
		return "", withLiftCause("finish_reason_unknown", fmt.Errorf("provider returned unknown finish reason %q", finishReason))
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

// liftCauseError tags a lift failure with a closed, worker-defined cause. The
// cause, unlike the message, is safe to report: it never carries provider
// text.
type liftCauseError struct {
	cause string
	err   error
}

func (err liftCauseError) Error() string { return err.err.Error() }

func (err liftCauseError) Unwrap() error { return err.err }

func withLiftCause(cause string, err error) error {
	return liftCauseError{cause: cause, err: err}
}

// liftFailure is the accepted invalid response for a lift error. Its safe
// details name the cause and the provider's finish reason when that is a
// short token, so a rejected response is diagnosable without its content.
func liftFailure(call provider.Call, requestID, responseID, finishReason string, err error) *provider.Error {
	mapped := invalidResponseError(call, requestID, err.Error())
	mapped.Provider.ResponseID = responseID
	cause := "response_invalid"
	var tagged liftCauseError
	if errors.As(err, &tagged) {
		cause = tagged.cause
	}
	mapped.SafeDetails = map[string]string{"lift_cause": cause}
	if safeFinishReason(finishReason) {
		mapped.SafeDetails["finish_reason"] = finishReason
	}
	return mapped
}

func safeFinishReason(value string) bool {
	if value == "" || len(value) > 32 {
		return false
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && r != '_' {
			return false
		}
	}
	return true
}

// jsonContentShape classifies JSON text that failed validation without
// reporting any of it: empty, wrapped in a code fence, preceded or followed by
// other text, not JSON at all, or valid JSON the schema rejects.
func jsonContentShape(content string) string {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return "json_text_empty"
	}
	if strings.HasPrefix(trimmed, "```") {
		return "json_fenced"
	}
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	var value json.RawMessage
	if err := decoder.Decode(&value); err != nil {
		if index := strings.IndexAny(trimmed, "{["); index > 0 && json.Valid([]byte(trimmed[index:])) {
			return "json_leading_text"
		}
		return "json_invalid"
	}
	if decoder.InputOffset() < int64(len(trimmed)) {
		return "json_trailing_text"
	}
	return "json_schema_violation"
}
