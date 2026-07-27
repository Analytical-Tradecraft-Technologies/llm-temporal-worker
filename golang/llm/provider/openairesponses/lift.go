package openairesponses

import (
	"encoding/json"
	"fmt"

	"github.com/openai/openai-go/v3/responses"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	llmschema "github.com/mfow/llm-temporal-worker/golang/llm/schema"
)

func liftResponse(call provider.Call, response *responses.Response, requestID string) (llm.Response, error) {
	if response == nil {
		return llm.Response{}, invalidResponseError(call, requestID, "provider returned an empty response")
	}
	actual, err := serviceClassForTier(response.ServiceTier)
	if err != nil {
		mapped := invalidResponseError(call, requestID, err.Error())
		mapped.Provider.ResponseID = response.ID
		return llm.Response{}, mapped
	}
	output, hasToolCalls, hasRefusal, err := liftOutput(response.Output)
	if err != nil {
		mapped := invalidResponseError(call, requestID, err.Error())
		mapped.Provider.ResponseID = response.ID
		return llm.Response{}, mapped
	}
	status, err := liftStatus(response.Status, response.IncompleteDetails.Reason, hasToolCalls, hasRefusal)
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
	ids := make([]string, 0, len(response.Output))
	for _, item := range response.Output {
		if item.ID != "" {
			ids = append(ids, item.ID)
		}
	}
	if len(ids) > 0 {
		encoded, _ := json.Marshal(ids)
		providerRaw["output_item_ids"] = encoded
	}
	providerFacts := llm.ProviderFacts{
		ResponseID:   response.ID,
		RequestID:    requestID,
		FinishReason: string(response.Status),
		Raw:          providerRaw,
	}
	usage := llm.Usage{
		InputTokens:      response.Usage.InputTokens,
		OutputTokens:     response.Usage.OutputTokens,
		ReasoningTokens:  response.Usage.OutputTokensDetails.ReasoningTokens,
		CacheReadTokens:  response.Usage.InputTokensDetails.CachedTokens,
		CacheWriteTokens: response.Usage.InputTokensDetails.CacheWriteTokens,
	}
	if response.Usage.TotalTokens > 0 {
		encoded, _ := json.Marshal(response.Usage.TotalTokens)
		usage.ProviderRaw = map[string]json.RawMessage{"total_tokens": encoded}
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
			APIFamily:      string(provider.FamilyOpenAIResponses),
			RequestedModel: call.Model,
			ResolvedModel:  string(response.Model),
		},
		Service:      service,
		Usage:        usage,
		Provider:     providerFacts,
		Continuation: continuationForResponse(call, response),
	}
	return result, nil
}

// validateFinalJSON enforces the requested Responses text-format contract at
// the semantic boundary. Provider-side structured-output promises are not a
// substitute for validating the bytes that will enter Temporal history.
func validateFinalJSON(call provider.Call, output []llm.Item, hasToolCalls, hasRefusal bool) error {
	if hasToolCalls || hasRefusal || call.SDKParams == nil {
		return nil
	}
	params, ok := call.SDKParams.(responses.ResponseNewParams)
	if !ok {
		if pointer, pointerOK := call.SDKParams.(*responses.ResponseNewParams); pointerOK && pointer != nil {
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
		Text struct {
			Format json.RawMessage `json:"format"`
		} `json:"text"`
	}
	if err := json.Unmarshal(wire, &envelope); err != nil || len(envelope.Text.Format) == 0 || string(envelope.Text.Format) == "null" {
		return nil
	}
	var format struct {
		Type   string          `json:"type"`
		Schema json.RawMessage `json:"schema"`
	}
	if err := json.Unmarshal(envelope.Text.Format, &format); err != nil {
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
		compiled, err := llmschema.Parse(format.Schema)
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

func serviceClassForTier(tier responses.ResponseServiceTier) (*llm.ServiceClass, error) {
	var class llm.ServiceClass
	switch tier {
	case responses.ResponseServiceTierFlex:
		class = llm.ServiceClassEconomy
	case responses.ResponseServiceTierDefault:
		class = llm.ServiceClassStandard
	case responses.ResponseServiceTierPriority:
		class = llm.ServiceClassPriority
	case "":
		return nil, fmt.Errorf("provider response omitted service tier")
	default:
		return nil, fmt.Errorf("provider returned unsupported service tier %q", tier)
	}
	return &class, nil
}

func liftStatus(status responses.ResponseStatus, incompleteReason string, hasToolCalls, hasRefusal bool) (llm.ResponseStatus, error) {
	switch status {
	case responses.ResponseStatusCompleted:
		if hasToolCalls {
			return llm.ResponseStatusToolCalls, nil
		}
		if hasRefusal {
			return llm.ResponseStatusRefused, nil
		}
		return llm.ResponseStatusCompleted, nil
	case responses.ResponseStatusIncomplete:
		if incompleteReason == "content_filter" {
			return llm.ResponseStatusContentFiltered, nil
		}
		return llm.ResponseStatusLength, nil
	case responses.ResponseStatusFailed, responses.ResponseStatusInProgress, responses.ResponseStatusCancelled, responses.ResponseStatusQueued:
		return "", fmt.Errorf("provider response status %q is not a terminal semantic response", status)
	default:
		return "", fmt.Errorf("provider returned unknown response status %q", status)
	}
}

func liftOutput(items []responses.ResponseOutputItemUnion) ([]llm.Item, bool, bool, error) {
	output := make([]llm.Item, 0, len(items))
	toolCalls := false
	refusal := false
	for index, item := range items {
		switch item.Type {
		case "message":
			message := item.AsMessage()
			content := make([]llm.Part, 0, len(message.Content))
			for contentIndex, part := range message.Content {
				switch part.Type {
				case "output_text":
					content = append(content, llm.TextPart{Text: part.Text})
				case "refusal":
					refusal = true
					content = append(content, llm.RefusalPart{Text: part.Refusal, ProviderCode: "openai.refusal"})
				default:
					return nil, false, false, fmt.Errorf("output item %d content %d has unsupported type %q", index, contentIndex, part.Type)
				}
			}
			output = append(output, llm.Message{Actor: llm.ActorModel, Content: content})
		case "function_call":
			call := item.AsFunctionCall()
			if call.CallID == "" {
				call.CallID = call.ID
			}
			if call.CallID == "" || call.Name == "" {
				return nil, false, false, fmt.Errorf("function call output item %d is missing call ID or name", index)
			}
			if !json.Valid([]byte(call.Arguments)) {
				return nil, false, false, fmt.Errorf("function call %q arguments are invalid JSON", call.CallID)
			}
			toolCalls = true
			output = append(output, llm.ToolCall{ID: call.CallID, Name: call.Name, Arguments: []byte(call.Arguments)})
		case "function_call_output":
			callOutput := item.AsFunctionCallOutput()
			if callOutput.CallID == "" {
				return nil, false, false, fmt.Errorf("function call output item %d is missing call ID", index)
			}
			text := callOutput.Output.OfString
			if text == "" && len(callOutput.Output.OfOutputContentList) > 0 {
				encoded, err := json.Marshal(callOutput.Output.OfOutputContentList)
				if err != nil {
					return nil, false, false, err
				}
				text = string(encoded)
			}
			output = append(output, llm.ToolResult{CallID: callOutput.CallID, Content: []llm.Part{llm.TextPart{Text: text}}})
		case "reasoning":
			reasoning := item.AsReasoning()
			raw := []byte(reasoning.RawJSON())
			if len(raw) == 0 {
				raw = []byte(item.RawJSON())
			}
			output = append(output, llm.ProviderState{Provider: "openai", EndpointFamily: "responses", MediaType: "application/vnd.openai.reasoning+json", Opaque: raw})
		default:
			return nil, false, false, fmt.Errorf("output item %d has unsupported type %q", index, item.Type)
		}
	}
	return output, toolCalls, refusal, nil
}

func continuationForResponse(call provider.Call, response *responses.Response) *llm.Continuation {
	if response.ID == "" || !statefulContinuationEnabled(call) {
		return nil
	}
	return &llm.Continuation{
		Handle:     "openai-responses:" + response.ID,
		EndpointID: call.EndpointID,
		Model:      string(response.Model),
		Pinned:     true,
		ProviderStates: []llm.ProviderState{{
			Provider:       "openai",
			EndpointFamily: "responses",
			MediaType:      "application/vnd.openai.response+json",
			Opaque:         []byte(response.ID),
		}},
	}
}

func statefulContinuationEnabled(call provider.Call) bool {
	params, ok := call.SDKParams.(responses.ResponseNewParams)
	if !ok {
		if pointer, pointerOK := call.SDKParams.(*responses.ResponseNewParams); pointerOK && pointer != nil {
			params = *pointer
			ok = true
		}
	}
	if !ok {
		return true
	}
	return !params.Store.Valid() || params.Store.Value
}

func invalidResponseError(call provider.Call, requestID, message string) *provider.Error {
	mapped := provider.NewError(provider.CodeProviderInvalidResponse, provider.PhaseLift, provider.DispatchAccepted, provider.RetryNever, message)
	mapped.Provider.RequestID = requestID
	mapped.OperationID = call.OperationKey
	return mapped
}
