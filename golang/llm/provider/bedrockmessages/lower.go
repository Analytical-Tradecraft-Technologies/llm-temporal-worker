package bedrockmessages

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/packages/param"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/anthropicschema"
)

// lowerRequest builds the Anthropic body InvokeModel forwards to the model.
// The Bedrock service tier is not part of that body: InvokeModel takes it as
// the X-Amzn-Bedrock-Service-Tier request header, which Invoke sets from the
// call metadata.
func lowerRequest(request llm.Request, profile Profile) (anthropic.MessageNewParams, error) {
	return lowerRequestWithStrict(request, profile, false)
}

func lowerRequestWithStrict(request llm.Request, profile Profile, strict bool) (anthropic.MessageNewParams, error) {
	if strict && hasMixedInstructionLevels(request.Instructions) {
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
		if emptyModelMessage(item) {
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
		if err := lowerOutput(*request.Output, target, strict); err != nil {
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
		if err := lowerSampling(*request.Sampling, target); err != nil {
			return anthropic.MessageNewParams{}, err
		}
	}
	if request.Reasoning != nil {
		thinking, err := lowerReasoning(*request.Reasoning, strict)
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
	} else if request.ToolPolicy.Mode != "" && request.ToolPolicy.Mode != llm.ToolChoiceAuto && request.ToolPolicy.Mode != llm.ToolChoiceNone {
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

// emptyModelMessage reports a replayed model turn with no parts (for example a
// lifted content_filter or empty stop reply). Bedrock Messages rejects an assistant message
// without content, and it carries no history to preserve.
func emptyModelMessage(item llm.Item) bool {
	message, ok := item.(llm.Message)
	return ok && message.Actor == llm.ActorModel && len(message.Content) == 0
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
		if tool.Kind != "" && tool.Kind != llm.ToolKindFunction {
			return nil, fmt.Errorf("tool %d kind %q is not supported by Bedrock Messages", index, tool.Kind)
		}
		var schema map[string]any
		if err := json.Unmarshal(tool.InputSchema, &schema); err != nil || schema == nil {
			return nil, fmt.Errorf("tool %q input schema must be an object", tool.Name)
		}
		// Strict tool use only accepts a closed schema subset the worker does
		// not enforce, so tools are sent non-strict like Responses.
		result = append(result, map[string]any{"name": tool.Name, "description": tool.Description, "input_schema": schema})
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

func lowerOutput(output llm.OutputSpec, target map[string]any, strict bool) error {
	switch output.Format.Kind {
	case "", llm.OutputKindText:
		return nil
	case llm.OutputKindJSON:
		// Structured output requires a closed schema; a bare object schema is
		// not valid and would otherwise constrain the answer to {}.
		return fmt.Errorf("output format %q without a schema is not supported by Bedrock Messages", output.Format.Kind)
	case llm.OutputKindJSONSchema:
		// Structured output returns a 400 for keywords outside its subset, so
		// the wire carries a lowered schema; the lift validates the final
		// JSON against the caller's original.
		schema, err := anthropicschema.Lower(output.Format.Schema, strict)
		if err != nil {
			return err
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
		if *sampling.Temperature < 0 || *sampling.Temperature > 1 {
			return fmt.Errorf("temperature %v is outside the Bedrock Messages range 0 to 1", *sampling.Temperature)
		}
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

// lowerReasoning returns the Messages thinking object, or nil when the request
// leaves thinking to the model default. Effort is lowered separately to
// output_config.effort, which the API accepts independently of thinking, so an
// effort or summary preference alone never turns thinking on.
func lowerReasoning(reasoning llm.ReasoningSpec, strict bool) (map[string]any, error) {
	if reasoning.Effort == llm.ReasoningEffortExtraHigh {
		return nil, fmt.Errorf("xhigh reasoning effort is not supported by bedrockmessages")
	}
	mode := reasoning.Mode
	if mode == "" {
		mode = llm.ReasoningModeProviderDefault
	}
	// The only display controls are "summarized" (the provider default) and
	// "omitted"; a summary detail level cannot be expressed.
	display := ""
	switch reasoning.Summary {
	case "", llm.ReasoningSummaryProviderDefault:
	case llm.ReasoningSummaryNone:
		display = "omitted"
	case llm.ReasoningSummaryAuto:
		display = "summarized"
	case llm.ReasoningSummaryConcise, llm.ReasoningSummaryDetailed:
		if strict {
			return nil, provider.NewStrictPortabilityError(fmt.Sprintf("reasoning summary %q is not supported by Bedrock Messages", reasoning.Summary))
		}
		display = "summarized"
	default:
		return nil, fmt.Errorf("reasoning summary %q is not supported by Bedrock Messages", reasoning.Summary)
	}
	if mode == llm.ReasoningModeProviderDefault {
		if reasoning.TokenBudget == nil {
			// Thinking is opt-in: with no thinking object the response has no
			// thinking blocks, so there is nothing to summarize or omit and
			// dropping the summary preference loses nothing, even in strict
			// mode. display has no wire form outside that object.
			return nil, nil
		}
		mode = llm.ReasoningModeEnabled
	}
	if reasoning.Effort != "" && reasoning.Effort != llm.ReasoningEffortProviderDefault && mode != llm.ReasoningModeAdaptive {
		return nil, fmt.Errorf("reasoning effort %q requires adaptive Bedrock thinking", reasoning.Effort)
	}
	result := map[string]any{}
	switch mode {
	case llm.ReasoningModeDisabled:
		return map[string]any{"type": "disabled"}, nil
	case llm.ReasoningModeAdaptive:
		result["type"] = "adaptive"
	case llm.ReasoningModeEnabled:
		if reasoning.TokenBudget == nil || *reasoning.TokenBudget < 1024 {
			return nil, fmt.Errorf("Bedrock thinking token_budget must be at least 1024")
		}
		result["type"] = "enabled"
		result["budget_tokens"] = *reasoning.TokenBudget
	default:
		return nil, fmt.Errorf("reasoning mode %q is not supported by Bedrock Messages", reasoning.Mode)
	}
	if display != "" {
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
