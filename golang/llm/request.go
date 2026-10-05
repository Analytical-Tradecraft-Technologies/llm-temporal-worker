package llm

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

type Request struct {
	APIVersion            string
	OperationKey          string
	Context               RequestContext
	Model                 string
	ServiceClass          ServiceClass
	ServiceClassFallbacks []ServiceClass
	Portability           PortabilityMode
	Instructions          []Instruction
	Input                 []Item
	Tools                 []Tool
	ToolPolicy            ToolPolicy
	Output                *OutputSpec
	Sampling              *SamplingSpec
	Reasoning             *ReasoningSpec
	Continuation          *Continuation
	Extensions            map[string]json.RawMessage
}

type RequestContext struct {
	Tenant  string
	Project string
	Actor   string
	Tags    map[string]string
}

func (context RequestContext) empty() bool {
	return context.Tenant == "" && context.Project == "" && context.Actor == "" && len(context.Tags) == 0
}

func (context RequestContext) MarshalJSON() ([]byte, error) {
	fields := make(map[string]any)
	if context.Tenant != "" {
		fields["tenant"] = context.Tenant
	}
	if context.Project != "" {
		fields["project"] = context.Project
	}
	if context.Actor != "" {
		fields["actor"] = context.Actor
	}
	if len(context.Tags) > 0 {
		fields["tags"] = context.Tags
	}
	return marshalObject(fields)
}

func decodeRequestContext(data []byte) (RequestContext, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return RequestContext{}, err
	}
	if err := checkUnknownFields(fields, "tenant", "project", "actor", "tags"); err != nil {
		return RequestContext{}, err
	}
	tenant, _, err := optionalString(fields, "tenant")
	if err != nil {
		return RequestContext{}, err
	}
	project, _, err := optionalString(fields, "project")
	if err != nil {
		return RequestContext{}, err
	}
	actor, _, err := optionalString(fields, "actor")
	if err != nil {
		return RequestContext{}, err
	}
	var tags map[string]string
	if raw, ok := fields["tags"]; ok {
		if err := decodeJSON(raw, &tags); err != nil {
			return RequestContext{}, fmt.Errorf("context tags: %w", err)
		}
	}
	return RequestContext{Tenant: tenant, Project: project, Actor: actor, Tags: tags}, nil
}

type InstructionKind string

const (
	InstructionKindText  InstructionKind = "text"
	InstructionKindParts InstructionKind = "parts"
)

type InstructionLevel string

const (
	InstructionLevelApplication InstructionLevel = "application"
	InstructionLevelPolicy      InstructionLevel = "policy"
)

type Instruction struct {
	Kind    InstructionKind
	Level   InstructionLevel
	Text    string
	Content []Part
}

func (instruction Instruction) MarshalJSON() ([]byte, error) {
	kind := instruction.Kind
	if kind == "" {
		if len(instruction.Content) == 0 {
			kind = InstructionKindText
		} else {
			kind = InstructionKindParts
		}
	}
	if kind != InstructionKindText && kind != InstructionKindParts {
		return nil, fmt.Errorf("instruction kind %q is invalid", kind)
	}
	level := instruction.Level
	if level == "" {
		level = InstructionLevelApplication
	}
	if level != InstructionLevelApplication && level != InstructionLevelPolicy {
		return nil, fmt.Errorf("instruction level %q is invalid", level)
	}
	fields := map[string]any{"kind": kind}
	if level != InstructionLevelApplication {
		fields["level"] = level
	}
	switch kind {
	case InstructionKindText:
		if len(instruction.Content) > 0 {
			if len(instruction.Content) != 1 {
				return nil, fmt.Errorf("text instruction cannot contain multiple parts")
			}
			text, ok := instruction.Content[0].(TextPart)
			if !ok {
				return nil, fmt.Errorf("text instruction content must be a text part")
			}
			if instruction.Text != "" && instruction.Text != text.Text {
				return nil, fmt.Errorf("text instruction has conflicting text and content")
			}
			instruction.Text = text.Text
		}
		fields["text"] = instruction.Text
	case InstructionKindParts:
		content := instruction.Content
		if content == nil {
			content = []Part{}
		}
		fields["content"] = content
	}
	return marshalObject(fields)
}

func decodeInstruction(data []byte) (Instruction, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return Instruction{}, err
	}
	if err := checkUnknownFields(fields, "kind", "level", "text", "content"); err != nil {
		return Instruction{}, err
	}
	kindValue, err := requiredString(fields, "kind")
	if err != nil {
		return Instruction{}, err
	}
	kind := InstructionKind(kindValue)
	if kind != InstructionKindText && kind != InstructionKindParts {
		return Instruction{}, fmt.Errorf("instruction kind %q is invalid", kind)
	}
	levelValue, _, err := optionalString(fields, "level")
	if err != nil {
		return Instruction{}, err
	}
	level := InstructionLevel(levelValue)
	if level == "" {
		level = InstructionLevelApplication
	}
	if level != InstructionLevelApplication && level != InstructionLevelPolicy {
		return Instruction{}, fmt.Errorf("instruction level %q is invalid", level)
	}
	instruction := Instruction{Kind: kind, Level: level}
	switch kind {
	case InstructionKindText:
		text, err := requiredString(fields, "text")
		if err != nil {
			return Instruction{}, err
		}
		if _, ok := fields["content"]; ok {
			return Instruction{}, fmt.Errorf("text instruction cannot contain content")
		}
		instruction.Text = text
	case InstructionKindParts:
		contentRaw, err := requireField(fields, "content")
		if err != nil {
			return Instruction{}, err
		}
		content, err := decodeParts(contentRaw)
		if err != nil {
			return Instruction{}, fmt.Errorf("instruction content: %w", err)
		}
		if _, ok := fields["text"]; ok {
			return Instruction{}, fmt.Errorf("parts instruction cannot contain text")
		}
		instruction.Content = content
	}
	return instruction, nil
}

type PortabilityMode string

const (
	PortabilityStrict     PortabilityMode = "strict"
	PortabilityBestEffort PortabilityMode = "best_effort"
)

func (mode PortabilityMode) Valid() bool {
	return mode == PortabilityStrict || mode == PortabilityBestEffort
}

type OutputKind string

const (
	OutputKindText       OutputKind = "text"
	OutputKindJSON       OutputKind = "json"
	OutputKindJSONSchema OutputKind = "json_schema"
)

// DefaultOutputFormatName is the json_schema name sent to providers that
// require one when the caller omitted it. It is a constant so the wire body
// stays deterministic.
const DefaultOutputFormatName = "response"

// validateOutputFormatName applies the strictest provider rule for a
// json_schema name (OpenAI Responses and Chat: a-z, A-Z, 0-9, underscores and
// dashes, at most 64 characters). The name itself stays optional.
func validateOutputFormatName(name string) error {
	if len(name) > 64 {
		return fmt.Errorf("output format name must contain at most 64 ASCII characters")
	}
	for _, char := range name {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_' || char == '-' {
			continue
		}
		return fmt.Errorf("output format name contains invalid character %q", char)
	}
	return nil
}

type OutputFormat struct {
	Kind        OutputKind
	Name        string
	Description string
	Strict      bool
	Schema      json.RawMessage
}

func (format OutputFormat) MarshalJSON() ([]byte, error) {
	kind := format.Kind
	if kind == "" {
		kind = OutputKindText
	}
	if kind != OutputKindText && kind != OutputKindJSON && kind != OutputKindJSONSchema {
		return nil, fmt.Errorf("output format kind %q is invalid", kind)
	}
	if err := validateOutputFormatName(format.Name); err != nil {
		return nil, err
	}
	fields := map[string]any{"kind": kind}
	if format.Name != "" {
		fields["name"] = format.Name
	}
	if format.Description != "" {
		fields["description"] = format.Description
	}
	if kind == OutputKindJSONSchema {
		if !validObjectJSON(format.Schema) {
			return nil, errorsForField("json_schema output format", "schema")
		}
		fields["schema"] = copyRaw(format.Schema)
		fields["strict"] = format.Strict
	} else if len(format.Schema) > 0 {
		return nil, fmt.Errorf("output schema is only valid for json_schema format")
	} else if format.Strict {
		return nil, fmt.Errorf("output strict is only valid for json_schema format")
	}
	return marshalObject(fields)
}

func decodeOutputFormat(data []byte) (OutputFormat, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return OutputFormat{}, err
	}
	if err := checkUnknownFields(fields, "kind", "name", "description", "strict", "schema"); err != nil {
		return OutputFormat{}, err
	}
	kindValue, present, err := optionalString(fields, "kind")
	if err != nil {
		return OutputFormat{}, err
	}
	kind := OutputKind(kindValue)
	if !present || kind == "" {
		kind = OutputKindText
	}
	if kind != OutputKindText && kind != OutputKindJSON && kind != OutputKindJSONSchema {
		return OutputFormat{}, fmt.Errorf("output format kind %q is invalid", kind)
	}
	name, namePresent, err := optionalString(fields, "name")
	if err != nil {
		return OutputFormat{}, err
	}
	if namePresent && name == "" {
		return OutputFormat{}, fmt.Errorf("output format name must not be empty when present")
	}
	if err := validateOutputFormatName(name); err != nil {
		return OutputFormat{}, err
	}
	description, _, err := optionalString(fields, "description")
	if err != nil {
		return OutputFormat{}, err
	}
	strict, _, err := optionalBool(fields, "strict")
	if err != nil {
		return OutputFormat{}, err
	}
	var schema json.RawMessage
	if value, ok := fields["schema"]; ok {
		if !validObjectJSON(value) {
			return OutputFormat{}, errorsForField("json_schema output format", "schema")
		}
		schema = copyRaw(value)
	}
	if kind == OutputKindJSONSchema {
		if len(schema) == 0 {
			return OutputFormat{}, errorsForField("json_schema output format", "schema")
		}
	} else if len(schema) > 0 || strict {
		return OutputFormat{}, fmt.Errorf("schema and strict are only valid for json_schema format")
	}
	return OutputFormat{Kind: kind, Name: name, Description: description, Strict: strict, Schema: schema}, nil
}

type OutputSpec struct {
	MaxTokens *int
	Format    OutputFormat
}

func (output OutputSpec) MarshalJSON() ([]byte, error) {
	fields := map[string]any{"format": output.Format}
	if output.MaxTokens != nil {
		if *output.MaxTokens < 0 {
			return nil, fmt.Errorf("output max_tokens must not be negative")
		}
		fields["max_tokens"] = *output.MaxTokens
	}
	return marshalObject(fields)
}

func decodeOutput(data []byte) (*OutputSpec, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return nil, err
	}
	if err := checkUnknownFields(fields, "max_tokens", "format"); err != nil {
		return nil, err
	}
	var maxTokens *int
	if value, ok := fields["max_tokens"]; ok {
		parsed, err := decodeInt(value)
		if err != nil {
			return nil, fmt.Errorf("output max_tokens: %w", err)
		}
		if parsed < 0 {
			return nil, fmt.Errorf("output max_tokens must not be negative")
		}
		maxTokens = &parsed
	}
	format := OutputFormat{Kind: OutputKindText}
	if value, ok := fields["format"]; ok {
		format, err = decodeOutputFormat(value)
		if err != nil {
			return nil, fmt.Errorf("output format: %w", err)
		}
	}
	return &OutputSpec{MaxTokens: maxTokens, Format: format}, nil
}

type SamplingSpec struct {
	Temperature      *float64
	TopP             *float64
	TopK             *int
	Seed             *int64
	PresencePenalty  *float64
	FrequencyPenalty *float64
	StopSequences    []string
}

func (sampling SamplingSpec) MarshalJSON() ([]byte, error) {
	fields := make(map[string]any)
	if sampling.Temperature != nil {
		fields["temperature"] = *sampling.Temperature
	}
	if sampling.TopP != nil {
		fields["top_p"] = *sampling.TopP
	}
	if sampling.TopK != nil {
		fields["top_k"] = *sampling.TopK
	}
	if sampling.Seed != nil {
		fields["seed"] = *sampling.Seed
	}
	if sampling.PresencePenalty != nil {
		fields["presence_penalty"] = *sampling.PresencePenalty
	}
	if sampling.FrequencyPenalty != nil {
		fields["frequency_penalty"] = *sampling.FrequencyPenalty
	}
	if sampling.StopSequences != nil {
		fields["stop_sequences"] = sampling.StopSequences
	}
	return marshalObject(fields)
}

func decodeSampling(data []byte) (*SamplingSpec, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return nil, err
	}
	if err := checkUnknownFields(fields, "temperature", "top_p", "top_k", "seed", "presence_penalty", "frequency_penalty", "stop_sequences"); err != nil {
		return nil, err
	}
	result := &SamplingSpec{}
	if raw, ok := fields["temperature"]; ok {
		value, err := decodeFloat64(raw)
		if err != nil {
			return nil, fmt.Errorf("sampling temperature: %w", err)
		}
		result.Temperature = &value
	}
	if raw, ok := fields["top_p"]; ok {
		value, err := decodeFloat64(raw)
		if err != nil {
			return nil, fmt.Errorf("sampling top_p: %w", err)
		}
		result.TopP = &value
	}
	if raw, ok := fields["top_k"]; ok {
		value, err := decodeInt(raw)
		if err != nil {
			return nil, fmt.Errorf("sampling top_k: %w", err)
		}
		result.TopK = &value
	}
	if raw, ok := fields["seed"]; ok {
		value, err := decodeInt64(raw)
		if err != nil {
			return nil, fmt.Errorf("sampling seed: %w", err)
		}
		result.Seed = &value
	}
	if raw, ok := fields["presence_penalty"]; ok {
		value, err := decodeFloat64(raw)
		if err != nil {
			return nil, fmt.Errorf("sampling presence_penalty: %w", err)
		}
		result.PresencePenalty = &value
	}
	if raw, ok := fields["frequency_penalty"]; ok {
		value, err := decodeFloat64(raw)
		if err != nil {
			return nil, fmt.Errorf("sampling frequency_penalty: %w", err)
		}
		result.FrequencyPenalty = &value
	}
	if raw, ok := fields["stop_sequences"]; ok {
		if err := decodeJSON(raw, &result.StopSequences); err != nil {
			return nil, fmt.Errorf("sampling stop_sequences: %w", err)
		}
	}
	return result, nil
}

type ReasoningMode string

const (
	ReasoningModeProviderDefault ReasoningMode = "provider_default"
	ReasoningModeDisabled        ReasoningMode = "disabled"
	ReasoningModeAdaptive        ReasoningMode = "adaptive"
	ReasoningModeEnabled         ReasoningMode = "enabled"
)

type ReasoningEffort string

const (
	ReasoningEffortProviderDefault ReasoningEffort = "provider_default"
	ReasoningEffortMinimal         ReasoningEffort = "minimal"
	ReasoningEffortLow             ReasoningEffort = "low"
	ReasoningEffortMedium          ReasoningEffort = "medium"
	ReasoningEffortHigh            ReasoningEffort = "high"
	ReasoningEffortMaximum         ReasoningEffort = "maximum"
)

type ReasoningSummary string

const (
	ReasoningSummaryProviderDefault ReasoningSummary = "provider_default"
	ReasoningSummaryNone            ReasoningSummary = "none"
	ReasoningSummaryAuto            ReasoningSummary = "auto"
	ReasoningSummaryConcise         ReasoningSummary = "concise"
	ReasoningSummaryDetailed        ReasoningSummary = "detailed"
)

type ReasoningSpec struct {
	Mode        ReasoningMode
	Effort      ReasoningEffort
	TokenBudget *int
	Summary     ReasoningSummary
}

func (reasoning ReasoningSpec) MarshalJSON() ([]byte, error) {
	fields := make(map[string]any)
	if reasoning.Mode != "" {
		fields["mode"] = reasoning.Mode
	}
	if reasoning.Effort != "" {
		fields["effort"] = reasoning.Effort
	}
	if reasoning.TokenBudget != nil {
		if *reasoning.TokenBudget < 0 {
			return nil, fmt.Errorf("reasoning token_budget must not be negative")
		}
		fields["token_budget"] = *reasoning.TokenBudget
	}
	if reasoning.Summary != "" {
		fields["summary"] = reasoning.Summary
	}
	return marshalObject(fields)
}

func decodeReasoning(data []byte) (*ReasoningSpec, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return nil, err
	}
	if err := checkUnknownFields(fields, "mode", "effort", "token_budget", "summary"); err != nil {
		return nil, err
	}
	modeValue, _, err := optionalString(fields, "mode")
	if err != nil {
		return nil, err
	}
	effortValue, _, err := optionalString(fields, "effort")
	if err != nil {
		return nil, err
	}
	summaryValue, _, err := optionalString(fields, "summary")
	if err != nil {
		return nil, err
	}
	result := &ReasoningSpec{
		Mode:    ReasoningMode(modeValue),
		Effort:  ReasoningEffort(effortValue),
		Summary: ReasoningSummary(summaryValue),
	}
	if raw, ok := fields["token_budget"]; ok {
		value, err := decodeInt(raw)
		if err != nil {
			return nil, fmt.Errorf("reasoning token_budget: %w", err)
		}
		if value < 0 {
			return nil, fmt.Errorf("reasoning token_budget must not be negative")
		}
		result.TokenBudget = &value
	}
	if err := validateReasoning(*result); err != nil {
		return nil, err
	}
	return result, nil
}

type Continuation struct {
	Handle         string
	EndpointID     string
	Model          string
	ExpiresAt      *time.Time
	Pinned         bool
	ProviderStates []ProviderState
}

func (continuation Continuation) MarshalJSON() ([]byte, error) {
	if continuation.Handle == "" {
		return nil, errorsForField("continuation", "handle")
	}
	fields := map[string]any{
		"handle": continuation.Handle,
		"pinned": continuation.Pinned,
	}
	if continuation.EndpointID != "" {
		fields["endpoint_id"] = continuation.EndpointID
	}
	if continuation.Model != "" {
		fields["model"] = continuation.Model
	}
	if continuation.ExpiresAt != nil {
		fields["expires_at"] = continuation.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	if continuation.ProviderStates != nil {
		fields["provider_state"] = continuation.ProviderStates
	}
	return marshalObject(fields)
}

func decodeContinuation(data []byte) (*Continuation, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return nil, err
	}
	if err := checkUnknownFields(fields, "handle", "endpoint_id", "model", "expires_at", "pinned", "provider_state"); err != nil {
		return nil, err
	}
	handle, err := requiredString(fields, "handle")
	if err != nil {
		return nil, err
	}
	endpointID, _, err := optionalString(fields, "endpoint_id")
	if err != nil {
		return nil, err
	}
	model, _, err := optionalString(fields, "model")
	if err != nil {
		return nil, err
	}
	pinned, _, err := optionalBool(fields, "pinned")
	if err != nil {
		return nil, err
	}
	var expiresAt *time.Time
	if raw, ok := fields["expires_at"]; ok {
		value, err := requiredString(map[string]json.RawMessage{"expires_at": raw}, "expires_at")
		if err != nil {
			return nil, err
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return nil, fmt.Errorf("continuation expires_at: %w", err)
		}
		expiresAt = &parsed
	}
	var states []ProviderState
	if raw, ok := fields["provider_state"]; ok {
		var values []json.RawMessage
		if err := decodeJSON(raw, &values); err != nil {
			return nil, fmt.Errorf("continuation provider_state: %w", err)
		}
		states = make([]ProviderState, 0, len(values))
		for index, value := range values {
			state, err := decodeProviderState(value)
			if err != nil {
				return nil, fmt.Errorf("continuation provider_state %d: %w", index, err)
			}
			states = append(states, state)
		}
	}
	return &Continuation{Handle: handle, EndpointID: endpointID, Model: model, ExpiresAt: expiresAt, Pinned: pinned, ProviderStates: states}, nil
}

func (request Request) MarshalJSON() ([]byte, error) {
	if request.APIVersion != "" && request.APIVersion != APIVersion {
		return nil, fmt.Errorf("api_version %q is unsupported", request.APIVersion)
	}
	if request.OperationKey == "" {
		return nil, errorsForField("request", "operation_key")
	}
	if request.Model == "" {
		return nil, errorsForField("request", "model")
	}
	serviceClass, err := NormalizeServiceClass(request.ServiceClass)
	if err != nil {
		return nil, err
	}
	fallbacks := request.ServiceClassFallbacks
	if fallbacks == nil {
		fallbacks = []ServiceClass{}
	}
	if err := ValidateServiceClassFallbacks(serviceClass, fallbacks); err != nil {
		return nil, err
	}
	portability := request.Portability
	if portability == "" {
		portability = PortabilityStrict
	}
	if !portability.Valid() {
		return nil, fmt.Errorf("portability %q is invalid", portability)
	}
	instructions := request.Instructions
	if instructions == nil {
		instructions = []Instruction{}
	}
	input := request.Input
	if input == nil {
		input = []Item{}
	}
	tools := request.Tools
	if tools == nil {
		tools = []Tool{}
	}
	extensions := copyRawMap(request.Extensions)
	if extensions == nil {
		extensions = map[string]json.RawMessage{}
	}
	fields := map[string]any{
		"api_version":             APIVersion,
		"operation_key":           request.OperationKey,
		"model":                   request.Model,
		"service_class":           serviceClass,
		"service_class_fallbacks": fallbacks,
		"portability":             portability,
		"instructions":            instructions,
		"input":                   input,
		"tools":                   tools,
		"tool_policy":             request.ToolPolicy,
		"continuation":            request.Continuation,
		"extensions":              extensions,
	}
	if !request.Context.empty() {
		fields["context"] = request.Context
	}
	if request.Output != nil {
		fields["output"] = request.Output
	}
	if request.Sampling != nil {
		fields["sampling"] = request.Sampling
	}
	if request.Reasoning != nil {
		fields["reasoning"] = request.Reasoning
	}
	return marshalObject(fields)
}

func (request *Request) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "api_version", "operation_key", "context", "model", "service_class", "service_class_fallbacks", "portability", "instructions", "input", "tools", "tool_policy", "output", "sampling", "reasoning", "continuation", "extensions"); err != nil {
		return err
	}
	apiVersion, err := requiredString(fields, "api_version")
	if err != nil {
		return err
	}
	if apiVersion != APIVersion {
		return fmt.Errorf("api_version %q is unsupported", apiVersion)
	}
	operationKey, err := requiredString(fields, "operation_key")
	if err != nil {
		return err
	}
	model, err := requiredString(fields, "model")
	if err != nil {
		return err
	}
	result := Request{APIVersion: apiVersion, OperationKey: operationKey, Model: model, Portability: PortabilityStrict}
	if raw, ok := fields["context"]; ok {
		result.Context, err = decodeRequestContext(raw)
		if err != nil {
			return fmt.Errorf("context: %w", err)
		}
	}
	serviceClass := ServiceClass("")
	if raw, ok := fields["service_class"]; ok {
		value, err := requiredString(map[string]json.RawMessage{"service_class": raw}, "service_class")
		if err != nil {
			return err
		}
		serviceClass = ServiceClass(value)
	}
	result.ServiceClass, err = NormalizeServiceClass(serviceClass)
	if err != nil {
		return err
	}
	if raw, ok := fields["service_class_fallbacks"]; ok {
		if err := decodeJSON(raw, &result.ServiceClassFallbacks); err != nil {
			return fmt.Errorf("service_class_fallbacks: %w", err)
		}
	}
	if result.ServiceClassFallbacks == nil {
		result.ServiceClassFallbacks = []ServiceClass{}
	}
	if err := ValidateServiceClassFallbacks(result.ServiceClass, result.ServiceClassFallbacks); err != nil {
		return err
	}
	if raw, ok := fields["portability"]; ok {
		value, err := requiredString(map[string]json.RawMessage{"portability": raw}, "portability")
		if err != nil {
			return err
		}
		result.Portability = PortabilityMode(value)
		if !result.Portability.Valid() {
			return fmt.Errorf("portability %q is invalid", result.Portability)
		}
	}
	if raw, ok := fields["instructions"]; ok {
		result.Instructions, err = decodeInstructions(raw)
		if err != nil {
			return fmt.Errorf("instructions: %w", err)
		}
	}
	if raw, ok := fields["input"]; ok {
		result.Input, err = decodeItems(raw)
		if err != nil {
			return fmt.Errorf("input: %w", err)
		}
	}
	if raw, ok := fields["tools"]; ok {
		result.Tools, err = decodeTools(raw)
		if err != nil {
			return fmt.Errorf("tools: %w", err)
		}
	}
	result.ToolPolicy = ToolPolicy{Mode: ToolChoiceAuto}
	if raw, ok := fields["tool_policy"]; ok {
		result.ToolPolicy, err = decodeToolPolicy(raw)
		if err != nil {
			return fmt.Errorf("tool_policy: %w", err)
		}
	}
	if raw, ok := fields["output"]; ok {
		if string(raw) != "null" {
			result.Output, err = decodeOutput(raw)
			if err != nil {
				return fmt.Errorf("output: %w", err)
			}
		}
	}
	if raw, ok := fields["sampling"]; ok {
		if string(raw) != "null" {
			result.Sampling, err = decodeSampling(raw)
			if err != nil {
				return fmt.Errorf("sampling: %w", err)
			}
		}
	}
	if raw, ok := fields["reasoning"]; ok {
		if string(raw) != "null" {
			result.Reasoning, err = decodeReasoning(raw)
			if err != nil {
				return fmt.Errorf("reasoning: %w", err)
			}
		}
	}
	if raw, ok := fields["continuation"]; ok && string(raw) != "null" {
		result.Continuation, err = decodeContinuation(raw)
		if err != nil {
			return fmt.Errorf("continuation: %w", err)
		}
	}
	if raw, ok := fields["extensions"]; ok {
		if err := decodeJSON(raw, &result.Extensions); err != nil {
			return fmt.Errorf("extensions: %w", err)
		}
		result.Extensions = copyRawMap(result.Extensions)
	}
	if result.Instructions == nil {
		result.Instructions = []Instruction{}
	}
	if result.Input == nil {
		result.Input = []Item{}
	}
	if result.Tools == nil {
		result.Tools = []Tool{}
	}
	if result.Extensions == nil {
		result.Extensions = map[string]json.RawMessage{}
	}
	*request = result
	return nil
}

func decodeInstructions(data []byte) ([]Instruction, error) {
	var values []json.RawMessage
	if err := decodeJSON(data, &values); err != nil {
		return nil, err
	}
	result := make([]Instruction, 0, len(values))
	for index, value := range values {
		instruction, err := decodeInstruction(value)
		if err != nil {
			return nil, fmt.Errorf("instruction %d: %w", index, err)
		}
		result = append(result, instruction)
	}
	return result, nil
}

func decodeTools(data []byte) ([]Tool, error) {
	var values []json.RawMessage
	if err := decodeJSON(data, &values); err != nil {
		return nil, err
	}
	result := make([]Tool, 0, len(values))
	for index, value := range values {
		tool, err := decodeTool(value)
		if err != nil {
			return nil, fmt.Errorf("tool %d: %w", index, err)
		}
		result = append(result, tool)
	}
	return result, nil
}

func copyRawMap(source map[string]json.RawMessage) map[string]json.RawMessage {
	if source == nil {
		return nil
	}
	result := make(map[string]json.RawMessage, len(source))
	for key, value := range source {
		result[key] = copyRaw(value)
	}
	return result
}

func decodeInt(data []byte) (int, error) {
	var value int
	if err := decodeJSON(data, &value); err != nil {
		return 0, err
	}
	return value, nil
}

func decodeInt64(data []byte) (int64, error) {
	var value int64
	if err := decodeJSON(data, &value); err != nil {
		return 0, err
	}
	return value, nil
}

func decodeFloat64(data []byte) (float64, error) {
	var value float64
	if err := decodeJSON(data, &value); err != nil {
		return 0, err
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("number must be finite")
	}
	return value, nil
}

func validateReasoning(reasoning ReasoningSpec) error {
	if reasoning.Mode != "" && reasoning.Mode != ReasoningModeProviderDefault && reasoning.Mode != ReasoningModeDisabled && reasoning.Mode != ReasoningModeAdaptive && reasoning.Mode != ReasoningModeEnabled {
		return fmt.Errorf("reasoning mode %q is invalid", reasoning.Mode)
	}
	if reasoning.Effort != "" && reasoning.Effort != ReasoningEffortProviderDefault && reasoning.Effort != ReasoningEffortMinimal && reasoning.Effort != ReasoningEffortLow && reasoning.Effort != ReasoningEffortMedium && reasoning.Effort != ReasoningEffortHigh && reasoning.Effort != ReasoningEffortMaximum {
		return fmt.Errorf("reasoning effort %q is invalid", reasoning.Effort)
	}
	if reasoning.Summary != "" && reasoning.Summary != ReasoningSummaryProviderDefault && reasoning.Summary != ReasoningSummaryNone && reasoning.Summary != ReasoningSummaryAuto && reasoning.Summary != ReasoningSummaryConcise && reasoning.Summary != ReasoningSummaryDetailed {
		return fmt.Errorf("reasoning summary %q is invalid", reasoning.Summary)
	}
	return nil
}
