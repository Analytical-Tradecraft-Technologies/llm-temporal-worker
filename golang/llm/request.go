package llm

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// MaxWebSearchCalls bounds provider-hosted search execution per generation.
const MaxWebSearchCalls = 3

type Request struct {
	WebSearch             bool
	WebFetch              bool
	CodeExecution         bool
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
	setIfNotEmpty(fields, "tenant", context.Tenant)
	setIfNotEmpty(fields, "project", context.Project)
	setIfNotEmpty(fields, "actor", context.Actor)
	if len(context.Tags) > 0 {
		fields["tags"] = context.Tags
	}
	return marshalObject(fields)
}

func decodeRequestContext(data []byte) (RequestContext, error) {
	fields, err := decodeClosedObject(data, "tenant", "project", "actor", "tags")
	if err != nil {
		return RequestContext{}, err
	}
	context, err := decodeContextIdentity(fields)
	if err != nil {
		return RequestContext{}, err
	}
	if err := decodeOptionalField(fields, "tags", &context.Tags, decodeJSONValue[map[string]string]); err != nil {
		return RequestContext{}, fmt.Errorf("context %w", err)
	}
	return context, nil
}

// decodeContextIdentity reads the optional tenant, project and actor strings
// the engine and v1 context objects share.
func decodeContextIdentity(fields map[string]json.RawMessage) (RequestContext, error) {
	reader := fieldReader{fields: fields}
	context := RequestContext{
		Tenant:  reader.optionalString("tenant"),
		Project: reader.optionalString("project"),
		Actor:   reader.optionalString("actor"),
	}
	if reader.err != nil {
		return RequestContext{}, reader.err
	}
	return context, nil
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

var instructionLevels = []InstructionLevel{InstructionLevelApplication, InstructionLevelPolicy}

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
	level, err := defaultEnum(instruction.Level, InstructionLevelApplication, "instruction level", instructionLevels...)
	if err != nil {
		return nil, err
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
		fields["content"] = emptyIfNil(instruction.Content)
	}
	return marshalObject(fields)
}

func decodeInstruction(data []byte) (Instruction, error) {
	fields, err := decodeClosedObject(data, "kind", "level", "text", "content")
	if err != nil {
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
	reader := fieldReader{fields: fields}
	level := readEnum(&reader, "level", InstructionLevelApplication, "instruction level", instructionLevels...)
	if reader.err != nil {
		return Instruction{}, reader.err
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

var outputKinds = []OutputKind{OutputKindText, OutputKindJSON, OutputKindJSONSchema}

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
	kind, err := defaultEnum(format.Kind, OutputKindText, "output format kind", outputKinds...)
	if err != nil {
		return nil, err
	}
	if err := validateOutputFormatName(format.Name); err != nil {
		return nil, err
	}
	fields := map[string]any{"kind": kind}
	setIfNotEmpty(fields, "name", format.Name)
	setIfNotEmpty(fields, "description", format.Description)
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
	fields, err := decodeClosedObject(data, "kind", "name", "description", "strict", "schema")
	if err != nil {
		return OutputFormat{}, err
	}
	reader := fieldReader{fields: fields}
	kind := readEnum(&reader, "kind", OutputKindText, "output format kind", outputKinds...)
	name, namePresent := reader.optionalStringPresent("name")
	if reader.err != nil {
		return OutputFormat{}, reader.err
	}
	if namePresent && name == "" {
		return OutputFormat{}, fmt.Errorf("output format name must not be empty when present")
	}
	if err := validateOutputFormatName(name); err != nil {
		return OutputFormat{}, err
	}
	description := reader.optionalString("description")
	strict := reader.optionalBool("strict")
	if reader.err != nil {
		return OutputFormat{}, reader.err
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
	fields, err := decodeClosedObject(data, "max_tokens", "format")
	if err != nil {
		return nil, err
	}
	var maxTokens *int
	if err := decodeOptionalField(fields, "max_tokens", &maxTokens, pointerTo(decodeInt)); err != nil {
		return nil, fmt.Errorf("output %w", err)
	}
	if maxTokens != nil && *maxTokens < 0 {
		return nil, fmt.Errorf("output max_tokens must not be negative")
	}
	format := OutputFormat{Kind: OutputKindText}
	if err := decodeOptionalField(fields, "format", &format, decodeOutputFormat); err != nil {
		return nil, fmt.Errorf("output %w", err)
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
	fields, err := decodeClosedObject(data, "temperature", "top_p", "top_k", "seed", "presence_penalty", "frequency_penalty", "stop_sequences")
	if err != nil {
		return nil, err
	}
	result := &SamplingSpec{}
	reader := fieldReader{fields: fields}
	readOptionalField(&reader, "temperature", &result.Temperature, pointerTo(decodeFloat64))
	readOptionalField(&reader, "top_p", &result.TopP, pointerTo(decodeFloat64))
	readOptionalField(&reader, "top_k", &result.TopK, pointerTo(decodeInt))
	readOptionalField(&reader, "seed", &result.Seed, pointerTo(decodeInt64))
	readOptionalField(&reader, "presence_penalty", &result.PresencePenalty, pointerTo(decodeFloat64))
	readOptionalField(&reader, "frequency_penalty", &result.FrequencyPenalty, pointerTo(decodeFloat64))
	readOptionalField(&reader, "stop_sequences", &result.StopSequences, decodeJSONValue[[]string])
	if reader.err != nil {
		return nil, fmt.Errorf("sampling %w", reader.err)
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
	ReasoningEffortExtraHigh       ReasoningEffort = "xhigh"
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
	setIfNotEmpty(fields, "mode", reasoning.Mode)
	setIfNotEmpty(fields, "effort", reasoning.Effort)
	if reasoning.TokenBudget != nil {
		if *reasoning.TokenBudget < 0 {
			return nil, fmt.Errorf("reasoning token_budget must not be negative")
		}
		fields["token_budget"] = *reasoning.TokenBudget
	}
	setIfNotEmpty(fields, "summary", reasoning.Summary)
	return marshalObject(fields)
}

func decodeReasoning(data []byte) (*ReasoningSpec, error) {
	fields, err := decodeClosedObject(data, "mode", "effort", "token_budget", "summary")
	if err != nil {
		return nil, err
	}
	reader := fieldReader{fields: fields}
	result := &ReasoningSpec{
		Mode:    ReasoningMode(reader.optionalString("mode")),
		Effort:  ReasoningEffort(reader.optionalString("effort")),
		Summary: ReasoningSummary(reader.optionalString("summary")),
	}
	if reader.err != nil {
		return nil, reader.err
	}
	if err := decodeOptionalField(fields, "token_budget", &result.TokenBudget, pointerTo(decodeInt)); err != nil {
		return nil, fmt.Errorf("reasoning %w", err)
	}
	if result.TokenBudget != nil && *result.TokenBudget < 0 {
		return nil, fmt.Errorf("reasoning token_budget must not be negative")
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
	setIfNotEmpty(fields, "endpoint_id", continuation.EndpointID)
	setIfNotEmpty(fields, "model", continuation.Model)
	if continuation.ExpiresAt != nil {
		fields["expires_at"] = continuation.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	if continuation.ProviderStates != nil {
		fields["provider_state"] = continuation.ProviderStates
	}
	return marshalObject(fields)
}

func decodeContinuation(data []byte) (*Continuation, error) {
	fields, err := decodeClosedObject(data, "handle", "endpoint_id", "model", "expires_at", "pinned", "provider_state")
	if err != nil {
		return nil, err
	}
	reader := fieldReader{fields: fields}
	handle := reader.requiredString("handle")
	endpointID := reader.optionalString("endpoint_id")
	model := reader.optionalString("model")
	pinned := reader.optionalBool("pinned")
	if reader.err != nil {
		return nil, reader.err
	}
	var expiresAt *time.Time
	if raw, ok := fields["expires_at"]; ok {
		value, err := decodeStringField("expires_at", raw)
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
		if states, err = decodeEach(values, "continuation provider_state", decodeProviderState); err != nil {
			return nil, err
		}
	}
	return &Continuation{Handle: handle, EndpointID: endpointID, Model: model, ExpiresAt: expiresAt, Pinned: pinned, ProviderStates: states}, nil
}

func (request Request) MarshalJSON() ([]byte, error) {
	fields, err := request.jsonFields()
	if err != nil {
		return nil, err
	}
	return marshalObject(fields)
}

// jsonFields is shared by the public wire encoder and request identity hashing.
// Keep their defaults and validation identical without decoding an encoded
// transcript merely to remove operation_key from the digest (#1112).
func (request Request) jsonFields() (map[string]any, error) {
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
	fallbacks := emptyIfNil(request.ServiceClassFallbacks)
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
		"instructions":            emptyIfNil(request.Instructions),
		"input":                   emptyIfNil(request.Input),
		"tools":                   emptyIfNil(request.Tools),
		"tool_policy":             request.ToolPolicy,
		"continuation":            request.Continuation,
		"extensions":              extensions,
	}
	if request.WebFetch {
		fields["web_fetch"] = true
	}
	if request.CodeExecution {
		fields["code_execution"] = true
	}
	if request.WebSearch {
		fields["web_search"] = true
	}
	if !request.Context.empty() {
		fields["context"] = request.Context
	}
	setIfNotNil(fields, "output", request.Output)
	setIfNotNil(fields, "sampling", request.Sampling)
	setIfNotNil(fields, "reasoning", request.Reasoning)
	return fields, nil
}

func (request *Request) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "api_version", "operation_key", "context", "model", "service_class", "service_class_fallbacks", "portability", "instructions", "input", "tools", "tool_policy", "output", "sampling", "reasoning", "continuation", "extensions", "web_search", "web_fetch", "code_execution")
	if err != nil {
		return err
	}
	apiVersion, operationKey, err := decodeEngineHeader(fields)
	if err != nil {
		return err
	}
	reader := fieldReader{fields: fields}
	result := Request{APIVersion: apiVersion, OperationKey: operationKey, Model: reader.requiredString("model"), Portability: PortabilityStrict}
	readOptionalField(&reader, "context", &result.Context, decodeRequestContext)
	serviceClass := ServiceClass(reader.optionalString("service_class"))
	if reader.err != nil {
		return reader.err
	}
	result.ServiceClass, err = NormalizeServiceClass(serviceClass)
	if err != nil {
		return err
	}
	if err := decodeOptionalField(fields, "service_class_fallbacks", &result.ServiceClassFallbacks, decodeJSONValue[[]ServiceClass]); err != nil {
		return err
	}
	result.ServiceClassFallbacks = emptyIfNil(result.ServiceClassFallbacks)
	if err := ValidateServiceClassFallbacks(result.ServiceClass, result.ServiceClassFallbacks); err != nil {
		return err
	}
	if raw, ok := fields["portability"]; ok {
		value, err := decodeStringField("portability", raw)
		if err != nil {
			return err
		}
		result.Portability = PortabilityMode(value)
		if !result.Portability.Valid() {
			return fmt.Errorf("portability %q is invalid", result.Portability)
		}
	}
	readOptionalField(&reader, "instructions", &result.Instructions, decodeInstructions)
	readOptionalField(&reader, "input", &result.Input, decodeItems)
	readOptionalField(&reader, "tools", &result.Tools, decodeTools)
	result.ToolPolicy = ToolPolicy{Mode: ToolChoiceAuto}
	readOptionalField(&reader, "tool_policy", &result.ToolPolicy, decodeToolPolicy)
	readNullableField(&reader, "output", &result.Output, decodeOutput)
	readNullableField(&reader, "sampling", &result.Sampling, decodeSampling)
	readNullableField(&reader, "reasoning", &result.Reasoning, decodeReasoning)
	readNullableField(&reader, "continuation", &result.Continuation, decodeContinuation)
	if reader.err != nil {
		return reader.err
	}
	if err := decodeNonNullBool(fields, "web_fetch", &result.WebFetch); err != nil {
		return err
	}
	if err := decodeNonNullBool(fields, "code_execution", &result.CodeExecution); err != nil {
		return err
	}
	if string(fields["web_search"]) == "null" {
		return fmt.Errorf("web_search must be boolean")
	}
	readOptionalField(&reader, "web_search", &result.WebSearch, decodeJSONValue[bool])
	readOptionalField(&reader, "extensions", &result.Extensions, decodeJSONValue[map[string]json.RawMessage])
	if reader.err != nil {
		return reader.err
	}
	result.Extensions = copyRawMap(result.Extensions)
	result.Instructions = emptyIfNil(result.Instructions)
	result.Input = emptyIfNil(result.Input)
	result.Tools = emptyIfNil(result.Tools)
	if result.Extensions == nil {
		result.Extensions = map[string]json.RawMessage{}
	}
	*request = result
	return nil
}

// decodeEngineHeader reads the api_version and operation_key the engine
// Request and Response start with. Unlike decodeContractHeader it reports a
// missing api_version as a missing field.
func decodeEngineHeader(fields map[string]json.RawMessage) (string, string, error) {
	apiVersion, err := requiredString(fields, "api_version")
	if err != nil {
		return "", "", err
	}
	if apiVersion != APIVersion {
		return "", "", fmt.Errorf("api_version %q is unsupported", apiVersion)
	}
	operationKey, err := requiredString(fields, "operation_key")
	if err != nil {
		return "", "", err
	}
	return apiVersion, operationKey, nil
}

// decodeNonNullBool decodes the optional boolean field name into target. A
// null value is reported as "<name> must be boolean".
func decodeNonNullBool(fields map[string]json.RawMessage, name string, target *bool) error {
	raw, ok := fields[name]
	if !ok {
		return nil
	}
	if string(raw) == "null" {
		return fmt.Errorf("%s must be boolean", name)
	}
	return decodeJSON(raw, target)
}

func decodeInstructions(data []byte) ([]Instruction, error) {
	return decodeArray(data, "instruction", decodeInstruction)
}

func decodeTools(data []byte) ([]Tool, error) {
	return decodeArray(data, "tool", decodeTool)
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

func decodeInt(data []byte) (int, error) { return decodeIntJSON[int](data) }

func decodeInt64(data []byte) (int64, error) { return decodeIntJSON[int64](data) }

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
	if reasoning.Effort != "" && reasoning.Effort != ReasoningEffortProviderDefault && reasoning.Effort != ReasoningEffortMinimal && reasoning.Effort != ReasoningEffortLow && reasoning.Effort != ReasoningEffortMedium && reasoning.Effort != ReasoningEffortHigh && reasoning.Effort != ReasoningEffortExtraHigh && reasoning.Effort != ReasoningEffortMaximum {
		return fmt.Errorf("reasoning effort %q is invalid", reasoning.Effort)
	}
	if reasoning.Summary != "" && reasoning.Summary != ReasoningSummaryProviderDefault && reasoning.Summary != ReasoningSummaryNone && reasoning.Summary != ReasoningSummaryAuto && reasoning.Summary != ReasoningSummaryConcise && reasoning.Summary != ReasoningSummaryDetailed {
		return fmt.Errorf("reasoning summary %q is invalid", reasoning.Summary)
	}
	return nil
}
