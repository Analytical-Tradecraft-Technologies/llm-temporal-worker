package llm

import (
	"encoding/json"
	"time"
	"unicode/utf8"
)

// NormalizeRequest fills deterministic v1 defaults and returns an independent
// value. The result is defined as a JSON round trip through the public
// contract (normalizeRequestRoundTrip): that validates the request, fills
// defaults, turns nil collections into empty ones and copies byte slices,
// maps, and typed union values so callers cannot mutate the normalized request
// through the original input.
//
// A round trip of a large transcript is expensive, and the planner and the
// cloud runtime normalize the same request several times per step (#1112).
// normalizeRequestCopy produces the identical value with a typed deep copy for
// every request whose round trip it can predict exactly; anything else,
// including every request the round trip rejects, still takes the round trip,
// which therefore also produces every error.
func NormalizeRequest(request Request) (Request, error) {
	if normalized, ok := normalizeRequestCopy(request); ok {
		return normalized, nil
	}
	return normalizeRequestRoundTrip(request)
}

// normalizeRequestRoundTrip is the reference definition of NormalizeRequest.
func normalizeRequestRoundTrip(request Request) (Request, error) {
	if request.APIVersion == "" {
		request.APIVersion = APIVersion
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return Request{}, err
	}
	var normalized Request
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return Request{}, err
	}
	return normalized, nil
}

// normalizeRequestCopy returns what normalizeRequestRoundTrip returns, with
// ok set, when it can establish that the round trip succeeds and what it
// yields. It reports false, and the caller falls back to the round trip, for
// anything it does not handle exactly: a value either MarshalJSON or the
// decoders would reject, a string encoding/json would rewrite (invalid UTF-8),
// raw JSON with invalid UTF-8 or deep nesting, or an item or part that is not
// one of the concrete value types the decoders produce.
//
// Each case below mirrors the MarshalJSON method and the decoder of the type
// it copies, including the defaults and nil/empty forms the decoders return
// and the compaction and escaping of raw JSON (marshaledRawJSON).
func normalizeRequestCopy(request Request) (Request, bool) {
	if request.APIVersion != "" && request.APIVersion != APIVersion {
		return Request{}, false
	}
	if request.OperationKey == "" || request.Model == "" || !stableStrings(request.OperationKey, request.Model) {
		return Request{}, false
	}
	serviceClass, err := NormalizeServiceClass(request.ServiceClass)
	if err != nil || ValidateServiceClassFallbacks(serviceClass, request.ServiceClassFallbacks) != nil {
		return Request{}, false
	}
	portability := request.Portability
	if portability == "" {
		portability = PortabilityStrict
	}
	if !portability.Valid() {
		return Request{}, false
	}
	result := Request{
		WebSearch:             request.WebSearch,
		WebFetch:              request.WebFetch,
		CodeExecution:         request.CodeExecution,
		APIVersion:            APIVersion,
		OperationKey:          request.OperationKey,
		Model:                 request.Model,
		ServiceClass:          serviceClass,
		ServiceClassFallbacks: append(make([]ServiceClass, 0, len(request.ServiceClassFallbacks)), request.ServiceClassFallbacks...),
		Portability:           portability,
	}
	var ok bool
	if result.Context, ok = copyRequestContext(request.Context); !ok {
		return Request{}, false
	}
	result.Instructions = make([]Instruction, 0, len(request.Instructions))
	for _, instruction := range request.Instructions {
		copied, ok := copyInstruction(instruction)
		if !ok {
			return Request{}, false
		}
		result.Instructions = append(result.Instructions, copied)
	}
	if result.Input, ok = copyItems(request.Input); !ok {
		return Request{}, false
	}
	result.Tools = make([]Tool, 0, len(request.Tools))
	for _, tool := range request.Tools {
		copied, ok := copyTool(tool)
		if !ok {
			return Request{}, false
		}
		result.Tools = append(result.Tools, copied)
	}
	if result.ToolPolicy, ok = copyToolPolicy(request.ToolPolicy); !ok {
		return Request{}, false
	}
	if request.Output != nil {
		if result.Output, ok = copyOutputSpec(*request.Output); !ok {
			return Request{}, false
		}
	}
	if request.Sampling != nil {
		if result.Sampling, ok = copySamplingSpec(*request.Sampling); !ok {
			return Request{}, false
		}
	}
	if request.Reasoning != nil {
		if result.Reasoning, ok = copyReasoningSpec(*request.Reasoning); !ok {
			return Request{}, false
		}
	}
	if request.Continuation != nil {
		if result.Continuation, ok = copyContinuation(*request.Continuation); !ok {
			return Request{}, false
		}
	}
	result.Extensions = make(map[string]json.RawMessage, len(request.Extensions))
	for key, value := range request.Extensions {
		encoded, ok := marshaledRawJSON(value)
		if !ok || !utf8.ValidString(key) {
			return Request{}, false
		}
		result.Extensions[key] = encoded
	}
	return result, true
}

// The context is omitted when empty and its tags when they have no entries,
// so either decodes back as absent.
func copyRequestContext(context RequestContext) (RequestContext, bool) {
	if context.empty() {
		return RequestContext{}, true
	}
	if !stableStrings(context.Tenant, context.Project, context.Actor) {
		return RequestContext{}, false
	}
	result := RequestContext{Tenant: context.Tenant, Project: context.Project, Actor: context.Actor}
	if len(context.Tags) > 0 {
		result.Tags = make(map[string]string, len(context.Tags))
		for key, value := range context.Tags {
			if !stableStrings(key, value) {
				return RequestContext{}, false
			}
			result.Tags[key] = value
		}
	}
	return result, true
}

// A text instruction keeps only its text (taken from a single text part when
// it has content); a parts instruction keeps only its content. The decoder
// fills the default level.
func copyInstruction(instruction Instruction) (Instruction, bool) {
	kind := instruction.Kind
	if kind == "" {
		if len(instruction.Content) == 0 {
			kind = InstructionKindText
		} else {
			kind = InstructionKindParts
		}
	}
	level := instruction.Level
	if level == "" {
		level = InstructionLevelApplication
	}
	if level != InstructionLevelApplication && level != InstructionLevelPolicy {
		return Instruction{}, false
	}
	switch kind {
	case InstructionKindText:
		text := instruction.Text
		if len(instruction.Content) > 0 {
			if len(instruction.Content) != 1 {
				return Instruction{}, false
			}
			part, ok := instruction.Content[0].(TextPart)
			if !ok || (text != "" && text != part.Text) {
				return Instruction{}, false
			}
			text = part.Text
		}
		if !utf8.ValidString(text) {
			return Instruction{}, false
		}
		return Instruction{Kind: kind, Level: level, Text: text}, true
	case InstructionKindParts:
		content, ok := copyParts(instruction.Content)
		if !ok {
			return Instruction{}, false
		}
		return Instruction{Kind: kind, Level: level, Content: content}, true
	default:
		return Instruction{}, false
	}
}

func copyItems(items []Item) ([]Item, bool) {
	result := make([]Item, 0, len(items))
	for _, item := range items {
		var copied Item
		switch value := item.(type) {
		case Message:
			if !value.Actor.Valid() {
				return nil, false
			}
			content, ok := copyParts(value.Content)
			if !ok {
				return nil, false
			}
			copied = Message{Actor: value.Actor, Content: content}
		case ToolCall:
			arguments, ok := marshaledRawJSON(value.Arguments)
			if !ok || value.ID == "" || validateToolName(value.Name) != nil || !utf8.ValidString(value.ID) {
				return nil, false
			}
			copied = ToolCall{ID: value.ID, Name: value.Name, Arguments: arguments}
		case ToolResult:
			if value.CallID == "" || !stableStrings(value.CallID, value.Name) {
				return nil, false
			}
			content, ok := copyParts(value.Content)
			if !ok {
				return nil, false
			}
			copied = ToolResult{CallID: value.CallID, Name: value.Name, Content: content, IsError: value.IsError}
		case ProviderState:
			if value.Provider == "" || value.EndpointFamily == "" || value.MediaType == "" || !stableStrings(value.Provider, value.EndpointFamily, value.MediaType) {
				return nil, false
			}
			copied = ProviderState{Provider: value.Provider, EndpointFamily: value.EndpointFamily, MediaType: value.MediaType, Opaque: copyOpaque(value.Opaque)}
		case Reference:
			if !utf8.ValidString(value.URI) || validateURI(value.URI) != nil {
				return nil, false
			}
			reference := Reference{URI: value.URI}
			if len(value.Metadata) > 0 {
				reference.Metadata = make(map[string]json.RawMessage, len(value.Metadata))
				for key, raw := range value.Metadata {
					encoded, ok := marshaledRawJSON(raw)
					if !ok || key == "" || !utf8.ValidString(key) {
						return nil, false
					}
					reference.Metadata[key] = encoded
				}
			}
			copied = reference
		default:
			return nil, false
		}
		result = append(result, copied)
	}
	return result, true
}

func copyParts(parts []Part) ([]Part, bool) {
	result := make([]Part, 0, len(parts))
	for _, part := range parts {
		var copied Part
		switch value := part.(type) {
		case TextPart:
			if !utf8.ValidString(value.Text) {
				return nil, false
			}
			copied = TextPart{Text: value.Text}
		case ImagePart:
			if validateMediaSource(value.URL, value.Bytes, value.Blob, value.MediaType, "image") != nil || !stableStrings(value.URL, value.MediaType, value.Detail) {
				return nil, false
			}
			blob, ok := copyBlobRef(value.Blob)
			if !ok {
				return nil, false
			}
			copied = ImagePart{URL: value.URL, Bytes: copyBytes(value.Bytes), Blob: blob, MediaType: value.MediaType, Detail: value.Detail}
		case DocumentPart:
			if validateMediaSource(value.URL, value.Bytes, value.Blob, value.MediaType, "document") != nil || !stableStrings(value.URL, value.MediaType, value.Title) {
				return nil, false
			}
			blob, ok := copyBlobRef(value.Blob)
			if !ok {
				return nil, false
			}
			copied = DocumentPart{URL: value.URL, Bytes: copyBytes(value.Bytes), Blob: blob, MediaType: value.MediaType, Title: value.Title}
		case JSONPart:
			encoded, ok := marshaledRawJSON(value.Value)
			if !ok {
				return nil, false
			}
			copied = JSONPart{Value: encoded}
		case RefusalPart:
			if !stableStrings(value.Text, value.ProviderCode) {
				return nil, false
			}
			copied = RefusalPart{Text: value.Text, ProviderCode: value.ProviderCode}
		case ProviderStatePart:
			if value.Provider == "" || value.EndpointFamily == "" || value.MediaType == "" || !stableStrings(value.Provider, value.EndpointFamily, value.MediaType) {
				return nil, false
			}
			copied = ProviderStatePart{Provider: value.Provider, EndpointFamily: value.EndpointFamily, MediaType: value.MediaType, Opaque: copyOpaque(value.Opaque)}
		default:
			return nil, false
		}
		result = append(result, copied)
	}
	return result, true
}

func copyBlobRef(blob *BlobRef) (*BlobRef, bool) {
	if blob == nil {
		return nil, true
	}
	if blob.Digest == "" || blob.ByteLength < 0 || blob.MediaType == "" || blob.Locator == "" || !stableStrings(blob.Digest, blob.MediaType, blob.Locator) {
		return nil, false
	}
	copied := *blob
	return &copied, true
}

// copyOpaque mirrors an opaque provider payload's round trip: it is always
// encoded (as "" when empty) and decodes to nil when empty.
func copyOpaque(value []byte) []byte {
	return copyBytes(value)
}

func copyTool(tool Tool) (Tool, bool) {
	kind := tool.Kind
	if kind == "" {
		kind = ToolKindFunction
	}
	if kind != ToolKindFunction && kind != ToolKindProvider && kind != ToolKindRemoteMCP {
		return Tool{}, false
	}
	inputSchema, ok := marshaledRawJSON(tool.InputSchema)
	if !ok || validateToolName(tool.Name) != nil || !utf8.ValidString(tool.Description) || !validObjectJSON(tool.InputSchema) {
		return Tool{}, false
	}
	result := Tool{Kind: kind, Name: tool.Name, Description: tool.Description, InputSchema: inputSchema}
	if len(tool.OutputSchema) > 0 {
		if result.OutputSchema, ok = marshaledRawJSON(tool.OutputSchema); !ok || !validObjectJSON(tool.OutputSchema) {
			return Tool{}, false
		}
	}
	return result, true
}

func copyToolPolicy(policy ToolPolicy) (ToolPolicy, bool) {
	mode := policy.Mode
	if mode == "" {
		mode = ToolChoiceAuto
	}
	switch mode {
	case ToolChoiceNamed:
		if validateToolName(policy.Name) != nil {
			return ToolPolicy{}, false
		}
	case ToolChoiceNone, ToolChoiceAuto, ToolChoiceRequired:
		if policy.Name != "" {
			return ToolPolicy{}, false
		}
	default:
		return ToolPolicy{}, false
	}
	return ToolPolicy{Mode: mode, Name: policy.Name, Parallel: policy.Parallel}, true
}

func copyOutputSpec(output OutputSpec) (*OutputSpec, bool) {
	result := &OutputSpec{}
	if output.MaxTokens != nil {
		if *output.MaxTokens < 0 {
			return nil, false
		}
		maxTokens := *output.MaxTokens
		result.MaxTokens = &maxTokens
	}
	format := output.Format
	kind := format.Kind
	if kind == "" {
		kind = OutputKindText
	}
	if kind != OutputKindText && kind != OutputKindJSON && kind != OutputKindJSONSchema {
		return nil, false
	}
	if validateOutputFormatName(format.Name) != nil || !utf8.ValidString(format.Description) {
		return nil, false
	}
	result.Format = OutputFormat{Kind: kind, Name: format.Name, Description: format.Description}
	if kind == OutputKindJSONSchema {
		schema, ok := marshaledRawJSON(format.Schema)
		if !ok || !validObjectJSON(format.Schema) {
			return nil, false
		}
		result.Format.Schema = schema
		result.Format.Strict = format.Strict
	} else if len(format.Schema) > 0 || format.Strict {
		return nil, false
	}
	return result, true
}

// Each pointer field is encoded only when set; stop sequences are encoded
// whenever non-nil and an empty list decodes as an empty, non-nil slice.
func copySamplingSpec(sampling SamplingSpec) (*SamplingSpec, bool) {
	for _, value := range []*float64{sampling.Temperature, sampling.TopP, sampling.PresencePenalty, sampling.FrequencyPenalty} {
		if value != nil && !finite(*value) {
			return nil, false
		}
	}
	result := &SamplingSpec{
		Temperature:      copyPointer(sampling.Temperature),
		TopP:             copyPointer(sampling.TopP),
		TopK:             copyPointer(sampling.TopK),
		Seed:             copyPointer(sampling.Seed),
		PresencePenalty:  copyPointer(sampling.PresencePenalty),
		FrequencyPenalty: copyPointer(sampling.FrequencyPenalty),
	}
	if sampling.StopSequences != nil {
		if !stableStrings(sampling.StopSequences...) {
			return nil, false
		}
		result.StopSequences = append(make([]string, 0, len(sampling.StopSequences)), sampling.StopSequences...)
	}
	return result, true
}

func copyReasoningSpec(reasoning ReasoningSpec) (*ReasoningSpec, bool) {
	if validateReasoning(reasoning) != nil || (reasoning.TokenBudget != nil && *reasoning.TokenBudget < 0) {
		return nil, false
	}
	return &ReasoningSpec{Mode: reasoning.Mode, Effort: reasoning.Effort, TokenBudget: copyPointer(reasoning.TokenBudget), Summary: reasoning.Summary}, true
}

// The expiry is encoded as UTC RFC 3339 with nanoseconds, which drops the
// location and monotonic reading; it is reproduced by that same conversion.
func copyContinuation(continuation Continuation) (*Continuation, bool) {
	if continuation.Handle == "" || !stableStrings(continuation.Handle, continuation.EndpointID, continuation.Model) {
		return nil, false
	}
	result := &Continuation{Handle: continuation.Handle, EndpointID: continuation.EndpointID, Model: continuation.Model, Pinned: continuation.Pinned}
	if continuation.ExpiresAt != nil {
		parsed, err := time.Parse(time.RFC3339Nano, continuation.ExpiresAt.UTC().Format(time.RFC3339Nano))
		if err != nil {
			return nil, false
		}
		result.ExpiresAt = &parsed
	}
	if continuation.ProviderStates != nil {
		result.ProviderStates = make([]ProviderState, 0, len(continuation.ProviderStates))
		for _, state := range continuation.ProviderStates {
			if state.Provider == "" || state.EndpointFamily == "" || state.MediaType == "" || !stableStrings(state.Provider, state.EndpointFamily, state.MediaType) {
				return nil, false
			}
			result.ProviderStates = append(result.ProviderStates, ProviderState{Provider: state.Provider, EndpointFamily: state.EndpointFamily, MediaType: state.MediaType, Opaque: copyOpaque(state.Opaque)})
		}
	}
	return result, true
}

func copyPointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func finite(value float64) bool {
	return value-value == 0
}

// stableStrings reports whether encoding/json encodes and decodes every value
// unchanged. It replaces invalid UTF-8 with U+FFFD.
func stableStrings(values ...string) bool {
	for _, value := range values {
		if !utf8.ValidString(value) {
			return false
		}
	}
	return true
}

// maxStableRawDepth keeps raw JSON well inside the encoding/json nesting limit
// once it is embedded in a request.
const maxStableRawDepth = 512

const lowerHex = "0123456789abcdef"

// marshaledRawJSON returns, as a new slice, the bytes a json.RawMessage
// decodes to after the round trip, when the decoders accept it (valid JSON
// without duplicate keys). json.Marshal emits a RawMessage through
// appendCompact with HTML escaping: it drops insignificant whitespace and
// replaces each of the characters <, >, &, U+2028 and U+2029 with its six-byte
// backslash-u escape. Those characters can only occur inside string literals
// of valid JSON. This reproduces exactly that. Invalid UTF-8 and deep nesting are left
// to the round trip.
func marshaledRawJSON(value json.RawMessage) (json.RawMessage, bool) {
	if !utf8.Valid(value) || !validRawJSON(value) {
		return nil, false
	}
	var out []byte
	start := 0
	emit := func(end int) {
		if out == nil {
			out = make([]byte, 0, len(value))
		}
		out = append(out, value[start:end]...)
	}
	inString, escaped, depth := false, false, 0
	for index := 0; index < len(value); index++ {
		char := value[index]
		switch {
		case char == '<' || char == '>' || char == '&':
			emit(index)
			out = append(out, '\\', 'u', '0', '0', lowerHex[char>>4], lowerHex[char&0xF])
			start = index + 1
			continue
		case char == 0xE2 && index+2 < len(value) && value[index+1] == 0x80 && value[index+2]&^1 == 0xA8:
			emit(index)
			out = append(out, '\\', 'u', '2', '0', '2', lowerHex[value[index+2]&0xF])
			start = index + 3
			index += 2
			continue
		}
		if inString {
			switch {
			case escaped:
				escaped = false
			case char == '\\':
				escaped = true
			case char == '"':
				inString = false
			}
			continue
		}
		switch char {
		case '"':
			inString = true
		case ' ', '\t', '\n', '\r':
			emit(index)
			start = index + 1
		case '{', '[':
			depth++
			if depth > maxStableRawDepth {
				return nil, false
			}
		case '}', ']':
			depth--
		}
	}
	if out == nil {
		return copyRaw(value), true
	}
	return append(out, value[start:]...), true
}
