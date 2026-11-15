package llm

// This file contains the closed wire records introduced by the forkable
// conversation contract. The existing Request/Response types remain the
// provider-neutral engine model; these records are the Temporal-facing v1
// boundary and deliberately do not expose transcript or provider payloads.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func requiredBool(fields map[string]json.RawMessage, name string) (bool, error) {
	raw, err := requireField(fields, name)
	if err != nil {
		return false, err
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, fmt.Errorf("%s must be boolean", name)
	}
	return value, nil
}

func decodeInt32(raw []byte) (int32, error) {
	value, err := decodeInt(raw)
	if err != nil {
		return 0, err
	}
	if value < -1<<31 || value > 1<<31-1 {
		return 0, fmt.Errorf("integer is outside int32 range")
	}
	return int32(value), nil
}

const (
	CompactAPIVersion   = "llm.temporal/compact/v1"
	QueryAPIVersion     = "llm.temporal/query/v1"
	CompactActivityName = "llm.compact.v1"
	QueryActivityName   = "llm.query.v1"
)

type CheckpointHandle string

// CheckpointHandleMaxCodePoints bounds every checkpoint handle, including the
// handles this worker issues.
const CheckpointHandleMaxCodePoints = checkpointHandleMaxCodePoints

func (handle CheckpointHandle) valid() bool {
	return validateCodePointLength("checkpoint handle", string(handle), 1, checkpointHandleMaxCodePoints) == nil
}

type CachePolicyV1 struct {
	// Zero means the field is omitted: eligible successes have no age limit.
	MaxAgeSeconds int64 `json:"max_age_seconds,omitempty"`
	Variant       int32 `json:"variant,omitempty"`
}

// DecimalV1 is the exact decimal spelling used by the v1 wire contract for
// temperature.  Keeping the value as its validated canonical string avoids
// routing a 38,18-compatible value through float64 and silently changing the
// cache fingerprint or provider request.  It is intentionally a distinct
// type: callers must not confuse a wire decimal with a provider sampling
// float.
type DecimalV1 string

// NewDecimalV1 validates and canonicalizes a non-negative decimal string.
// The accepted range is the v1 NUMERIC(38,18) subset: up to 20 integer digits
// and up to 18 fractional digits.
func NewDecimalV1(value string) (DecimalV1, error) {
	canonical, err := normalizeDecimalV1(value)
	if err != nil {
		return "", err
	}
	return DecimalV1(canonical), nil
}

func (value DecimalV1) String() string { return string(value) }

// Float64 converts a validated decimal for provider adapters that expose a
// floating-point temperature.  The v1 wire representation remains exact even
// when a downstream provider API cannot represent all 18 fractional digits.
func (value DecimalV1) Float64() (float64, error) {
	canonical, err := normalizeDecimalV1(string(value))
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseFloat(canonical, 64)
	if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
		return 0, fmt.Errorf("decimal cannot be represented as float64")
	}
	return parsed, nil
}

func (value DecimalV1) MarshalJSON() ([]byte, error) {
	canonical, err := normalizeDecimalV1(string(value))
	if err != nil {
		return nil, err
	}
	return json.Marshal(canonical)
}

func (value *DecimalV1) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("decimal must be a JSON string: %w", err)
	}
	canonical, err := normalizeDecimalV1(raw)
	if err != nil {
		return err
	}
	*value = DecimalV1(canonical)
	return nil
}

func normalizeDecimalV1(value string) (string, error) {
	if value == "" || strings.HasPrefix(value, "+") || strings.HasPrefix(value, "-") {
		return "", fmt.Errorf("decimal must be non-negative")
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 || parts[0] == "" || len(parts[0]) > 20 ||
		(len(parts[0]) > 1 && parts[0][0] == '0') {
		return "", fmt.Errorf("decimal must be within NUMERIC(38,18)")
	}
	for _, character := range parts[0] {
		if character < '0' || character > '9' {
			return "", fmt.Errorf("decimal must contain only decimal digits")
		}
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
		if len(fraction) == 0 || len(fraction) > 18 {
			return "", fmt.Errorf("decimal must have between 1 and 18 fractional digits")
		}
		for _, character := range fraction {
			if character < '0' || character > '9' {
				return "", fmt.Errorf("decimal must contain only decimal digits")
			}
		}
		fraction = strings.TrimRight(fraction, "0")
	}
	if fraction == "" {
		return parts[0], nil
	}
	return parts[0] + "." + fraction, nil
}

func (policy *CachePolicyV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return fmt.Errorf("cache policy must be an object")
	}
	if err := checkUnknownFields(fields, "max_age_seconds", "variant"); err != nil {
		return err
	}
	result := CachePolicyV1{}
	if raw, present := fields["max_age_seconds"]; present {
		result.MaxAgeSeconds, err = decodeInt64(raw)
		if err != nil || result.MaxAgeSeconds <= 0 {
			return fmt.Errorf("cache max_age_seconds must be a positive integer when specified")
		}
	}
	if err := decodePresentField(fields, "variant", &result.Variant, decodeInt32); err != nil {
		return err
	}
	if err := result.validate(); err != nil {
		return err
	}
	*policy = result
	return nil
}

func (policy CachePolicyV1) MarshalJSON() ([]byte, error) {
	if err := policy.validate(); err != nil {
		return nil, err
	}
	type wire CachePolicyV1
	return json.Marshal(wire(policy))
}

func (policy CachePolicyV1) validate() error {
	if policy.MaxAgeSeconds < 0 || policy.MaxAgeSeconds > 31536000 {
		return fmt.Errorf("cache max_age_seconds must be between 1 and 31536000")
	}
	if policy.Variant < 0 {
		return fmt.Errorf("cache variant is invalid")
	}
	return nil
}

// Patch preserves omitted, set, and clear as distinct wire states. A nil Set
// pointer means the leaf was omitted; Clear and Set are mutually exclusive.
type Patch[T any] struct {
	Set   *T
	Clear bool
}

func (patch Patch[T]) validate() error {
	if patch.Set != nil && patch.Clear {
		return fmt.Errorf("patch cannot contain both set and clear")
	}
	return nil
}

type SettingsPatchV1 struct {
	WebSearch             Patch[bool]
	WebFetch              Patch[bool]
	CodeExecution         Patch[bool]
	Model                 Patch[string]
	ServiceClass          Patch[ServiceClass]
	ServiceClassFallbacks Patch[[]ServiceClass]
	Portability           Patch[PortabilityMode]
	Instructions          Patch[[]Instruction]
	Tools                 Patch[[]Tool]
	ToolPolicy            Patch[ToolPolicy]
	Output                Patch[OutputSpec]
	Temperature           Patch[DecimalV1]
	// TopP, StopSequences and Seed are optional sampling leaves. TopP uses the
	// same exact decimal-string spelling as Temperature.
	TopP          Patch[DecimalV1]
	StopSequences Patch[[]string]
	Seed          Patch[int64]
	// ReasoningMode and ReasoningTokenBudget select an explicit thinking mode
	// and budget; adapters reject what their provider cannot honour.
	ReasoningMode        Patch[ReasoningMode]
	ReasoningTokenBudget Patch[int]
	ReasoningEffort      Patch[ReasoningEffort]
	ReasoningSummary     Patch[ReasoningSummary]
	CompactionPolicy     Patch[json.RawMessage]
	Extensions           Patch[map[string]json.RawMessage]
}

// Bounds for the v1 sampling and reasoning leaves. They are deliberately
// provider-neutral: an adapter with a tighter limit (for example Chat
// Completions' four stop sequences) still rejects the request when lowering.
const (
	MaxStopSequencesV1        = 16
	MaxStopSequenceLengthV1   = 256
	MaxSeedV1                 = 1<<53 - 1
	MaxReasoningTokenBudgetV1 = math.MaxInt32
)

// ValidateTopPV1 accepts a canonical decimal in the half-open range (0, 1].
func ValidateTopPV1(value DecimalV1) error {
	canonical, err := NewDecimalV1(value.String())
	if err != nil {
		return fmt.Errorf("top_p: %w", err)
	}
	if canonical == "0" || (canonical != "1" && !strings.HasPrefix(string(canonical), "0.")) {
		return fmt.Errorf("top_p must be greater than 0 and at most 1")
	}
	return nil
}

// ValidateStopSequencesV1 requires between one and MaxStopSequencesV1 distinct,
// non-empty sequences of at most MaxStopSequenceLengthV1 characters. Clearing
// the leaf, not an empty set, removes inherited stop sequences.
func ValidateStopSequencesV1(values []string) error {
	if len(values) == 0 || len(values) > MaxStopSequencesV1 {
		return fmt.Errorf("stop_sequences must contain between 1 and %d values", MaxStopSequencesV1)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" || !utf8.ValidString(value) || utf8.RuneCountInString(value) > MaxStopSequenceLengthV1 {
			return fmt.Errorf("stop_sequences values must be non-empty and at most %d characters", MaxStopSequenceLengthV1)
		}
		if _, exists := seen[value]; exists {
			return fmt.Errorf("stop_sequences must not contain duplicates")
		}
		seen[value] = struct{}{}
	}
	return nil
}

// ValidateSeedV1 keeps the seed within the integers every JSON client can
// represent exactly.
func ValidateSeedV1(value int64) error {
	if value < 0 || value > MaxSeedV1 {
		return fmt.Errorf("seed must be between 0 and %d", int64(MaxSeedV1))
	}
	return nil
}

func ValidateReasoningTokenBudgetV1(value int) error {
	if value < 1 || value > MaxReasoningTokenBudgetV1 {
		return fmt.Errorf("reasoning_token_budget must be between 1 and %d", MaxReasoningTokenBudgetV1)
	}
	return nil
}

// ValidReasoningModeV1 reports whether value is a closed v1 reasoning mode.
func ValidReasoningModeV1(value ReasoningMode) bool {
	switch value {
	case ReasoningModeProviderDefault, ReasoningModeDisabled, ReasoningModeAdaptive, ReasoningModeEnabled:
		return true
	default:
		return false
	}
}

func (patch SettingsPatchV1) MarshalJSON() ([]byte, error) {
	if patch.ServiceClass.Set != nil && !patch.ServiceClass.Set.Valid() {
		return nil, fmt.Errorf("service_class: invalid value %q", *patch.ServiceClass.Set)
	}
	if patch.ServiceClassFallbacks.Set != nil {
		seen := make(map[ServiceClass]struct{}, len(*patch.ServiceClassFallbacks.Set))
		for _, class := range *patch.ServiceClassFallbacks.Set {
			if !class.Valid() {
				return nil, fmt.Errorf("service_class_fallbacks: invalid value %q", class)
			}
			if _, exists := seen[class]; exists {
				return nil, fmt.Errorf("service_class_fallbacks: duplicate value %q", class)
			}
			seen[class] = struct{}{}
		}
	}
	if patch.Portability.Set != nil && !patch.Portability.Set.Valid() {
		return nil, fmt.Errorf("portability: invalid value %q", *patch.Portability.Set)
	}
	if patch.ReasoningEffort.Set != nil && !validReasoningEffort(*patch.ReasoningEffort.Set) {
		return nil, fmt.Errorf("reasoning_effort: invalid value %q", *patch.ReasoningEffort.Set)
	}
	if patch.ReasoningSummary.Set != nil && !validReasoningSummary(*patch.ReasoningSummary.Set) {
		return nil, fmt.Errorf("reasoning_summary: invalid value %q", *patch.ReasoningSummary.Set)
	}
	if patch.ReasoningMode.Set != nil && !ValidReasoningModeV1(*patch.ReasoningMode.Set) {
		return nil, fmt.Errorf("reasoning_mode: invalid value %q", *patch.ReasoningMode.Set)
	}
	if patch.ReasoningTokenBudget.Set != nil {
		if err := ValidateReasoningTokenBudgetV1(*patch.ReasoningTokenBudget.Set); err != nil {
			return nil, err
		}
	}
	if patch.TopP.Set != nil {
		if err := ValidateTopPV1(*patch.TopP.Set); err != nil {
			return nil, err
		}
	}
	if patch.StopSequences.Set != nil {
		if err := ValidateStopSequencesV1(*patch.StopSequences.Set); err != nil {
			return nil, err
		}
	}
	if patch.Seed.Set != nil {
		if err := ValidateSeedV1(*patch.Seed.Set); err != nil {
			return nil, err
		}
	}
	if patch.CompactionPolicy.Set != nil {
		if err := validateCompactionPolicyPatchV1(*patch.CompactionPolicy.Set); err != nil {
			return nil, err
		}
	}
	if patch.Extensions.Set != nil {
		if *patch.Extensions.Set == nil {
			return nil, fmt.Errorf("extensions.set must be an object")
		}
	}
	writer := patchWriter{fields: map[string]any{}}
	writePatch(&writer, "model", patch.Model)
	writePatch(&writer, "service_class", patch.ServiceClass)
	writePatch(&writer, "service_class_fallbacks", emptyNilSet(patch.ServiceClassFallbacks))
	writePatch(&writer, "portability", patch.Portability)
	writePatch(&writer, "instructions", emptyNilSet(patch.Instructions))
	writePatch(&writer, "tools", emptyNilSet(patch.Tools))
	writePatch(&writer, "tool_policy", patch.ToolPolicy)
	writePatch(&writer, "output", patch.Output)
	temperature := patch.Temperature
	if writer.err == nil && temperature.Set != nil && !temperature.Clear {
		value, err := NewDecimalV1(temperature.Set.String())
		if err != nil {
			return nil, fmt.Errorf("temperature: %w", err)
		}
		temperature.Set = &value
	}
	writePatch(&writer, "temperature", temperature)
	writePatch(&writer, "top_p", patch.TopP)
	writePatch(&writer, "stop_sequences", patch.StopSequences)
	writePatch(&writer, "seed", patch.Seed)
	writePatch(&writer, "reasoning_mode", patch.ReasoningMode)
	writePatch(&writer, "reasoning_token_budget", patch.ReasoningTokenBudget)
	writePatch(&writer, "reasoning_effort", patch.ReasoningEffort)
	writePatch(&writer, "reasoning_summary", patch.ReasoningSummary)
	writePatch(&writer, "web_fetch", patch.WebFetch)
	writePatch(&writer, "code_execution", patch.CodeExecution)
	writePatch(&writer, "web_search", patch.WebSearch)
	writePatch(&writer, "compaction_policy", patch.CompactionPolicy)
	writePatch(&writer, "extensions", patch.Extensions)
	if writer.err != nil {
		return nil, writer.err
	}
	return marshalObject(writer.fields)
}

// emptyNilSet makes a Set of a nil slice encode as [] rather than null,
// which decodePatch rejects, so the codec never writes a patch it cannot
// read (#1663).
func emptyNilSet[T any](patch Patch[[]T]) Patch[[]T] {
	if patch.Set != nil && *patch.Set == nil {
		empty := []T{}
		patch.Set = &empty
	}
	return patch
}

// patchWriter encodes settings patch leaves into fields and keeps the first
// validation error, so a run of leaves needs a single error check.
type patchWriter struct {
	fields map[string]any
	err    error
}

// writePatch encodes one leaf as {"clear":true} or {"set":value}; an omitted
// leaf writes nothing.
func writePatch[T any](writer *patchWriter, name string, patch Patch[T]) {
	if writer.err != nil {
		return
	}
	if err := patch.validate(); err != nil {
		writer.err = fmt.Errorf("%s: %w", name, err)
		return
	}
	if patch.Clear {
		writer.fields[name] = map[string]any{"clear": true}
	} else if patch.Set != nil {
		writer.fields[name] = map[string]any{"set": *patch.Set}
	}
}

func decodePatch[T any](raw json.RawMessage, name string) (Patch[T], error) {
	set, clear, err := decodePatchEnvelope(raw, name)
	if err != nil {
		return Patch[T]{}, err
	}
	if clear {
		return Patch[T]{Clear: true}, nil
	}
	value, err := decodePatchValue[T](set, name)
	if err != nil {
		return Patch[T]{}, err
	}
	return Patch[T]{Set: &value}, nil
}

// decodePatchEnvelope splits a patch leaf into its set value or its clear
// flag, which must be true.
func decodePatchEnvelope(raw json.RawMessage, name string) (json.RawMessage, bool, error) {
	fields, err := decodeClosedObject(raw, "set", "clear")
	if err != nil {
		return nil, false, fmt.Errorf("%s: %w", name, err)
	}
	set, hasSet := fields["set"]
	clearRaw, hasClear := fields["clear"]
	if hasSet == hasClear {
		return nil, false, fmt.Errorf("%s must contain exactly one of set or clear", name)
	}
	if hasClear {
		var clear bool
		if err := json.Unmarshal(clearRaw, &clear); err != nil || !clear {
			return nil, false, fmt.Errorf("%s.clear must be true", name)
		}
		return nil, true, nil
	}
	return set, false, nil
}

// readPatch decodes the optional patch leaf name into target.
func readPatch[T any](reader *fieldReader, name string, target *Patch[T]) {
	readPresentField(reader, name, target, func(raw []byte) (Patch[T], error) {
		return decodePatch[T](raw, name)
	})
}

// decodePatchValue routes polymorphic settings values through the same strict
// wire decoders used by the provider-neutral request model. A plain
// json.Unmarshal would silently ignore the snake_case fields and cannot
// reconstruct the Part interface used by parts instructions.
func decodePatchValue[T any](raw json.RawMessage, name string) (T, error) {
	var zero T
	switch any(zero).(type) {
	case bool:
		var value bool
		if string(raw) == "null" || json.Unmarshal(raw, &value) != nil {
			return zero, fmt.Errorf("%s.set: value must be a boolean", name)
		}
		return any(value).(T), nil
	case []Instruction:
		value, err := decodeInstructions(raw)
		if err != nil {
			return zero, fmt.Errorf("%s.set: %w", name, err)
		}
		return any(value).(T), nil
	case []Tool:
		value, err := decodeTools(raw)
		if err != nil {
			return zero, fmt.Errorf("%s.set: %w", name, err)
		}
		return any(value).(T), nil
	case ToolPolicy:
		value, err := decodeToolPolicyPatch(raw)
		if err != nil {
			return zero, fmt.Errorf("%s.set: %w", name, err)
		}
		return any(value).(T), nil
	case OutputSpec:
		value, err := decodeOutput(raw)
		if err != nil {
			return zero, fmt.Errorf("%s.set: %w", name, err)
		}
		return any(*value).(T), nil
	case ServiceClass:
		var value ServiceClass
		if err := json.Unmarshal(raw, &value); err != nil || !value.Valid() {
			return zero, fmt.Errorf("%s.set: service class is invalid", name)
		}
		return any(value).(T), nil
	case []ServiceClass:
		var value []ServiceClass
		if err := json.Unmarshal(raw, &value); err != nil || value == nil {
			return zero, fmt.Errorf("%s.set: service class fallbacks must be an array", name)
		}
		seen := make(map[ServiceClass]struct{}, len(value))
		for _, class := range value {
			if !class.Valid() {
				return zero, fmt.Errorf("%s.set: service class fallback is invalid", name)
			}
			if _, exists := seen[class]; exists {
				return zero, fmt.Errorf("%s.set: duplicate service class fallback", name)
			}
			seen[class] = struct{}{}
		}
		return any(value).(T), nil
	case PortabilityMode:
		var value PortabilityMode
		if err := json.Unmarshal(raw, &value); err != nil || !value.Valid() {
			return zero, fmt.Errorf("%s.set: portability mode is invalid", name)
		}
		return any(value).(T), nil
	case ReasoningEffort:
		var value ReasoningEffort
		if err := json.Unmarshal(raw, &value); err != nil || !validReasoningEffort(value) {
			return zero, fmt.Errorf("%s.set: reasoning effort is invalid", name)
		}
		return any(value).(T), nil
	case ReasoningSummary:
		var value ReasoningSummary
		if err := json.Unmarshal(raw, &value); err != nil || !validReasoningSummary(value) {
			return zero, fmt.Errorf("%s.set: reasoning summary is invalid", name)
		}
		return any(value).(T), nil
	case ReasoningMode:
		var value ReasoningMode
		if err := json.Unmarshal(raw, &value); err != nil || !ValidReasoningModeV1(value) {
			return zero, fmt.Errorf("%s.set: reasoning mode is invalid", name)
		}
		return any(value).(T), nil
	case DecimalV1:
		// Only top_p uses this path. Unlike temperature it has no legacy
		// numeric spelling, so only the canonical decimal string is accepted.
		var value DecimalV1
		if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &value) != nil {
			return zero, fmt.Errorf("%s.set must be a decimal string", name)
		}
		if err := ValidateTopPV1(value); err != nil {
			return zero, fmt.Errorf("%s.set: %w", name, err)
		}
		return any(value).(T), nil
	case []string:
		var value []string
		if err := decodeJSON(raw, &value); err != nil || value == nil {
			return zero, fmt.Errorf("%s.set: value must be an array of strings", name)
		}
		if err := ValidateStopSequencesV1(value); err != nil {
			return zero, fmt.Errorf("%s.set: %w", name, err)
		}
		return any(value).(T), nil
	case int64:
		value, err := decodeInt64(raw)
		if err != nil {
			return zero, fmt.Errorf("%s.set: value must be an integer", name)
		}
		if err := ValidateSeedV1(value); err != nil {
			return zero, fmt.Errorf("%s.set: %w", name, err)
		}
		return any(value).(T), nil
	case int:
		value, err := decodeInt(raw)
		if err != nil {
			return zero, fmt.Errorf("%s.set: value must be an integer", name)
		}
		if err := ValidateReasoningTokenBudgetV1(value); err != nil {
			return zero, fmt.Errorf("%s.set: %w", name, err)
		}
		return any(value).(T), nil
	case json.RawMessage:
		if _, err := decodeObject(raw); err != nil {
			return zero, fmt.Errorf("%s.set: value must be an object", name)
		}
		return any(copyRaw(raw)).(T), nil
	case map[string]json.RawMessage:
		fields, err := decodeObject(raw)
		if err != nil {
			return zero, fmt.Errorf("%s.set: value must be an object", name)
		}
		return any(fields).(T), nil
	default:
		value := zero
		if err := json.Unmarshal(raw, &value); err != nil {
			return zero, fmt.Errorf("%s.set: %w", name, err)
		}
		return value, nil
	}
}

func validReasoningEffort(value ReasoningEffort) bool {
	switch value {
	case ReasoningEffortProviderDefault, ReasoningEffortMinimal, ReasoningEffortLow, ReasoningEffortMedium, ReasoningEffortHigh, ReasoningEffortExtraHigh, ReasoningEffortMaximum:
		return true
	default:
		return false
	}
}

func validReasoningSummary(value ReasoningSummary) bool {
	switch value {
	case ReasoningSummaryProviderDefault, ReasoningSummaryNone, ReasoningSummaryAuto, ReasoningSummaryConcise, ReasoningSummaryDetailed:
		return true
	default:
		return false
	}
}

func (patch *SettingsPatchV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "model", "service_class", "service_class_fallbacks", "portability", "instructions", "tools", "tool_policy", "output", "temperature", "top_p", "stop_sequences", "seed", "reasoning_mode", "reasoning_token_budget", "reasoning_effort", "reasoning_summary", "compaction_policy", "extensions", "web_search", "web_fetch", "code_execution")
	if err != nil {
		return err
	}
	result := SettingsPatchV1{}
	reader := fieldReader{fields: fields}
	readPatch(&reader, "model", &result.Model)
	readPatch(&reader, "service_class", &result.ServiceClass)
	readPatch(&reader, "service_class_fallbacks", &result.ServiceClassFallbacks)
	readPatch(&reader, "portability", &result.Portability)
	readPatch(&reader, "instructions", &result.Instructions)
	readPatch(&reader, "tools", &result.Tools)
	readPatch(&reader, "tool_policy", &result.ToolPolicy)
	readPatch(&reader, "output", &result.Output)
	readPresentField(&reader, "temperature", &result.Temperature, decodeTemperaturePatch)
	readPatch(&reader, "top_p", &result.TopP)
	readPatch(&reader, "stop_sequences", &result.StopSequences)
	readPatch(&reader, "seed", &result.Seed)
	readPatch(&reader, "reasoning_mode", &result.ReasoningMode)
	readPatch(&reader, "reasoning_token_budget", &result.ReasoningTokenBudget)
	readPatch(&reader, "reasoning_effort", &result.ReasoningEffort)
	readPatch(&reader, "reasoning_summary", &result.ReasoningSummary)
	readPatch(&reader, "web_fetch", &result.WebFetch)
	readPatch(&reader, "code_execution", &result.CodeExecution)
	readPatch(&reader, "web_search", &result.WebSearch)
	readPatch(&reader, "compaction_policy", &result.CompactionPolicy)
	if reader.err == nil && result.CompactionPolicy.Set != nil {
		reader.err = validateCompactionPolicyPatchV1(*result.CompactionPolicy.Set)
	}
	readPatch(&reader, "extensions", &result.Extensions)
	if reader.err != nil {
		return reader.err
	}
	*patch = result
	return nil
}

// decodeTemperaturePatch accepts the canonical v1 decimal-string spelling.
// Numeric input remains accepted for one compatibility window so older Go
// producers can be upgraded independently; MarshalJSON always emits the
// canonical string form.
func decodeTemperaturePatch(raw []byte) (Patch[DecimalV1], error) {
	set, clear, err := decodePatchEnvelope(raw, "temperature")
	if err != nil {
		return Patch[DecimalV1]{}, err
	}
	if clear {
		return Patch[DecimalV1]{Clear: true}, nil
	}
	var value string
	if len(set) > 0 && set[0] == '"' {
		if err := json.Unmarshal(set, &value); err != nil {
			return Patch[DecimalV1]{}, fmt.Errorf("temperature.set must be a decimal string")
		}
	} else {
		// Compatibility with the pre-Task-17 numeric wire representation. Decode
		// into json.Number so the compatibility path does not lose digits before
		// canonicalization.
		var numeric json.Number
		decoder := json.NewDecoder(bytes.NewReader(set))
		decoder.UseNumber()
		if err := decoder.Decode(&numeric); err != nil {
			return Patch[DecimalV1]{}, fmt.Errorf("temperature.set must be a decimal string or number")
		}
		value = numeric.String()
	}
	canonical, err := NewDecimalV1(value)
	if err != nil {
		return Patch[DecimalV1]{}, fmt.Errorf("temperature.set: %w", err)
	}
	return Patch[DecimalV1]{Set: &canonical}, nil
}

type GenerateRequestV1 struct {
	APIVersion    string
	OperationKey  string
	Context       RequestContext
	Parent        *CheckpointHandle
	Append        []Item
	SettingsPatch SettingsPatchV1
	Cache         *CachePolicyV1
}

func validateRequestContextV1(context RequestContext) error {
	if len(context.Tags) > 0 {
		return fmt.Errorf("v1 context tags are not supported")
	}
	if context.Tenant == "" || context.Project == "" || context.Actor == "" {
		return fmt.Errorf("v1 context requires tenant, project, and actor")
	}
	return nil
}

func marshalRequestContextV1(context RequestContext) (json.RawMessage, error) {
	if err := validateRequestContextV1(context); err != nil {
		return nil, err
	}
	data, err := marshalObject(map[string]any{
		"tenant":  context.Tenant,
		"project": context.Project,
		"actor":   context.Actor,
	})
	if err != nil {
		return nil, fmt.Errorf("v1 context: %w", err)
	}
	return json.RawMessage(data), nil
}

func decodeRequestContextV1(data []byte) (RequestContext, error) {
	fields, err := decodeClosedObject(data, "tenant", "project", "actor")
	if err != nil {
		return RequestContext{}, fmt.Errorf("v1 context: %w", err)
	}
	context, err := decodeContextIdentity(fields)
	if err != nil {
		return RequestContext{}, fmt.Errorf("v1 context: %w", err)
	}
	if err := validateRequestContextV1(context); err != nil {
		return RequestContext{}, err
	}
	return context, nil
}

func (request GenerateRequestV1) MarshalJSON() ([]byte, error) {
	if request.APIVersion != "" && request.APIVersion != APIVersion {
		return nil, fmt.Errorf("api_version %q is unsupported", request.APIVersion)
	}
	if request.OperationKey == "" || request.Context.Tenant == "" || request.Context.Project == "" || request.Context.Actor == "" {
		return nil, fmt.Errorf("operation_key and complete context are required")
	}
	if err := validateGenerateRequestV1Bounds(request); err != nil {
		return nil, err
	}
	if err := validateGenerateRequestV1MediaURLs(request); err != nil {
		return nil, err
	}
	if err := validateGenerateRequestV1Content(request); err != nil {
		return nil, err
	}
	if request.Parent != nil && !request.Parent.valid() {
		return nil, fmt.Errorf("parent checkpoint is invalid")
	}
	if request.Cache != nil {
		if err := request.Cache.validate(); err != nil {
			return nil, err
		}
	}
	context, err := marshalRequestContextV1(request.Context)
	if err != nil {
		return nil, err
	}
	fields := map[string]any{"api_version": APIVersion, "operation_key": request.OperationKey, "context": context, "append": emptyIfNil(request.Append)}
	if request.Parent != nil {
		fields["parent"] = string(*request.Parent)
	}
	if patch, err := request.SettingsPatch.MarshalJSON(); err != nil {
		return nil, err
	} else if string(patch) != "{}" {
		fields["settings_patch"] = json.RawMessage(patch)
	}
	setIfNotNil(fields, "cache", request.Cache)
	return marshalObject(fields)
}

func (request *GenerateRequestV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "api_version", "operation_key", "context", "parent", "append", "settings_patch", "cache")
	if err != nil {
		return err
	}
	version, key, err := decodeContractHeader(fields, APIVersion)
	if err != nil {
		return err
	}
	context, err := decodeContractContext(fields)
	if err != nil {
		return err
	}
	result := GenerateRequestV1{APIVersion: version, OperationKey: key, Context: context}
	reader := fieldReader{fields: fields}
	readRequiredField(&reader, "append", &result.Append, decodeItems)
	if reader.err != nil {
		return reader.err
	}
	if result.Parent, err = decodeParentHandle(fields, "parent checkpoint is invalid"); err != nil {
		return err
	}
	readPresentField(&reader, "settings_patch", &result.SettingsPatch, unmarshalValue[SettingsPatchV1])
	readPresentField(&reader, "cache", &result.Cache, decodeCachePolicy)
	if reader.err != nil {
		return reader.err
	}
	if err := validateGenerateRequestV1Bounds(result); err != nil {
		return err
	}
	if err := validateGenerateRequestV1MediaURLs(result); err != nil {
		return err
	}
	if err := validateGenerateRequestV1Content(result); err != nil {
		return err
	}
	*request = result
	return nil
}

type CheckpointMetadata struct {
	Handle CheckpointHandle  `json:"handle"`
	Parent *CheckpointHandle `json:"parent,omitempty"`
	Kind   string            `json:"kind"`
	Depth  int32             `json:"depth"`
}
type CacheDispositionV1 struct {
	Disposition     string `json:"disposition"`
	Variant         int32  `json:"variant"`
	EntryAgeSeconds *int64 `json:"entry_age_seconds,omitempty"`
}

func (disposition CacheDispositionV1) MarshalJSON() ([]byte, error) {
	if !validateCacheDisposition(disposition.Disposition) || disposition.Variant < 0 {
		return nil, fmt.Errorf("invalid cache disposition")
	}
	if disposition.EntryAgeSeconds != nil && *disposition.EntryAgeSeconds < 0 {
		return nil, fmt.Errorf("entry_age_seconds must not be negative")
	}
	fields := map[string]any{"disposition": disposition.Disposition, "variant": disposition.Variant}
	if disposition.EntryAgeSeconds != nil {
		fields["entry_age_seconds"] = *disposition.EntryAgeSeconds
	}
	return marshalObject(fields)
}

type CostV1 struct {
	Status         string  `json:"status"`
	ActualCostUSD  *string `json:"actual_cost_usd"`
	Method         string  `json:"method,omitempty"`
	CatalogVersion string  `json:"catalog_version,omitempty"`
	UnknownReason  string  `json:"unknown_reason,omitempty"`
}

func (cost CostV1) MarshalJSON() ([]byte, error) {
	actualCost, err := cost.canonicalActualCost()
	if err != nil {
		return nil, err
	}
	fields := map[string]any{"status": cost.Status, "actual_cost_usd": actualCost}
	setIfNotEmpty(fields, "method", cost.Method)
	setIfNotEmpty(fields, "catalog_version", cost.CatalogVersion)
	setIfNotEmpty(fields, "unknown_reason", cost.UnknownReason)
	return marshalObject(fields)
}

// canonicalActualCost validates the cost and returns its actual cost in
// canonical decimal form.
func (cost CostV1) canonicalActualCost() (*string, error) {
	if err := cost.validate(); err != nil {
		return nil, err
	}
	if cost.ActualCostUSD == nil {
		return nil, nil
	}
	canonical, err := canonicalDecimalString(*cost.ActualCostUSD)
	if err != nil {
		return nil, err
	}
	return &canonical, nil
}

func (metadata *CheckpointMetadata) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "handle", "parent", "kind", "depth")
	if err != nil {
		return err
	}
	reader := fieldReader{fields: fields}
	handle := reader.requiredString("handle")
	kind := reader.requiredString("kind")
	if reader.err != nil {
		return reader.err
	}
	switch kind {
	case "generation", "compaction", "cache_replay":
	default:
		return fmt.Errorf("checkpoint kind %q is invalid", kind)
	}
	depth, err := decodeInt(fields["depth"])
	if err != nil {
		return err
	}
	if depth < 0 || depth > math.MaxInt32 {
		return fmt.Errorf("checkpoint metadata is invalid")
	}
	result := CheckpointMetadata{Handle: CheckpointHandle(handle), Kind: kind, Depth: int32(depth)}
	if !result.Handle.valid() {
		return fmt.Errorf("checkpoint metadata is invalid")
	}
	if result.Parent, err = decodeParentHandle(fields, "checkpoint parent is invalid"); err != nil {
		return err
	}
	*metadata = result
	return nil
}

func (disposition *CacheDispositionV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "disposition", "variant", "entry_age_seconds")
	if err != nil {
		return err
	}
	value, err := requiredString(fields, "disposition")
	if err != nil {
		return err
	}
	if !validateCacheDisposition(value) {
		return fmt.Errorf("cache disposition %q is invalid", value)
	}
	variant, err := decodeInt32(fields["variant"])
	if err != nil {
		return err
	}
	if variant < 0 {
		return fmt.Errorf("cache variant must not be negative")
	}
	result := CacheDispositionV1{Disposition: value, Variant: variant}
	if raw, ok := fields["entry_age_seconds"]; ok {
		age, err := decodeInt64(raw)
		if err != nil || age < 0 {
			return fmt.Errorf("entry_age_seconds must not be negative")
		}
		result.EntryAgeSeconds = &age
	}
	*disposition = result
	return nil
}

func (cost *CostV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "status", "actual_cost_usd", "method", "catalog_version", "unknown_reason")
	if err != nil {
		return err
	}
	reader := fieldReader{fields: fields}
	result := CostV1{Status: reader.requiredString("status")}
	readPresentField(&reader, "actual_cost_usd", &result.ActualCostUSD, decodeActualCostUSD)
	result.Method = reader.optionalString("method")
	result.CatalogVersion = reader.optionalString("catalog_version")
	result.UnknownReason = reader.optionalString("unknown_reason")
	if reader.err != nil {
		return reader.err
	}
	if err := result.validate(); err != nil {
		return err
	}
	*cost = result
	return nil
}

func validateCacheDisposition(value string) bool {
	switch value {
	case "disabled", "miss_populated", "hit", "miss_not_populated":
		return true
	default:
		return false
	}
}

var decimalPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{1,18})?$`)

// decodeActualCostUSD decodes a nullable actual_cost_usd decimal string in
// its canonical form; null decodes as nil.
func decodeActualCostUSD(data []byte) (*string, error) {
	if string(data) == "null" {
		return nil, nil
	}
	value, err := decodeStringField("actual_cost_usd", data)
	if err != nil {
		return nil, err
	}
	canonical, err := canonicalDecimalString(value)
	if err != nil {
		return nil, err
	}
	return &canonical, nil
}

func canonicalDecimalString(value string) (string, error) {
	if !decimalPattern.MatchString(value) {
		return "", fmt.Errorf("decimal %q is invalid", value)
	}
	parts := strings.SplitN(value, ".", 2)
	if len(parts) == 1 {
		return value, nil
	}
	fraction := strings.TrimRight(parts[1], "0")
	if fraction == "" {
		return parts[0], nil
	}
	return parts[0] + "." + fraction, nil
}

func (cost CostV1) validate() error {
	switch cost.Status {
	case "exact":
		if cost.ActualCostUSD == nil || !decimalPattern.MatchString(*cost.ActualCostUSD) || cost.Method == "" || cost.UnknownReason != "" {
			return fmt.Errorf("exact cost requires decimal actual_cost_usd and method")
		}
		switch cost.Method {
		case "provider_reported", "catalog_usage", "control_query_zero":
		default:
			return fmt.Errorf("exact cost method %q is invalid", cost.Method)
		}
	case "unknown":
		if cost.ActualCostUSD != nil || cost.Method != "" || cost.CatalogVersion != "" || cost.UnknownReason == "" {
			return fmt.Errorf("unknown cost requires null actual_cost_usd, reason, and no method")
		}
		switch cost.UnknownReason {
		case "provider_did_not_report_cost", "catalog_incomplete", "state_unavailable", "ambiguous_dispatch":
		default:
			return fmt.Errorf("unknown cost reason %q is invalid", cost.UnknownReason)
		}
	default:
		return fmt.Errorf("cost status %q is invalid", cost.Status)
	}
	return nil
}

type GenerateResponseV1 struct {
	APIVersion   string
	OperationKey string
	OperationID  string
	Status       ResponseStatus
	Output       []Item
	Checkpoint   CheckpointMetadata
	Cache        CacheDispositionV1
	Route        *RouteFacts
	Service      *ServiceFacts
	Usage        *Usage
	Cost         CostV1
	Diagnostics  []Diagnostic
}

func (response GenerateResponseV1) MarshalJSON() ([]byte, error) {
	if response.OperationKey == "" || response.OperationID == "" || !response.Status.Valid() {
		return nil, fmt.Errorf("response identity and status are required")
	}
	if !response.Checkpoint.Handle.valid() || response.Checkpoint.Depth < 0 || (response.Checkpoint.Kind != "generation" && response.Checkpoint.Kind != "cache_replay") {
		return nil, fmt.Errorf("invalid generation checkpoint")
	}
	if response.Cache.Variant < 0 || !validateCacheDisposition(response.Cache.Disposition) {
		return nil, fmt.Errorf("invalid cache disposition")
	}
	if err := response.Cost.validate(); err != nil {
		return nil, err
	}
	fields := map[string]any{"api_version": APIVersion, "operation_key": response.OperationKey, "operation_id": response.OperationID, "status": response.Status, "output": emptyIfNil(response.Output), "checkpoint": response.Checkpoint, "cache": response.Cache, "cost": response.Cost}
	setIfNotNil(fields, "route", response.Route)
	setIfNotNil(fields, "service", response.Service)
	setIfNotNil(fields, "usage", response.Usage)
	if response.Diagnostics != nil {
		fields["diagnostics"] = response.Diagnostics
	}
	return marshalObject(fields)
}

func (response *GenerateResponseV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "api_version", "operation_key", "operation_id", "status", "output", "checkpoint", "cache", "route", "service", "usage", "cost", "diagnostics")
	if err != nil {
		return err
	}
	version, key, err := decodeContractHeader(fields, APIVersion)
	if err != nil {
		return err
	}
	reader := fieldReader{fields: fields}
	result := GenerateResponseV1{APIVersion: version, OperationKey: key, OperationID: reader.requiredString("operation_id"), Status: ResponseStatus(reader.requiredString("status"))}
	if reader.err != nil {
		return reader.err
	}
	if !result.Status.Valid() {
		return fmt.Errorf("status %q is invalid", result.Status)
	}
	readRequiredField(&reader, "output", &result.Output, decodeItems)
	if reader.err != nil {
		return reader.err
	}
	if err := decodeRequiredJSON(fields, "checkpoint", &result.Checkpoint); err != nil {
		return err
	}
	if kind := result.Checkpoint.Kind; kind != "generation" && kind != "cache_replay" {
		return fmt.Errorf("invalid generation checkpoint kind %q", kind)
	}
	if err := decodeCacheAndCost(fields, &result.Cache, &result.Cost); err != nil {
		return err
	}
	if err := result.Cost.validate(); err != nil {
		return err
	}
	readPresentField(&reader, "route", &result.Route, pointerTo(decodeRouteFacts))
	// The v1 contract requires fallback_index; only stored internal
	// responses written before the field existed may omit it.
	readPresentField(&reader, "service", &result.Service, pointerTo(decodeServiceFactsStrict))
	if err := decodeUsageAndDiagnostics(&reader, &result.Usage, &result.Diagnostics); err != nil {
		return err
	}
	*response = result
	return nil
}

// decodeCacheAndCost reads the required cache disposition and cost every v1
// response carries.
func decodeCacheAndCost(fields map[string]json.RawMessage, cache *CacheDispositionV1, cost *CostV1) error {
	if err := decodeRequiredJSON(fields, "cache", cache); err != nil {
		return err
	}
	return decodeRequiredJSON(fields, "cost", cost)
}

// decodeUsageAndDiagnostics reads the optional usage and diagnostics members
// that end the v1 Generate and Compact responses.
func decodeUsageAndDiagnostics(reader *fieldReader, usage **Usage, diagnostics *[]Diagnostic) error {
	readPresentField(reader, "usage", usage, pointerTo(decodeUsage))
	readPresentField(reader, "diagnostics", diagnostics, decodeDiagnostics)
	return reader.err
}

type CompactRequestV1 struct {
	APIVersion   string
	OperationKey string
	Context      RequestContext
	Parent       CheckpointHandle
	Policy       json.RawMessage
	// Model and ServiceClass optionally route the summarizer instead of the
	// parent's settings. A ServiceClass override is exact: the parent's
	// fallback classes are dropped with it. Neither changes the settings the
	// compaction child inherits.
	Model        string
	ServiceClass ServiceClass
	Cache        *CachePolicyV1
}

// GenerateCompaction is the automatic compaction a Generate runs before its
// turn. Its summarizer routes on the model and service class the Generate's
// settings_patch sets, the route the turn itself is planned on, so a model
// retired since the parent was written does not strand the lineage. A
// Generate that patches neither keeps the parent-routed request unchanged.
// The summary always uses the compaction cache with variant 0 and no age
// bound, whatever the Generate's own cache policy, which governs only its
// final response; changing that would change recorded child inputs.
func GenerateCompaction(generate GenerateRequestV1, operationKey string) CompactRequestV1 {
	request := CompactRequestV1{OperationKey: operationKey, Context: generate.Context, Cache: &CachePolicyV1{}}
	if generate.Parent != nil {
		request.Parent = *generate.Parent
	}
	// A patched model outside the override's bound cannot be a catalog name;
	// leave it to the Generate's own routing to reject.
	if model := generate.SettingsPatch.Model.Set; model != nil && validCompactModel(*model) {
		request.Model = *model
	}
	if generate.SettingsPatch.ServiceClass.Set != nil {
		request.ServiceClass = *generate.SettingsPatch.ServiceClass.Set
	}
	return request
}

// MaxCompactModelLengthV1 bounds a Compact request's summarizer model in
// Unicode code points, as the schema does.
const MaxCompactModelLengthV1 = 256

func validCompactModel(model string) bool {
	return model != "" && utf8.ValidString(model) && utf8.RuneCountInString(model) <= MaxCompactModelLengthV1
}

// ParentRoutedCompaction is request without its summarizer overrides: the
// form GenerateCompaction produced before it carried them, still accepted
// for compactions that began under it.
func (request CompactRequestV1) ParentRoutedCompaction() CompactRequestV1 {
	request.Model, request.ServiceClass = "", ""
	return request
}

func (request CompactRequestV1) MarshalJSON() ([]byte, error) {
	if request.APIVersion != "" && request.APIVersion != CompactAPIVersion {
		return nil, fmt.Errorf("api_version %q is unsupported", request.APIVersion)
	}
	if request.OperationKey == "" || request.Parent == "" || request.Context.Tenant == "" || request.Context.Project == "" || request.Context.Actor == "" {
		return nil, fmt.Errorf("compact operation, parent, and context are required")
	}
	if !request.Parent.valid() {
		return nil, fmt.Errorf("compact parent checkpoint is invalid")
	}
	if len(request.Policy) > 0 {
		if err := validateCompactPolicy(request.Policy); err != nil {
			return nil, err
		}
	}
	if request.Model != "" && !validCompactModel(request.Model) {
		return nil, fmt.Errorf("compact model must be at most %d code points", MaxCompactModelLengthV1)
	}
	if request.ServiceClass != "" && !request.ServiceClass.Valid() {
		return nil, fmt.Errorf("compact service_class: invalid value %q", request.ServiceClass)
	}
	if request.Cache != nil {
		if err := request.Cache.validate(); err != nil {
			return nil, err
		}
	}
	context, err := marshalRequestContextV1(request.Context)
	if err != nil {
		return nil, err
	}
	fields := map[string]any{"api_version": CompactAPIVersion, "operation_key": request.OperationKey, "context": context, "parent": string(request.Parent)}
	if len(request.Policy) > 0 {
		if !validObjectJSON(request.Policy) {
			return nil, fmt.Errorf("compact policy must be an object")
		}
		fields["policy"] = json.RawMessage(request.Policy)
	}
	setIfNotEmpty(fields, "model", request.Model)
	setIfNotEmpty(fields, "service_class", request.ServiceClass)
	setIfNotNil(fields, "cache", request.Cache)
	return marshalObject(fields)
}
func (request *CompactRequestV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "api_version", "operation_key", "context", "parent", "policy", "model", "service_class", "cache")
	if err != nil {
		return err
	}
	version, key, err := decodeContractHeader(fields, CompactAPIVersion)
	if err != nil {
		return err
	}
	parent, err := requiredString(fields, "parent")
	if err != nil {
		return err
	}
	context, err := decodeContractContext(fields)
	if err != nil {
		return err
	}
	result := CompactRequestV1{APIVersion: version, OperationKey: key, Parent: CheckpointHandle(parent), Context: context}
	if err := validateCompactRequestV1Bounds(result); err != nil {
		return err
	}
	if !result.Parent.valid() {
		return fmt.Errorf("compact parent checkpoint is invalid")
	}
	if raw, ok := fields["policy"]; ok {
		if err := validateCompactPolicy(raw); err != nil {
			return err
		}
		result.Policy = copyRaw(raw)
	}
	if model, ok, err := optionalString(fields, "model"); err != nil {
		return err
	} else if ok {
		if !validCompactModel(model) {
			return fmt.Errorf("compact model must be 1 to %d code points", MaxCompactModelLengthV1)
		}
		result.Model = model
	}
	if class, ok, err := optionalString(fields, "service_class"); err != nil {
		return err
	} else if ok {
		if !ServiceClass(class).Valid() {
			return fmt.Errorf("compact service_class: invalid value %q", class)
		}
		result.ServiceClass = ServiceClass(class)
	}
	if err := decodePresentField(fields, "cache", &result.Cache, decodeCachePolicy); err != nil {
		return err
	}
	*request = result
	return nil
}

// validateCompactPolicy keeps compaction's control-plane policy separate from
// Generate settings. It intentionally has no tools, tool choice, output, or
// provider-specific fields: compaction is a lossily summarized child and
// application tool/structured-output configuration is restored by Generate.
// target_tokens is only range-checked: it is merged into the parent
// checkpoint's policy, whose trigger_tokens is not known at decode, and
// request preparation rejects a target at or above that effective trigger as
// invalid_argument before any provider work.
func validateCompactPolicy(raw json.RawMessage) error {
	fields, err := decodeObject(raw)
	if err != nil {
		return fmt.Errorf("compact policy must be an object")
	}
	if err := checkUnknownFields(fields, "target_tokens", "summary_style"); err != nil {
		return fmt.Errorf("compact policy: %w", err)
	}
	if value, ok := fields["target_tokens"]; ok {
		target, err := decodeInt(value)
		if err != nil || target < 1 || target > 10000000 {
			return fmt.Errorf("compact policy target_tokens must be between 1 and 10000000")
		}
	}
	if value, ok := fields["summary_style"]; ok {
		style, err := decodeStringField("summary_style", value)
		if err != nil {
			return err
		}
		switch style {
		case "concise", "balanced", "detailed":
		default:
			return fmt.Errorf("compact policy summary_style %q is invalid", style)
		}
	}
	return nil
}

type CompactResponseV1 struct {
	APIVersion   string
	OperationKey string
	OperationID  string
	Checkpoint   CheckpointMetadata
	Cache        CacheDispositionV1
	Provenance   json.RawMessage
	Usage        *Usage
	Cost         CostV1
	Diagnostics  []Diagnostic
}

func (response CompactResponseV1) MarshalJSON() ([]byte, error) {
	if response.APIVersion != "" && response.APIVersion != CompactAPIVersion {
		return nil, fmt.Errorf("api_version %q is unsupported", response.APIVersion)
	}
	if response.OperationKey == "" || response.OperationID == "" || response.Checkpoint.Kind != "compaction" || !response.Checkpoint.Handle.valid() || response.Checkpoint.Depth < 0 {
		return nil, fmt.Errorf("compact response identity is invalid")
	}
	if response.Checkpoint.Parent == nil || !response.Checkpoint.Parent.valid() {
		return nil, fmt.Errorf("compact response parent checkpoint is invalid")
	}
	if response.Cache.Variant < 0 || !validateCacheDisposition(response.Cache.Disposition) {
		return nil, fmt.Errorf("invalid compact cache disposition")
	}
	if err := response.Cost.validate(); err != nil {
		return nil, err
	}
	fields := map[string]any{"api_version": CompactAPIVersion, "operation_key": response.OperationKey, "operation_id": response.OperationID, "status": "completed", "checkpoint": response.Checkpoint, "cache": response.Cache, "cost": response.Cost}
	if len(response.Provenance) > 0 {
		fields["provenance"] = json.RawMessage(response.Provenance)
	}
	setIfNotNil(fields, "usage", response.Usage)
	if response.Diagnostics != nil {
		fields["diagnostics"] = response.Diagnostics
	}
	return marshalObject(fields)
}

func (response *CompactResponseV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "api_version", "operation_key", "operation_id", "status", "checkpoint", "cache", "provenance", "usage", "cost", "diagnostics")
	if err != nil {
		return err
	}
	version, key, err := decodeContractHeader(fields, CompactAPIVersion)
	if err != nil {
		return err
	}
	id, err := requiredString(fields, "operation_id")
	if err != nil {
		return err
	}
	status, err := requiredString(fields, "status")
	if err != nil || status != "completed" {
		return fmt.Errorf("compact status must be completed")
	}
	checkpointRaw, err := requireField(fields, "checkpoint")
	if err != nil {
		return err
	}
	result := CompactResponseV1{APIVersion: version, OperationKey: key, OperationID: id}
	if err := json.Unmarshal(checkpointRaw, &result.Checkpoint); err != nil || result.Checkpoint.Kind != "compaction" {
		return fmt.Errorf("compact checkpoint must be compaction")
	}
	if parent := result.Checkpoint.Parent; parent == nil || !parent.valid() {
		return fmt.Errorf("compact checkpoint parent is invalid")
	}
	if err := decodeCacheAndCost(fields, &result.Cache, &result.Cost); err != nil {
		return err
	}
	if raw, ok := fields["provenance"]; ok {
		if !validObjectJSON(raw) {
			return fmt.Errorf("provenance must be an object")
		}
		result.Provenance = copyRaw(raw)
	}
	if err := decodeUsageAndDiagnostics(&fieldReader{fields: fields}, &result.Usage, &result.Diagnostics); err != nil {
		return err
	}
	*response = result
	return nil
}

type QueryKind string

const (
	QueryProviderStatus QueryKind = "provider_status"
	QueryModelInventory QueryKind = "model_inventory"
	QueryCreditStatus   QueryKind = "credit_status"
	QueryBudgetStatus   QueryKind = "budget_status"
	QuerySpendSummary   QueryKind = "spend_summary"
)

func (kind QueryKind) valid() bool {
	return kind == QueryProviderStatus || kind == QueryModelInventory || kind == QueryCreditStatus || kind == QueryBudgetStatus || kind == QuerySpendSummary
}

// decodeQueryKind reads the required query kind.
func decodeQueryKind(fields map[string]json.RawMessage) (QueryKind, error) {
	value, err := requiredString(fields, "kind")
	if err != nil {
		return "", err
	}
	kind := QueryKind(value)
	if !kind.valid() {
		return "", fmt.Errorf("query kind %q is invalid", kind)
	}
	return kind, nil
}

type QueryRequestV1 struct {
	APIVersion   string
	OperationKey string
	Context      RequestContext
	Kind         QueryKind
	Query        json.RawMessage
}

func (request QueryRequestV1) MarshalJSON() ([]byte, error) {
	if request.OperationKey == "" || request.Context.Tenant == "" || request.Context.Project == "" || request.Context.Actor == "" || !request.Kind.valid() || !validObjectJSON(request.Query) || validateQueryObject(request.Kind, request.Query) != nil {
		return nil, fmt.Errorf("query request is invalid")
	}
	context, err := marshalRequestContextV1(request.Context)
	if err != nil {
		return nil, err
	}
	return marshalObject(map[string]any{"api_version": QueryAPIVersion, "operation_key": request.OperationKey, "context": context, "kind": request.Kind, "query": json.RawMessage(request.Query)})
}
func (request *QueryRequestV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "api_version", "operation_key", "context", "kind", "query")
	if err != nil {
		return err
	}
	version, key, err := decodeContractHeader(fields, QueryAPIVersion)
	if err != nil {
		return err
	}
	if err := validateCodePointLength("operation_key", key, 1, generateOperationKeyMaxCodePoints); err != nil {
		return err
	}
	kind, err := decodeQueryKind(fields)
	if err != nil {
		return err
	}
	context, err := decodeContractContext(fields)
	if err != nil {
		return err
	}
	query, err := requireField(fields, "query")
	if err != nil || !validObjectJSON(query) {
		return fmt.Errorf("query must be an object")
	}
	if err := validateQueryObject(kind, query); err != nil {
		return err
	}
	*request = QueryRequestV1{APIVersion: version, OperationKey: key, Context: context, Kind: kind, Query: copyRaw(query)}
	return nil
}

func validateQueryObject(kind QueryKind, raw json.RawMessage) error {
	fields, err := decodeObject(raw)
	if err != nil {
		return fmt.Errorf("query must be an object")
	}
	allowed := map[QueryKind][]string{
		QueryProviderStatus: {"provider", "endpoint", "availability", "include_healthy", "refresh_if_older_than_seconds", "page_size", "cursor"},
		QueryModelInventory: {"provider", "endpoint", "model_prefix", "lifecycle", "refresh_if_older_than_seconds", "page_size", "cursor"},
		QueryCreditStatus:   {"provider", "endpoint", "include_ok", "refresh_if_older_than_seconds", "page_size", "cursor"},
		QueryBudgetStatus:   {"policy_key", "active_at", "include_windows"},
		QuerySpendSummary:   {"start_time", "end_time", "group_by", "operation_kinds"},
	}
	if err := checkUnknownFields(fields, allowed[kind]...); err != nil {
		return err
	}
	if raw, ok := fields["page_size"]; ok {
		value, err := decodeInt(raw)
		if err != nil || value < 1 || value > 1000 {
			return fmt.Errorf("query page_size must be between 1 and 1000")
		}
	}
	if raw, ok := fields["cursor"]; ok && string(raw) != "null" {
		value, err := decodeStringField("cursor", raw)
		if err != nil || len(value) > 512 {
			return fmt.Errorf("query cursor is invalid")
		}
	}
	// The JSON schema permits these members only when they are omitted or a
	// string.  encoding/json decodes JSON null into a string as an empty value,
	// so validate the optional strings explicitly instead of allowing null to
	// silently become an unfiltered query.
	for _, name := range []string{"provider", "endpoint", "model_prefix", "policy_key"} {
		if raw, ok := fields[name]; ok {
			if string(raw) == "null" {
				return fmt.Errorf("query %s must be a string", name)
			}
			if _, err := decodeStringField(name, raw); err != nil {
				return err
			}
		}
	}
	if raw, ok := fields["refresh_if_older_than_seconds"]; ok {
		value, err := decodeInt64(raw)
		if err != nil || value < 1 || value > 86400 {
			return fmt.Errorf("refresh_if_older_than_seconds is invalid")
		}
	}
	if raw, ok := fields["availability"]; ok {
		if err := validateQueryEnum(raw, "availability", "available", "degraded", "unavailable", "unknown"); err != nil {
			return err
		}
	}
	if raw, ok := fields["lifecycle"]; ok {
		if err := validateQueryEnum(raw, "lifecycle", "available", "deprecated", "unavailable", "unknown"); err != nil {
			return err
		}
	}
	if kind == QuerySpendSummary {
		if err := validateTimestampFields(fields, "start_time", "end_time"); err != nil {
			return err
		}
		startValue, _ := requiredString(fields, "start_time")
		endValue, _ := requiredString(fields, "end_time")
		start, _ := time.Parse(time.RFC3339Nano, startValue)
		end, _ := time.Parse(time.RFC3339Nano, endValue)
		if !end.After(start) {
			return fmt.Errorf("spend summary end_time must be after start_time")
		}
		if raw, ok := fields["group_by"]; ok {
			if err := validateQueryEnumArray(raw, "group_by", "operation_kind", "provider", "model"); err != nil {
				return err
			}
		}
		if raw, ok := fields["operation_kinds"]; ok {
			if err := validateQueryEnumArray(raw, "operation_kinds", "generate", "compact", "query"); err != nil {
				return err
			}
		}
	}
	if _, ok := fields["active_at"]; ok && kind == QueryBudgetStatus {
		return validateTimestampFields(fields, "active_at")
	}
	return nil
}

func validateQueryEnum(raw json.RawMessage, field string, allowed ...string) error {
	value, err := decodeStringField(field, raw)
	if err != nil {
		return fmt.Errorf("%s must be a string: %w", field, err)
	}
	for _, candidate := range allowed {
		if value == candidate {
			return nil
		}
	}
	return fmt.Errorf("%s %q is invalid", field, value)
}

func validateQueryEnumArray(raw json.RawMessage, field string, allowed ...string) error {
	if string(raw) == "null" {
		return fmt.Errorf("%s must be an array of strings", field)
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return fmt.Errorf("%s must be an array of strings", field)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, duplicate := seen[value]; duplicate {
			return fmt.Errorf("%s contains duplicate value %q", field, value)
		}
		seen[value] = struct{}{}
		valid := false
		for _, candidate := range allowed {
			if value == candidate {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("%s value %q is invalid", field, value)
		}
	}
	return nil
}

func validateQueryTimestamp(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", field)
	}
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return fmt.Errorf("%s must be an RFC3339 timestamp: %w", field, err)
	}
	return nil
}

type QueryResult interface{ queryResultKind() QueryKind }
type ProviderStatusPage struct {
	Routes []json.RawMessage `json:"routes"`
}

func (ProviderStatusPage) queryResultKind() QueryKind { return QueryProviderStatus }

type ModelInventoryPage struct {
	Models []json.RawMessage `json:"models"`
}

func (ModelInventoryPage) queryResultKind() QueryKind { return QueryModelInventory }

type CreditStatusPage struct {
	Endpoints []json.RawMessage `json:"endpoints"`
}

func (CreditStatusPage) queryResultKind() QueryKind { return QueryCreditStatus }

type BudgetStatus struct {
	ActiveAt            string            `json:"active_at"`
	GenerationID        string            `json:"generation_id"`
	ManifestDigest      string            `json:"manifest_digest"`
	StreamHighWaterMark string            `json:"stream_high_water_mark"`
	Windows             []json.RawMessage `json:"windows"`
}

func (BudgetStatus) queryResultKind() QueryKind { return QueryBudgetStatus }

type SpendSummary struct {
	StartTime string            `json:"start_time"`
	EndTime   string            `json:"end_time"`
	Buckets   []json.RawMessage `json:"buckets"`
}

func (SpendSummary) queryResultKind() QueryKind { return QuerySpendSummary }

// unmarshalQueryResult decodes raw as the result page T.
func unmarshalQueryResult[T QueryResult](raw []byte) (QueryResult, error) {
	var value T
	err := json.Unmarshal(raw, &value)
	return value, err
}

type QueryResponseV1 struct {
	APIVersion       string
	OperationKey     string
	QueryExecutionID string
	Kind             QueryKind
	ObservedAt       string
	Source           string
	Freshness        string
	Complete         bool
	NextCursor       *string
	Result           QueryResult
	Cost             CostV1
}

func (response QueryResponseV1) MarshalJSON() ([]byte, error) {
	if response.OperationKey == "" || response.QueryExecutionID == "" || !response.Kind.valid() || response.Result == nil || response.Result.queryResultKind() != response.Kind {
		return nil, fmt.Errorf("query response kind/result mismatch")
	}
	if err := response.validate(); err != nil {
		return nil, err
	}
	actualCost, err := response.Cost.canonicalActualCost()
	if err != nil {
		return nil, err
	}
	fields := map[string]any{"api_version": QueryAPIVersion, "operation_key": response.OperationKey, "query_execution_id": response.QueryExecutionID, "kind": response.Kind, "observed_at": response.ObservedAt, "source": response.Source, "freshness": response.Freshness, "complete": response.Complete, "result": response.Result, "cost_status": response.Cost.Status, "actual_cost_usd": actualCost}
	if response.NextCursor != nil {
		fields["next_cursor"] = *response.NextCursor
	}
	setIfNotEmpty(fields, "cost_method", response.Cost.Method)
	setIfNotEmpty(fields, "cost_unknown_reason_code", response.Cost.UnknownReason)
	return marshalObject(fields)
}

func (response *QueryResponseV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "api_version", "operation_key", "query_execution_id", "kind", "observed_at", "source", "freshness", "complete", "next_cursor", "result", "cost_status", "actual_cost_usd", "cost_method", "cost_unknown_reason_code")
	if err != nil {
		return err
	}
	version, key, err := decodeContractHeader(fields, QueryAPIVersion)
	if err != nil {
		return err
	}
	execution, err := requiredString(fields, "query_execution_id")
	if err != nil {
		return err
	}
	kind, err := decodeQueryKind(fields)
	if err != nil {
		return err
	}
	reader := fieldReader{fields: fields}
	responseValue := QueryResponseV1{
		APIVersion:       version,
		OperationKey:     key,
		QueryExecutionID: execution,
		Kind:             kind,
		ObservedAt:       reader.requiredString("observed_at"),
		Source:           reader.requiredString("source"),
		Freshness:        reader.requiredString("freshness"),
		Complete:         reader.requiredBool("complete"),
	}
	resultRaw := reader.requireField("result")
	if reader.err != nil {
		return reader.err
	}
	switch kind {
	case QueryProviderStatus:
		responseValue.Result, err = unmarshalQueryResult[ProviderStatusPage](resultRaw)
	case QueryModelInventory:
		responseValue.Result, err = unmarshalQueryResult[ModelInventoryPage](resultRaw)
	case QueryCreditStatus:
		responseValue.Result, err = unmarshalQueryResult[CreditStatusPage](resultRaw)
	case QueryBudgetStatus:
		responseValue.Result, err = unmarshalQueryResult[BudgetStatus](resultRaw)
	case QuerySpendSummary:
		responseValue.Result, err = unmarshalQueryResult[SpendSummary](resultRaw)
	}
	if err != nil {
		return err
	}
	responseValue.Cost.Status = reader.requiredString("cost_status")
	reader.requireField("actual_cost_usd")
	readPresentField(&reader, "actual_cost_usd", &responseValue.Cost.ActualCostUSD, decodeActualCostUSD)
	responseValue.Cost.Method = reader.optionalString("cost_method")
	responseValue.Cost.UnknownReason = reader.optionalString("cost_unknown_reason_code")
	if reader.err != nil {
		return reader.err
	}
	if err := responseValue.Cost.validate(); err != nil {
		return err
	}
	if raw, ok := fields["next_cursor"]; ok && string(raw) != "null" {
		value, err := decodeStringField("next_cursor", raw)
		if err != nil {
			return err
		}
		responseValue.NextCursor = &value
	}
	if err := responseValue.validate(); err != nil {
		return err
	}
	*response = responseValue
	return nil
}

func (response QueryResponseV1) validate() error {
	if err := validateQueryResult(response.Kind, response.Result); err != nil {
		return err
	}
	if err := validateQueryTimestamp("observed_at", response.ObservedAt); err != nil {
		return err
	}
	switch response.Source {
	case "persisted", "persisted_and_refreshed", "redis_budget_generation":
	default:
		return fmt.Errorf("query source %q is invalid", response.Source)
	}
	switch response.Freshness {
	case "current", "stale", "unknown":
	default:
		return fmt.Errorf("query freshness %q is invalid", response.Freshness)
	}
	if response.NextCursor != nil && (len(*response.NextCursor) == 0 || len(*response.NextCursor) > 512) {
		return fmt.Errorf("query next_cursor is invalid")
	}
	// Budget and spend queries are bounded snapshots rather than keyset pages.
	// A continuation on either result would be ambiguous: there is no matching
	// cursor field in their filters and the caller cannot safely resume it.
	if response.NextCursor != nil && (response.Kind == QueryBudgetStatus || response.Kind == QuerySpendSummary) {
		return fmt.Errorf("query %s response must not include next_cursor", response.Kind)
	}
	// Keyset pages must make their continuation state explicit. A complete page
	// has no continuation, while an incomplete page must carry one so callers
	// cannot silently stop or repeat pagination. Snapshot queries are always
	// complete because they have no supported continuation cursor.
	switch response.Kind {
	case QueryProviderStatus, QueryModelInventory, QueryCreditStatus:
		if response.Complete && response.NextCursor != nil {
			return fmt.Errorf("query %s complete response must not include next_cursor", response.Kind)
		}
		if !response.Complete && response.NextCursor == nil {
			return fmt.Errorf("query %s incomplete response requires next_cursor", response.Kind)
		}
	case QueryBudgetStatus, QuerySpendSummary:
		if !response.Complete {
			return fmt.Errorf("query %s response must be complete", response.Kind)
		}
	}
	if response.Cost.Status == "exact" && response.Cost.Method != "control_query_zero" && response.Cost.Method != "provider_reported" && response.Cost.Method != "catalog_usage" {
		return fmt.Errorf("query cost method %q is invalid", response.Cost.Method)
	}
	if response.Cost.Status == "exact" && response.Cost.Method == "control_query_zero" && response.Cost.ActualCostUSD != nil && !isZeroDecimal(*response.Cost.ActualCostUSD) {
		return fmt.Errorf("control_query_zero requires zero actual cost")
	}
	switch response.Kind {
	case QueryBudgetStatus:
		return validateTypedResult(response.Result, "budget status", func(budget BudgetStatus) error {
			return validateQueryTimestamp("budget active_at", budget.ActiveAt)
		})
	case QuerySpendSummary:
		return validateTypedResult(response.Result, "spend summary", func(spend SpendSummary) error {
			if err := validateQueryTimestamp("spend start_time", spend.StartTime); err != nil {
				return err
			}
			return validateQueryTimestamp("spend end_time", spend.EndTime)
		})
	}
	return nil
}

func isZeroDecimal(value string) bool {
	for _, character := range value {
		if character != '0' && character != '.' {
			return false
		}
	}
	return value != ""
}

// ValidateVariantTemperature validates the independent sample index and optional
// temperature. A sample index does not require a positive temperature.
func ValidateVariantTemperature(variant int32, temperature *float64) error {
	if variant < 0 {
		return fmt.Errorf("variant must not be negative")
	}
	if temperature != nil && (*temperature < 0 || math.IsNaN(*temperature) || math.IsInf(*temperature, 0)) {
		return fmt.Errorf("temperature is invalid")
	}
	return nil
}

// ValidateVariantDecimalTemperature validates the independent sample index and
// exact v1 wire decimal. This keeps validation independent of a
// float64 conversion (which may round an 18-digit value or overflow for a
// syntactically valid NUMERIC(38,18) value).
func ValidateVariantDecimalTemperature(variant int32, temperature *DecimalV1) error {
	if variant < 0 {
		return fmt.Errorf("variant must not be negative")
	}
	if temperature == nil {
		return nil
	}
	_, err := NewDecimalV1(temperature.String())
	if err != nil {
		return fmt.Errorf("temperature is invalid: %w", err)
	}
	return nil
}

// SampleIndex is only a cache discriminator. Changing it requests an independent
// sample without changing the provider request. Omission selects sample zero.
func (policy *CachePolicyV1) SampleIndex() int64 {
	if policy == nil {
		return 0
	}
	return int64(policy.Variant)
}

// decodeContractHeader reads the api_version and operation_key every v1
// contract object starts with. A missing or different api_version is
// reported as unsupported.
func decodeContractHeader(fields map[string]json.RawMessage, apiVersion string) (string, string, error) {
	version, err := requiredString(fields, "api_version")
	if err != nil || version != apiVersion {
		return "", "", fmt.Errorf("api_version %q is unsupported", version)
	}
	key, err := requiredString(fields, "operation_key")
	if err != nil {
		return "", "", err
	}
	return version, key, nil
}

// decodeContractContext reads the required v1 context object.
func decodeContractContext(fields map[string]json.RawMessage) (RequestContext, error) {
	raw, err := requireField(fields, "context")
	if err != nil {
		return RequestContext{}, err
	}
	return decodeRequestContextV1(raw)
}

// decodeParentHandle reads the optional, nullable parent checkpoint handle.
// A handle that is not valid is reported as invalid.
func decodeParentHandle(fields map[string]json.RawMessage, invalid string) (*CheckpointHandle, error) {
	raw, ok := fields["parent"]
	if !ok || string(raw) == "null" {
		return nil, nil
	}
	value, err := decodeStringField("parent", raw)
	if err != nil {
		return nil, err
	}
	handle := CheckpointHandle(value)
	if !handle.valid() {
		return nil, errors.New(invalid)
	}
	return &handle, nil
}

// decodeCachePolicy decodes and validates a v1 cache policy.
func decodeCachePolicy(data []byte) (*CachePolicyV1, error) {
	var policy CachePolicyV1
	if err := json.Unmarshal(data, &policy); err != nil {
		return nil, err
	}
	if err := policy.validate(); err != nil {
		return nil, err
	}
	return &policy, nil
}

// decodeRequiredJSON unmarshals the required field name into target.
func decodeRequiredJSON(fields map[string]json.RawMessage, name string, target any) error {
	raw, err := requireField(fields, name)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}
