package llm

// This file contains the closed wire records introduced by the forkable
// conversation contract. The existing Request/Response types remain the
// provider-neutral engine model; these records are the Temporal-facing v1
// boundary and deliberately do not expose transcript or provider payloads.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
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

func decodeInt32(raw json.RawMessage) (int32, error) {
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

func (handle CheckpointHandle) valid() bool { return len(handle) > 0 && len(handle) <= 512 }

type CachePolicyV1 struct {
	MaxAgeSeconds int64 `json:"max_age_seconds"`
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
	maxAgeRaw, err := requireField(fields, "max_age_seconds")
	if err != nil {
		return err
	}
	maxAge, err := decodeInt64(maxAgeRaw)
	if err != nil {
		return err
	}
	variant := int32(0)
	if raw, ok := fields["variant"]; ok {
		variant, err = decodeInt32(raw)
		if err != nil {
			return err
		}
	}
	result := CachePolicyV1{MaxAgeSeconds: maxAge, Variant: variant}
	if err := result.validate(false); err != nil {
		return err
	}
	*policy = result
	return nil
}

func (policy CachePolicyV1) validate(compact bool) error {
	if policy.MaxAgeSeconds <= 0 || policy.MaxAgeSeconds > 31536000 {
		return fmt.Errorf("cache max_age_seconds must be between 1 and 31536000")
	}
	if policy.Variant < 0 || (compact && policy.Variant != 0) {
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
	Model                 Patch[string]
	ServiceClass          Patch[ServiceClass]
	ServiceClassFallbacks Patch[[]ServiceClass]
	Portability           Patch[PortabilityMode]
	Instructions          Patch[[]Instruction]
	Tools                 Patch[[]Tool]
	ToolPolicy            Patch[ToolPolicy]
	Output                Patch[OutputSpec]
	Temperature           Patch[DecimalV1]
	ReasoningEffort       Patch[ReasoningEffort]
	ReasoningSummary      Patch[ReasoningSummary]
	CompactionPolicy      Patch[json.RawMessage]
	Extensions            Patch[map[string]json.RawMessage]
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
	if patch.CompactionPolicy.Set != nil {
		if _, err := decodeObject(*patch.CompactionPolicy.Set); err != nil {
			return nil, fmt.Errorf("compaction_policy.set must be an object")
		}
	}
	if patch.Extensions.Set != nil {
		if *patch.Extensions.Set == nil {
			return nil, fmt.Errorf("extensions.set must be an object")
		}
	}
	fields := map[string]any{}
	add := func(name string, value any, clear bool) error {
		if clear {
			fields[name] = map[string]any{"clear": true}
		} else if value != nil {
			fields[name] = map[string]any{"set": value}
		}
		return nil
	}
	if err := addPatch("model", patch.Model, fields, add); err != nil {
		return nil, err
	}
	if err := addPatch("service_class", patch.ServiceClass, fields, add); err != nil {
		return nil, err
	}
	if err := addPatch("service_class_fallbacks", patch.ServiceClassFallbacks, fields, add); err != nil {
		return nil, err
	}
	if err := addPatch("portability", patch.Portability, fields, add); err != nil {
		return nil, err
	}
	if err := addPatch("instructions", patch.Instructions, fields, add); err != nil {
		return nil, err
	}
	if err := addPatch("tools", patch.Tools, fields, add); err != nil {
		return nil, err
	}
	if err := addPatch("tool_policy", patch.ToolPolicy, fields, add); err != nil {
		return nil, err
	}
	if err := addPatch("output", patch.Output, fields, add); err != nil {
		return nil, err
	}
	if patch.Temperature.Clear {
		if err := patch.Temperature.validate(); err != nil {
			return nil, fmt.Errorf("temperature: %w", err)
		}
		if err := add("temperature", nil, true); err != nil {
			return nil, err
		}
	} else if patch.Temperature.Set != nil {
		if err := patch.Temperature.validate(); err != nil {
			return nil, fmt.Errorf("temperature: %w", err)
		}
		value, err := NewDecimalV1(patch.Temperature.Set.String())
		if err != nil {
			return nil, fmt.Errorf("temperature: %w", err)
		}
		if err := add("temperature", value, false); err != nil {
			return nil, err
		}
	}
	if err := addPatch("reasoning_effort", patch.ReasoningEffort, fields, add); err != nil {
		return nil, err
	}
	if err := addPatch("reasoning_summary", patch.ReasoningSummary, fields, add); err != nil {
		return nil, err
	}
	if err := addPatch("compaction_policy", patch.CompactionPolicy, fields, add); err != nil {
		return nil, err
	}
	if err := addPatch("extensions", patch.Extensions, fields, add); err != nil {
		return nil, err
	}
	return marshalObject(fields)
}

func addPatch[T any](name string, patch Patch[T], fields map[string]any, add func(string, any, bool) error) error {
	if err := patch.validate(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if patch.Clear {
		return add(name, nil, true)
	}
	if patch.Set != nil {
		return add(name, *patch.Set, false)
	}
	return nil
}

func decodePatch[T any](raw json.RawMessage, name string) (Patch[T], error) {
	fields, err := decodeObject(raw)
	if err != nil {
		return Patch[T]{}, fmt.Errorf("%s: %w", name, err)
	}
	if err := checkUnknownFields(fields, "set", "clear"); err != nil {
		return Patch[T]{}, fmt.Errorf("%s: %w", name, err)
	}
	_, hasSet := fields["set"]
	_, hasClear := fields["clear"]
	if hasSet == hasClear {
		return Patch[T]{}, fmt.Errorf("%s must contain exactly one of set or clear", name)
	}
	if hasClear {
		var clear bool
		if err := json.Unmarshal(fields["clear"], &clear); err != nil || !clear {
			return Patch[T]{}, fmt.Errorf("%s.clear must be true", name)
		}
		return Patch[T]{Clear: true}, nil
	}
	value, err := decodePatchValue[T](fields["set"], name)
	if err != nil {
		return Patch[T]{}, err
	}
	return Patch[T]{Set: &value}, nil
}

// decodePatchValue routes polymorphic settings values through the same strict
// wire decoders used by the provider-neutral request model. A plain
// json.Unmarshal would silently ignore the snake_case fields and cannot
// reconstruct the Part interface used by parts instructions.
func decodePatchValue[T any](raw json.RawMessage, name string) (T, error) {
	var zero T
	switch any(zero).(type) {
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
	case ReasoningEffortProviderDefault, ReasoningEffortMinimal, ReasoningEffortLow, ReasoningEffortMedium, ReasoningEffortHigh, ReasoningEffortMaximum:
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
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "model", "service_class", "service_class_fallbacks", "portability", "instructions", "tools", "tool_policy", "output", "temperature", "reasoning_effort", "reasoning_summary", "compaction_policy", "extensions"); err != nil {
		return err
	}
	result := SettingsPatchV1{}
	if raw, ok := fields["model"]; ok {
		result.Model, err = decodePatch[string](raw, "model")
	}
	if raw, ok := fields["service_class"]; ok && err == nil {
		result.ServiceClass, err = decodePatch[ServiceClass](raw, "service_class")
	}
	if raw, ok := fields["service_class_fallbacks"]; ok && err == nil {
		result.ServiceClassFallbacks, err = decodePatch[[]ServiceClass](raw, "service_class_fallbacks")
	}
	if raw, ok := fields["portability"]; ok && err == nil {
		result.Portability, err = decodePatch[PortabilityMode](raw, "portability")
	}
	if raw, ok := fields["instructions"]; ok && err == nil {
		result.Instructions, err = decodePatch[[]Instruction](raw, "instructions")
	}
	if raw, ok := fields["tools"]; ok && err == nil {
		result.Tools, err = decodePatch[[]Tool](raw, "tools")
	}
	if raw, ok := fields["tool_policy"]; ok && err == nil {
		result.ToolPolicy, err = decodePatch[ToolPolicy](raw, "tool_policy")
	}
	if raw, ok := fields["output"]; ok && err == nil {
		result.Output, err = decodePatch[OutputSpec](raw, "output")
	}
	if raw, ok := fields["temperature"]; ok && err == nil {
		result.Temperature, err = decodeTemperaturePatch(raw)
	}
	if raw, ok := fields["reasoning_effort"]; ok && err == nil {
		result.ReasoningEffort, err = decodePatch[ReasoningEffort](raw, "reasoning_effort")
	}
	if raw, ok := fields["reasoning_summary"]; ok && err == nil {
		result.ReasoningSummary, err = decodePatch[ReasoningSummary](raw, "reasoning_summary")
	}
	if raw, ok := fields["compaction_policy"]; ok && err == nil {
		result.CompactionPolicy, err = decodePatch[json.RawMessage](raw, "compaction_policy")
	}
	if raw, ok := fields["extensions"]; ok && err == nil {
		result.Extensions, err = decodePatch[map[string]json.RawMessage](raw, "extensions")
	}
	if err != nil {
		return err
	}
	*patch = result
	return nil
}

// decodeTemperaturePatch accepts the canonical v1 decimal-string spelling.
// Numeric input remains accepted for one compatibility window so older Go
// producers can be upgraded independently; MarshalJSON always emits the
// canonical string form.
func decodeTemperaturePatch(raw json.RawMessage) (Patch[DecimalV1], error) {
	fields, err := decodeObject(raw)
	if err != nil {
		return Patch[DecimalV1]{}, fmt.Errorf("temperature: %w", err)
	}
	if err := checkUnknownFields(fields, "set", "clear"); err != nil {
		return Patch[DecimalV1]{}, fmt.Errorf("temperature: %w", err)
	}
	set, hasSet := fields["set"]
	clear, hasClear := fields["clear"]
	if hasSet == hasClear {
		return Patch[DecimalV1]{}, fmt.Errorf("temperature must contain exactly one of set or clear")
	}
	if hasClear {
		var value bool
		if err := json.Unmarshal(clear, &value); err != nil || !value {
			return Patch[DecimalV1]{}, fmt.Errorf("temperature.clear must be true")
		}
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

func marshalRequestContextV1(context RequestContext) (json.RawMessage, error) {
	if len(context.Tags) > 0 {
		return nil, fmt.Errorf("v1 context tags are not supported")
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
	fields, err := decodeObject(data)
	if err != nil {
		return RequestContext{}, fmt.Errorf("v1 context: %w", err)
	}
	if err := checkUnknownFields(fields, "tenant", "project", "actor"); err != nil {
		return RequestContext{}, fmt.Errorf("v1 context: %w", err)
	}
	tenant, _, err := optionalString(fields, "tenant")
	if err != nil {
		return RequestContext{}, fmt.Errorf("v1 context: %w", err)
	}
	project, _, err := optionalString(fields, "project")
	if err != nil {
		return RequestContext{}, fmt.Errorf("v1 context: %w", err)
	}
	actor, _, err := optionalString(fields, "actor")
	if err != nil {
		return RequestContext{}, fmt.Errorf("v1 context: %w", err)
	}
	return RequestContext{Tenant: tenant, Project: project, Actor: actor}, nil
}

func (request GenerateRequestV1) MarshalJSON() ([]byte, error) {
	if request.APIVersion != "" && request.APIVersion != APIVersion {
		return nil, fmt.Errorf("api_version %q is unsupported", request.APIVersion)
	}
	if request.OperationKey == "" || request.Context.Tenant == "" || request.Context.Project == "" || request.Context.Actor == "" {
		return nil, fmt.Errorf("operation_key and complete context are required")
	}
	if request.Parent != nil && !request.Parent.valid() {
		return nil, fmt.Errorf("parent checkpoint is invalid")
	}
	if request.Cache != nil {
		if err := request.Cache.validate(false); err != nil {
			return nil, err
		}
	}
	context, err := marshalRequestContextV1(request.Context)
	if err != nil {
		return nil, err
	}
	appendItems := request.Append
	if appendItems == nil {
		appendItems = []Item{}
	}
	fields := map[string]any{"api_version": APIVersion, "operation_key": request.OperationKey, "context": context, "append": appendItems}
	if request.Parent != nil {
		fields["parent"] = string(*request.Parent)
	}
	if patch, err := request.SettingsPatch.MarshalJSON(); err != nil {
		return nil, err
	} else if string(patch) != "{}" {
		fields["settings_patch"] = json.RawMessage(patch)
	}
	if request.Cache != nil {
		fields["cache"] = request.Cache
	}
	return marshalObject(fields)
}

func (request *GenerateRequestV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "api_version", "operation_key", "context", "parent", "append", "settings_patch", "cache"); err != nil {
		return err
	}
	version, err := requiredString(fields, "api_version")
	if err != nil || version != APIVersion {
		return fmt.Errorf("api_version %q is unsupported", version)
	}
	key, err := requiredString(fields, "operation_key")
	if err != nil {
		return err
	}
	contextRaw, err := requireField(fields, "context")
	if err != nil {
		return err
	}
	context, err := decodeRequestContextV1(contextRaw)
	if err != nil {
		return err
	}
	if context.Tenant == "" || context.Project == "" || context.Actor == "" {
		return fmt.Errorf("context requires tenant, project, and actor")
	}
	appendRaw, err := requireField(fields, "append")
	if err != nil {
		return err
	}
	items, err := decodeItems(appendRaw)
	if err != nil {
		return fmt.Errorf("append: %w", err)
	}
	result := GenerateRequestV1{APIVersion: version, OperationKey: key, Context: context, Append: items}
	if raw, ok := fields["parent"]; ok && string(raw) != "null" {
		value, err := requiredString(map[string]json.RawMessage{"parent": raw}, "parent")
		if err != nil {
			return err
		}
		handle := CheckpointHandle(value)
		if !handle.valid() {
			return fmt.Errorf("parent checkpoint is invalid")
		}
		result.Parent = &handle
	}
	if raw, ok := fields["settings_patch"]; ok {
		if err := json.Unmarshal(raw, &result.SettingsPatch); err != nil {
			return err
		}
	}
	if raw, ok := fields["cache"]; ok {
		var policy CachePolicyV1
		if err := json.Unmarshal(raw, &policy); err != nil {
			return err
		}
		if err := policy.validate(false); err != nil {
			return err
		}
		result.Cache = &policy
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
	if err := cost.validate(); err != nil {
		return nil, err
	}
	actualCost := cost.ActualCostUSD
	if actualCost != nil {
		canonical, err := canonicalDecimalString(*actualCost)
		if err != nil {
			return nil, err
		}
		actualCost = &canonical
	}
	fields := map[string]any{"status": cost.Status, "actual_cost_usd": actualCost}
	if cost.Method != "" {
		fields["method"] = cost.Method
	}
	if cost.CatalogVersion != "" {
		fields["catalog_version"] = cost.CatalogVersion
	}
	if cost.UnknownReason != "" {
		fields["unknown_reason"] = cost.UnknownReason
	}
	return marshalObject(fields)
}

func (metadata *CheckpointMetadata) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "handle", "parent", "kind", "depth"); err != nil {
		return err
	}
	handle, err := requiredString(fields, "handle")
	if err != nil {
		return err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return err
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
	result := CheckpointMetadata{Handle: CheckpointHandle(handle), Kind: kind, Depth: int32(depth)}
	if !result.Handle.valid() || depth < 0 || depth > math.MaxInt32 {
		return fmt.Errorf("checkpoint metadata is invalid")
	}
	if raw, ok := fields["parent"]; ok && string(raw) != "null" {
		value, err := requiredString(map[string]json.RawMessage{"parent": raw}, "parent")
		if err != nil {
			return err
		}
		parsed := CheckpointHandle(value)
		if !parsed.valid() {
			return fmt.Errorf("checkpoint parent is invalid")
		}
		result.Parent = &parsed
	}
	*metadata = result
	return nil
}

func (disposition *CacheDispositionV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "disposition", "variant", "entry_age_seconds"); err != nil {
		return err
	}
	value, err := requiredString(fields, "disposition")
	if err != nil {
		return err
	}
	switch value {
	case "disabled", "miss_populated", "hit", "miss_not_populated":
	default:
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
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "status", "actual_cost_usd", "method", "catalog_version", "unknown_reason"); err != nil {
		return err
	}
	status, err := requiredString(fields, "status")
	if err != nil {
		return err
	}
	result := CostV1{Status: status}
	if raw, ok := fields["actual_cost_usd"]; ok && string(raw) != "null" {
		value, err := requiredString(map[string]json.RawMessage{"actual_cost_usd": raw}, "actual_cost_usd")
		if err != nil {
			return err
		}
		canonical, err := canonicalDecimalString(value)
		if err != nil {
			return err
		}
		result.ActualCostUSD = &canonical
	}
	if result.Method, _, err = optionalString(fields, "method"); err != nil {
		return err
	}
	if result.CatalogVersion, _, err = optionalString(fields, "catalog_version"); err != nil {
		return err
	}
	if result.UnknownReason, _, err = optionalString(fields, "unknown_reason"); err != nil {
		return err
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
	output := response.Output
	if output == nil {
		output = []Item{}
	}
	fields := map[string]any{"api_version": APIVersion, "operation_key": response.OperationKey, "operation_id": response.OperationID, "status": response.Status, "output": output, "checkpoint": response.Checkpoint, "cache": response.Cache, "cost": response.Cost}
	if response.Route != nil {
		fields["route"] = response.Route
	}
	if response.Usage != nil {
		fields["usage"] = response.Usage
	}
	if response.Diagnostics != nil {
		fields["diagnostics"] = response.Diagnostics
	}
	return marshalObject(fields)
}

func (response *GenerateResponseV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "api_version", "operation_key", "operation_id", "status", "output", "checkpoint", "cache", "route", "usage", "cost", "diagnostics"); err != nil {
		return err
	}
	version, err := requiredString(fields, "api_version")
	if err != nil || version != APIVersion {
		return fmt.Errorf("api_version %q is unsupported", version)
	}
	key, err := requiredString(fields, "operation_key")
	if err != nil {
		return err
	}
	id, err := requiredString(fields, "operation_id")
	if err != nil {
		return err
	}
	statusValue, err := requiredString(fields, "status")
	if err != nil {
		return err
	}
	status := ResponseStatus(statusValue)
	if !status.Valid() {
		return fmt.Errorf("status %q is invalid", status)
	}
	outputRaw, err := requireField(fields, "output")
	if err != nil {
		return err
	}
	output, err := decodeItems(outputRaw)
	if err != nil {
		return err
	}
	checkpointRaw, err := requireField(fields, "checkpoint")
	if err != nil {
		return err
	}
	var checkpoint CheckpointMetadata
	if err := json.Unmarshal(checkpointRaw, &checkpoint); err != nil {
		return err
	}
	if checkpoint.Kind != "generation" && checkpoint.Kind != "cache_replay" {
		return fmt.Errorf("invalid generation checkpoint kind %q", checkpoint.Kind)
	}
	cacheRaw, err := requireField(fields, "cache")
	if err != nil {
		return err
	}
	var cache CacheDispositionV1
	if err := json.Unmarshal(cacheRaw, &cache); err != nil {
		return err
	}
	costRaw, err := requireField(fields, "cost")
	if err != nil {
		return err
	}
	var cost CostV1
	if err := json.Unmarshal(costRaw, &cost); err != nil {
		return err
	}
	if err := cost.validate(); err != nil {
		return err
	}
	result := GenerateResponseV1{APIVersion: version, OperationKey: key, OperationID: id, Status: status, Output: output, Checkpoint: checkpoint, Cache: cache, Cost: cost}
	if raw, ok := fields["route"]; ok {
		route, err := decodeRouteFacts(raw)
		if err != nil {
			return err
		}
		result.Route = &route
	}
	if raw, ok := fields["usage"]; ok {
		usage, err := decodeUsage(raw)
		if err != nil {
			return err
		}
		result.Usage = &usage
	}
	if raw, ok := fields["diagnostics"]; ok {
		var err error
		result.Diagnostics, err = decodeDiagnostics(raw)
		if err != nil {
			return err
		}
	}
	*response = result
	return nil
}

type CompactRequestV1 struct {
	APIVersion   string
	OperationKey string
	Context      RequestContext
	Parent       CheckpointHandle
	Policy       json.RawMessage
	Cache        *CachePolicyV1
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
	if request.Cache != nil {
		if err := request.Cache.validate(true); err != nil {
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
	if request.Cache != nil {
		fields["cache"] = request.Cache
	}
	return marshalObject(fields)
}
func (request *CompactRequestV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "api_version", "operation_key", "context", "parent", "policy", "cache"); err != nil {
		return err
	}
	version, err := requiredString(fields, "api_version")
	if err != nil || version != CompactAPIVersion {
		return fmt.Errorf("api_version %q is unsupported", version)
	}
	key, err := requiredString(fields, "operation_key")
	if err != nil {
		return err
	}
	parent, err := requiredString(fields, "parent")
	if err != nil {
		return err
	}
	contextRaw, err := requireField(fields, "context")
	if err != nil {
		return err
	}
	context, err := decodeRequestContextV1(contextRaw)
	if err != nil {
		return err
	}
	result := CompactRequestV1{APIVersion: version, OperationKey: key, Parent: CheckpointHandle(parent), Context: context}
	if !result.Parent.valid() {
		return fmt.Errorf("compact parent checkpoint is invalid")
	}
	if raw, ok := fields["policy"]; ok {
		if err := validateCompactPolicy(raw); err != nil {
			return err
		}
		result.Policy = copyRaw(raw)
	}
	if raw, ok := fields["cache"]; ok {
		var policy CachePolicyV1
		if err := json.Unmarshal(raw, &policy); err != nil {
			return err
		}
		if err := policy.validate(true); err != nil {
			return err
		}
		result.Cache = &policy
	}
	*request = result
	return nil
}

// validateCompactPolicy keeps compaction's control-plane policy separate from
// Generate settings. It intentionally has no tools, tool choice, output, or
// provider-specific fields: compaction is a lossily summarized child and
// application tool/structured-output configuration is restored by Generate.
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
		style, err := requiredString(map[string]json.RawMessage{"summary_style": value}, "summary_style")
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
	if response.Usage != nil {
		fields["usage"] = response.Usage
	}
	if response.Diagnostics != nil {
		fields["diagnostics"] = response.Diagnostics
	}
	return marshalObject(fields)
}

func (response *CompactResponseV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "api_version", "operation_key", "operation_id", "status", "checkpoint", "cache", "provenance", "usage", "cost", "diagnostics"); err != nil {
		return err
	}
	version, err := requiredString(fields, "api_version")
	if err != nil || version != CompactAPIVersion {
		return fmt.Errorf("api_version %q is unsupported", version)
	}
	key, err := requiredString(fields, "operation_key")
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
	var checkpoint CheckpointMetadata
	if err := json.Unmarshal(checkpointRaw, &checkpoint); err != nil || checkpoint.Kind != "compaction" {
		return fmt.Errorf("compact checkpoint must be compaction")
	}
	if checkpoint.Parent == nil || !checkpoint.Parent.valid() {
		return fmt.Errorf("compact checkpoint parent is invalid")
	}
	cacheRaw, err := requireField(fields, "cache")
	if err != nil {
		return err
	}
	var cache CacheDispositionV1
	if err := json.Unmarshal(cacheRaw, &cache); err != nil {
		return err
	}
	costRaw, err := requireField(fields, "cost")
	if err != nil {
		return err
	}
	var cost CostV1
	if err := json.Unmarshal(costRaw, &cost); err != nil {
		return err
	}
	result := CompactResponseV1{APIVersion: version, OperationKey: key, OperationID: id, Checkpoint: checkpoint, Cache: cache, Cost: cost}
	if raw, ok := fields["provenance"]; ok {
		if !validObjectJSON(raw) {
			return fmt.Errorf("provenance must be an object")
		}
		result.Provenance = copyRaw(raw)
	}
	if raw, ok := fields["usage"]; ok {
		var usage Usage
		if err := json.Unmarshal(raw, &usage); err != nil {
			return err
		}
		result.Usage = &usage
	}
	if raw, ok := fields["diagnostics"]; ok {
		var err error
		result.Diagnostics, err = decodeDiagnostics(raw)
		if err != nil {
			return err
		}
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
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "api_version", "operation_key", "context", "kind", "query"); err != nil {
		return err
	}
	version, err := requiredString(fields, "api_version")
	if err != nil || version != QueryAPIVersion {
		return fmt.Errorf("api_version %q is unsupported", version)
	}
	key, err := requiredString(fields, "operation_key")
	if err != nil {
		return err
	}
	kindValue, err := requiredString(fields, "kind")
	if err != nil {
		return err
	}
	kind := QueryKind(kindValue)
	if !kind.valid() {
		return fmt.Errorf("query kind %q is invalid", kind)
	}
	contextRaw, err := requireField(fields, "context")
	if err != nil {
		return err
	}
	context, err := decodeRequestContextV1(contextRaw)
	if err != nil {
		return err
	}
	if context.Tenant == "" || context.Project == "" || context.Actor == "" {
		return fmt.Errorf("context requires tenant, project, and actor")
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
		value, err := requiredString(map[string]json.RawMessage{"cursor": raw}, "cursor")
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
			if _, err := requiredString(map[string]json.RawMessage{name: raw}, name); err != nil {
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
		if err := validateQueryEnum(raw, "availability", "available", "degraded", "unavailable"); err != nil {
			return err
		}
	}
	if raw, ok := fields["lifecycle"]; ok {
		if err := validateQueryEnum(raw, "lifecycle", "available", "deprecated", "unavailable", "unknown"); err != nil {
			return err
		}
	}
	if kind == QuerySpendSummary {
		var start, end time.Time
		for _, name := range []string{"start_time", "end_time"} {
			value, err := requiredString(fields, name)
			if err != nil {
				return err
			}
			if err := validateQueryTimestamp(name, value); err != nil {
				return err
			}
			parsed, _ := time.Parse(time.RFC3339Nano, value)
			if name == "start_time" {
				start = parsed
			} else {
				end = parsed
			}
		}
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
	if kind == QueryBudgetStatus {
		if raw, ok := fields["active_at"]; ok {
			value, err := requiredString(map[string]json.RawMessage{"active_at": raw}, "active_at")
			if err != nil {
				return err
			}
			if err := validateQueryTimestamp("active_at", value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateQueryEnum(raw json.RawMessage, field string, allowed ...string) error {
	value, err := requiredString(map[string]json.RawMessage{field: raw}, field)
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
	if err := response.Cost.validate(); err != nil {
		return nil, err
	}
	actualCost := response.Cost.ActualCostUSD
	if actualCost != nil {
		canonical, err := canonicalDecimalString(*actualCost)
		if err != nil {
			return nil, err
		}
		actualCost = &canonical
	}
	fields := map[string]any{"api_version": QueryAPIVersion, "operation_key": response.OperationKey, "query_execution_id": response.QueryExecutionID, "kind": response.Kind, "observed_at": response.ObservedAt, "source": response.Source, "freshness": response.Freshness, "complete": response.Complete, "result": response.Result, "cost_status": response.Cost.Status, "actual_cost_usd": actualCost}
	if response.NextCursor != nil {
		fields["next_cursor"] = *response.NextCursor
	}
	if response.Cost.Method != "" {
		fields["cost_method"] = response.Cost.Method
	}
	if response.Cost.UnknownReason != "" {
		fields["cost_unknown_reason_code"] = response.Cost.UnknownReason
	}
	return marshalObject(fields)
}

func (response *QueryResponseV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "api_version", "operation_key", "query_execution_id", "kind", "observed_at", "source", "freshness", "complete", "next_cursor", "result", "cost_status", "actual_cost_usd", "cost_method", "cost_unknown_reason_code"); err != nil {
		return err
	}
	version, err := requiredString(fields, "api_version")
	if err != nil || version != QueryAPIVersion {
		return fmt.Errorf("api_version %q is unsupported", version)
	}
	key, err := requiredString(fields, "operation_key")
	if err != nil {
		return err
	}
	execution, err := requiredString(fields, "query_execution_id")
	if err != nil {
		return err
	}
	kindValue, err := requiredString(fields, "kind")
	if err != nil {
		return err
	}
	kind := QueryKind(kindValue)
	if !kind.valid() {
		return fmt.Errorf("query kind %q is invalid", kind)
	}
	observed, err := requiredString(fields, "observed_at")
	if err != nil {
		return err
	}
	source, err := requiredString(fields, "source")
	if err != nil {
		return err
	}
	freshness, err := requiredString(fields, "freshness")
	if err != nil {
		return err
	}
	complete, err := requiredBool(fields, "complete")
	if err != nil {
		return err
	}
	resultRaw, err := requireField(fields, "result")
	if err != nil {
		return err
	}
	var result QueryResult
	switch kind {
	case QueryProviderStatus:
		var value ProviderStatusPage
		err = json.Unmarshal(resultRaw, &value)
		result = value
	case QueryModelInventory:
		var value ModelInventoryPage
		err = json.Unmarshal(resultRaw, &value)
		result = value
	case QueryCreditStatus:
		var value CreditStatusPage
		err = json.Unmarshal(resultRaw, &value)
		result = value
	case QueryBudgetStatus:
		var value BudgetStatus
		err = json.Unmarshal(resultRaw, &value)
		result = value
	case QuerySpendSummary:
		var value SpendSummary
		err = json.Unmarshal(resultRaw, &value)
		result = value
	}
	if err != nil {
		return err
	}
	status, err := requiredString(fields, "cost_status")
	if err != nil {
		return err
	}
	cost := CostV1{Status: status}
	if raw, ok := fields["actual_cost_usd"]; ok && string(raw) != "null" {
		value, err := requiredString(map[string]json.RawMessage{"actual_cost_usd": raw}, "actual_cost_usd")
		if err != nil {
			return err
		}
		canonical, err := canonicalDecimalString(value)
		if err != nil {
			return err
		}
		cost.ActualCostUSD = &canonical
	}
	if cost.Method, _, err = optionalString(fields, "cost_method"); err != nil {
		return err
	}
	if cost.UnknownReason, _, err = optionalString(fields, "cost_unknown_reason_code"); err != nil {
		return err
	}
	if err := cost.validate(); err != nil {
		return err
	}
	responseValue := QueryResponseV1{APIVersion: version, OperationKey: key, QueryExecutionID: execution, Kind: kind, ObservedAt: observed, Source: source, Freshness: freshness, Complete: complete, Result: result, Cost: cost}
	if raw, ok := fields["next_cursor"]; ok && string(raw) != "null" {
		value, err := requiredString(map[string]json.RawMessage{"next_cursor": raw}, "next_cursor")
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
		budget, ok := response.Result.(BudgetStatus)
		if !ok {
			if pointer, pointerOK := response.Result.(*BudgetStatus); pointerOK && pointer != nil {
				budget, ok = *pointer, true
			}
		}
		if !ok {
			return fmt.Errorf("budget status result has unexpected type")
		}
		if err := validateQueryTimestamp("budget active_at", budget.ActiveAt); err != nil {
			return err
		}
	case QuerySpendSummary:
		spend, ok := response.Result.(SpendSummary)
		if !ok {
			if pointer, pointerOK := response.Result.(*SpendSummary); pointerOK && pointer != nil {
				spend, ok = *pointer, true
			}
		}
		if !ok {
			return fmt.Errorf("spend summary result has unexpected type")
		}
		if err := validateQueryTimestamp("spend start_time", spend.StartTime); err != nil {
			return err
		}
		if err := validateQueryTimestamp("spend end_time", spend.EndTime); err != nil {
			return err
		}
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

// ValidateVariantTemperature applies the part of cache validation that is
// locally knowable before inherited settings are materialized by the worker.
func ValidateVariantTemperature(variant int32, temperature *float64) error {
	if variant < 0 {
		return fmt.Errorf("variant must not be negative")
	}
	if temperature != nil && (*temperature < 0 || math.IsNaN(*temperature) || math.IsInf(*temperature, 0)) {
		return fmt.Errorf("temperature is invalid")
	}
	if temperature != nil && *temperature == 0 && variant != 0 {
		return fmt.Errorf("temperature zero requires variant zero")
	}
	return nil
}

// ValidateVariantDecimalTemperature applies the cache variant rule directly
// to the exact v1 wire decimal. This keeps validation independent of a
// float64 conversion (which may round an 18-digit value or overflow for a
// syntactically valid NUMERIC(38,18) value).
func ValidateVariantDecimalTemperature(variant int32, temperature *DecimalV1) error {
	if variant < 0 {
		return fmt.Errorf("variant must not be negative")
	}
	if temperature == nil {
		return nil
	}
	canonical, err := NewDecimalV1(temperature.String())
	if err != nil {
		return fmt.Errorf("temperature is invalid: %w", err)
	}
	if canonical == "0" && variant != 0 {
		return fmt.Errorf("temperature zero requires variant zero")
	}
	return nil
}
