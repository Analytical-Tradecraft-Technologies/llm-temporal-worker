package llm

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

const (
	generateOperationKeyMaxCodePoints = 256
	checkpointHandleMaxCodePoints     = 512
	generateAppendMaxItems            = 10000
	generateContextMaxCodePoints      = 256
	generateModelMaxCodePoints        = 256
)

// validateGenerateRequestV1Bounds keeps the Go codec aligned with JSON
// Schema's minLength, maxLength, and maxItems keywords. JSON Schema measures
// string lengths in Unicode code points rather than UTF-8 bytes.
func validateGenerateRequestV1Bounds(request GenerateRequestV1) error {
	if err := validateCodePointLength("operation_key", request.OperationKey, 1, generateOperationKeyMaxCodePoints); err != nil {
		return err
	}
	if request.Parent != nil {
		if err := validateCodePointLength("parent", string(*request.Parent), 1, checkpointHandleMaxCodePoints); err != nil {
			return err
		}
	}
	if len(request.Append) > generateAppendMaxItems {
		return fmt.Errorf("append contains %d items; maximum is %d", len(request.Append), generateAppendMaxItems)
	}
	for _, component := range []struct {
		name  string
		value string
	}{
		{name: "context.tenant", value: request.Context.Tenant},
		{name: "context.project", value: request.Context.Project},
		{name: "context.actor", value: request.Context.Actor},
	} {
		if err := validateCodePointLength(component.name, component.value, 1, generateContextMaxCodePoints); err != nil {
			return err
		}
	}
	if request.SettingsPatch.Model.Set != nil {
		if err := validateCodePointLength("settings_patch.model.set", *request.SettingsPatch.Model.Set, 1, generateModelMaxCodePoints); err != nil {
			return err
		}
	}
	return nil
}

// validateCompactRequestV1Bounds applies the Compact schema's code-point
// bounds, which match Generate's (#1662).
func validateCompactRequestV1Bounds(request CompactRequestV1) error {
	if err := validateCodePointLength("operation_key", request.OperationKey, 1, generateOperationKeyMaxCodePoints); err != nil {
		return err
	}
	for _, component := range []struct {
		name  string
		value string
	}{
		{name: "context.tenant", value: request.Context.Tenant},
		{name: "context.project", value: request.Context.Project},
		{name: "context.actor", value: request.Context.Actor},
	} {
		if err := validateCodePointLength(component.name, component.value, 1, generateContextMaxCodePoints); err != nil {
			return err
		}
	}
	return nil
}

// compactionPolicyPatchRanges are the published schema's integer bounds for
// settings_patch.compaction_policy.set.
var compactionPolicyPatchRanges = map[string][2]int{
	"trigger_tokens":                  {1, 10000000},
	"target_tokens":                   {1, 9999999},
	"recent_turns":                    {0, 1000},
	"output_reserve_tokens":           {1, 1000000},
	"materialization_threshold_bytes": {0, 1073741824},
}

// validateCompactionPolicyPatchV1 decodes settings_patch.compaction_policy.set
// against the published schema's closed field set and value ranges, so a bad
// policy fails at decode with its field rather than as a content-free
// preparation failure (#1662). Whether target_tokens is below trigger_tokens
// after defaults apply is still checked during request preparation.
func validateCompactionPolicyPatchV1(raw json.RawMessage) error {
	fields, err := decodeObject(raw)
	if err != nil {
		return fmt.Errorf("compaction_policy.set must be an object")
	}
	allowed := []string{"version", "prompt_version", "summary_style"}
	for name := range compactionPolicyPatchRanges {
		allowed = append(allowed, name)
	}
	if err := checkUnknownFields(fields, allowed...); err != nil {
		return fmt.Errorf("compaction_policy.set: %w", err)
	}
	for name, want := range map[string]string{"version": "generic-2026-07-18", "prompt_version": "v1"} {
		if value, ok := fields[name]; ok {
			if text, err := requiredString(map[string]json.RawMessage{name: value}, name); err != nil || text != want {
				return fmt.Errorf("compaction_policy.set.%s must be %q", name, want)
			}
		}
	}
	if value, ok := fields["summary_style"]; ok {
		style, err := requiredString(map[string]json.RawMessage{"summary_style": value}, "summary_style")
		if err != nil || (style != "concise" && style != "balanced" && style != "detailed") {
			return fmt.Errorf("compaction_policy.set.summary_style must be concise, balanced or detailed")
		}
	}
	for name, bounds := range compactionPolicyPatchRanges {
		value, ok := fields[name]
		if !ok {
			continue
		}
		number, err := decodeInt(value)
		if err != nil || number < bounds[0] || number > bounds[1] {
			return fmt.Errorf("compaction_policy.set.%s must be an integer between %d and %d", name, bounds[0], bounds[1])
		}
	}
	return nil
}

// validateGenerateRequestV1MediaURLs is the request ingress point of the
// remote-media URL policy. It covers every position where a Generate request
// can introduce content: appended items and instructions set by the settings
// patch. Compact requests carry no content.
func validateGenerateRequestV1MediaURLs(request GenerateRequestV1) error {
	var instructions []Instruction
	if request.SettingsPatch.Instructions.Set != nil {
		instructions = *request.SettingsPatch.Instructions.Set
	}
	return ValidateMediaURLs(instructions, request.Append)
}

// validateGenerateRequestV1Content rejects empty message content, empty
// instructions, and empty text parts at request ingress. Every provider
// family rejects them (Azure requires a non-empty content array and
// Anthropic requires non-empty text blocks), so admitting them only defers an
// invalid_argument failure to the provider. Like the media URL policy, this
// is a request check rather than an item decode check, so stored transcripts
// that contain empty model output stay decodable.
func validateGenerateRequestV1Content(request GenerateRequestV1) error {
	if request.SettingsPatch.Instructions.Set != nil {
		for index, instruction := range *request.SettingsPatch.Instructions.Set {
			if err := validateInstructionContent(instruction); err != nil {
				return fmt.Errorf("settings_patch.instructions.set %d: %w", index, err)
			}
		}
	}
	for index, item := range request.Append {
		var content []Part
		switch item := item.(type) {
		case Message:
			content = item.Content
		case *Message:
			if item == nil {
				continue
			}
			content = item.Content
		default:
			continue
		}
		if len(content) == 0 {
			return fmt.Errorf("append %d: message content must not be empty", index)
		}
		if err := rejectEmptyTextParts(content); err != nil {
			return fmt.Errorf("append %d: %w", index, err)
		}
	}
	return nil
}

func validateInstructionContent(instruction Instruction) error {
	kind := instruction.Kind
	if kind == "" && len(instruction.Content) == 0 {
		kind = InstructionKindText
	}
	if kind == InstructionKindText && len(instruction.Content) == 0 {
		if instruction.Text == "" {
			return fmt.Errorf("instruction text must not be empty")
		}
		return nil
	}
	if len(instruction.Content) == 0 {
		return fmt.Errorf("instruction content must not be empty")
	}
	return rejectEmptyTextParts(instruction.Content)
}

func rejectEmptyTextParts(parts []Part) error {
	for index, part := range parts {
		empty := false
		switch part := part.(type) {
		case TextPart:
			empty = part.Text == ""
		case *TextPart:
			empty = part != nil && part.Text == ""
		}
		if empty {
			return fmt.Errorf("part %d: text must not be empty", index)
		}
	}
	return nil
}

func validateCodePointLength(name, value string, minimum, maximum int) error {
	length := utf8.RuneCountInString(value)
	if length < minimum || length > maximum {
		return fmt.Errorf("%s contains %d Unicode code points; must contain between %d and %d", name, length, minimum, maximum)
	}
	return nil
}
