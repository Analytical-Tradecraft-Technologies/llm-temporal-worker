package llm

import (
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
