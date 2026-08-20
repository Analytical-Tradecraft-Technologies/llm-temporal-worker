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

func validateCodePointLength(name, value string, minimum, maximum int) error {
	length := utf8.RuneCountInString(value)
	if length < minimum || length > maximum {
		return fmt.Errorf("%s contains %d Unicode code points; must contain between %d and %d", name, length, minimum, maximum)
	}
	return nil
}
