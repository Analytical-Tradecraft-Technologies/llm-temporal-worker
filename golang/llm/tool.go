package llm

import (
	"encoding/json"
	"fmt"
)

type ToolKind string

const (
	ToolKindFunction  ToolKind = "function"
	ToolKindProvider  ToolKind = "provider"
	ToolKindRemoteMCP ToolKind = "remote_mcp"
)

var toolKinds = []ToolKind{ToolKindFunction, ToolKindProvider, ToolKindRemoteMCP}

type Tool struct {
	Kind         ToolKind
	Name         string
	Description  string
	InputSchema  json.RawMessage
	OutputSchema json.RawMessage
}

func (tool Tool) MarshalJSON() ([]byte, error) {
	if _, err := defaultEnum(tool.Kind, ToolKindFunction, "tool kind", toolKinds...); err != nil {
		return nil, err
	}
	if err := validateToolName(tool.Name); err != nil {
		return nil, err
	}
	if !validObjectJSON(tool.InputSchema) {
		return nil, fmt.Errorf("tool %q input_schema must be a JSON object", tool.Name)
	}
	fields := map[string]any{
		"name":         tool.Name,
		"description":  tool.Description,
		"input_schema": copyRaw(tool.InputSchema),
	}
	if tool.Kind != "" && tool.Kind != ToolKindFunction {
		fields["kind"] = tool.Kind
	}
	if len(tool.OutputSchema) > 0 {
		if !validObjectJSON(tool.OutputSchema) {
			return nil, fmt.Errorf("tool %q output_schema must be a JSON object", tool.Name)
		}
		fields["output_schema"] = copyRaw(tool.OutputSchema)
	}
	return marshalObject(fields)
}

func decodeTool(data []byte) (Tool, error) {
	fields, err := decodeClosedObject(data, "kind", "name", "description", "input_schema", "output_schema")
	if err != nil {
		return Tool{}, err
	}
	reader := fieldReader{fields: fields}
	kind := readEnum(&reader, "kind", ToolKindFunction, "tool kind", toolKinds...)
	name := reader.requiredString("name")
	if reader.err != nil {
		return Tool{}, reader.err
	}
	if err := validateToolName(name); err != nil {
		return Tool{}, err
	}
	// The schema requires description and the encoder always writes it, so
	// the decoder requires it too (#1662).
	description := reader.requiredString("description")
	inputSchema := reader.requireField("input_schema")
	if reader.err != nil {
		return Tool{}, reader.err
	}
	if !validObjectJSON(inputSchema) {
		return Tool{}, fmt.Errorf("tool %q input_schema must be a JSON object", name)
	}
	var outputSchema json.RawMessage
	if value, ok := fields["output_schema"]; ok {
		if !validObjectJSON(value) {
			return Tool{}, fmt.Errorf("tool %q output_schema must be a JSON object", name)
		}
		outputSchema = copyRaw(value)
	}
	return Tool{
		Kind:         kind,
		Name:         name,
		Description:  description,
		InputSchema:  copyRaw(inputSchema),
		OutputSchema: outputSchema,
	}, nil
}

type ToolChoiceMode string

const (
	ToolChoiceNone     ToolChoiceMode = "none"
	ToolChoiceAuto     ToolChoiceMode = "auto"
	ToolChoiceRequired ToolChoiceMode = "required"
	ToolChoiceNamed    ToolChoiceMode = "named"
)

type ToolPolicy struct {
	Mode     ToolChoiceMode
	Name     string
	Parallel bool
}

func (policy ToolPolicy) MarshalJSON() ([]byte, error) {
	mode, err := defaultEnum(policy.Mode, ToolChoiceAuto, "tool policy mode", toolChoiceModes...)
	if err != nil {
		return nil, err
	}
	if err := validateToolPolicyName(mode, policy.Name); err != nil {
		return nil, err
	}
	fields := map[string]any{
		"mode":     mode,
		"parallel": policy.Parallel,
	}
	setIfNotEmpty(fields, "name", policy.Name)
	return marshalObject(fields)
}

var toolChoiceModes = []ToolChoiceMode{ToolChoiceNone, ToolChoiceAuto, ToolChoiceRequired, ToolChoiceNamed}

// validateToolPolicyName requires a valid tool name in named mode and no
// name in any other mode.
func validateToolPolicyName(mode ToolChoiceMode, name string) error {
	if mode == ToolChoiceNamed {
		if err := validateToolName(name); err != nil {
			return fmt.Errorf("named tool policy: %w", err)
		}
	} else if name != "" {
		return fmt.Errorf("tool policy name is only valid with named mode")
	}
	return nil
}

// decodeToolPolicyPatch decodes a settings patch value. The published schema
// requires mode and parallel there, so a partial object is rejected instead of
// being completed with defaults; decodeToolPolicy keeps those defaults for the
// provider-neutral request model.
func decodeToolPolicyPatch(data []byte) (ToolPolicy, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return ToolPolicy{}, err
	}
	for _, name := range []string{"mode", "parallel"} {
		if _, ok := fields[name]; !ok {
			return ToolPolicy{}, fmt.Errorf("tool policy %s is required", name)
		}
	}
	return decodeToolPolicy(data)
}

func decodeToolPolicy(data []byte) (ToolPolicy, error) {
	fields, err := decodeClosedObject(data, "mode", "name", "parallel")
	if err != nil {
		return ToolPolicy{}, err
	}
	reader := fieldReader{fields: fields}
	policy := ToolPolicy{
		Mode:     readEnum(&reader, "mode", ToolChoiceAuto, "tool policy mode", toolChoiceModes...),
		Name:     reader.optionalString("name"),
		Parallel: reader.optionalBool("parallel"),
	}
	if reader.err != nil {
		return ToolPolicy{}, reader.err
	}
	if err := validateToolPolicyName(policy.Mode, policy.Name); err != nil {
		return ToolPolicy{}, err
	}
	return policy, nil
}

func validObjectJSON(value json.RawMessage) bool {
	if !validRawJSON(value) {
		return false
	}
	_, err := decodeObject(value)
	return err == nil
}
