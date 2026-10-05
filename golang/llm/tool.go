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

type Tool struct {
	Kind         ToolKind
	Name         string
	Description  string
	InputSchema  json.RawMessage
	OutputSchema json.RawMessage
}

func (tool Tool) MarshalJSON() ([]byte, error) {
	if tool.Kind != "" && tool.Kind != ToolKindFunction && tool.Kind != ToolKindProvider && tool.Kind != ToolKindRemoteMCP {
		return nil, fmt.Errorf("tool kind %q is invalid", tool.Kind)
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
	fields, err := decodeObject(data)
	if err != nil {
		return Tool{}, err
	}
	if err := checkUnknownFields(fields, "kind", "name", "description", "input_schema", "output_schema"); err != nil {
		return Tool{}, err
	}
	kindValue, _, err := optionalString(fields, "kind")
	if err != nil {
		return Tool{}, err
	}
	kind := ToolKind(kindValue)
	if kind == "" {
		kind = ToolKindFunction
	}
	if kind != ToolKindFunction && kind != ToolKindProvider && kind != ToolKindRemoteMCP {
		return Tool{}, fmt.Errorf("tool kind %q is invalid", kind)
	}
	name, err := requiredString(fields, "name")
	if err != nil {
		return Tool{}, err
	}
	if err := validateToolName(name); err != nil {
		return Tool{}, err
	}
	description, _, err := optionalString(fields, "description")
	if err != nil {
		return Tool{}, err
	}
	inputSchema, err := requireField(fields, "input_schema")
	if err != nil {
		return Tool{}, err
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
	mode := policy.Mode
	if mode == "" {
		mode = ToolChoiceAuto
	}
	if mode != ToolChoiceNone && mode != ToolChoiceAuto && mode != ToolChoiceRequired && mode != ToolChoiceNamed {
		return nil, fmt.Errorf("tool policy mode %q is invalid", mode)
	}
	if mode == ToolChoiceNamed {
		if err := validateToolName(policy.Name); err != nil {
			return nil, fmt.Errorf("named tool policy: %w", err)
		}
	} else if policy.Name != "" {
		return nil, fmt.Errorf("tool policy name is only valid with named mode")
	}
	fields := map[string]any{
		"mode":     mode,
		"parallel": policy.Parallel,
	}
	if policy.Name != "" {
		fields["name"] = policy.Name
	}
	return marshalObject(fields)
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
	fields, err := decodeObject(data)
	if err != nil {
		return ToolPolicy{}, err
	}
	if err := checkUnknownFields(fields, "mode", "name", "parallel"); err != nil {
		return ToolPolicy{}, err
	}
	modeValue, present, err := optionalString(fields, "mode")
	if err != nil {
		return ToolPolicy{}, err
	}
	mode := ToolChoiceMode(modeValue)
	if !present || mode == "" {
		mode = ToolChoiceAuto
	}
	if mode != ToolChoiceNone && mode != ToolChoiceAuto && mode != ToolChoiceRequired && mode != ToolChoiceNamed {
		return ToolPolicy{}, fmt.Errorf("tool policy mode %q is invalid", mode)
	}
	name, _, err := optionalString(fields, "name")
	if err != nil {
		return ToolPolicy{}, err
	}
	parallel, _, err := optionalBool(fields, "parallel")
	if err != nil {
		return ToolPolicy{}, err
	}
	policy := ToolPolicy{Mode: mode, Name: name, Parallel: parallel}
	if mode == ToolChoiceNamed {
		if err := validateToolName(name); err != nil {
			return ToolPolicy{}, fmt.Errorf("named tool policy: %w", err)
		}
	} else if name != "" {
		return ToolPolicy{}, fmt.Errorf("tool policy name is only valid with named mode")
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
