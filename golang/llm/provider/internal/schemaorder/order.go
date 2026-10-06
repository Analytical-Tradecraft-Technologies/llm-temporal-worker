// Package schemaorder serializes a JSON Schema with a deliberate property
// order.
//
// Providers that generate structured output in schema property order emit
// fields in the order the schema lists them, so a reasoning-first schema
// depends on it. The worker's canonical request form sorts object keys, which
// loses the caller's order, and the provider SDKs re-encode schemas from maps.
// Arrays keep their order, so each object's "properties" are written in the
// order of its sibling "required" array, then any remaining properties in
// sorted order. Every other object is written with sorted keys.
package schemaorder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// Ordered returns schema re-encoded with properties ordered by "required".
func Ordered(schema any) (json.RawMessage, error) {
	data, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := encode(&out, value); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func encode(out *bytes.Buffer, value any) error {
	switch typed := value.(type) {
	case map[string]any:
		return encodeObject(out, typed, sortedKeys(typed))
	case []any:
		out.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				out.WriteByte(',')
			}
			if err := encode(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
		return nil
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return err
		}
		out.Write(data)
		return nil
	}
}

func encodeObject(out *bytes.Buffer, object map[string]any, keys []string) error {
	out.WriteByte('{')
	for index, key := range keys {
		if index > 0 {
			out.WriteByte(',')
		}
		name, err := json.Marshal(key)
		if err != nil {
			return err
		}
		out.Write(name)
		out.WriteByte(':')
		if properties, ok := object[key].(map[string]any); ok && key == "properties" {
			if err := encodeProperties(out, properties, object["required"]); err != nil {
				return err
			}
			continue
		}
		if err := encode(out, object[key]); err != nil {
			return err
		}
	}
	out.WriteByte('}')
	return nil
}

// encodeProperties writes a properties map in "required" order, then the rest
// sorted. Each property schema is itself encoded recursively.
func encodeProperties(out *bytes.Buffer, properties map[string]any, required any) error {
	keys := make([]string, 0, len(properties))
	seen := make(map[string]struct{}, len(properties))
	if list, ok := required.([]any); ok {
		for _, item := range list {
			name, ok := item.(string)
			if !ok {
				continue
			}
			if _, exists := properties[name]; !exists {
				continue
			}
			if _, duplicate := seen[name]; duplicate {
				continue
			}
			seen[name] = struct{}{}
			keys = append(keys, name)
		}
	}
	for _, name := range sortedKeys(properties) {
		if _, done := seen[name]; !done {
			keys = append(keys, name)
		}
	}
	if len(keys) != len(properties) {
		return fmt.Errorf("schema property order lost a property")
	}
	return encodeObject(out, properties, keys)
}

func sortedKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Override is one ordered schema to write into a request body at Path.
type Override struct {
	Path  string
	Value json.RawMessage
}

// MessagesOverrides returns ordered replacements for the output schema
// (output_config.format.schema) and each tool's input_schema in an Anthropic
// Messages request body, whichever SDK encodes it.
func MessagesOverrides(params any) ([]Override, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	var body struct {
		OutputConfig *struct {
			Format *struct {
				Schema map[string]any `json:"schema"`
			} `json:"format"`
		} `json:"output_config"`
		Tools []struct {
			InputSchema map[string]any `json:"input_schema"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, err
	}
	var overrides []Override
	if body.OutputConfig != nil && body.OutputConfig.Format != nil && body.OutputConfig.Format.Schema != nil {
		ordered, err := Ordered(body.OutputConfig.Format.Schema)
		if err != nil {
			return nil, err
		}
		overrides = append(overrides, Override{Path: "output_config.format.schema", Value: ordered})
	}
	for index, tool := range body.Tools {
		if tool.InputSchema == nil {
			continue
		}
		ordered, err := Ordered(tool.InputSchema)
		if err != nil {
			return nil, err
		}
		overrides = append(overrides, Override{Path: fmt.Sprintf("tools.%d.input_schema", index), Value: ordered})
	}
	return overrides, nil
}
