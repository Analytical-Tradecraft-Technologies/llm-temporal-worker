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
	"io"
	"sort"
)

// Decode retains exact JSON numeric literals and rejects trailing values.
func Decode(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

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
	if err := Decode(data, &body); err != nil {
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

// OpenAIOverrides orders schemas from the actual serialized SDK request. Its
// validated wire override can retain numbers that the SDK's typed maps round.
func OpenAIOverrides(params any, responses bool) ([]Override, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	var body struct {
		ResponseFormat json.RawMessage `json:"response_format"`
		Text           json.RawMessage `json:"text"`
		Tools          []struct {
			Function *struct {
				Parameters map[string]any `json:"parameters"`
			} `json:"function"`
			Parameters   map[string]any `json:"parameters"`
			OutputSchema map[string]any `json:"output_schema"`
		} `json:"tools"`
	}
	if err := Decode(data, &body); err != nil {
		return nil, err
	}
	var result []Override
	appendSchema := func(path string, schema map[string]any) error {
		if schema == nil {
			return nil
		}
		ordered, err := Ordered(schema)
		if err != nil {
			return err
		}
		result = append(result, Override{Path: path, Value: ordered})
		return nil
	}
	if responses {
		if raw := bytes.TrimSpace(body.Text); len(raw) > 0 && raw[0] == '{' {
			var text struct {
				Format *struct {
					Schema map[string]any `json:"schema"`
				} `json:"format"`
			}
			if err := Decode(raw, &text); err != nil {
				return nil, err
			}
			if text.Format != nil {
				if err := appendSchema("text.format.schema", text.Format.Schema); err != nil {
					return nil, err
				}
			}
		}
	} else if raw := bytes.TrimSpace(body.ResponseFormat); len(raw) > 0 && raw[0] == '{' {
		var format struct {
			JSONSchema *struct {
				Schema map[string]any `json:"schema"`
			} `json:"json_schema"`
		}
		if err := Decode(raw, &format); err != nil {
			return nil, err
		}
		if format.JSONSchema != nil {
			if err := appendSchema("response_format.json_schema.schema", format.JSONSchema.Schema); err != nil {
				return nil, err
			}
		}
	}
	for index, tool := range body.Tools {
		if responses {
			if err := appendSchema(fmt.Sprintf("tools.%d.parameters", index), tool.Parameters); err != nil {
				return nil, err
			}
			if err := appendSchema(fmt.Sprintf("tools.%d.output_schema", index), tool.OutputSchema); err != nil {
				return nil, err
			}
		} else if tool.Function != nil {
			if err := appendSchema(fmt.Sprintf("tools.%d.function.parameters", index), tool.Function.Parameters); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}
