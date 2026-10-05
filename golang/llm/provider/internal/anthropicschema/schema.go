// Package anthropicschema adapts caller output schemas to the JSON Schema
// subset Claude structured outputs accept, for the Anthropic and Bedrock
// Messages adapters.
package anthropicschema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	llmschema "github.com/mfow/llm-temporal-worker/golang/llm/schema"
)

// UnsupportedError reports a schema the provider cannot represent even after
// lowering. Path is a JSON pointer into the caller schema.
type UnsupportedError struct {
	Path   string
	Reason string
}

func (err *UnsupportedError) Error() string {
	path := err.Path
	if path == "" {
		path = "/"
	}
	return fmt.Sprintf("output schema at %s: %s", path, err.Reason)
}

// supportedKeywords and supportedStringFormats mirror the transform the
// official SDK applies (anthropic-sdk-go schemautil.go supportedSchemaKeys and
// supportedStringFormats). Every other keyword returns a 400 from the
// provider, so it is moved into the description instead.
var supportedKeywords = map[string]struct{}{
	"$ref": {}, "$defs": {}, "type": {}, "anyOf": {}, "oneOf": {}, "allOf": {},
	"description": {}, "title": {}, "enum": {}, "const": {},
	"properties": {}, "additionalProperties": {}, "required": {},
	"items": {}, "minItems": {}, "format": {}, "pattern": {},
}

var supportedStringFormats = map[string]struct{}{
	"date-time": {}, "time": {}, "date": {}, "duration": {}, "email": {},
	"hostname": {}, "uri": {}, "ipv4": {}, "ipv6": {}, "uuid": {},
}

// identifierKeywords carry no constraint and are dropped without a note.
var identifierKeywords = map[string]struct{}{"$schema": {}, "$id": {}, "$anchor": {}, "$comment": {}}

const defsPrefix = "#/$defs/"

// Lower returns the provider-valid form of a caller output schema. Keywords
// the provider rejects are moved into the description so the model still sees
// them, and objects are closed with additionalProperties:false. The provider
// therefore enforces a looser schema than the caller wrote: the caller schema
// must still be applied to the final JSON with Validate.
//
// An explicitly open object cannot be represented. It is rejected in strict
// mode; best-effort closes it when it declares properties, which only narrows
// the answer. Recursive schemas are rejected in both modes.
func Lower(schema json.RawMessage, strict bool) (map[string]any, error) {
	compiled, err := llmschema.Parse(schema)
	if err != nil {
		return nil, fmt.Errorf("output schema: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(compiled.Canonical()))
	decoder.UseNumber()
	var root map[string]any
	if err := decoder.Decode(&root); err != nil || root == nil {
		return nil, fmt.Errorf("output schema must be an object")
	}
	if err := rejectRecursion(root); err != nil {
		return nil, err
	}
	return lowerSchema(root, "", strict)
}

// Validate applies the caller schema to the first model text of a completed
// response, exactly as the Responses and Chat lifts do.
func Validate(schema json.RawMessage, output []llm.Item) error {
	content, ok := firstModelText(output)
	if !ok {
		return fmt.Errorf("provider response did not contain JSON text content")
	}
	compiled, err := llmschema.Parse(schema)
	if err != nil {
		return fmt.Errorf("response schema validation setup: %w", err)
	}
	if err := compiled.Validate([]byte(content)); err != nil {
		return fmt.Errorf("provider JSON response does not satisfy schema: %w", err)
	}
	return nil
}

func firstModelText(output []llm.Item) (string, bool) {
	for _, item := range output {
		message, ok := item.(llm.Message)
		if !ok || message.Actor != llm.ActorModel {
			continue
		}
		for _, part := range message.Content {
			if text, ok := part.(llm.TextPart); ok {
				return text.Text, true
			}
		}
	}
	return "", false
}

func lowerSchema(source map[string]any, path string, strict bool) (map[string]any, error) {
	if ref, ok := source["$ref"]; ok {
		// The provider does not accept $ref alongside other keywords; only
		// the definitions it may point at are kept.
		result := map[string]any{"$ref": ref}
		if defs, ok := source["$defs"].(map[string]any); ok {
			lowered := make(map[string]any, len(defs))
			for name, child := range defs {
				nested, err := lowerChild(child, pointerJoin(pointerJoin(path, "$defs"), name), strict)
				if err != nil {
					return nil, err
				}
				lowered[name] = nested
			}
			result["$defs"] = lowered
		}
		return result, nil
	}
	result := make(map[string]any, len(source))
	extras := make(map[string]any)
	for key, value := range source {
		if _, drop := identifierKeywords[key]; drop {
			continue
		}
		if _, ok := supportedKeywords[key]; !ok {
			extras[key] = value
			continue
		}
		childPath := pointerJoin(path, key)
		switch key {
		case "$defs", "properties":
			children, _ := value.(map[string]any)
			lowered := make(map[string]any, len(children))
			for name, child := range children {
				nested, err := lowerChild(child, pointerJoin(childPath, name), strict)
				if err != nil {
					return nil, err
				}
				lowered[name] = nested
			}
			result[key] = lowered
		case "items":
			nested, err := lowerChild(value, childPath, strict)
			if err != nil {
				return nil, err
			}
			result[key] = nested
		case "anyOf", "allOf", "oneOf":
			children, _ := value.([]any)
			lowered := make([]any, 0, len(children))
			for index, child := range children {
				nested, err := lowerChild(child, fmt.Sprintf("%s/%d", childPath, index), strict)
				if err != nil {
					return nil, err
				}
				lowered = append(lowered, nested)
			}
			result[key] = lowered
		case "minItems":
			if number, ok := value.(json.Number); ok && (number.String() == "0" || number.String() == "1") {
				result[key] = value
			} else {
				extras[key] = value
			}
		case "format":
			name, _ := value.(string)
			if _, ok := supportedStringFormats[name]; ok {
				result[key] = value
			} else {
				extras[key] = value
			}
		case "additionalProperties":
			// Decided below, once the object's declared properties are known.
		default:
			result[key] = value
		}
	}
	// oneOf is not accepted; anyOf is the nearest looser form, and the caller
	// schema still enforces exclusivity on the final JSON.
	if variants, ok := result["oneOf"]; ok {
		if _, both := result["anyOf"]; both {
			extras["oneOf"] = source["oneOf"]
		} else {
			result["anyOf"] = variants
		}
		delete(result, "oneOf")
	}
	if isObjectSchema(source) {
		properties, _ := result["properties"].(map[string]any)
		if additional, present := source["additionalProperties"]; present && additional != false {
			if strict {
				return nil, &UnsupportedError{Path: pointerJoin(path, "additionalProperties"), Reason: "additionalProperties other than false is not supported by Claude structured output"}
			}
			if len(properties) == 0 {
				// Closing it would constrain the answer to {}.
				return nil, &UnsupportedError{Path: pointerJoin(path, "additionalProperties"), Reason: "an open object without declared properties is not supported by Claude structured output"}
			}
			extras["additionalProperties"] = additional
		}
		if properties == nil {
			result["properties"] = map[string]any{}
		}
		result["additionalProperties"] = false
	} else if additional, present := source["additionalProperties"]; present {
		extras["additionalProperties"] = additional
	}
	if len(extras) > 0 {
		note, err := describeExtras(extras)
		if err != nil {
			return nil, fmt.Errorf("output schema at %s: %w", displayPath(path), err)
		}
		if description, ok := result["description"].(string); ok && description != "" {
			note = description + "\n\n" + note
		}
		result["description"] = note
	}
	return result, nil
}

func lowerChild(value any, path string, strict bool) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, &UnsupportedError{Path: path, Reason: "schema must be an object"}
	}
	return lowerSchema(object, path, strict)
}

func isObjectSchema(schema map[string]any) bool {
	if _, ok := schema["properties"]; ok {
		return true
	}
	switch value := schema["type"].(type) {
	case string:
		return value == "object"
	case []any:
		for _, item := range value {
			if item == "object" {
				return true
			}
		}
	}
	return false
}

// describeExtras renders moved keywords in the SDK's "{key: value, ...}" form
// with sorted keys, so equal schemas always lower to equal bytes.
func describeExtras(extras map[string]any) (string, error) {
	keys := make([]string, 0, len(extras))
	for key := range extras {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		rendered, ok := extras[key].(string)
		if !ok {
			var buffer bytes.Buffer
			encoder := json.NewEncoder(&buffer)
			encoder.SetEscapeHTML(false)
			if err := encoder.Encode(extras[key]); err != nil {
				return "", err
			}
			rendered = strings.TrimSuffix(buffer.String(), "\n")
		}
		parts = append(parts, key+": "+rendered)
	}
	return "{" + strings.Join(parts, ", ") + "}", nil
}

// rejectRecursion walks the reference graph. Nodes are the schema root and
// each #/$defs entry; an edge is a $ref anywhere inside a node. References
// that are not a direct #/$defs entry would not survive lowering and are
// rejected too.
func rejectRecursion(root map[string]any) error {
	defs, _ := root["$defs"].(map[string]any)
	const visiting, done = 1, 2
	state := map[string]int{}
	var visit func(node any, path string) error
	visit = func(node any, path string) error {
		schema, ok := node.(map[string]any)
		if !ok {
			return nil
		}
		keys := make([]string, 0, len(schema))
		for key := range schema {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			childPath := pointerJoin(path, key)
			switch key {
			case "$defs", "properties":
				children, _ := schema[key].(map[string]any)
				names := make([]string, 0, len(children))
				for name := range children {
					names = append(names, name)
				}
				sort.Strings(names)
				for _, name := range names {
					if err := visit(children[name], pointerJoin(childPath, name)); err != nil {
						return err
					}
				}
			case "additionalProperties", "propertyNames", "items", "not", "if", "then", "else":
				if err := visit(schema[key], childPath); err != nil {
					return err
				}
			case "prefixItems", "allOf", "anyOf", "oneOf":
				children, _ := schema[key].([]any)
				for index, child := range children {
					if err := visit(child, fmt.Sprintf("%s/%d", childPath, index)); err != nil {
						return err
					}
				}
			case "$ref":
				ref, _ := schema[key].(string)
				if ref == "#" {
					return &UnsupportedError{Path: childPath, Reason: "recursive schemas are not supported by Claude structured output"}
				}
				name := strings.ReplaceAll(strings.ReplaceAll(strings.TrimPrefix(ref, defsPrefix), "~1", "/"), "~0", "~")
				target, ok := defs[name]
				if !strings.HasPrefix(ref, defsPrefix) || !ok {
					return &UnsupportedError{Path: childPath, Reason: "only #/$defs/<name> references are supported by Claude structured output"}
				}
				switch state[name] {
				case visiting:
					return &UnsupportedError{Path: childPath, Reason: "recursive schemas are not supported by Claude structured output"}
				case done:
					continue
				}
				state[name] = visiting
				if err := visit(target, pointerJoin("/$defs", name)); err != nil {
					return err
				}
				state[name] = done
			}
		}
		return nil
	}
	return visit(root, "")
}

func pointerJoin(base, token string) string {
	return base + "/" + strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
}

func displayPath(path string) string {
	if path == "" {
		return "/"
	}
	return path
}
