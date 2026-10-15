// Package openaischema adapts caller output schemas to the JSON Schema subset
// OpenAI strict structured outputs accept, and checks the JSON-mode context
// rule, for the OpenAI Responses and Chat Completions adapters.
package openaischema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	llmschema "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

// UnsupportedError reports a request OpenAI structured output cannot accept
// even after lowering. Path is a JSON pointer into the caller schema, or empty
// when the request as a whole is at fault.
type UnsupportedError struct {
	Path   string
	Reason string
}

func (err *UnsupportedError) Error() string {
	if err.Path == "" {
		return err.Reason
	}
	return fmt.Sprintf("output schema at %s: %s", err.Path, err.Reason)
}

// supportedKeywords is the strict subset both OpenAI and Azure OpenAI accept.
// OpenAI also enforces some type-specific constraints (pattern, format,
// numeric bounds, item counts) that Azure's structured outputs reject, so
// those move into the description on every route, like any other keyword
// outside the subset that would otherwise be rejected with a 400.
var supportedKeywords = map[string]struct{}{
	"$ref": {}, "$defs": {}, "type": {}, "anyOf": {}, "oneOf": {},
	"description": {}, "title": {}, "enum": {}, "const": {},
	"properties": {}, "additionalProperties": {}, "required": {},
	"items": {},
}

// identifierKeywords carry no constraint and are dropped without a note.
var identifierKeywords = map[string]struct{}{"$schema": {}, "$id": {}, "$anchor": {}, "$comment": {}}

const defsPrefix = "#/$defs/"

// LowerStrict returns the form of a caller output schema that OpenAI strict
// structured output accepts. Every object is closed with
// additionalProperties:false and lists every declared property as required,
// optional ones after the caller's required order in sorted order, so the
// provider can only narrow the answer. Keywords outside the strict subset are
// moved into the description so the model still sees them. The caller schema
// must still be applied to the final JSON.
//
// A root that is not an object, an explicitly open object, allOf, a required
// name that is not declared and a reference other than # or #/$defs/<name>
// cannot be represented and are rejected.
func LowerStrict(schema json.RawMessage) (map[string]any, error) {
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
	if _, ref := root["$ref"]; ref || !isObjectSchema(root) {
		return nil, &UnsupportedError{Path: "/", Reason: "the root of an OpenAI strict output schema must be an object schema"}
	}
	return lowerSchema(root, root, "")
}

// MentionsJSON reports whether any string in values contains "json" in any
// case. OpenAI rejects JSON mode unless the word appears in the context.
func MentionsJSON(values ...any) bool {
	for _, value := range values {
		switch typed := value.(type) {
		case string:
			if strings.Contains(strings.ToLower(typed), "json") {
				return true
			}
		case map[string]any:
			for _, child := range typed {
				if MentionsJSON(child) {
					return true
				}
			}
		case []any:
			for _, child := range typed {
				if MentionsJSON(child) {
					return true
				}
			}
		case []map[string]any:
			for _, child := range typed {
				if MentionsJSON(child) {
					return true
				}
			}
		}
	}
	return false
}

// JSONModeContextError reports JSON mode requested without the word "JSON"
// anywhere in the instructions or input.
func JSONModeContextError() error {
	return &UnsupportedError{Reason: `output format "json" requires the word "JSON" in the instructions or input`}
}

func lowerSchema(root, source map[string]any, path string) (map[string]any, error) {
	if ref, ok := source["$ref"]; ok {
		if err := checkRef(root, ref, pointerJoin(path, "$ref")); err != nil {
			return nil, err
		}
		// A reference is not accepted alongside other keywords; only the
		// definitions it may point at are kept.
		result := map[string]any{"$ref": ref}
		if defs, ok := source["$defs"].(map[string]any); ok {
			lowered, err := lowerChildren(root, defs, pointerJoin(path, "$defs"))
			if err != nil {
				return nil, err
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
		childPath := pointerJoin(path, key)
		if key == "allOf" {
			return nil, &UnsupportedError{Path: childPath, Reason: "allOf is not supported by OpenAI strict structured output"}
		}
		if _, ok := supportedKeywords[key]; !ok {
			extras[key] = value
			continue
		}
		switch key {
		case "$defs", "properties":
			children, _ := value.(map[string]any)
			lowered, err := lowerChildren(root, children, childPath)
			if err != nil {
				return nil, err
			}
			result[key] = lowered
		case "items":
			nested, err := lowerChild(root, value, childPath)
			if err != nil {
				return nil, err
			}
			result[key] = nested
		case "anyOf", "oneOf":
			children, _ := value.([]any)
			lowered := make([]any, 0, len(children))
			for index, child := range children {
				nested, err := lowerChild(root, child, fmt.Sprintf("%s/%d", childPath, index))
				if err != nil {
					return nil, err
				}
				lowered = append(lowered, nested)
			}
			result[key] = lowered
		case "additionalProperties", "required":
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
		if additional, present := source["additionalProperties"]; present && additional != false {
			return nil, &UnsupportedError{Path: pointerJoin(path, "additionalProperties"), Reason: "additionalProperties other than false is not supported by OpenAI strict structured output"}
		}
		properties, _ := result["properties"].(map[string]any)
		if properties == nil {
			properties = map[string]any{}
			result["properties"] = properties
		}
		required, err := requireAll(source["required"], properties, pointerJoin(path, "required"))
		if err != nil {
			return nil, err
		}
		result["required"] = required
		result["additionalProperties"] = false
	} else {
		for _, key := range []string{"additionalProperties", "required"} {
			if value, present := source[key]; present {
				extras[key] = value
			}
		}
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

// requireAll returns the caller's required names in order, then every other
// declared property in sorted order, so property generation order is kept.
func requireAll(value any, properties map[string]any, path string) ([]any, error) {
	declared, _ := value.([]any)
	result := make([]any, 0, len(properties))
	seen := make(map[string]struct{}, len(properties))
	for index, item := range declared {
		name, _ := item.(string)
		if _, ok := properties[name]; !ok {
			return nil, &UnsupportedError{Path: fmt.Sprintf("%s/%d", path, index), Reason: fmt.Sprintf("required property %q is not declared in properties, which OpenAI strict structured output does not support", name)}
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	optional := make([]string, 0, len(properties)-len(seen))
	for name := range properties {
		if _, ok := seen[name]; !ok {
			optional = append(optional, name)
		}
	}
	sort.Strings(optional)
	for _, name := range optional {
		result = append(result, name)
	}
	return result, nil
}

// checkRef accepts the references OpenAI resolves: the root and a direct
// root #/$defs entry. Recursion through either is supported by the provider.
func checkRef(root map[string]any, value any, path string) error {
	ref, _ := value.(string)
	if ref == "#" {
		return nil
	}
	defs, _ := root["$defs"].(map[string]any)
	name := strings.ReplaceAll(strings.ReplaceAll(strings.TrimPrefix(ref, defsPrefix), "~1", "/"), "~0", "~")
	if _, ok := defs[name]; strings.HasPrefix(ref, defsPrefix) && ok {
		return nil
	}
	return &UnsupportedError{Path: path, Reason: "only # and #/$defs/<name> references are supported by OpenAI strict structured output"}
}

func lowerChildren(root, children map[string]any, path string) (map[string]any, error) {
	lowered := make(map[string]any, len(children))
	for name, child := range children {
		nested, err := lowerChild(root, child, pointerJoin(path, name))
		if err != nil {
			return nil, err
		}
		lowered[name] = nested
	}
	return lowered, nil
}

func lowerChild(root map[string]any, value any, path string) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, &UnsupportedError{Path: path, Reason: "schema must be an object"}
	}
	return lowerSchema(root, object, path)
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

// describeExtras renders moved keywords in "{key: value, ...}" form with
// sorted keys, so equal schemas always lower to equal bytes.
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

func pointerJoin(base, token string) string {
	return base + "/" + strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
}

func displayPath(path string) string {
	if path == "" {
		return "/"
	}
	return path
}
