// Package schemawire holds the JSON Schema helpers the Claude and OpenAI
// output-schema lowerings and the final-JSON checks share.
package schemawire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// IsObjectSchema reports whether a schema describes an object: it declares
// properties or its type is, or includes, "object".
func IsObjectSchema(schema map[string]any) bool {
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

// DescribeExtras renders schema keywords the provider cannot enforce as a
// deterministic, sorted note for the schema description, as in
// "{maxLength: 5, pattern: ^a}".
func DescribeExtras(extras map[string]any) (string, error) {
	keys := slices.Sorted(maps.Keys(extras))
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

// LowerFunc lowers one child schema found at path.
type LowerFunc func(child any, path string) (map[string]any, error)

// LowerMap lowers each named child schema at path/<name>.
func LowerMap(children map[string]any, path string, lower LowerFunc) (map[string]any, error) {
	lowered := make(map[string]any, len(children))
	for name, child := range children {
		nested, err := lower(child, PointerJoin(path, name))
		if err != nil {
			return nil, err
		}
		lowered[name] = nested
	}
	return lowered, nil
}

// LowerList lowers each child schema at path/<index>.
func LowerList(children []any, path string, lower LowerFunc) ([]any, error) {
	lowered := make([]any, 0, len(children))
	for index, child := range children {
		nested, err := lower(child, fmt.Sprintf("%s/%d", path, index))
		if err != nil {
			return nil, err
		}
		lowered = append(lowered, nested)
	}
	return lowered, nil
}

// MergeOneOf replaces a lowered oneOf, which the providers do not accept,
// with anyOf, the nearest looser form; the caller schema still enforces
// exclusivity on the final JSON. When the schema also has anyOf, the source
// oneOf moves to extras instead.
func MergeOneOf(result, source, extras map[string]any) {
	if variants, ok := result["oneOf"]; ok {
		if _, both := result["anyOf"]; both {
			extras["oneOf"] = source["oneOf"]
		} else {
			result["anyOf"] = variants
		}
		delete(result, "oneOf")
	}
}

// NoteExtras appends the DescribeExtras note for any extras to the lowered
// schema's description at path.
func NoteExtras(result, extras map[string]any, path string) error {
	if len(extras) == 0 {
		return nil
	}
	note, err := DescribeExtras(extras)
	if err != nil {
		return fmt.Errorf("output schema at %s: %w", DisplayPath(path), err)
	}
	if description, ok := result["description"].(string); ok && description != "" {
		note = description + "\n\n" + note
	}
	result["description"] = note
	return nil
}

// PointerJoin appends token to the JSON pointer base, escaping it.
func PointerJoin(base, token string) string {
	return base + "/" + strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
}

// DisplayPath renders a JSON pointer for an error message, with the root as
// "/".
func DisplayPath(path string) string {
	if path == "" {
		return "/"
	}
	return path
}

// FirstModelText returns the first text part of the first model message that
// has one.
func FirstModelText(output []llm.Item) (string, bool) {
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
