// Package schemawire holds the JSON Schema helpers the Claude and OpenAI
// output-schema lowerings and the final-JSON checks share.
package schemawire

import (
	"bytes"
	"encoding/json"
	"sort"
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
