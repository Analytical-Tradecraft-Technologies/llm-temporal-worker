package openaischema

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestLowerStrictClosesObjectsAndMovesUnsupportedKeywords(t *testing.T) {
	schema := json.RawMessage(`{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"type": "object",
		"properties": {
			"total": {"type": "number", "minimum": 0},
			"lines": {"type": "array", "items": {"$ref": "#/$defs/line"}, "uniqueItems": true, "maxItems": 9},
			"kind": {"oneOf": [{"const": "a"}, {"type": "object", "properties": {"b": {"type": "string", "format": "regex"}}}]},
			"code": {"type": "string", "not": {"const": "x"}, "if": {"minLength": 2}, "then": {"maxLength": 4}}
		},
		"required": ["total"],
		"$defs": {"line": {"type": "object", "properties": {"sku": {"type": "string"}, "next": {"$ref": "#"}}, "required": ["sku"], "additionalProperties": false}}
	}`)
	lowered, err := LowerStrict(schema)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(lowered)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"$defs":{"line":{"additionalProperties":false,"properties":{"next":{"$ref":"#"},"sku":{"type":"string"}},"required":["sku","next"],"type":"object"}},` +
		`"additionalProperties":false,"properties":{` +
		`"code":{"description":"{if: {\"minLength\":2}, not: {\"const\":\"x\"}, then: {\"maxLength\":4}}","type":"string"},` +
		`"kind":{"anyOf":[{"const":"a"},{"additionalProperties":false,"properties":{"b":{"description":"{format: regex}","type":"string"}},"required":["b"],"type":"object"}]},` +
		`"lines":{"description":"{maxItems: 9, uniqueItems: true}","items":{"$ref":"#/$defs/line"},"type":"array"},` +
		`"total":{"description":"{minimum: 0}","type":"number"}},` +
		`"required":["total","code","kind","lines"],"type":"object"}`
	if string(got) != want {
		t.Fatalf("lowered schema\n got: %s\nwant: %s", got, want)
	}
}

func TestLowerStrictRejectsUnrepresentableSchemas(t *testing.T) {
	for _, test := range []struct {
		name, schema, path string
	}{
		{"array root", `{"type":"array","items":{"type":"string"}}`, "/"},
		{"open object", `{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":true}`, "/additionalProperties"},
		{"dictionary", `{"type":"object","properties":{"a":{"type":"object","additionalProperties":{"type":"string"}}}}`, "/properties/a/additionalProperties"},
		{"allOf", `{"type":"object","properties":{"a":{"allOf":[{"type":"string"}]}}}`, "/properties/a/allOf"},
		{"undeclared required", `{"type":"object","properties":{"a":{"type":"string"}},"required":["a","b"]}`, "/required/1"},
		{"anchor reference", `{"type":"object","properties":{"a":{"$ref":"#leaf"}},"$defs":{"leaf":{"$anchor":"leaf","type":"string"}}}`, "/properties/a/$ref"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var unsupported *UnsupportedError
			if _, err := LowerStrict(json.RawMessage(test.schema)); !errors.As(err, &unsupported) || unsupported.Path != test.path {
				t.Fatalf("LowerStrict error = %v, want unsupported at %s", err, test.path)
			}
		})
	}
}

func TestMentionsJSON(t *testing.T) {
	input := []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_text", "text": "List three colours."}}}}
	if MentionsJSON(input) {
		t.Fatal("input without the word mentions JSON")
	}
	input = append(input, map[string]any{"role": "developer", "content": "Answer as Json."})
	if !MentionsJSON(input) {
		t.Fatal("a case-insensitive mention was missed")
	}
}
