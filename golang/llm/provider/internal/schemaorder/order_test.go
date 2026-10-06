package schemaorder

import (
	"encoding/json"
	"testing"
)

func TestOrderedFollowsRequiredOrderRecursively(t *testing.T) {
	var schema any
	if err := json.Unmarshal([]byte(`{
		"type": "object",
		"required": ["reasoning", "answer"],
		"properties": {
			"answer": {"type": "string"},
			"reasoning": {"type": "object", "required": ["steps", "confidence"], "properties": {"confidence": {"type": "number"}, "steps": {"type": "array", "items": {"type": "string"}}}},
			"extra": {"type": "integer", "minimum": 1.50}
		},
		"additionalProperties": false
	}`), &schema); err != nil {
		t.Fatal(err)
	}
	got, err := Ordered(schema)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"additionalProperties":false,"properties":{"reasoning":{"properties":{"steps":{"items":{"type":"string"},"type":"array"},"confidence":{"type":"number"}},"required":["steps","confidence"],"type":"object"},"answer":{"type":"string"},"extra":{"minimum":1.5,"type":"integer"}},"required":["reasoning","answer"],"type":"object"}`
	if string(got) != want {
		t.Fatalf("Ordered() =\n%s\nwant\n%s", got, want)
	}
	// A "properties" key that is a property name, not a keyword, keeps its
	// schema intact.
	if _, err := Ordered(map[string]any{"properties": map[string]any{"properties": map[string]any{"type": "string"}}, "required": []any{"properties", "missing"}}); err != nil {
		t.Fatal(err)
	}
}
