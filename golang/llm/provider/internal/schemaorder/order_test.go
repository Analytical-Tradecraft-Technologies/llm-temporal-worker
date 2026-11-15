package schemaorder

import (
	"encoding/json"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"testing"
)

func TestOrderedFollowsRequiredOrderRecursively(t *testing.T) {
	var schema any
	testassert.NoError(t, json.Unmarshal([]byte(`{
		"type": "object",
		"required": ["reasoning", "answer"],
		"properties": {
			"answer": {"type": "string"},
			"reasoning": {"type": "object", "required": ["steps", "confidence"], "properties": {"confidence": {"type": "number"}, "steps": {"type": "array", "items": {"type": "string"}}}},
			"extra": {"type": "integer", "minimum": 1.50}
		},
		"additionalProperties": false
	}`), &schema))
	got, err := Ordered(schema)
	testassert.NoError(t, err)
	want := `{"additionalProperties":false,"properties":{"reasoning":{"properties":{"steps":{"items":{"type":"string"},"type":"array"},"confidence":{"type":"number"}},"required":["steps","confidence"],"type":"object"},"answer":{"type":"string"},"extra":{"minimum":1.5,"type":"integer"}},"required":["reasoning","answer"],"type":"object"}`
	if string(got) != want {
		t.Fatalf("Ordered() =\n%s\nwant\n%s", got, want)
	}
	// A "properties" key that is a property name, not a keyword, keeps its
	// schema intact.
	testassert.NoError(t, testassert.Err(Ordered(map[string]any{"properties": map[string]any{"properties": map[string]any{"type": "string"}}, "required": []any{"properties", "missing"}})))
}

func TestDecodeRetainsNumbersAndRejectsTrailingValues(t *testing.T) {
	for _, input := range []string{`{"value":9007199254740993}`, `{"value":0.100000000000000000001}`, `{"value":1.234567890123456789e+20}`} {
		var value any
		testassert.NoError(t, Decode([]byte(input), &value))
		encoded, err := json.Marshal(value)
		if err != nil || string(encoded) != input {
			t.Fatalf("Decode changed numeric literal: %s, %v", encoded, err)
		}
	}
	for _, input := range []string{`{} {}`, `{} true`, `{} garbage`, `{"value":`, ``, `{"value":NaN}`} {
		var value any
		if err := Decode([]byte(input), &value); err == nil {
			t.Fatalf("accepted invalid JSON %q", input)
		}
	}
}
