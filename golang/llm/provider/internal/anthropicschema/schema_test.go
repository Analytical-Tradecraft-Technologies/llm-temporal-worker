package anthropicschema

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestLowerMovesUnsupportedKeywordsIntoDescriptions(t *testing.T) {
	schema := json.RawMessage(`{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"type": "object",
		"properties": {
			"name": {"type": "string", "description": "Display name", "minLength": 3, "maxLength": 40, "pattern": "^[a-z]+$"},
			"age": {"type": "integer", "minimum": 0, "exclusiveMaximum": 150, "multipleOf": 1},
			"tags": {"type": "array", "items": {"type": "string", "format": "uuid"}, "minItems": 1, "maxItems": 4},
			"pairs": {"type": "array", "items": {"type": "string", "format": "regex"}, "minItems": 2, "uniqueItems": true},
			"kind": {"oneOf": [{"const": "a"}, {"type": "object", "properties": {"b": {"type": "number", "maximum": 1.5}}}]},
			"ref": {"$ref": "#/$defs/leaf", "description": "dropped beside a reference"}
		},
		"required": ["name"],
		"$defs": {"leaf": {"type": "object"}}
	}`)
	lowered, err := Lower(schema, true)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(lowered)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"$defs":{"leaf":{"additionalProperties":false,"properties":{},"type":"object"}},"additionalProperties":false,"properties":{` +
		`"age":{"description":"{exclusiveMaximum: 150, minimum: 0, multipleOf: 1}","type":"integer"},` +
		`"kind":{"anyOf":[{"const":"a"},{"additionalProperties":false,"properties":{"b":{"description":"{maximum: 1.5}","type":"number"}},"type":"object"}]},` +
		`"name":{"description":"Display name\n\n{maxLength: 40, minLength: 3}","pattern":"^[a-z]+$","type":"string"},` +
		`"pairs":{"description":"{minItems: 2, uniqueItems: true}","items":{"description":"{format: regex}","type":"string"},"type":"array"},` +
		`"ref":{"$ref":"#/$defs/leaf"},` +
		`"tags":{"description":"{maxItems: 4}","items":{"format":"uuid","type":"string"},"minItems":1,"type":"array"}},` +
		`"required":["name"],"type":"object"}`
	if string(got) != want {
		t.Fatalf("lowered schema\n got: %s\nwant: %s", got, want)
	}
	again, err := Lower(schema, true)
	if err != nil {
		t.Fatal(err)
	}
	if repeated, _ := json.Marshal(again); string(repeated) != want {
		t.Fatalf("lowering is not deterministic: %s", repeated)
	}
}

func TestLowerOpenObjects(t *testing.T) {
	declared := json.RawMessage(`{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":true}`)
	dictionary := json.RawMessage(`{"type":"object","additionalProperties":{"type":"string"}}`)
	for _, schema := range []json.RawMessage{declared, dictionary} {
		var unsupported *UnsupportedError
		if _, err := Lower(schema, true); !errors.As(err, &unsupported) || unsupported.Path != "/additionalProperties" {
			t.Fatalf("strict open object %s = %v", schema, err)
		}
	}
	lowered, err := Lower(declared, false)
	if err != nil {
		t.Fatal(err)
	}
	if lowered["additionalProperties"] != false || lowered["description"] != "{additionalProperties: true}" {
		t.Fatalf("best-effort open object = %#v", lowered)
	}
	var unsupported *UnsupportedError
	if _, err := Lower(dictionary, false); !errors.As(err, &unsupported) {
		t.Fatalf("best-effort dictionary object = %v", err)
	}
}

func TestLowerRejectsRecursiveAndUnrepresentableReferences(t *testing.T) {
	for name, schema := range map[string]string{
		"root":     `{"type":"object","properties":{"child":{"$ref":"#"}}}`,
		"self":     `{"type":"object","properties":{"node":{"$ref":"#/$defs/node"}},"$defs":{"node":{"type":"object","properties":{"next":{"$ref":"#/$defs/node"}}}}}`,
		"mutual":   `{"type":"object","properties":{"a":{"$ref":"#/$defs/a"}},"$defs":{"a":{"type":"object","properties":{"b":{"$ref":"#/$defs/b"}}},"b":{"type":"array","items":{"$ref":"#/$defs/a"}}}}`,
		"non-defs": `{"type":"object","properties":{"a":{"type":"string"},"b":{"$ref":"#/properties/a"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			for _, strict := range []bool{true, false} {
				var unsupported *UnsupportedError
				if _, err := Lower(json.RawMessage(schema), strict); !errors.As(err, &unsupported) {
					t.Fatalf("strict=%t error = %v", strict, err)
				}
			}
		})
	}
	// A definition shared by two properties is a DAG, not recursion, and a
	// property that happens to be named $ref is not a reference.
	shared := `{"type":"object","properties":{"a":{"$ref":"#/$defs/leaf"},"b":{"$ref":"#/$defs/leaf"},"$ref":{"type":"string"}},"$defs":{"leaf":{"type":"string"}}}`
	if _, err := Lower(json.RawMessage(shared), true); err != nil {
		t.Fatalf("shared definition = %v", err)
	}
}

func TestLowerRejectsSchemasOutsideTheLocalSubset(t *testing.T) {
	_, err := Lower(json.RawMessage(`{"type":"object","patternProperties":{"^x":{"type":"string"}}}`), false)
	if err == nil || !strings.Contains(err.Error(), "patternProperties") {
		t.Fatalf("subset error = %v", err)
	}
}

// The local v1 subset only allows a boolean schema under additionalProperties,
// so boolean children never reach the lowering and are refused at parse.
func TestLowerRejectsBooleanChildSchemasAtParse(t *testing.T) {
	for _, schema := range []string{
		`{"type":"object","properties":{"tags":{"type":"array","items":false}}}`,
		`{"type":"object","properties":{"never":false}}`,
	} {
		_, err := Lower(json.RawMessage(schema), false)
		var unsupported *UnsupportedError
		if err == nil || errors.As(err, &unsupported) || !strings.Contains(err.Error(), "must be an object") {
			t.Fatalf("boolean child %s = %v", schema, err)
		}
	}
}

func TestValidateAppliesTheCallerSchema(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"name":{"type":"string","minLength":3}},"required":["name"]}`)
	output := func(text string) []llm.Item {
		return []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: text}}}}
	}
	if err := Validate(schema, output(`{"name":"abc"}`)); err != nil {
		t.Fatalf("conforming JSON = %v", err)
	}
	if err := Validate(schema, output(`{"name":"ab"}`)); err == nil || !strings.Contains(err.Error(), "does not satisfy schema") {
		t.Fatalf("short string = %v", err)
	}
	if err := Validate(schema, nil); err == nil {
		t.Fatal("missing text accepted")
	}
}
