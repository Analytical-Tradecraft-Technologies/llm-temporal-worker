package schema_test

import (
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm/schema"
)

func TestSupportedSubsetAcceptsCompositionAndLocalDefs(t *testing.T) {
	data := []byte(`{
  "$schema":"https://json-schema.org/draft/2020-12/schema",
  "$defs":{"name":{"type":"string","minLength":1}},
  "type":"object",
  "properties":{"name":{"$ref":"#/$defs/name"}},
  "required":["name"],
  "additionalProperties":false,
  "allOf":[{"type":"object"}]
}`)
	if _, err := schema.Parse(data); err != nil {
		t.Fatal(err)
	}
}

func TestSupportedSubsetAcceptsBooleanChildSchemas(t *testing.T) {
	data := []byte(`{
  "$schema":"https://json-schema.org/draft/2020-12/schema",
  "type":"array",
  "prefixItems":[{"type":"string"}],
  "items":false
}`)
	compiled, err := schema.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate([]byte(`["only"]`)); err != nil {
		t.Fatalf("valid tuple: %v", err)
	}
	if err := compiled.Validate([]byte(`["only","extra"]`)); err == nil {
		t.Fatal("tuple accepted an item rejected by the boolean child schema")
	}
}

func TestSupportedSubsetRejectsProviderExecutionKeywords(t *testing.T) {
	for _, keyword := range []string{"patternProperties", "dependentSchemas", "contains", "unevaluatedProperties", "contentSchema"} {
		data := []byte(`{"` + keyword + `":{}}`)
		if _, err := schema.Parse(data); err == nil {
			t.Errorf("accepted unsupported keyword %q", keyword)
		}
	}
}
