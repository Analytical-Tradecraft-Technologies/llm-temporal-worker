package schema_test

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
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

func TestSupportedSubsetRejectsProviderExecutionKeywords(t *testing.T) {
	for _, keyword := range []string{"patternProperties", "dependentSchemas", "contains", "unevaluatedProperties", "contentSchema"} {
		data := []byte(`{"` + keyword + `":{}}`)
		if _, err := schema.Parse(data); err == nil {
			t.Errorf("accepted unsupported keyword %q", keyword)
		}
	}
}
