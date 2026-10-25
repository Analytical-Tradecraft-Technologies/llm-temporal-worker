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

// format, contentEncoding and contentMediaType are accepted as annotations
// only, so local validation never rejects a value for them (#1664). A change
// to assert them must update docs/architecture/unified-api.md.
func TestAnnotationOnlyKeywordsAreNotAsserted(t *testing.T) {
	for _, test := range []struct{ schema, instance string }{
		{`{"type":"string","format":"date-time"}`, `"not a date"`},
		{`{"type":"string","format":"email"}`, `"nobody"`},
		{`{"type":"string","contentEncoding":"base64"}`, `"!!!"`},
		{`{"type":"string","contentMediaType":"application/json","contentEncoding":"base64"}`, `"e30"`},
		{`{"type":"string","contentMediaType":"application/json"}`, `"{"`},
	} {
		compiled, err := schema.Parse([]byte(test.schema))
		if err != nil {
			t.Fatalf("Parse(%s) = %v", test.schema, err)
		}
		if err := compiled.Validate([]byte(test.instance)); err != nil {
			t.Fatalf("Validate(%s, %s) = %v, want the keyword ignored", test.schema, test.instance, err)
		}
	}
}
