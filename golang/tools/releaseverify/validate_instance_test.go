package main

import (
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

func TestValidateInstanceWordsItsErrors(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	testassert.NoError(t, compiler.AddResource("mem://object.json", map[string]any{"type": "object"}))
	schema, err := compiler.Compile("mem://object.json")
	testassert.NoError(t, err)
	instance, err := validateInstance(schema, []byte(`{"kind":"x"}`), "doc", "the schema")
	if document, ok := instance.(map[string]any); err != nil || !ok || document["kind"] != "x" {
		t.Fatalf("valid instance = %#v, %v", instance, err)
	}
	if _, err := validateInstance(schema, []byte(`{`), "doc", "the schema"); err == nil || err.Error() != "doc is not valid JSON" {
		t.Fatalf("invalid JSON = %v", err)
	}
	if _, err := validateInstance(schema, []byte(`[]`), "doc", "the schema"); err == nil || !strings.HasPrefix(err.Error(), "doc does not satisfy the schema: ") {
		t.Fatalf("schema mismatch = %v", err)
	}
}
