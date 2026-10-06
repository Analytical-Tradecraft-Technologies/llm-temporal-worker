package schema_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

func TestParseAndValidateFixtures(t *testing.T) {
	tests := []struct {
		name     string
		instance string
	}{
		{name: "object", instance: `{"name":"Ada","age":37}`},
		{name: "array", instance: `["a","b"]`},
		{name: "enum-number", instance: `2.5`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			compiled := parseFixture(t, filepath.Join("valid", test.name+".json"))
			if err := compiled.Validate([]byte(test.instance)); err != nil {
				t.Fatalf("valid instance rejected: %v", err)
			}
			if len(compiled.Canonical()) == 0 {
				t.Fatal("compiled schema has no canonical form")
			}
			if compiled.Digest() == ([32]byte{}) {
				t.Fatal("compiled schema has empty digest")
			}
		})
	}
}

func TestValidationReturnsSafePointerAndKeyword(t *testing.T) {
	compiled := parseFixture(t, filepath.Join("valid", "object.json"))
	var validationErr *schema.ValidationError
	err := compiled.Validate([]byte(`{"name":"Ada","age":-1}`))
	if !errors.As(err, &validationErr) {
		t.Fatalf("error = %T %v, want ValidationError", err, err)
	}
	if validationErr.Path != "/age" || validationErr.Keyword != "minimum" {
		t.Fatalf("validation error = %#v, want /age minimum", validationErr)
	}
	if validationErr.Message == "" || validationErr.Instance != "" {
		t.Fatalf("validation error leaked or omitted safe message: %#v", validationErr)
	}
}

func TestParseRejectsUnsupportedRefsKeywordsAndLimits(t *testing.T) {
	cases := []struct {
		name string
		data string
	}{
		{name: "unsupported keyword", data: `{"type":"object","patternProperties":{"^x":{"type":"string"}}}`},
		{name: "unresolved local ref", data: `{"$ref":"#/$defs/missing","$defs":{}}`},
		{name: "remote ref", data: `{"$ref":"https://example.com/schema.json"}`},
		{name: "duplicate key", data: `{"type":"object","properties":{"x":{"type":"string","type":"number"}}}`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := schema.Parse([]byte(test.data)); err == nil {
				t.Fatal("accepted invalid schema")
			}
		})
	}
	deep := `{"$defs":{"x":{"$ref":"#/$defs/x"}},"$ref":"#/$defs/x"}`
	if _, err := schema.ParseWithLimits([]byte(deep), schema.Limits{MaxBytes: 1024, MaxDepth: 1}); err == nil {
		t.Fatal("accepted schema beyond depth limit")
	}
	if _, err := schema.ParseWithLimits([]byte(`{"type":"string"}`), schema.Limits{MaxBytes: 4, MaxDepth: 64}); err == nil {
		t.Fatal("accepted schema beyond byte limit")
	}
}

func TestSchemaInstanceRejectsInvalidJSON(t *testing.T) {
	compiled := parseFixture(t, filepath.Join("valid", "array.json"))
	if err := compiled.Validate([]byte(`{"not":"an array"}`)); err == nil {
		t.Fatal("accepted invalid instance")
	}
	if err := compiled.Validate([]byte(`[{"duplicate":1,"duplicate":2}]`)); err == nil {
		t.Fatal("accepted instance with duplicate keys")
	}
}

func TestPublicAPISchemasCompileLocally(t *testing.T) {
	for _, name := range []string{
		"generate-request.schema.json", "generate-response.schema.json",
		"compact-request.schema.json", "compact-response.schema.json",
		"query-request.schema.json", "query-response.schema.json",
	} {
		data, err := os.ReadFile(filepath.Join("..", "..", "api", "schema", "v1", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := schema.Parse(data); err != nil {
			t.Fatalf("parse public schema %s: %v", name, err)
		}
	}
}

func TestGenerateResponseSchemaAcceptsExactUSDWithoutLegacyMoneyFields(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "api", "schema", "v1", "generate-response.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(data)
	if err != nil {
		t.Fatalf("parse response schema: %v", err)
	}
	instance := []byte(`{
  "api_version": "llm.temporal/v1",
  "operation_key": "exact-usd",
  "operation_id": "op-exact-usd",
  "status": "completed",
  "output": [],
  "checkpoint": {"handle": "checkpoint-1", "kind": "generation", "depth": 0},
  "cache": {"disposition": "disabled", "variant": 0},
  "cost": {"status": "exact", "actual_cost_usd": "0.000000000000000001", "method": "catalog_usage", "catalog_version": "v1"}
}`)
	if err := compiled.Validate(instance); err != nil {
		t.Fatalf("exact-only response rejected: %v", err)
	}
}

func parseFixture(t *testing.T, name string) *schema.Schema {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(data)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return compiled
}
