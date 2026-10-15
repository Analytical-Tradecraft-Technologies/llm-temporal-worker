package schema_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

func TestV1ContractFixturesValidate(t *testing.T) {
	cases := []struct{ schemaName, fixture string }{
		{"generate-request.schema.json", "generate-root.json"},
		{"generate-request.schema.json", "generate-cache-any-age.json"},
		{"compact-request.schema.json", "compact-cache-any-age.json"},
		{"generate-request.schema.json", "generate-delta.json"},
		{"generate-response.schema.json", "generate-response.json"},
		{"generate-response.schema.json", "generate-response-disabled-cache.json"},
		{"generate-response.schema.json", "generate-response-cache-hit.json"},
		{"generate-response.schema.json", "generate-response-miss-not-populated.json"},
		{"compact-request.schema.json", "compact-request.json"},
		{"compact-response.schema.json", "compact-response.json"},
		{"query-request.schema.json", "query-provider-status.json"},
		{"query-response.schema.json", "query-provider-response.json"},
	}
	for _, test := range cases {
		t.Run(test.fixture, func(t *testing.T) {
			schemaData, err := os.ReadFile(filepath.Join("..", "..", "api", "schema", "v1", test.schemaName))
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := schema.Parse(schemaData)
			if err != nil {
				t.Fatal(err)
			}
			fixture, err := os.ReadFile(filepath.Join("..", "testdata", "v1", test.fixture))
			if err != nil {
				t.Fatal(err)
			}
			if err := compiled.Validate(fixture); err != nil {
				t.Fatalf("fixture rejected: %v", err)
			}
		})
	}
}

func TestV1ContractRejectsUnknownAndLegacyResponseFields(t *testing.T) {
	schemaData, err := os.ReadFile(filepath.Join("..", "..", "api", "schema", "v1", "generate-request.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	bad, err := os.ReadFile(filepath.Join("..", "testdata", "v1", "negative-unknown-field.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(bad); err == nil {
		t.Fatal("unknown transcript field accepted")
	}
	legacy := []byte(`{"api_version":"llm.temporal/v1","operation_key":"x","context":{"tenant":"t","project":"p","actor":"a"},"append":[],"transcript":[]}`)
	if err := compiled.Validate(legacy); err == nil {
		t.Fatal("legacy transcript field accepted")
	}
}

func TestV1ContractFixtureMatrix(t *testing.T) {
	valid := []struct {
		schemaName string
		fixture    string
	}{
		{"generate-request.schema.json", "generate-fork-patch-set.json"},
		{"generate-request.schema.json", "generate-fork-patch-clear.json"},
		{"generate-request.schema.json", "generate-fork-patch-compaction.json"},
		{"generate-request.schema.json", "generate-variant-unknown-temperature.json"},
		{"generate-request.schema.json", "generate-variant-positive-temperature.json"},
		{"compact-request.schema.json", "compact-request-no-cache.json"},
		{"compact-request.schema.json", "compact-positive-variant.json"},
		{"generate-request.schema.json", "generate-variant-zero-temperature.json"},
		{"query-request.schema.json", "query-model-inventory.json"},
		{"query-request.schema.json", "query-credit-status.json"},
		{"query-request.schema.json", "query-budget-status.json"},
		{"query-request.schema.json", "query-spend-summary.json"},
		{"query-response.schema.json", "query-provider-response.json"},
		{"query-response.schema.json", "query-model-inventory-response.json"},
		{"query-response.schema.json", "query-credit-status-response.json"},
		{"query-response.schema.json", "query-budget-status-response.json"},
		{"query-response.schema.json", "query-spend-summary-response.json"},
	}
	for _, test := range valid {
		t.Run("valid/"+test.fixture, func(t *testing.T) {
			compiled := readV1Schema(t, test.schemaName)
			if err := compiled.Validate(readV1Fixture(t, test.fixture)); err != nil {
				t.Fatalf("fixture rejected: %v", err)
			}
		})
	}

	invalid := []struct {
		schemaName string
		fixture    string
	}{
		{"generate-response.schema.json", "negative-generate-transcript.json"},
		{"generate-response.schema.json", "negative-generate-compaction-checkpoint.json"},
		{"generate-response.schema.json", "negative-generate-depth-overflow.json"},
		{"generate-request.schema.json", "negative-generate-cache-field.json"},
		{"generate-request.schema.json", "negative-generate-enum-patch.json"},
		{"generate-request.schema.json", "negative-generate-compaction-scalar.json"},
		{"generate-request.schema.json", "negative-generate-extensions-null.json"},
		{"generate-request.schema.json", "negative-generate-null-append.json"},
		{"generate-request.schema.json", "negative-generate-output-name.json"},
		{"generate-response.schema.json", "negative-generate-null-output.json"},
		{"generate-response.schema.json", "negative-generate-cost-enum.json"},
		{"generate-response.schema.json", "negative-generate-cost-cross-variant.json"},
		{"generate-response.schema.json", "negative-generate-diagnostics.json"},
		{"generate-response.schema.json", "negative-currency-field.json"},
		{"generate-response.schema.json", "negative-numeric-usd.json"},
		{"compact-request.schema.json", "negative-compact-tools.json"},
		{"compact-request.schema.json", "negative-compact-structured-output.json"},
		{"query-response.schema.json", "negative-query-mismatched-result.json"},
		{"query-request.schema.json", "negative-query-page-size.json"},
		{"query-request.schema.json", "negative-query-cursor.json"},
	}
	for _, test := range invalid {
		t.Run("invalid/"+test.fixture, func(t *testing.T) {
			compiled := readV1Schema(t, test.schemaName)
			if err := compiled.Validate(readV1Fixture(t, test.fixture)); err == nil {
				t.Fatal("negative fixture was accepted")
			}
		})
	}
}

// The output-name fixture is rejected for its name alone: the same document
// is accepted once the name fits the provider pattern or is omitted.
func TestV1ContractOutputFormatNamePattern(t *testing.T) {
	compiled := readV1Schema(t, "generate-request.schema.json")
	negative := string(readV1Fixture(t, "negative-generate-output-name.json"))
	const invalid = `"name":"claim summary",`
	if !strings.Contains(negative, invalid) {
		t.Fatalf("fixture does not contain %s", invalid)
	}
	for name, test := range map[string]struct {
		replacement string
		valid       bool
	}{
		"fixture":          {replacement: invalid},
		"omitted":          {replacement: ``, valid: true},
		"explicitly empty": {replacement: `"name":"",`},
		"provider charset": {replacement: `"name":"Claim_summary-2",`, valid: true},
		"64 characters":    {replacement: `"name":"` + strings.Repeat("a", 64) + `",`, valid: true},
		"65 characters":    {replacement: `"name":"` + strings.Repeat("a", 65) + `",`},
		"dot":              {replacement: `"name":"claim.summary",`},
	} {
		t.Run(name, func(t *testing.T) {
			err := compiled.Validate([]byte(strings.Replace(negative, invalid, test.replacement, 1)))
			if test.valid != (err == nil) {
				t.Fatalf("schema validation error = %v, want valid=%t", err, test.valid)
			}
		})
	}
}

// The compaction fixture carries every policy field. The schema keeps the
// worker's per-field bounds, so a policy edited out of them is rejected.
func TestV1ContractCompactionPolicyBounds(t *testing.T) {
	compiled := readV1Schema(t, "generate-request.schema.json")
	fixture := readV1Fixture(t, "generate-fork-patch-compaction.json")
	for name, test := range map[string]struct {
		field string
		value any
		valid bool
	}{
		"fixture":                 {valid: true},
		"omitted field":           {field: "recent_turns", valid: true},
		"zero recent turns":       {field: "recent_turns", value: 0, valid: true},
		"too many recent turns":   {field: "recent_turns", value: 1001},
		"negative recent turns":   {field: "recent_turns", value: -1},
		"fractional recent turns": {field: "recent_turns", value: 1.5},
		"null recent turns":       {field: "recent_turns", value: json.RawMessage(`null`)},
		"unknown field":           {field: "max_turns", value: 1},
		"unknown version":         {field: "version", value: "generic-2099-01-01"},
		"unknown prompt version":  {field: "prompt_version", value: "v2"},
		"unknown summary style":   {field: "summary_style", value: "verbose"},
		"zero trigger":            {field: "trigger_tokens", value: 0},
		"oversized trigger":       {field: "trigger_tokens", value: 10000001},
		"zero output reserve":     {field: "output_reserve_tokens", value: 0},
		"oversized threshold":     {field: "materialization_threshold_bytes", value: 1073741825},
		"negative threshold":      {field: "materialization_threshold_bytes", value: -1},
		"zero threshold":          {field: "materialization_threshold_bytes", value: 0, valid: true},
		"maximum trigger":         {field: "trigger_tokens", value: 10000000, valid: true},
		"target at trigger bound": {field: "target_tokens", value: 10000000},
	} {
		t.Run(name, func(t *testing.T) {
			var document map[string]any
			if err := json.Unmarshal(fixture, &document); err != nil {
				t.Fatal(err)
			}
			policy := document["settings_patch"].(map[string]any)["compaction_policy"].(map[string]any)["set"].(map[string]any)
			if test.field != "" {
				if test.value == nil {
					delete(policy, test.field)
				} else {
					policy[test.field] = test.value
				}
			}
			encoded, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			err = compiled.Validate(encoded)
			if test.valid != (err == nil) {
				t.Fatalf("schema validation error = %v, want valid=%t", err, test.valid)
			}
		})
	}
}

func readV1Schema(t *testing.T, name string) *schema.Schema {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "api", "schema", "v1", name))
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func readV1Fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "testdata", "v1", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}
