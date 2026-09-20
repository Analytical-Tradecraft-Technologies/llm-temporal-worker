package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm/schema"
)

func TestV1ContractFixturesValidate(t *testing.T) {
	cases := []struct{ schemaName, fixture string }{
		{"generate-request.schema.json", "generate-root.json"},
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
		{"generate-request.schema.json", "generate-variant-unknown-temperature.json"},
		{"generate-request.schema.json", "generate-variant-positive-temperature.json"},
		{"compact-request.schema.json", "compact-request-no-cache.json"},
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
		{"generate-response.schema.json", "negative-generate-null-output.json"},
		{"generate-response.schema.json", "negative-generate-cost-enum.json"},
		{"generate-response.schema.json", "negative-generate-cost-cross-variant.json"},
		{"generate-response.schema.json", "negative-generate-diagnostics.json"},
		{"generate-response.schema.json", "negative-currency-field.json"},
		{"generate-response.schema.json", "negative-numeric-usd.json"},
		{"compact-request.schema.json", "negative-compact-tools.json"},
		{"compact-request.schema.json", "negative-compact-structured-output.json"},
		{"compact-request.schema.json", "negative-compact-positive-variant.json"},
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

func TestPollAndPendingSchemas(t *testing.T) {
	handle := `{"operation_id":"op","kind":"generate","provider":"openai","endpoint_id":"endpoint","provider_operation_id":"resp_1"}`
	cases := []struct{ name, payload string }{
		{"generate-response.schema.json", `{"api_version":"llm.temporal/v1","operation_key":"key","operation_id":"op","status":"pending","pending":` + handle + `}`},
		{"poll-request.schema.json", `{"api_version":"llm.temporal/poll/v1","context":{"tenant":"t","project":"p","actor":"a"},"pending":` + handle + `}`},
		{"poll-response.schema.json", `{"api_version":"llm.temporal/poll/v1","status":"pending","pending":` + handle + `}`},
		{"poll-response.schema.json", `{"api_version":"llm.temporal/poll/v1","status":"failed","failure":{"code":"provider_failed","cost_unknown":true},"pending":` + handle + `}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", "..", "api", "schema", "v1", c.name))
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := schema.Parse(data)
			if err != nil {
				t.Fatal(err)
			}
			if err = compiled.Validate([]byte(c.payload)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
