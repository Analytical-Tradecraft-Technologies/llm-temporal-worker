package config_test

import (
	"os"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

// configSchema compiles the published v1 configuration JSON Schema.
func configSchema(t *testing.T) *schema.Schema {
	t.Helper()
	data, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}
