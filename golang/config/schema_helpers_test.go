package config_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
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

// validateConfigSchema validates the JSON encoding of value against the
// published configuration schema.
func validateConfigSchema(t *testing.T, value any) error {
	t.Helper()
	return configSchema(t).Validate(mustMarshalJSON(t, value))
}

// mustLoadConfig loads data with config.Load, failing the test on an error.
func mustLoadConfig(t *testing.T, data []byte) config.Config {
	t.Helper()
	loaded, err := config.Load(data)
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

// mustMarshalJSON encodes value as JSON, failing the test on an error.
func mustMarshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
