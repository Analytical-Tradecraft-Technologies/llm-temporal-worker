package config_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

// configSchema compiles the published v1 configuration JSON Schema.
func configSchema(t *testing.T) *schema.Schema {
	t.Helper()
	data, err := os.ReadFile("../api/schema/v1/config.schema.json")
	testassert.NoError(t, err)
	compiled, err := schema.Parse(data)
	testassert.NoError(t, err)
	return compiled
}

// validateConfigSchema validates the JSON encoding of value against the
// published configuration schema.
func validateConfigSchema(t *testing.T, value any) error {
	t.Helper()
	return configSchema(t).Validate(testassert.MarshalJSON(t, value))
}

// mustLoadConfig loads data with config.Load, failing the test on an error.
func mustLoadConfig(t *testing.T, data []byte) config.Config {
	t.Helper()
	loaded, err := config.Load(data)
	testassert.NoError(t, err)
	return loaded
}

// mustCompile compiles data without a reference resolver, failing the test on
// an error.
func mustCompile(t *testing.T, data []byte) *config.Snapshot {
	t.Helper()
	snapshot, err := config.Compile(context.Background(), data, nil)
	testassert.NoError(t, err)
	return snapshot
}

// requireLoadRejected fails the test with the formatted message when
// config.Load accepts data.
func requireLoadRejected(t *testing.T, data []byte, format string, args ...any) {
	t.Helper()
	if _, err := config.Load(data); err == nil {
		t.Fatalf(format, args...)
	}
}

// jsonDocument returns the JSON encoding of value decoded as a generic object.
func jsonDocument(t *testing.T, value any) map[string]any {
	t.Helper()
	var document map[string]any
	testassert.NoError(t, json.Unmarshal(testassert.MarshalJSON(t, value), &document))
	return document
}

// editEndpoint applies edit to endpoint id of value in place.
func editEndpoint(value *config.Config, id string, edit func(*config.EndpointConfig)) {
	endpoint := value.Endpoints[id]
	edit(&endpoint)
	value.Endpoints[id] = endpoint
}

// loadExampleWith loads the shipped example after one exact replacement.
func loadExampleWith(t *testing.T, old, replacement string) config.Config {
	t.Helper()
	return mustLoadConfig(t, replaceExample(t, old, replacement))
}
