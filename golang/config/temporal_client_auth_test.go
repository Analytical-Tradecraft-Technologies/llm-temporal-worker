package config_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm/schema"
)

// Production needs an encrypted, client-authenticated Temporal connection
// under trusted_temporal, unless a service mesh supplies both (#996).
func TestLoadRequiresAuthenticatedTemporalTransportInProduction(t *testing.T) {
	const credentials = "    cert_file: /var/run/ca/temporal-client.pem\n    key_file: /var/run/ca/temporal-client-key.pem\n"
	base := string(exampleYAML(t))
	if !strings.Contains(base, credentials) {
		t.Fatal("example no longer configures a Temporal client certificate")
	}
	noCredentials := strings.Replace(base, credentials, "", 1)
	plaintext := strings.Replace(noCredentials, "  tls:\n    enabled: true\n    server_name: temporal.example.internal", "  tls:\n    enabled: false\n    server_name: temporal.example.internal", 1)
	mesh := strings.Replace(plaintext, "  identity_prefix: llmtw\n", "  identity_prefix: llmtw\n  mesh_transport: true\n", 1)
	apiKey := strings.Replace(noCredentials, "  identity_prefix: llmtw\n", "  identity_prefix: llmtw\n  api_key_file: /var/run/ca/temporal-api-key\n", 1)
	plaintextAPIKey := strings.Replace(mesh, "  mesh_transport: true\n", "  mesh_transport: true\n  api_key_file: /var/run/ca/temporal-api-key\n", 1)
	certOnly := strings.Replace(base, "    key_file: /var/run/ca/temporal-client-key.pem\n", "", 1)
	for name, test := range map[string]struct {
		data string
		want string
	}{
		"mtls":                {data: base},
		"api key":             {data: apiKey},
		"mesh":                {data: mesh},
		"plaintext":           {data: plaintext, want: "temporal.tls.enabled must be true in production"},
		"no credentials":      {data: noCredentials, want: "temporal.api_key_file is required in production"},
		"certificate only":    {data: certOnly, want: "must be set together"},
		"api key without tls": {data: plaintextAPIKey, want: "require temporal.tls.enabled"},
	} {
		if !strings.Contains(test.data, "temporal:") || (name != "mtls" && test.data == base) {
			t.Fatalf("%s: fixture rewrite did not apply", name)
		}
		_, err := config.Load([]byte(test.data))
		if test.want == "" {
			if err != nil {
				t.Fatalf("%s: Load() = %v", name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("%s: Load() = %v, want %q", name, err, test.want)
		}
	}
}

// The public schema mirrors the Temporal credential rules so tooling that
// validates against it rejects what startup would reject.
func TestConfigSchemaMirrorsTemporalCredentialRules(t *testing.T) {
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		mutate func(*config.Config)
		valid  bool
	}{
		"mtls": {mutate: func(*config.Config) {}, valid: true},
		"api key": {mutate: func(c *config.Config) {
			c.Temporal.TLS.CertFile, c.Temporal.TLS.KeyFile, c.Temporal.APIKeyFile = "", "", "/key"
		}, valid: true},
		"mesh": {mutate: func(c *config.Config) {
			c.Temporal.TLS.Enabled, c.Temporal.TLS.CertFile, c.Temporal.TLS.KeyFile, c.Temporal.MeshTransport = false, "", "", true
		}, valid: true},
		"no credentials":   {mutate: func(c *config.Config) { c.Temporal.TLS.CertFile, c.Temporal.TLS.KeyFile = "", "" }},
		"plaintext":        {mutate: func(c *config.Config) { c.Temporal.TLS.Enabled = false }},
		"certificate only": {mutate: func(c *config.Config) { c.Temporal.TLS.KeyFile = "" }},
		"key only":         {mutate: func(c *config.Config) { c.Temporal.TLS.CertFile = "" }},
		"api key without tls": {mutate: func(c *config.Config) {
			c.Temporal.TLS.Enabled, c.Temporal.MeshTransport, c.Temporal.APIKeyFile = false, true, "/key"
		}},
	} {
		loaded, err := config.Load(exampleYAML(t))
		if err != nil {
			t.Fatal(err)
		}
		test.mutate(&loaded)
		encoded, err := json.Marshal(loaded)
		if err != nil {
			t.Fatal(err)
		}
		if err := compiled.Validate(encoded); (err == nil) != test.valid {
			t.Errorf("%s: schema validation = %v, want valid=%t", name, err, test.valid)
		}
	}
}
