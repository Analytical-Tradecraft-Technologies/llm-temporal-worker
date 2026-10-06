package config_test

import (
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
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
