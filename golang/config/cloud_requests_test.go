package config_test

import (
	"context"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
)

const cloudRequestSettings = `  requests:
    provider:
      type: aws
      aws:
        region: ap-southeast-2
      key_value_stores:
        requests: physical-requests
      blob_stores:
        payloads: physical-payloads
    request_table: requests
    payload_store: payloads
    namespace: worker-v1
    secret:
      kind: env
      name: CLOUD_REQUEST_KEY
`

func TestCloudRequestConfigStrictParsingAndSnapshotIsolation(t *testing.T) {
	data := strings.Replace(string(exampleYAML(t)), "state:\n", "state:\n"+cloudRequestSettings, 1)
	snapshot, err := config.Compile(context.Background(), []byte(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	copy := snapshot.Config()
	if copy.State.Requests.Provider.KeyValueStores["requests"] != "physical-requests" {
		t.Fatal("lost alias mapping")
	}
	copy.State.Requests.Provider.KeyValueStores["requests"] = "mutated"
	copy.State.Requests.Secret.Name = "mutated"
	if snapshot.Config().State.Requests.Provider.KeyValueStores["requests"] != "physical-requests" || snapshot.Config().State.Requests.Secret.Name != "CLOUD_REQUEST_KEY" {
		t.Fatal("mutable snapshot")
	}
	next, err := config.Compile(context.Background(), []byte(strings.Replace(data, "physical-requests", "new-table", 1)), nil)
	if err != nil || next.Digest() == snapshot.Digest() {
		t.Fatal("store mapping omitted from snapshot digest", err)
	}
	for _, test := range []struct{ old, replacement string }{
		{"type: aws", "type: azure"},
		{"request_table: requests", "request_table: missing"},
		{"payload_store: payloads", "payload_store: missing"},
		{"namespace: worker-v1", "namespace: ../unsafe"},
		{"region: ap-southeast-2", "region: ''"},
		{"      kind: env\n      name: CLOUD_REQUEST_KEY", "      kind: workload_identity\n      audience: cloud"},
		{"region: ap-southeast-2", "region: ap-southeast-2\n        access_key: inline-secret"},
	} {
		if _, err := config.Load([]byte(strings.Replace(data, test.old, test.replacement, 1))); err == nil {
			t.Fatalf("accepted %s", test.replacement)
		}
	}
}

func TestCloudRequestConfigDoesNotRequirePostgres(t *testing.T) {
	data := string(exampleYAML(t))
	start, end := strings.Index(data, "  postgres:\n"), strings.Index(data, "\nblob_store:")
	if start < 0 || end <= start {
		t.Fatal("missing example PostgreSQL section")
	}
	data = data[:start] + data[end:]
	if _, err := config.Load([]byte(data)); err == nil {
		t.Fatal("legacy durable mode accepted missing PostgreSQL")
	}
	data = strings.Replace(data, "state:\n", "state:\n"+cloudRequestSettings, 1)
	snapshot, err := config.Compile(context.Background(), []byte(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Config().State.Requests == nil {
		t.Fatal("cloud backend missing")
	}
	dormant := strings.Replace(data, "state:\n", "state:\n  postgres:\n    addresses: [invalid-address]\n    max_connections: -1\n", 1)
	if _, err := config.Load([]byte(dormant)); err != nil {
		t.Fatalf("unused PostgreSQL configuration blocked cloud mode: %v", err)
	}
	for _, test := range []struct{ name, old, replacement string }{
		{"cloud alias", "request_table: requests", "request_table: missing"},
		{"Redis persistence", "required_persistence: aof_and_rdb", "required_persistence: none"},
		{"Redis TLS", "enabled: true\n      server_name: redis.example.internal", "enabled: false\n      server_name: redis.example.internal"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if !strings.Contains(data, test.old) {
				t.Fatalf("missing example setting %q", test.old)
			}
			if _, err := config.Load([]byte(strings.Replace(data, test.old, test.replacement, 1))); err == nil {
				t.Fatal("cloud mode accepted invalid required dependency")
			}
		})
	}
}
