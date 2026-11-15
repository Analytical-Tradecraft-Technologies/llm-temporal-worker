package config_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
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
	data := string(exampleYAML(t))
	snapshot := mustCompile(t, []byte(data))
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
		requireLoadRejected(t, []byte(strings.Replace(data, test.old, test.replacement, 1)), "accepted %s", test.replacement)
	}
}

func TestDurableConfigRequiresCloudAndRejectsRemovedSQL(t *testing.T) {
	data := string(exampleYAML(t))
	requireLoadRejected(t, []byte(strings.Replace(data, cloudRequestSettings, "", 1)), "durable mode accepted missing cloud requests")
	requireLoadRejected(t, []byte(strings.Replace(data, "state:\n", "state:\n  postgres:\n    database: old_worker\n", 1)), "obsolete SQL config was ignored")
	for _, test := range []struct{ old, replacement string }{
		{"request_table: requests", "request_table: missing"},
		{"required_persistence: aof_and_rdb", "required_persistence: unsupported"},
		{"enabled: true\n      server_name: redis.example.internal", "enabled: false\n      server_name: redis.example.internal"},
	} {
		requireLoadRejected(t, []byte(strings.Replace(data, test.old, test.replacement, 1)), "accepted invalid dependency %s", test.old)
	}
}

func TestCloudRequestMRSCOptIn(t *testing.T) {
	data := string(exampleYAML(t))
	base := mustCompile(t, []byte(data))
	if base.Config().State.Requests.Provider.AWS.AllowMRSC {
		t.Fatal("MRSC enabled by default")
	}
	for _, value := range []string{"false", "true"} {
		fixture := strings.Replace(data, "region: ap-southeast-2", "region: ap-southeast-2\n        allow_mrsc: "+value, 1)
		snapshot := mustCompile(t, []byte(fixture))
		want := value == "true"
		if snapshot.Config().State.Requests.Provider.AWS.AllowMRSC != want {
			t.Fatal("lost MRSC setting")
		}
		if (snapshot.Digest() != base.Digest()) != want {
			t.Fatal("MRSC setting has incorrect snapshot identity")
		}
	}
	for _, value := range []string{"\"true\"", "1", "[]"} {
		fixture := strings.Replace(data, "region: ap-southeast-2", "region: ap-southeast-2\n        allow_mrsc: "+value, 1)
		requireLoadRejected(t, []byte(fixture), "accepted allow_mrsc: %s", value)
	}
}
