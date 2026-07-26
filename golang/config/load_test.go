package config_test

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func exampleYAML(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestLoadCompleteExample(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != config.APIVersion || loaded.Environment != "production" {
		t.Fatalf("loaded identity = %#v", loaded)
	}
	classes := loaded.Endpoints["openai-prod"].ServiceClasses
	if len(classes) != 3 {
		t.Fatalf("openai service classes = %#v", classes)
	}
	for _, class := range []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard, llm.ServiceClassPriority} {
		if _, ok := classes[class]; !ok {
			t.Fatalf("missing public service class %q", class)
		}
	}
	if _, ok := classes[llm.ServiceClass("provider_default")]; ok {
		t.Fatal("configuration exposed a provider-default public service class")
	}
	if got, want := time.Duration(loaded.Temporal.Worker.HeartbeatKeepaliveInterval), time.Second; got != want {
		t.Fatalf("worker heartbeat keepalive interval = %s, want %s", got, want)
	}
	postgres := loaded.State.Postgres
	if postgres.MinConnections != 8 || postgres.MaxConnections != 64 {
		t.Fatalf("PostgreSQL pool bounds = min %d max %d, want min 8 max 64", postgres.MinConnections, postgres.MaxConnections)
	}
	if got, want := time.Duration(postgres.IdleTransactionTimeout), 30*time.Second; got != want {
		t.Fatalf("PostgreSQL idle transaction timeout = %s, want %s", got, want)
	}
}

func TestLoadDefaultsAndValidatesHeartbeatKeepaliveInterval(t *testing.T) {
	withoutSetting := strings.Replace(string(exampleYAML(t)), "    heartbeat_keepalive_interval: 1s\n", "", 1)
	loaded, err := config.Load([]byte(withoutSetting))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := time.Duration(loaded.Temporal.Worker.HeartbeatKeepaliveInterval), time.Second; got != want {
		t.Fatalf("default worker heartbeat keepalive interval = %s, want %s", got, want)
	}

	invalid := strings.Replace(string(exampleYAML(t)), "    heartbeat_keepalive_interval: 1s", "    heartbeat_keepalive_interval: -1s", 1)
	if _, err := config.Load([]byte(invalid)); err == nil || !strings.Contains(err.Error(), "heartbeat_keepalive_interval") {
		t.Fatalf("invalid heartbeat keepalive interval error = %v", err)
	}
}

func TestLoadAcceptsDevelopmentFileBlobStore(t *testing.T) {
	data := strings.Replace(string(exampleYAML(t)), "environment: production", "environment: development", 1)
	data = strings.Replace(data, `blob_store:
  kind: s3
  inline_bytes: 262144
  s3:
    bucket: acme-llmtw-production
    region: ap-southeast-2
    prefix: v1
    auth:
      kind: aws_default_chain`, `blob_store:
  kind: file
  inline_bytes: 262144
  file:
    root: /var/lib/llmtw/blobs`, 1)
	loaded, err := config.Load([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		BlobStore struct {
			Kind string `json:"kind"`
			File struct {
				Root string `json:"root"`
			} `json:"file"`
		} `json:"blob_store"`
	}
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	if document.BlobStore.Kind != "file" || document.BlobStore.File.Root != "/var/lib/llmtw/blobs" {
		t.Fatalf("development file blob store = %#v", document.BlobStore)
	}
}

func TestLoadRejectsFileBlobStoreOutsideDevelopment(t *testing.T) {
	data := strings.Replace(string(exampleYAML(t)), `blob_store:
  kind: s3
  inline_bytes: 262144
  s3:
    bucket: acme-llmtw-production
    region: ap-southeast-2
    prefix: v1
    auth:
      kind: aws_default_chain`, `blob_store:
  kind: file
  inline_bytes: 262144
  file:
    root: /var/lib/llmtw/blobs`, 1)
	_, err := config.Load([]byte(data))
	if err == nil || !strings.Contains(err.Error(), "development") {
		t.Fatalf("file blob store outside development error = %v", err)
	}
}

func TestLoadRejectsProductionRedisWithoutTLS(t *testing.T) {
	_, err := config.Load(redisTLSDisabledYAML(t))
	if err == nil || !strings.Contains(err.Error(), "state.redis.tls.enabled") {
		t.Fatalf("production Redis without TLS error = %v", err)
	}
}

func TestLoadAcceptsDevelopmentRedisWithoutTLS(t *testing.T) {
	data := strings.Replace(string(redisTLSDisabledYAML(t)), "environment: production", "environment: development", 1)
	if _, err := config.Load([]byte(data)); err != nil {
		t.Fatalf("development Redis without TLS error = %v", err)
	}
}

func redisTLSDisabledYAML(t *testing.T) []byte {
	t.Helper()
	const enabled = `    tls:
      enabled: true
      server_name: redis.example.internal
      ca_file: /var/run/ca/redis.pem`
	const disabled = `    tls:
      enabled: false
      server_name: redis.example.internal
      ca_file: /var/run/ca/redis.pem`
	data := strings.Replace(string(exampleYAML(t)), enabled, disabled, 1)
	if data == string(exampleYAML(t)) {
		t.Fatal("example configuration did not contain the Redis TLS block")
	}
	return []byte(data)
}

func developmentFileBlobYAML(t *testing.T) []byte {
	t.Helper()
	data := strings.Replace(string(exampleYAML(t)), "environment: production", "environment: development", 1)
	data = strings.Replace(data, `blob_store:
  kind: s3
  inline_bytes: 262144
  s3:
    bucket: acme-llmtw-production
    region: ap-southeast-2
    prefix: v1
    auth:
      kind: aws_default_chain`, `blob_store:
  kind: file
  inline_bytes: 262144
  file:
    root: /var/lib/llmtw/blobs`, 1)
	return []byte(data)
}

func TestLoadBudgetPolicyAcceptsEveryDocumentedMatcher(t *testing.T) {
	data := strings.Replace(
		string(exampleYAML(t)),
		"match:\n        tenant: acme\n        environment: production",
		"match:\n        project: critical-workload\n        actor_prefix: service-\n        logical_model: reasoning\n        endpoint: openai-prod\n        service_class: priority",
		1,
	)
	loaded, err := config.Load([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	match := loaded.Budgets.Policies[0].Match
	if match.Project != "critical-workload" || match.ActorPrefix != "service-" || match.LogicalModel != "reasoning" || match.EndpointID != "openai-prod" || match.ServiceClass != llm.ServiceClassPriority {
		t.Fatalf("budget matcher = %#v", match)
	}
	if match.Tenant != "" || match.Environment != "" {
		t.Fatalf("budget matcher retained omitted restrictions = %#v", match)
	}
}

func TestLoadRejectsUnsafeBudgetMatchers(t *testing.T) {
	withEmptyMatch := strings.Replace(string(exampleYAML(t)), "match:\n        tenant: acme\n        environment: production", "match: {}", 1)
	if _, err := config.Load([]byte(withEmptyMatch)); err == nil {
		t.Fatal("accepted an unrestricted budget policy")
	}
	withUnknownClass := strings.Replace(string(exampleYAML(t)), "        environment: production", "        environment: production\n        service_class: provider_default", 1)
	if _, err := config.Load([]byte(withUnknownClass)); err == nil {
		t.Fatal("accepted provider_default as a budget service class")
	}
}

func TestLoadRejectsWildcardOnlyBudgetMatchers(t *testing.T) {
	for _, matcher := range []string{
		"tenant: \"*\"",
		"project: \"*\"",
		"environment: \"*\"",
		"logical_model: \"*\"",
		"endpoint: \"*\"",
	} {
		t.Run(matcher, func(t *testing.T) {
			data := strings.Replace(
				string(exampleYAML(t)),
				"match:\n        tenant: acme\n        environment: production",
				"match:\n        "+matcher,
				1,
			)
			if _, err := config.Load([]byte(data)); err == nil {
				t.Fatalf("accepted unrestricted wildcard matcher %q", matcher)
			}
		})
	}
}

func TestLoadAcceptsWildcardAlongsideBudgetRestriction(t *testing.T) {
	data := strings.Replace(string(exampleYAML(t)), "tenant: acme", "tenant: \"*\"", 1)
	if _, err := config.Load([]byte(data)); err != nil {
		t.Fatalf("rejected wildcard with environment restriction: %v", err)
	}
}

func TestExampleDeclaresExplicitReadinessAndRedisExecutionPolicy(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	server, _ := document["server"].(map[string]any)
	for field, want := range map[string]string{
		"readiness_probe_interval": "5s",
		"readiness_probe_timeout":  "2s",
	} {
		if got, _ := server[field].(string); got != want {
			t.Fatalf("server.%s = %q, want %q", field, got, want)
		}
	}
	state, _ := document["state"].(map[string]any)
	redis, _ := state["redis"].(map[string]any)
	for field, want := range map[string]string{
		"admission_mode":    "function",
		"admission_version": "admission_v1",
	} {
		if got, _ := redis[field].(string); got != want {
			t.Fatalf("state.redis.%s = %q, want %q", field, got, want)
		}
	}
	digest, _ := redis["admission_digest"].(string)
	if len(digest) != 64 {
		t.Fatalf("state.redis.admission_digest = %q, want a SHA-256 hex digest", digest)
	}
}

func TestLoadCanonicalizesAdmissionDigest(t *testing.T) {
	data := strings.Replace(
		string(exampleYAML(t)),
		"admission_digest: c09e24d73750bebee4aad8cd9b1f05abaa22001528cef0ff6842f2241bb8c20b",
		"admission_digest: C09E24D73750BEBEE4AAD8CD9B1F05ABAA22001528CEF0FF6842F2241BB8C20B",
		1,
	)
	loaded, err := config.Load([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := loaded.State.Redis.AdmissionDigest, "c09e24d73750bebee4aad8cd9b1f05abaa22001528cef0ff6842f2241bb8c20b"; got != want {
		t.Fatalf("admission digest = %q, want canonical lowercase %q", got, want)
	}
}

func TestLoadAppliesPostgresNamespaceEnvironmentOverrides(t *testing.T) {
	t.Setenv("LLMTW_POSTGRES_DATABASE", "worker_db")
	t.Setenv("LLMTW_POSTGRES_SCHEMA", "worker_state")
	t.Setenv("LLMTW_POSTGRES_TABLE_PREFIX", "tenant_")
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.Postgres.Database != "worker_db" || loaded.State.Postgres.Schema != "worker_state" || loaded.State.Postgres.TablePrefix != "tenant_" {
		t.Fatalf("unexpected PostgreSQL namespace: %#v", loaded.State.Postgres)
	}
}

func TestLoadDefaultsPostgresIdleTransactionTimeout(t *testing.T) {
	data := strings.Replace(string(exampleYAML(t)), "    idle_transaction_timeout: 30s\n", "", 1)
	loaded, err := config.Load([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := time.Duration(loaded.State.Postgres.IdleTransactionTimeout), 30*time.Second; got != want {
		t.Fatalf("default PostgreSQL idle transaction timeout = %s, want %s", got, want)
	}
}

func TestLoadRejectsRedisOnlyStateInProduction(t *testing.T) {
	data := strings.Replace(string(exampleYAML(t)), "  kind: durable\n", "  kind: redis\n", 1)
	if _, err := config.Load([]byte(data)); err == nil || !strings.Contains(err.Error(), "state.kind redis") {
		t.Fatalf("Redis-only production state was accepted: %v", err)
	}
}

func TestLoadCanonicalizesOutboundProviderHosts(t *testing.T) {
	data := strings.Replace(
		string(exampleYAML(t)),
		"outbound_hosts: [api.openai.com]",
		"outbound_hosts: [API.OPENAI.COM.]",
		1,
	)
	loaded, err := config.Load([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := loaded.Endpoints["openai-prod"].OutboundHosts, []string{"api.openai.com"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("outbound hosts = %#v, want %#v", got, want)
	}
}

func TestLoadAcceptsConfiguredAnthropicAWSGatewayEndpoint(t *testing.T) {
	loaded, err := config.Load([]byte(anthropicAWSGatewayYAML(t)))
	if err != nil {
		t.Fatal(err)
	}
	endpoint, ok := loaded.Endpoints["anthropic-aws-us-east-1"]
	if !ok {
		t.Fatal("Anthropic AWS gateway endpoint was not loaded")
	}
	if endpoint.Family != "anthropic_aws_messages" || endpoint.Region != "us-east-1" || endpoint.AWSWorkspaceID != "ws-example-123" || endpoint.Auth.Kind != "aws_default_chain" {
		t.Fatalf("loaded AWS gateway endpoint = %#v", endpoint)
	}
}

func TestLoadRejectsAnthropicAWSGatewayEndpointWithoutClosedAWSIdentity(t *testing.T) {
	tests := []struct {
		name string
		old  string
		new  string
		want string
	}{
		{name: "base URL", old: "    base_url: https://aws-external-anthropic.us-east-1.api.aws\n", want: "base_url must be an https URL"},
		{name: "region", old: "    region: us-east-1\n", want: "region is required for Anthropic AWS gateway"},
		{name: "workspace", old: "    aws_workspace_id: ws-example-123\n", want: "aws_workspace_id is required"},
		{name: "secret auth", old: "    aws_workspace_id: ws-example-123\n    auth:\n      kind: aws_default_chain", new: "    aws_workspace_id: ws-example-123\n    auth:\n      kind: bearer_env\n      name: ANTHROPIC_AWS_API_KEY", want: "aws_default_chain"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := strings.Replace(anthropicAWSGatewayYAML(t), test.old, test.new, 1)
			_, err := config.Load([]byte(data))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadRejectsAWSWorkspaceIDOnNonAWSGatewayEndpoint(t *testing.T) {
	data := strings.Replace(string(exampleYAML(t)), "  anthropic-direct:\n", "  anthropic-direct:\n    aws_workspace_id: ws-example-123\n", 1)
	_, err := config.Load([]byte(data))
	if err == nil || !strings.Contains(err.Error(), "only valid for Anthropic AWS gateway") {
		t.Fatalf("Load() error = %v", err)
	}
}

func anthropicAWSGatewayYAML(t *testing.T) string {
	t.Helper()
	return string(exampleYAML(t))
}

func TestLoadRejectsUnknownDuplicateAndFourthClass(t *testing.T) {
	unknown := append(exampleYAML(t), []byte("\nunknown_field: true\n")...)
	if _, err := config.Load(unknown); err == nil {
		t.Fatal("accepted an unknown top-level field")
	}
	duplicate := append(exampleYAML(t), []byte("\nversion: llm-temporal-worker/v1\n")...)
	if _, err := config.Load(duplicate); err == nil {
		t.Fatal("accepted a duplicate top-level field")
	}
	fourth := strings.Replace(string(exampleYAML(t)), "service_classes:\n      economy:", "service_classes:\n      turbo:\n        provider_value: turbo\n      economy:", 1)
	if _, err := config.Load([]byte(fourth)); err == nil {
		t.Fatal("accepted a fourth public service class")
	}
}

func TestLoadRejectsUnsafeValuesAndReferences(t *testing.T) {
	cases := map[string]string{
		"unsafe URL":                 strings.Replace(string(exampleYAML(t)), "https://api.openai.com/v1", "http://api.openai.com/v1", 1),
		"timeout":                    strings.Replace(string(exampleYAML(t)), "timeout: 115s", "timeout: 121s", 1),
		"readiness interval":         strings.Replace(string(exampleYAML(t)), "readiness_probe_interval: 5s", "readiness_probe_interval: 0s", 1),
		"readiness timeout ordering": strings.Replace(string(exampleYAML(t)), "readiness_probe_timeout: 2s", "readiness_probe_timeout: 6s", 1),
		"retention":                  strings.Replace(string(exampleYAML(t)), "ambiguous_retention: 90d", "ambiguous_retention: 1d", 1),
		"admission mode":             strings.Replace(string(exampleYAML(t)), "admission_mode: function", "admission_mode: automatic", 1),
		"admission digest":           strings.Replace(string(exampleYAML(t)), "admission_digest: c09e24d73750bebee4aad8cd9b1f05abaa22001528cef0ff6842f2241bb8c20b", "admission_digest: invalid", 1),
		"overflow":                   strings.Replace(string(exampleYAML(t)), "max_connections: 96", "max_connections: 999999999999999999999999", 1),
		"reference":                  strings.Replace(string(exampleYAML(t)), "endpoint: openai-prod", "endpoint: missing-endpoint", 1),
		"literal secret":             strings.Replace(string(exampleYAML(t)), "password:\n      kind: file\n      path: /var/run/secrets/redis-password", "password: plaintext-secret", 1),
		"missing outbound hosts":     strings.Replace(string(exampleYAML(t)), "    outbound_hosts: [api.openai.com]\n", "", 1),
		"unlisted base URL host":     strings.Replace(string(exampleYAML(t)), "outbound_hosts: [api.openai.com]", "outbound_hosts: [other.example]", 1),
		"literal outbound address":   strings.Replace(string(exampleYAML(t)), "outbound_hosts: [api.openai.com]", "outbound_hosts: [127.0.0.1]", 1),
		"outbound userinfo":          strings.Replace(string(exampleYAML(t)), "outbound_hosts: [api.openai.com]", "outbound_hosts: [user@api.openai.com]", 1),
	}
	for name, data := range cases {
		if _, err := config.Load([]byte(data)); err == nil {
			t.Errorf("accepted invalid %s", name)
		}
	}
}
