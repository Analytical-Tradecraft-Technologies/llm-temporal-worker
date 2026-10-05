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
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
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
	for _, test := range []struct {
		id     string
		family string
	}{
		{id: "bedrock-us-east-1", family: "bedrock_anthropic_messages"},
		{id: "bedrock-converse-us-east-1", family: "bedrock_converse"},
	} {
		endpoint, ok := loaded.Endpoints[test.id]
		if !ok {
			t.Fatalf("example is missing AWS Bedrock endpoint %q", test.id)
		}
		if endpoint.Family != test.family || endpoint.Region != "us-east-1" || endpoint.Auth.Kind != "aws_default_chain" {
			t.Fatalf("loaded Bedrock endpoint %q = %#v", test.id, endpoint)
		}
	}
	var converseRoute bool
	for _, route := range loaded.Models["invoice-summarizer"].Routes {
		if route.ID == "bedrock-converse" {
			converseRoute = route.Endpoint == "bedrock-converse-us-east-1" && route.Model == "amazon.nova-pro-v1:0"
			break
		}
	}
	if !converseRoute {
		t.Fatal("example is missing the Amazon Bedrock Converse route")
	}
	if got, want := time.Duration(loaded.Temporal.Worker.HeartbeatKeepaliveInterval), time.Second; got != want {
		t.Fatalf("worker heartbeat keepalive interval = %s, want %s", got, want)
	}
	if loaded.State.Redis.CoordinationStreamEnabled == nil || !*loaded.State.Redis.CoordinationStreamEnabled {
		t.Fatal("durable example must enable coordination stream readiness")
	}
	if got, want := time.Duration(loaded.State.Redis.StreamTrimSafety), 10*time.Minute; got != want {
		t.Fatalf("coordination stream trim safety = %s, want %s", got, want)
	}
}

func TestLoadDefaultsAndValidatesProviderResponseBytes(t *testing.T) {
	withoutSetting := strings.Replace(string(exampleYAML(t)), "  provider_response_bytes: 16777216\n", "", 1)
	loaded, err := config.Load([]byte(withoutSetting))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := loaded.Limits.ProviderResponseBytes, int64(16<<20); got != want {
		t.Fatalf("default provider response bytes = %d, want %d", got, want)
	}

	configured := strings.Replace(withoutSetting, "  provider_timeout: 120s\n", "  provider_timeout: 120s\n  provider_response_bytes: 8388608\n", 1)
	loaded, err = config.Load([]byte(configured))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := loaded.Limits.ProviderResponseBytes, int64(8<<20); got != want {
		t.Fatalf("configured provider response bytes = %d, want %d", got, want)
	}

	unsafe := strings.Replace(configured, "provider_response_bytes: 8388608", "provider_response_bytes: 67108865", 1)
	if _, err := config.Load([]byte(unsafe)); err == nil || !strings.Contains(err.Error(), "limits.provider_response_bytes") {
		t.Fatalf("unsafe provider response bytes error = %v", err)
	}

	negative := strings.Replace(configured, "provider_response_bytes: 8388608", "provider_response_bytes: -1", 1)
	if _, err := config.Load([]byte(negative)); err == nil || !strings.Contains(err.Error(), "limits.provider_response_bytes") {
		t.Fatalf("negative provider response bytes error = %v", err)
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

func TestLoadDefaultsCoordinationStreamByStateKind(t *testing.T) {
	withoutStreamFields := strings.Replace(string(exampleYAML(t)), "    coordination_stream_enabled: true\n    stream_trim_safety: 10m\n", "", 1)
	loaded, err := config.Load([]byte(withoutStreamFields))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.Redis.CoordinationStreamEnabled == nil || !*loaded.State.Redis.CoordinationStreamEnabled {
		t.Fatal("durable state must default coordination stream readiness on")
	}
	if got, want := time.Duration(loaded.State.Redis.StreamTrimSafety), 10*time.Minute; got != want {
		t.Fatalf("default coordination stream trim safety = %s, want %s", got, want)
	}

	fixture := strings.Replace(withoutCloudRequests(withoutStreamFields), "kind: durable", "kind: redis", 1)
	fixture = strings.Replace(fixture, "environment: production", "environment: development", -1)
	loaded, err = config.Load([]byte(fixture))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.Redis.CoordinationStreamEnabled == nil || *loaded.State.Redis.CoordinationStreamEnabled {
		t.Fatal("Redis-only fixture must default coordination stream readiness off")
	}

	explicitFalse := strings.Replace(withoutStreamFields, "    required_persistence: aof_and_rdb\n", "    required_persistence: aof_and_rdb\n    coordination_stream_enabled: false\n", 1)
	loaded, err = config.Load([]byte(explicitFalse))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.Redis.CoordinationStreamEnabled == nil || *loaded.State.Redis.CoordinationStreamEnabled {
		t.Fatal("explicitly disabled coordination stream unexpectedly enabled")
	}
}

func TestLoadAcceptsDevelopmentFileBlobStore(t *testing.T) {
	data := strings.Replace(string(exampleYAML(t)), "environment: production", "environment: development", -1)
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
	data := strings.Replace(string(redisTLSDisabledYAML(t)), "environment: production", "environment: development", -1)
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
	data := strings.Replace(string(exampleYAML(t)), "environment: production", "environment: development", -1)
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
		"match:\n        project: invoice-processing\n        actor_prefix: service-\n        logical_model: invoice-summarizer\n        endpoint: openai-prod\n        service_class: priority",
		1,
	)
	loaded, err := config.Load([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	match := loaded.Budgets.Policies[0].Match
	if match.Project != "invoice-processing" || match.ActorPrefix != "service-" || match.LogicalModel != "invoice-summarizer" || match.EndpointID != "openai-prod" || match.ServiceClass != llm.ServiceClassPriority {
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
		"actor_prefix: \"*\"",
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
	data := strings.Replace(string(exampleYAML(t)), "match:\n        tenant: acme", "match:\n        tenant: \"*\"", 1)
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
	if got, want := digest, redisstore.AdmissionFunctionDigest(); got != want {
		t.Fatalf("state.redis.admission_digest = %q, want embedded Function digest %q", got, want)
	}
}

func TestLoadCanonicalizesAdmissionDigest(t *testing.T) {
	data := strings.Replace(
		string(exampleYAML(t)),
		"admission_digest: c030680a921b24872bcc935f4d3110c9ea83ae89609e3595ce4d1f03ee623950",
		"admission_digest: C030680A921B24872BCC935F4D3110C9EA83AE89609E3595CE4D1F03EE623950",
		1,
	)
	loaded, err := config.Load([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := loaded.State.Redis.AdmissionDigest, "c030680a921b24872bcc935f4d3110c9ea83ae89609e3595ce4d1f03ee623950"; got != want {
		t.Fatalf("admission digest = %q, want canonical lowercase %q", got, want)
	}
}

func TestLoadRejectsRedisOnlyStateInProduction(t *testing.T) {
	data := strings.Replace(withoutCloudRequests(string(exampleYAML(t))), "  kind: durable\n", "  kind: redis\n", 1)
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

func TestLoadRejectsBedrockEndpointsWithoutClosedAWSIdentity(t *testing.T) {
	tests := []struct {
		name   string
		family string
		auth   string
	}{
		{
			name:   "anthropic messages bearer",
			family: "bedrock_anthropic_messages",
			auth:   "kind: bearer_env\n      name: BEDROCK_API_KEY",
		},
		{
			name:   "converse header",
			family: "bedrock_converse",
			auth:   "kind: header_env\n      name: BEDROCK_API_KEY",
		},
		{
			name:   "anthropic messages workload identity",
			family: "bedrock_anthropic_messages",
			auth:   "kind: workload_identity\n      audience: https://bedrock.amazonaws.com",
		},
		{
			name:   "converse Azure credential",
			family: "bedrock_converse",
			auth:   "kind: azure_default_credential",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := string(exampleYAML(t))
			if test.family == "bedrock_converse" {
				data = strings.Replace(data, "family: bedrock_anthropic_messages", "family: bedrock_converse", 1)
			}
			old := "  bedrock-us-east-1:\n    family: " + test.family + "\n    outbound_hosts: [bedrock-runtime.us-east-1.amazonaws.com]\n    region: us-east-1\n    auth:\n      kind: aws_default_chain"
			newBlock := strings.Replace(old, "kind: aws_default_chain", test.auth, 1)
			data = strings.Replace(data, old, newBlock, 1)
			if !strings.Contains(data, newBlock) {
				t.Fatal("test fixture block was not found")
			}
			if _, err := config.Load([]byte(data)); err == nil || !strings.Contains(err.Error(), "auth.kind must be aws_default_chain for Bedrock") {
				t.Fatalf("Load() error = %v, want Bedrock default-chain authentication rejection", err)
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
		"admission digest":           strings.Replace(string(exampleYAML(t)), "admission_digest: c030680a921b24872bcc935f4d3110c9ea83ae89609e3595ce4d1f03ee623950", "admission_digest: invalid", 1),
		"stream trim safety":         strings.Replace(string(exampleYAML(t)), "stream_trim_safety: 10m", "stream_trim_safety: 31d", 1),
		"stream trim safety minimum": strings.Replace(string(exampleYAML(t)), "stream_trim_safety: 10m", "stream_trim_safety: 1ns", 1),
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

func withoutCloudRequests(value string) string {
	start := strings.Index(value, "  requests:\n")
	end := strings.Index(value[start:], "\nblob_store:") + start
	return value[:start] + value[end:]
}

func TestExampleAzureAPIVersionIsAString(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Endpoints["azure-openai-au"].Extensions["azure"]["api_version"]; got != "2024-10-21" {
		t.Fatalf("example Azure api_version = %#v, want string 2024-10-21", got)
	}
}

func TestLoadRejectsUnquotedAzureExtensionScalars(t *testing.T) {
	for _, test := range []struct{ from, to, field string }{
		{from: `api_version: "2024-10-21"`, to: `api_version: 2024-10-21`, field: "api_version"},
		{from: `api_version: "2024-10-21"`, to: "api_version: \"2024-10-21\"\n        deployment: 7", field: "deployment"},
	} {
		data := strings.Replace(string(exampleYAML(t)), test.from, test.to, 1)
		if data == string(exampleYAML(t)) {
			t.Fatalf("example does not contain %q", test.from)
		}
		_, err := config.Load([]byte(data))
		if err == nil || !strings.Contains(err.Error(), "extensions.azure."+test.field+" must be a quoted string") {
			t.Fatalf("Load(%s) error = %v", test.field, err)
		}
	}
}

func TestLoadRejectsActivityBoundsTheWorkflowCannotHonour(t *testing.T) {
	for _, test := range []struct{ from, to, want string }{
		{from: "provider_timeout: 120s", to: "provider_timeout: 5m", want: "limits.provider_timeout must be shorter than"},
		{from: "heartbeat_keepalive_interval: 1s", to: "heartbeat_keepalive_interval: 11s", want: "heartbeat_keepalive_interval must be at most 10s"},
		{from: "inline_payload_bytes: 524288", to: "inline_payload_bytes: 4194304", want: "server.inline_payload_bytes must be between 1 and 2097152"},
	} {
		data := strings.Replace(string(exampleYAML(t)), test.from, test.to, 1)
		if data == string(exampleYAML(t)) {
			t.Fatalf("example does not contain %q", test.from)
		}
		if _, err := config.Load([]byte(data)); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("Load(%s) error = %v, want %q", test.to, err, test.want)
		}
	}
	for _, test := range []struct{ from, to string }{
		{from: "provider_timeout: 120s", to: "provider_timeout: 4m59s"},
		{from: "heartbeat_keepalive_interval: 1s", to: "heartbeat_keepalive_interval: 10s"},
		{from: "inline_payload_bytes: 524288", to: "inline_payload_bytes: 2097152"},
	} {
		data := strings.Replace(string(exampleYAML(t)), test.from, test.to, 1)
		if _, err := config.Load([]byte(data)); err != nil {
			t.Fatalf("Load(%s) error = %v", test.to, err)
		}
	}
}

func TestValidateMatchesRuntimeParsersForStartupSettings(t *testing.T) {
	for _, test := range []struct{ from, to, want string }{
		{from: `sample_ratio: "0.05"`, to: `sample_ratio: "1/20"`, want: "sample_ratio must be a decimal between 0 and 1"},
		{from: "metrics_address: 0.0.0.0:9090", to: "metrics_address: :8080", want: "must be identical to share a listener"},
		{from: "metrics_address: 0.0.0.0:9090", to: "metrics_address: 0.0.0.0:08080", want: "must be identical to share a listener"},
		{from: "metrics_address: 0.0.0.0:9090", to: "metrics_address: 0.0.0.0:99999", want: "port must be between 0 and 65535"},
		{from: "max_output_tokens: 32768", to: "max_output_tokens: 3000000000", want: "limits.max_output_tokens must not exceed 2147483647"},
	} {
		data := strings.Replace(string(exampleYAML(t)), test.from, test.to, 1)
		if data == string(exampleYAML(t)) {
			t.Fatalf("example does not contain %q", test.from)
		}
		if _, err := config.Load([]byte(data)); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("Load(%s) error = %v, want %q", test.to, err, test.want)
		}
	}
	shared := strings.Replace(string(exampleYAML(t)), "metrics_address: 0.0.0.0:9090", "metrics_address: 0.0.0.0:8080", 1)
	if _, err := config.Load([]byte(shared)); err != nil {
		t.Fatalf("identical shared listener rejected: %v", err)
	}
}

func TestWorkloadIdentityPathsListsEveryReference(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	if paths := loaded.WorkloadIdentityPaths(); len(paths) != 0 {
		t.Fatalf("example workload identity paths = %v", paths)
	}
	loaded.State.Redis.Password = config.SecretRef{Kind: config.SecretWorkloadIdentity, Audience: "redis"}
	endpoint := loaded.Endpoints["openai-prod"]
	endpoint.Auth = config.AuthConfig{Kind: "workload_identity", Audience: "provider"}
	loaded.Endpoints["openai-prod"] = endpoint
	if got := loaded.WorkloadIdentityPaths(); len(got) != 2 || got[0] != "endpoints.openai-prod.auth" || got[1] != "state.redis.password" {
		t.Fatalf("workload identity paths = %v", got)
	}
}
