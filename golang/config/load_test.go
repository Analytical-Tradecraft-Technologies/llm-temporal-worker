package config_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
)

func exampleYAML(t *testing.T) []byte {
	t.Helper()
	return testassert.ReadFile(t, "../config.example.yaml")
}

// assertExampleEditRejected replaces the first from in the example with to and
// checks that Load rejects the result with an error containing want.
func assertExampleEditRejected(t *testing.T, from, to, want string) {
	t.Helper()
	data := string(replaceExample(t, from, to))
	testassert.ErrorContains(t, testassert.Err(config.Load([]byte(data))), want, fmt.Sprintf("Load(%s)", to))
}

func TestLoadCompleteExample(t *testing.T) {
	loaded := mustLoadConfig(t, exampleYAML(t))
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
	requireHeartbeatKeepaliveSecond(t, loaded, "worker")
	requireCoordinationStream(t, loaded, "durable example")
}

// requireHeartbeatKeepaliveSecond fails unless loaded keeps the one-second
// worker heartbeat keepalive interval.
func requireHeartbeatKeepaliveSecond(t *testing.T, loaded config.Config, what string) {
	t.Helper()
	if got, want := time.Duration(loaded.Temporal.Worker.HeartbeatKeepaliveInterval), time.Second; got != want {
		t.Fatalf("%s heartbeat keepalive interval = %s, want %s", what, got, want)
	}
}

// requireCoordinationStream fails unless loaded enables coordination stream
// readiness with the ten-minute trim safety.
func requireCoordinationStream(t *testing.T, loaded config.Config, what string) {
	t.Helper()
	if loaded.State.Redis.CoordinationStreamEnabled == nil || !*loaded.State.Redis.CoordinationStreamEnabled {
		t.Fatalf("%s must enable coordination stream readiness", what)
	}
	if got, want := time.Duration(loaded.State.Redis.StreamTrimSafety), 10*time.Minute; got != want {
		t.Fatalf("%s coordination stream trim safety = %s, want %s", what, got, want)
	}
}

func TestLoadDefaultsAndValidatesProviderResponseBytes(t *testing.T) {
	withoutSetting := strings.Replace(string(exampleYAML(t)), "  provider_response_bytes: 983040\n", "", 1)
	loaded := mustLoadConfig(t, []byte(withoutSetting))
	if got, want := loaded.Limits.ProviderResponseBytes, int64(960<<10); got != want {
		t.Fatalf("default provider response bytes = %d, want %d", got, want)
	}

	configured := strings.Replace(withoutSetting, "  provider_timeout: 120s\n", "  provider_timeout: 120s\n  provider_response_bytes: 524288\n", 1)
	loaded = mustLoadConfig(t, []byte(configured))
	if got, want := loaded.Limits.ProviderResponseBytes, int64(512<<10); got != want {
		t.Fatalf("configured provider response bytes = %d, want %d", got, want)
	}

	unsafe := strings.Replace(configured, "provider_response_bytes: 524288", "provider_response_bytes: 67108865", 1)
	testassert.ErrorContains(t, testassert.Err(config.Load([]byte(unsafe))), "limits.provider_response_bytes")

	negative := strings.Replace(configured, "provider_response_bytes: 524288", "provider_response_bytes: -1", 1)
	testassert.ErrorContains(t, testassert.Err(config.Load([]byte(negative))), "limits.provider_response_bytes")
}

// TestLoadRejectsProviderResponseThatCannotBeSaved checks that every response
// limits.provider_response_bytes admits fits in its durable record beside the
// largest request server.inline_payload_bytes admits (#1319).
func TestLoadRejectsProviderResponseThatCannotBeSaved(t *testing.T) {
	// With the example's 512 KiB inline limit: (8 MiB - 512 KiB - 256 KiB) / 6.
	if got := config.MaxSavableProviderResponseBytes(512 << 10); got != 1267029 {
		t.Fatalf("most savable response bytes = %d, want 1267029", got)
	}
	// The default fits beside every inline limit, up to the 2 MiB cap.
	if got := config.MaxSavableProviderResponseBytes(config.TemporalBlobLimitBytes); got < config.DefaultProviderResponseBytes {
		t.Fatalf("default provider response bytes %d exceed %d, the most savable beside a 2 MiB request", config.DefaultProviderResponseBytes, got)
	}
	const setting = "provider_response_bytes: 983040"
	mustLoadConfig(t, replaceExample(t, setting, "provider_response_bytes: 1267029"))
	for _, test := range []struct{ to, want string }{
		// The former 16 MiB default is twice the record limit.
		{to: "provider_response_bytes: 16777216", want: "limits.provider_response_bytes must be at most 1267029 with server.inline_payload_bytes 524288"},
		{to: "provider_response_bytes: 1267030", want: "could be paid for but never saved in the 8388608-byte durable record"},
	} {
		assertExampleEditRejected(t, setting, test.to, test.want)
	}
	// A larger request leaves less of the record for the response.
	larger := strings.Replace(string(replaceExample(t, setting, "provider_response_bytes: 1267029")), "inline_payload_bytes: 524288", "inline_payload_bytes: 2097152", 1)
	testassert.ErrorContains(t, testassert.Err(config.Load([]byte(larger))), "limits.provider_response_bytes must be at most 1004885 with server.inline_payload_bytes 2097152")
}

func TestLoadDefaultsAndValidatesHeartbeatKeepaliveInterval(t *testing.T) {
	loaded := loadExampleWith(t, "    heartbeat_keepalive_interval: 1s\n", "")
	requireHeartbeatKeepaliveSecond(t, loaded, "default worker")

	invalid := strings.Replace(string(exampleYAML(t)), "    heartbeat_keepalive_interval: 1s", "    heartbeat_keepalive_interval: -1s", 1)
	testassert.ErrorContains(t, testassert.Err(config.Load([]byte(invalid))), "heartbeat_keepalive_interval")
}

func TestLoadDefaultsCoordinationStreamByStateKind(t *testing.T) {
	withoutStreamFields := strings.Replace(string(exampleYAML(t)), "    coordination_stream_enabled: true\n    stream_trim_safety: 10m\n", "", 1)
	loaded := mustLoadConfig(t, []byte(withoutStreamFields))
	requireCoordinationStream(t, loaded, "defaulted durable state")

	fixture := strings.Replace(withoutCloudRequests(withoutStreamFields), "kind: durable", "kind: redis", 1)
	fixture = strings.Replace(fixture, "environment: production", "environment: development", -1)
	loaded = mustLoadConfig(t, []byte(fixture))
	if loaded.State.Redis.CoordinationStreamEnabled == nil || *loaded.State.Redis.CoordinationStreamEnabled {
		t.Fatal("Redis-only fixture must default coordination stream readiness off")
	}

	explicitFalse := strings.Replace(withoutStreamFields, "    required_persistence: aof_and_rdb\n", "    required_persistence: aof_and_rdb\n    coordination_stream_enabled: false\n", 1)
	loaded = mustLoadConfig(t, []byte(explicitFalse))
	if loaded.State.Redis.CoordinationStreamEnabled == nil || *loaded.State.Redis.CoordinationStreamEnabled {
		t.Fatal("explicitly disabled coordination stream unexpectedly enabled")
	}
}

func TestLoadAcceptsDevelopmentFileBlobStore(t *testing.T) {
	loaded := mustLoadConfig(t, developmentFileBlobYAML(t))
	encoded := testassert.MarshalJSON(t, loaded)
	var document struct {
		BlobStore struct {
			Kind string `json:"kind"`
			File struct {
				Root string `json:"root"`
			} `json:"file"`
		} `json:"blob_store"`
	}
	testassert.NoError(t, json.Unmarshal(encoded, &document))
	if document.BlobStore.Kind != "file" || document.BlobStore.File.Root != "/var/lib/llmtw/blobs" {
		t.Fatalf("development file blob store = %#v", document.BlobStore)
	}
}

func TestLoadRejectsFileBlobStoreOutsideDevelopment(t *testing.T) {
	data := fileBlobStoreExample(t)
	_, err := config.Load([]byte(data))
	testassert.ErrorContains(t, err, "development")
}

func TestLoadRejectsProductionRedisWithoutTLS(t *testing.T) {
	_, err := config.Load(redisTLSDisabledYAML(t))
	testassert.ErrorContains(t, err, "state.redis.tls.enabled")
}

func TestLoadAcceptsDevelopmentRedisWithoutTLS(t *testing.T) {
	data := strings.Replace(string(redisTLSDisabledYAML(t)), "environment: production", "environment: development", -1)
	testassert.NoError(t, testassert.Err(config.Load([]byte(data))), "development Redis without TLS error")
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
	return replaceExample(t, enabled, disabled)
}

// loadEveryBudgetMatcherExample loads the example with its first budget
// policy restricted by every documented matcher except tenant and environment.
func loadEveryBudgetMatcherExample(t *testing.T) config.Config {
	t.Helper()
	return loadExampleWith(t,
		"match:\n        tenant: acme\n        environment: production",
		"match:\n        project: invoice-processing\n        actor_prefix: service-\n        logical_model: invoice-summarizer\n        endpoint: openai-prod\n        service_class: priority",
	)
}

// fileBlobStoreExample returns the example with its S3 blob store replaced by
// a file blob store.
func fileBlobStoreExample(t *testing.T) string {
	t.Helper()
	return string(replaceExample(t, `blob_store:
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
    root: /var/lib/llmtw/blobs`))
}

func developmentFileBlobYAML(t *testing.T) []byte {
	t.Helper()
	return []byte(strings.Replace(fileBlobStoreExample(t), "environment: production", "environment: development", -1))
}

func TestLoadBudgetPolicyAcceptsEveryDocumentedMatcher(t *testing.T) {
	loaded := loadEveryBudgetMatcherExample(t)
	match := loaded.Budgets.Policies[0].Match
	if match.Project != "invoice-processing" || match.ActorPrefix != "service-" || match.LogicalModel != "invoice-summarizer" || match.EndpointID != "openai-prod" || match.ServiceClass != llm.ServiceClassPriority {
		t.Fatalf("budget matcher = %#v", match)
	}
	if match.Tenant != "" || match.Environment != "" {
		t.Fatalf("budget matcher retained omitted restrictions = %#v", match)
	}
}

func TestLoadRejectsUnsafeBudgetMatchers(t *testing.T) {
	requireLoadRejected(t, replaceExample(t, "match:\n        tenant: acme\n        environment: production", "match: {}"), "accepted an unrestricted budget policy")
	requireLoadRejected(t, replaceExample(t, "        environment: production", "        environment: production\n        service_class: provider_default"), "accepted provider_default as a budget service class")
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
			data := replaceExample(t, "match:\n        tenant: acme\n        environment: production", "match:\n        "+matcher)
			requireLoadRejected(t, data, "accepted unrestricted wildcard matcher %q", matcher)
		})
	}
}

func TestLoadAcceptsWildcardAlongsideBudgetRestriction(t *testing.T) {
	testassert.NoError(t, testassert.Err(config.Load(replaceExample(t, "match:\n        tenant: acme", "match:\n        tenant: \"*\""))), "rejected wildcard with environment restriction")
}

func TestExampleDeclaresExplicitReadinessAndRedisExecutionPolicy(t *testing.T) {
	document := jsonDocument(t, mustLoadConfig(t, exampleYAML(t)))
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
		"admission_version": "admission_628f464157f0b550",
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
	loaded := loadExampleWith(t,
		"admission_digest: 1b7248f3703f71f4e27d847e82ade143076f38d0b62e819d807f9ca48ac3d78f",
		"admission_digest: 1B7248F3703F71F4E27D847E82ADE143076F38D0B62E819D807F9CA48AC3D78F",
	)
	if got, want := loaded.State.Redis.AdmissionDigest, "1b7248f3703f71f4e27d847e82ade143076f38d0b62e819d807f9ca48ac3d78f"; got != want {
		t.Fatalf("admission digest = %q, want canonical lowercase %q", got, want)
	}
}

func TestLoadRejectsRedisOnlyStateInProduction(t *testing.T) {
	data := strings.Replace(withoutCloudRequests(string(exampleYAML(t))), "  kind: durable\n", "  kind: redis\n", 1)
	testassert.ErrorContains(t, testassert.Err(config.Load([]byte(data))), "state.kind redis")
}

func TestLoadCanonicalizesOutboundProviderHosts(t *testing.T) {
	loaded := loadExampleWith(t, "outbound_hosts: [api.openai.com]", "outbound_hosts: [API.OPENAI.COM.]")
	if got, want := loaded.Endpoints["openai-prod"].OutboundHosts, []string{"api.openai.com"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("outbound hosts = %#v, want %#v", got, want)
	}
}

func TestLoadAcceptsConfiguredAnthropicAWSGatewayEndpoint(t *testing.T) {
	loaded := mustLoadConfig(t, []byte(anthropicAWSGatewayYAML(t)))
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
			testassert.ErrorContains(t, err, test.want)
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
			testassert.ErrorContains(t, testassert.Err(config.Load([]byte(data))), "auth.kind must be aws_default_chain for Bedrock")
		})
	}
}

func TestLoadRejectsAWSWorkspaceIDOnNonAWSGatewayEndpoint(t *testing.T) {
	_, err := config.Load(replaceExample(t, "  anthropic-direct:\n", "  anthropic-direct:\n    aws_workspace_id: ws-example-123\n"))
	testassert.ErrorContains(t, err, "only valid for Anthropic AWS gateway")
}

func anthropicAWSGatewayYAML(t *testing.T) string {
	t.Helper()
	return string(exampleYAML(t))
}

func TestLoadRejectsUnknownDuplicateAndFourthClass(t *testing.T) {
	unknown := append(exampleYAML(t), []byte("\nunknown_field: true\n")...)
	requireLoadRejected(t, unknown, "accepted an unknown top-level field")
	duplicate := append(exampleYAML(t), []byte("\nversion: llm-temporal-worker/v1\n")...)
	requireLoadRejected(t, duplicate, "accepted a duplicate top-level field")
	fourth := replaceExample(t, "service_classes:\n      economy:", "service_classes:\n      turbo:\n        provider_value: turbo\n      economy:")
	requireLoadRejected(t, fourth, "accepted a fourth public service class")
}

func TestLoadRejectsUnsafeValuesAndReferences(t *testing.T) {
	cases := map[string]string{
		"unsafe URL":                 strings.Replace(string(exampleYAML(t)), "https://api.openai.com/v1", "http://api.openai.com/v1", 1),
		"timeout":                    strings.Replace(string(exampleYAML(t)), "timeout: 115s", "timeout: 121s", 1),
		"readiness interval":         strings.Replace(string(exampleYAML(t)), "readiness_probe_interval: 5s", "readiness_probe_interval: 0s", 1),
		"readiness timeout ordering": strings.Replace(string(exampleYAML(t)), "readiness_probe_timeout: 2s", "readiness_probe_timeout: 6s", 1),
		"retention":                  strings.Replace(string(exampleYAML(t)), "ambiguous_retention: 90d", "ambiguous_retention: 1d", 1),
		"admission mode":             strings.Replace(string(exampleYAML(t)), "admission_mode: function", "admission_mode: automatic", 1),
		"admission digest":           strings.Replace(string(exampleYAML(t)), "admission_digest: 1b7248f3703f71f4e27d847e82ade143076f38d0b62e819d807f9ca48ac3d78f", "admission_digest: invalid", 1),
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
	loaded := mustLoadConfig(t, exampleYAML(t))
	if got := loaded.Endpoints["azure-openai-au"].Extensions["azure"]["api_version"]; got != "v1" {
		t.Fatalf("example Azure api_version = %#v, want string v1", got)
	}
}

func TestLoadRejectsUnquotedAzureExtensionScalars(t *testing.T) {
	for _, test := range []struct{ from, to, field string }{
		{from: `api_version: "v1"`, to: `api_version: 2024-10-21`, field: "api_version"},
		{from: `api_version: "v1"`, to: "api_version: \"v1\"\n        deployment: 7", field: "deployment"},
	} {
		_, err := config.Load(replaceExample(t, test.from, test.to))
		testassert.ErrorContains(t, err, "extensions.azure."+test.field+" must be a quoted string", fmt.Sprintf("Load(%s)", test.field))
	}
}

func TestLoadRejectsActivityBoundsTheWorkflowCannotHonour(t *testing.T) {
	for _, test := range []struct{ from, to, want string }{
		{from: "provider_timeout: 120s", to: "provider_timeout: 10m", want: "limits.provider_timeout must be shorter than"},
		{from: "heartbeat_keepalive_interval: 1s", to: "heartbeat_keepalive_interval: 11s", want: "heartbeat_keepalive_interval must be at most 10s"},
		{from: "inline_payload_bytes: 524288", to: "inline_payload_bytes: 4194304", want: "server.inline_payload_bytes must be between 1 and 2097152"},
	} {
		assertExampleEditRejected(t, test.from, test.to, test.want)
	}
	for _, test := range []struct{ from, to string }{
		{from: "provider_timeout: 120s", to: "provider_timeout: 9m59s"},
		{from: "heartbeat_keepalive_interval: 1s", to: "heartbeat_keepalive_interval: 10s"},
		{from: "inline_payload_bytes: 524288", to: "inline_payload_bytes: 2097152"},
	} {
		if _, err := config.Load(replaceExample(t, test.from, test.to)); err != nil {
			t.Fatalf("Load(%s) error = %v", test.to, err)
		}
	}
}

func TestLoadRejectsOutputReservationThatCannotBeReturned(t *testing.T) {
	example := string(exampleYAML(t))
	replace := func(data string, pairs ...string) string {
		t.Helper()
		for i := 0; i < len(pairs); i += 2 {
			if !strings.Contains(data, pairs[i]) {
				t.Fatalf("example does not contain %q", pairs[i])
			}
			data = strings.Replace(data, pairs[i], pairs[i+1], 1)
		}
		return data
	}
	for _, test := range []struct {
		pairs []string
		want  string
	}{
		{pairs: []string{"max_output_tokens: 32768", "max_output_tokens: 524288"}, want: "limits.max_output_tokens must be less than server.inline_payload_bytes (524288)"},
		{pairs: []string{"inline_payload_bytes: 524288", "inline_payload_bytes: 32768"}, want: "limits.max_output_tokens must be less than server.inline_payload_bytes (32768)"},
		{pairs: []string{"provider_response_bytes: 983040", "provider_response_bytes: 32768"}, want: "limits.max_output_tokens must be less than limits.provider_response_bytes (32768)"},
	} {
		testassert.ErrorContains(t, testassert.Err(config.Load([]byte(replace(example, test.pairs...)))), test.want, fmt.Sprintf("Load(%v)", test.pairs))
	}
	for _, pairs := range [][]string{
		{"max_output_tokens: 32768", "max_output_tokens: 524287"},
		{"inline_payload_bytes: 524288", "inline_payload_bytes: 32769", "provider_response_bytes: 983040", "provider_response_bytes: 32769"},
	} {
		if _, err := config.Load([]byte(replace(example, pairs...))); err != nil {
			t.Fatalf("Load(%v) error = %v", pairs, err)
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
		assertExampleEditRejected(t, test.from, test.to, test.want)
	}
	testassert.NoError(t, testassert.Err(config.Load(replaceExample(t, "metrics_address: 0.0.0.0:9090", "metrics_address: 0.0.0.0:8080"))), "identical shared listener rejected")
}

func TestResponsesAPIListenAddressIsOptionalAndNeedsItsOwnPort(t *testing.T) {
	example := string(exampleYAML(t))
	loaded := mustLoadConfig(t, []byte(example))
	if loaded.Server.ResponsesAPIListenAddress != "" {
		t.Fatalf("the example enables the responses API: %q", loaded.Server.ResponsesAPIListenAddress)
	}
	withAddress := func(address string) []byte {
		return []byte(strings.Replace(example, "metrics_address: 0.0.0.0:9090", "metrics_address: 0.0.0.0:9090\n  responses_api_listen_address: "+address, 1))
	}
	enabled := mustLoadConfig(t, withAddress("127.0.0.1:8000"))
	if enabled.Server.ResponsesAPIListenAddress != "127.0.0.1:8000" {
		t.Fatalf("address = %q", enabled.Server.ResponsesAPIListenAddress)
	}
	// Unset, the field is absent from the canonical JSON the digest covers.
	if canonical, err := json.Marshal(loaded); err != nil || strings.Contains(string(canonical), "responses_api_listen_address") {
		t.Fatalf("an unset responses API address is in the canonical JSON (err %v)", err)
	}
	for address, want := range map[string]string{
		"127.0.0.1:8080": "must use a different port",
		"127.0.0.1:9090": "must use a different port",
		"no-port":        "server.responses_api_listen_address must be host:port",
	} {
		testassert.ErrorContains(t, testassert.Err(config.Load(withAddress(address))), want, fmt.Sprintf("Load(%s)", address))
	}
}

func TestWorkloadIdentityPathsListsEveryReference(t *testing.T) {
	loaded := mustLoadConfig(t, exampleYAML(t))
	if paths := loaded.WorkloadIdentityPaths(); len(paths) != 0 {
		t.Fatalf("example workload identity paths = %v", paths)
	}
	loaded.State.Redis.Password = config.SecretRef{Kind: config.SecretWorkloadIdentity, Audience: "redis"}
	editEndpoint(&loaded, "openai-prod", func(endpoint *config.EndpointConfig) {
		endpoint.Auth = config.AuthConfig{Kind: "workload_identity", Audience: "provider"}
	})
	if got := loaded.WorkloadIdentityPaths(); len(got) != 2 || got[0] != "endpoints.openai-prod.auth" || got[1] != "state.redis.password" {
		t.Fatalf("workload identity paths = %v", got)
	}
}

// Settings that only accept the value the runtime implements reject any
// other value instead of being silently ignored (#950).
func TestLoadRejectsContinuationAndCapabilitySettingsTheRuntimeDoesNotImplement(t *testing.T) {
	for _, test := range []struct{ from, to, want string }{
		{from: "retain_canonical_transcript: true", to: "retain_canonical_transcript: false", want: "continuation.retain_canonical_transcript must be true"},
		{from: "unknown_in_strict_mode: reject", to: "unknown_in_strict_mode: allow", want: "capabilities.unknown_in_strict_mode must be reject"},
	} {
		assertExampleEditRejected(t, test.from, test.to, test.want)
	}
	loaded := loadExampleWith(t, "  retain_canonical_transcript: true\n", "")
	if loaded.Continuation.RetainCanonicalTranscript == nil || !*loaded.Continuation.RetainCanonicalTranscript {
		t.Fatalf("omitted retain_canonical_transcript = %v, want default true", loaded.Continuation.RetainCanonicalTranscript)
	}
}

// Redis computes budget window expiry in whole milliseconds, so a window
// with fractional-millisecond geometry is rejected rather than rounded.
func TestLoadRejectsFractionalMillisecondBudgetWindows(t *testing.T) {
	data := strings.Replace(string(exampleYAML(t)), "bucket: 1m", "bucket: 1500us", 1)
	if data == string(exampleYAML(t)) {
		t.Skip("example has no one-minute bucket to rewrite")
	}
	testassert.ErrorContains(t, testassert.Err(config.Load([]byte(data))), "whole milliseconds")
}

// The unauthenticated listener binds only the host in production unless the
// operator acknowledges a private network (#1514).
func TestResponsesAPIListenerBindsTheHostInProduction(t *testing.T) {
	example := string(exampleYAML(t))
	with := func(environment, lines string) []byte {
		data := strings.Replace(example, "metrics_address: 0.0.0.0:9090", "metrics_address: 0.0.0.0:9090\n"+lines, 1)
		return []byte(strings.Replace(data, "environment: production", "environment: "+environment, 1))
	}
	for _, address := range []string{"127.0.0.1:8000", "[::1]:8000", "localhost:8000", "169.254.10.1:8000", "[fe80::1%eth0]:8000"} {
		if _, err := config.Load(with("production", "  responses_api_listen_address: \""+address+"\"")); err != nil {
			t.Fatalf("host-local %s rejected: %v", address, err)
		}
	}
	const want = "must bind a loopback or link-local address in production unless server.responses_api_private_network is true"
	for _, address := range []string{"0.0.0.0:8000", ":8000", "10.0.0.5:8000", "[::]:8000", "worker.internal:8000"} {
		testassert.ErrorContains(t, testassert.Err(config.Load(with("production", "  responses_api_listen_address: \""+address+"\""))), want, fmt.Sprintf("Load(%s)", address))
		acknowledged, err := config.Load(with("production", "  responses_api_listen_address: \""+address+"\"\n  responses_api_private_network: true"))
		if err != nil || !acknowledged.Server.ResponsesAPIPrivateNetwork {
			t.Fatalf("acknowledged %s rejected: %v", address, err)
		}
	}
	testassert.ErrorContains(t, testassert.Err(config.Load(with("production", "  responses_api_private_network: true"))), "requires server.responses_api_listen_address")
	// Development keeps the shape-only rule.
	if _, err := config.Load(with("development", "  responses_api_listen_address: 0.0.0.0:8000")); err != nil && strings.Contains(err.Error(), "responses_api") {
		t.Fatalf("development listener rejected: %v", err)
	}
}

func TestLoadValidatesOperationKeyReuseHorizon(t *testing.T) {
	withHorizon := func(horizon, extra string) []byte {
		return replaceExample(t, "    stream_trim_safety: 10m\n", "    stream_trim_safety: 10m\n    operation_key_reuse_horizon: "+horizon+"\n"+extra)
	}
	loaded, err := config.Load(withHorizon("100d", ""))
	testassert.NoError(t, err, "100d horizon")
	if got, want := time.Duration(loaded.State.Redis.OperationKeyReuseHorizon), 100*24*time.Hour; got != want {
		t.Fatalf("horizon = %v, want %v", got, want)
	}
	if loaded, err := config.Load(exampleYAML(t)); err != nil || loaded.State.Redis.OperationKeyReuseHorizon != 0 {
		t.Fatalf("default horizon = %v, %v; want 0 (keep indefinitely)", loaded.State.Redis.OperationKeyReuseHorizon, err)
	}
	for name, data := range map[string][]byte{
		"below a day":                 withHorizon("1h", ""),
		"below ambiguous retention":   withHorizon("30d", ""),
		"evictable tombstones":        withHorizon("100d", "    maxmemory_policy: volatile-lru\n"),
		"beyond the Function's bound": withHorizon("40000d", ""),
	} {
		if _, err := config.Load(data); err == nil || !strings.Contains(err.Error(), "operation_key_reuse_horizon") {
			t.Errorf("%s: error = %v, want an operation_key_reuse_horizon error", name, err)
		}
	}
}
