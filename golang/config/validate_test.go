package config_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

func TestConfigValidationRequiresShutdownBudgetForGraceAndFinalization(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	loaded.Server.ShutdownTimeout = config.Duration(time.Duration(loaded.Temporal.Worker.GracefulStopTimeout) + time.Duration(loaded.Server.FinalizationTimeout))
	if err := loaded.Validate(); err == nil || !strings.Contains(err.Error(), "server.shutdown_timeout must exceed temporal.worker.graceful_stop_timeout + server.finalization_timeout") {
		t.Fatalf("shutdown budget validation error = %v", err)
	}

	loaded.Server.ShutdownTimeout = config.Duration(time.Duration(loaded.Temporal.Worker.GracefulStopTimeout) + time.Duration(loaded.Server.FinalizationTimeout) + time.Second)
	if err := loaded.Validate(); err != nil {
		t.Fatalf("shutdown budget with one-second residual flush margin rejected: %v", err)
	}
}

func TestConfigExampleMatchesJSONSchema(t *testing.T) {
	configData := exampleYAML(t)
	loaded, err := config.Load(configData)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(encoded); err != nil {
		t.Fatal(err)
	}
}

func TestConfigSchemaValidatesMRSCOptIn(t *testing.T) {
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
	aws := document["state"].(map[string]any)["requests"].(map[string]any)["provider"].(map[string]any)["aws"].(map[string]any)
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		value any
		valid bool
	}{
		{"enabled", true, true},
		{"disabled", false, true},
		{"string", "true", false},
		{"number", 1, false},
		{"null", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			aws["allow_mrsc"] = tc.value
			encoded, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if err := compiled.Validate(encoded); (err == nil) != tc.valid {
				t.Fatalf("allow_mrsc = %v: validation error = %v, want valid = %v", tc.value, err, tc.valid)
			}
		})
	}
}

func TestConfigSchemaAcceptsDevelopmentFileBlobStore(t *testing.T) {
	loaded, err := config.Load(developmentFileBlobYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(encoded); err != nil {
		t.Fatalf("development file blob store schema error: %v", err)
	}
}

func TestConfigSchemaAcceptsDevelopmentMemoryState(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	loaded.Environment = "development"
	loaded.State.Kind = config.StateKindMemory
	loaded.State.Redis = config.RedisConfig{}
	loaded.State.Requests = nil
	loaded.BlobStore.Kind = "memory"
	loaded.BlobStore.File = config.FileBlobConfig{}
	loaded.BlobStore.S3 = config.S3Config{}
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(encoded); err != nil {
		t.Fatalf("development memory state schema error: %v", err)
	}
}

func TestConfigSchemaRejectsFileBlobStoreOutsideDevelopment(t *testing.T) {
	loaded, err := config.Load(developmentFileBlobYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	loaded.Environment = "production"
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(encoded); err == nil {
		t.Fatal("schema accepted a production file blob store")
	}
}

func TestConfigSchemaRejectsProductionRedisWithoutTLS(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	loaded.State.Redis.TLS.Enabled = false
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(encoded); err == nil {
		t.Fatal("schema accepted a production Redis configuration without TLS")
	}
}

func TestConfigSchemaAcceptsDevelopmentRedisWithoutTLS(t *testing.T) {
	loaded, err := config.Load([]byte(strings.Replace(string(redisTLSDisabledYAML(t)), "environment: production", "environment: development", -1)))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(encoded); err != nil {
		t.Fatalf("development Redis without TLS schema error: %v", err)
	}
}

func TestConfigSchemaRejectsMixedBlobStoreBranches(t *testing.T) {
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		input func(*config.Config)
	}{
		{
			name: "file_s3_values",
			input: func(loaded *config.Config) {
				loaded.BlobStore.S3.Bucket = "unexpected-s3-bucket"
				loaded.BlobStore.S3.Region = "ap-southeast-2"
				loaded.BlobStore.S3.Prefix = "v1"
				loaded.BlobStore.S3.Auth.Kind = "aws_default_chain"
			},
		},
		{
			name: "file_s3_auth_metadata",
			input: func(loaded *config.Config) {
				loaded.BlobStore.S3.Auth.Path = "/unexpected/credentials"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			loaded, err := config.Load(developmentFileBlobYAML(t))
			if err != nil {
				t.Fatal(err)
			}
			test.input(&loaded)
			encoded, err := json.Marshal(loaded)
			if err != nil {
				t.Fatal(err)
			}
			if err := compiled.Validate(encoded); err == nil {
				t.Fatal("schema accepted populated s3 fields beside a file blob store")
			}
		})
	}
}

func TestConfigValidationRejectsMixedBlobStoreBranches(t *testing.T) {
	fileStore, err := config.Load(developmentFileBlobYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	fileStore.BlobStore.S3.Auth.Path = "/unexpected/credentials"
	if err := fileStore.Validate(); err == nil {
		t.Fatal("config validation accepted inactive s3 auth metadata beside a file blob store")
	}

	s3Store, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	s3Store.BlobStore.File.Root = " "
	if err := s3Store.Validate(); err == nil {
		t.Fatal("config validation accepted a non-empty inactive file root beside an s3 blob store")
	}
}

func TestConfigValidationBoundsAdmissionRouteFields(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	model := loaded.Models["invoice-summarizer"]
	model.Routes[0].ID = strings.Repeat("r", 257)
	loaded.Models["invoice-summarizer"] = model
	if err := loaded.Validate(); err == nil {
		t.Fatal("config accepted an oversized route identifier")
	}

	loaded, err = config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	model = loaded.Models["invoice-summarizer"]
	model.Routes[0].Model = strings.Repeat("m", 257)
	loaded.Models["invoice-summarizer"] = model
	if err := loaded.Validate(); err == nil {
		t.Fatal("config accepted an oversized route model")
	}

	loaded, err = config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	endpoints := loaded.Endpoints
	endpoint := endpoints["openai-prod"]
	delete(endpoints, "openai-prod")
	endpoints[strings.Repeat("e", 257)] = endpoint
	for index := range loaded.Models["invoice-summarizer"].Routes {
		model := loaded.Models["invoice-summarizer"]
		model.Routes[index].Endpoint = strings.Repeat("e", 257)
		loaded.Models["invoice-summarizer"] = model
		break
	}
	if err := loaded.Validate(); err == nil {
		t.Fatal("config accepted an oversized endpoint identifier")
	}
}

func TestConfigSchemaRejectsUnsafeFileBlobRoots(t *testing.T) {
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{"/", "relative/blobs", "/var/lib/../"} {
		t.Run(root, func(t *testing.T) {
			loaded, err := config.Load(developmentFileBlobYAML(t))
			if err != nil {
				t.Fatal(err)
			}
			loaded.BlobStore.File.Root = root
			encoded, err := json.Marshal(loaded)
			if err != nil {
				t.Fatal(err)
			}
			if err := compiled.Validate(encoded); err == nil {
				t.Fatalf("schema accepted unsafe file blob root %q", root)
			}
		})
	}
}

func TestConfigValidationRejectsUnsafeFileBlobRoots(t *testing.T) {
	for _, root := range []string{"/", "relative/blobs", "/var/lib/../"} {
		t.Run(root, func(t *testing.T) {
			loaded, err := config.Load(developmentFileBlobYAML(t))
			if err != nil {
				t.Fatal(err)
			}
			loaded.BlobStore.File.Root = root
			if err := loaded.Validate(); err == nil {
				t.Fatalf("config validation accepted unsafe file blob root %q", root)
			}
		})
	}
}

func TestConfigSchemaRejectsFourthServiceClass(t *testing.T) {
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	loaded.Endpoints["openai-prod"].ServiceClasses["turbo"] = loaded.Endpoints["openai-prod"].ServiceClasses["standard"]
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(encoded); err == nil {
		t.Fatal("schema accepted a fourth public service class")
	}
}

func TestConfigSchemaRequiresClosedAnthropicAWSGatewayIdentity(t *testing.T) {
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := loaded.Endpoints["anthropic-aws-us-east-1"]
	endpoint.AWSWorkspaceID = ""
	loaded.Endpoints["anthropic-aws-us-east-1"] = endpoint
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(encoded); err == nil {
		t.Fatal("schema accepted an Anthropic AWS gateway endpoint without a workspace ID")
	}

	endpoint.AWSWorkspaceID = "ws-example-123"
	endpoint.Auth.Kind = "bearer_env"
	loaded.Endpoints["anthropic-aws-us-east-1"] = endpoint
	encoded, err = json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(encoded); err == nil {
		t.Fatal("schema accepted secret auth for an Anthropic AWS gateway endpoint")
	}

}

func TestConfigSchemaAcceptsEveryBudgetMatcher(t *testing.T) {
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
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(encoded); err != nil {
		t.Fatal(err)
	}
}

func TestConfigAcceptsAzureOpenAIChatFamily(t *testing.T) {
	data := strings.Replace(string(exampleYAML(t)), "family: openai_responses", "family: azure_openai_chat", 1)
	loaded, err := config.Load([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Endpoints["openai-prod"].Family; got != "azure_openai_chat" {
		t.Fatalf("endpoint family = %q, want azure_openai_chat", got)
	}
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(encoded); err != nil {
		t.Fatal(err)
	}
}

func TestConfigSchemaRequiresCloudAndRejectsSQL(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"missing-cloud", "obsolete-sql"} {
		t.Run(variant, func(t *testing.T) {
			var document map[string]any
			if err := json.Unmarshal(encoded, &document); err != nil {
				t.Fatal(err)
			}
			state := document["state"].(map[string]any)
			if variant == "missing-cloud" {
				delete(state, "requests")
			} else {
				state["postgres"] = map[string]any{"database": "old_worker"}
			}
			invalid, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if err := compiled.Validate(invalid); err == nil {
				t.Fatal("schema accepted invalid durable storage")
			}
		})
	}
}

// environment is a free-form identifier. Production hardening applies to every
// value except the exact development name, so a near-miss cannot skip it.
func TestProductionHardeningAppliesToEveryNonDevelopmentEnvironment(t *testing.T) {
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	for _, environment := range []string{"Production", "prod", "staging", "live", "Development"} {
		rename := func(data string) string {
			return strings.ReplaceAll(data, "environment: production", "environment: "+environment)
		}
		if _, err := config.Load([]byte(rename(string(exampleYAML(t))))); err != nil {
			t.Fatalf("environment %q rejected the hardened example: %v", environment, err)
		}
		for name, test := range map[string]struct{ data, want string }{
			"redis tls":       {rename(string(redisTLSDisabledYAML(t))), "state.redis.tls.enabled must be true in production"},
			"content logging": {strings.Replace(rename(string(exampleYAML(t))), "content_logging: disabled", "content_logging: redacted", 1), "telemetry.content_logging must be disabled in production"},
		} {
			if _, err := config.Load([]byte(test.data)); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("environment %q %s error = %v, want %q", environment, name, err, test.want)
			}
		}
		loaded, err := config.Load(exampleYAML(t))
		if err != nil {
			t.Fatal(err)
		}
		loaded.Environment, loaded.State.Redis.TLS.Enabled = environment, false
		for index := range loaded.Budgets.Policies {
			if loaded.Budgets.Policies[index].Match.Environment != "" {
				loaded.Budgets.Policies[index].Match.Environment = environment
			}
		}
		encoded, err := json.Marshal(loaded)
		if err != nil {
			t.Fatal(err)
		}
		if err := compiled.Validate(encoded); err == nil {
			t.Fatalf("schema accepted environment %q with Redis TLS disabled", environment)
		}
	}
	if config.IsProductionEnvironment(config.DevelopmentEnvironment) {
		t.Fatal("development is treated as production")
	}
}
