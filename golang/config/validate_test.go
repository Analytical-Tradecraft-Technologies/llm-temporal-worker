package config_test

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/schema"
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

func TestConfigValidationRefusesInvalidOpenRouterEffortAndWire(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(extension map[string]any, endpoint *config.EndpointConfig)
		want   string
	}{
		{name: "top-level max is not an effort value", mutate: func(extension map[string]any, _ *config.EndpointConfig) {
			extension["reasoning_efforts"] = map[string]any{"maximum": "maximum"}
		}, want: `maps reasoning effort "maximum" to "maximum"`},
		{name: "misspelled effort", mutate: func(extension map[string]any, _ *config.EndpointConfig) {
			extension["reasoning_efforts"] = map[string]any{"high": "hihg"}
		}, want: `maps reasoning effort "high" to "hihg"`},
		{name: "effort without reasoning parameter", mutate: func(extension map[string]any, _ *config.EndpointConfig) {
			extension["supported_parameters"] = []any{"max_tokens", "response_format"}
		}, want: `support "reasoning"`},
		{name: "store parameter", mutate: func(extension map[string]any, _ *config.EndpointConfig) {
			extension["supported_parameters"] = []any{"max_tokens", "reasoning", "store"}
		}, want: `"store" is not a documented OpenRouter parameter`},
		{name: "undocumented tier", mutate: func(_ map[string]any, endpoint *config.EndpointConfig) {
			endpoint.ServiceClasses[llm.ServiceClassStandard] = config.TierConfig{ProviderValue: "standard"}
		}, want: `provider tier "standard" is not a documented OpenRouter tier`},
		{name: "missing tier policy", mutate: func(extension map[string]any, _ *config.EndpointConfig) {
			delete(extension, "missing_service_tier")
		}, want: "must declare missing_service_tier"},
		{name: "hidden fallback", mutate: func(extension map[string]any, _ *config.EndpointConfig) {
			extension["allow_fallbacks"] = true
		}, want: "allow_fallbacks must be false"},
		{name: "unknown field", mutate: func(extension map[string]any, _ *config.EndpointConfig) {
			extension["service_tier"] = "default"
		}, want: `field "service_tier" is not supported`},
	} {
		t.Run(test.name, func(t *testing.T) {
			loaded, err := config.Load(exampleYAML(t))
			if err != nil {
				t.Fatal(err)
			}
			endpoint := loaded.Endpoints["openrouter-pinned"]
			test.mutate(endpoint.Extensions["openrouter"], &endpoint)
			loaded.Endpoints["openrouter-pinned"] = endpoint
			if err := loaded.Validate(); err == nil || !strings.Contains(err.Error(), "endpoints.openrouter-pinned") || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestResourceCapacityAcceptsOnlyAuthorizedForecastEventInflight(t *testing.T) {
	for _, test := range []struct {
		events, stage2 int
		want           string
	}{
		{events: 2, stage2: 6},
		{events: 5, stage2: 15},
		{events: 3, stage2: 15, want: "forecast event admission policy is invalid"},
		{events: 4, stage2: 15, want: "forecast event admission policy is invalid"},
		{events: 5, stage2: 14, want: "Python Stage2 slots cannot admit every forecast event ensemble"},
	} {
		t.Run(fmt.Sprintf("events-%d-stage2-%d", test.events, test.stage2), func(t *testing.T) {
			loaded, err := config.Load(exampleYAML(t))
			if err != nil {
				t.Fatal(err)
			}
			loaded.ResourceCapacity.Limits.ForecastEventMaxInflight = test.events
			loaded.ResourceCapacity.Limits.PythonStage2MaxInflight = test.stage2
			err = loaded.Validate()
			if test.want == "" && err != nil || test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("validation error = %v, want %q", err, test.want)
			}
		})
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

func TestProductionTemporalRequiresTLSAndAPIKeyFile(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*config.Config)
		want   string
	}{
		{
			name:   "wrong target",
			mutate: func(value *config.Config) { value.Temporal.Target = "temporal.example.test:7233" },
			want:   "temporal.target must be",
		},
		{
			name:   "wrong namespace",
			mutate: func(value *config.Config) { value.Temporal.Namespace = "default" },
			want:   "temporal.namespace must be",
		},
		{
			name:   "wrong SNI",
			mutate: func(value *config.Config) { value.Temporal.TLS.ServerName = "temporal.example.test" },
			want:   "temporal.tls.server_name must be",
		},
		{
			name: "TLS disabled",
			mutate: func(value *config.Config) {
				value.Temporal.TLS.Enabled = false
				value.Temporal.APIKeyFile = ""
			},
			want: "temporal.tls.enabled must be true in production",
		},
		{
			name:   "API key missing",
			mutate: func(value *config.Config) { value.Temporal.APIKeyFile = "" },
			want:   "temporal.api_key_file is required in production",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := loaded
			test.mutate(&value)
			if err := value.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Temporal production validation error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestDevelopmentTemporalAllowsExplicitUnauthenticatedTransport(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	loaded.Environment = "development"
	loaded.Temporal.APIKeyFile = ""
	loaded.Temporal.TLS = config.TLSConfig{}
	if err := loaded.Validate(); err != nil {
		t.Fatalf("explicit unauthenticated development Temporal config rejected: %v", err)
	}
}

func TestProductionConfigRequiresS3KMSKeyID(t *testing.T) {
	document := string(exampleYAML(t))
	loaded, err := config.Load([]byte(document))
	if err != nil {
		t.Fatal(err)
	}
	kmsLine := "    kms_key_id: " + loaded.BlobStore.S3.KMSKeyID + "\n"
	withoutKMS := strings.Replace(document, kmsLine, "", 1)
	if withoutKMS == document {
		t.Fatal("production example did not contain the configured S3 KMS key")
	}
	if _, err := config.Load([]byte(withoutKMS)); err == nil || !strings.Contains(err.Error(), "blob_store.s3.kms_key_id is required") {
		t.Fatalf("missing production S3 KMS key validation error = %v", err)
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
	loaded.State.Postgres = config.PostgresConfig{}
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
	loaded, err := config.Load([]byte(strings.Replace(string(redisTLSDisabledYAML(t)), "environment: production", "environment: development", 1)))
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

func TestConfigSchemaAcceptsOnlyAuthorizedForecastEventInflight(t *testing.T) {
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	for events, valid := range map[int]bool{2: true, 5: true, 3: false, 4: false} {
		loaded, err := config.Load(exampleYAML(t))
		if err != nil {
			t.Fatal(err)
		}
		loaded.ResourceCapacity.Limits.ForecastEventMaxInflight = events
		loaded.ResourceCapacity.Limits.PythonStage2MaxInflight = 15
		encoded, err := json.Marshal(loaded)
		if err != nil {
			t.Fatal(err)
		}
		if err := compiled.Validate(encoded); (err == nil) != valid {
			t.Fatalf("forecast_event_max_inflight %d schema error = %v, want valid=%v", events, err, valid)
		}
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

func TestProviderAuthAcceptsPrivateFileReferences(t *testing.T) {
	for _, kind := range []string{"bearer_file", "header_file"} {
		auth := config.AuthConfig{Kind: kind, Path: "/var/run/secrets/providers/api-key"}
		if err := auth.Validate("endpoints.provider.auth"); err != nil {
			t.Fatalf("%s valid file auth error = %v", kind, err)
		}
		for _, invalid := range []config.AuthConfig{
			{Kind: kind, Path: "relative/key"},
			{Kind: kind, Name: "PROVIDER_KEY", Path: "/var/run/secrets/providers/api-key"},
			{Kind: kind, Path: "/var/run/secrets/providers/api-key", Audience: "provider"},
		} {
			if err := invalid.Validate("endpoints.provider.auth"); err == nil {
				t.Fatalf("%s accepted invalid file auth %#v", kind, invalid)
			}
		}
	}
}

func TestConfigSchemaAcceptsEveryBudgetMatcher(t *testing.T) {
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
