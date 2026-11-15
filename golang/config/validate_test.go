package config_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func TestConfigValidationRequiresShutdownBudgetForGraceAndFinalization(t *testing.T) {
	loaded := mustLoadConfig(t, exampleYAML(t))
	loaded.Server.ShutdownTimeout = config.Duration(time.Duration(loaded.Temporal.Worker.GracefulStopTimeout) + time.Duration(loaded.Server.FinalizationTimeout))
	testassert.ErrorContains(t, loaded.Validate(), "server.shutdown_timeout must exceed temporal.worker.graceful_stop_timeout + server.finalization_timeout")

	loaded.Server.ShutdownTimeout = config.Duration(time.Duration(loaded.Temporal.Worker.GracefulStopTimeout) + time.Duration(loaded.Server.FinalizationTimeout) + time.Second)
	testassert.NoError(t, loaded.Validate(), "shutdown budget with one-second residual flush margin rejected")
}

func TestConfigExampleMatchesJSONSchema(t *testing.T) {
	configData := exampleYAML(t)
	loaded := mustLoadConfig(t, configData)
	testassert.NoError(t, validateConfigSchema(t, loaded))
}

func TestConfigSchemaValidatesMRSCOptIn(t *testing.T) {
	document := jsonDocument(t, mustLoadConfig(t, exampleYAML(t)))
	aws := document["state"].(map[string]any)["requests"].(map[string]any)["provider"].(map[string]any)["aws"].(map[string]any)
	compiled := configSchema(t)
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
			if err := compiled.Validate(mustMarshalJSON(t, document)); (err == nil) != tc.valid {
				t.Fatalf("allow_mrsc = %v: validation error = %v, want valid = %v", tc.value, err, tc.valid)
			}
		})
	}
}

func TestConfigSchemaAcceptsDevelopmentFileBlobStore(t *testing.T) {
	loaded := mustLoadConfig(t, developmentFileBlobYAML(t))
	testassert.NoError(t, validateConfigSchema(t, loaded), "development file blob store schema error")
}

func TestConfigSchemaAcceptsDevelopmentMemoryState(t *testing.T) {
	loaded := mustLoadConfig(t, exampleYAML(t))
	loaded.Environment = "development"
	loaded.State.Kind = config.StateKindMemory
	loaded.State.Redis = config.RedisConfig{}
	loaded.State.Requests = nil
	loaded.BlobStore.Kind = "memory"
	loaded.BlobStore.File = config.FileBlobConfig{}
	loaded.BlobStore.S3 = config.S3Config{}
	testassert.NoError(t, validateConfigSchema(t, loaded), "development memory state schema error")
}

func TestConfigSchemaRejectsFileBlobStoreOutsideDevelopment(t *testing.T) {
	loaded := mustLoadConfig(t, developmentFileBlobYAML(t))
	loaded.Environment = "production"
	if err := validateConfigSchema(t, loaded); err == nil {
		t.Fatal("schema accepted a production file blob store")
	}
}

func TestConfigSchemaRejectsProductionRedisWithoutTLS(t *testing.T) {
	loaded := mustLoadConfig(t, exampleYAML(t))
	loaded.State.Redis.TLS.Enabled = false
	if err := validateConfigSchema(t, loaded); err == nil {
		t.Fatal("schema accepted a production Redis configuration without TLS")
	}
}

func TestConfigSchemaAcceptsDevelopmentRedisWithoutTLS(t *testing.T) {
	loaded := mustLoadConfig(t, []byte(strings.Replace(string(redisTLSDisabledYAML(t)), "environment: production", "environment: development", -1)))
	testassert.NoError(t, validateConfigSchema(t, loaded), "development Redis without TLS schema error")
}

func TestConfigSchemaRejectsMixedBlobStoreBranches(t *testing.T) {
	compiled := configSchema(t)
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
			loaded := mustLoadConfig(t, developmentFileBlobYAML(t))
			test.input(&loaded)
			if err := compiled.Validate(mustMarshalJSON(t, loaded)); err == nil {
				t.Fatal("schema accepted populated s3 fields beside a file blob store")
			}
		})
	}
}

func TestConfigValidationRejectsMixedBlobStoreBranches(t *testing.T) {
	fileStore := mustLoadConfig(t, developmentFileBlobYAML(t))
	fileStore.BlobStore.S3.Auth.Path = "/unexpected/credentials"
	if err := fileStore.Validate(); err == nil {
		t.Fatal("config validation accepted inactive s3 auth metadata beside a file blob store")
	}

	s3Store := mustLoadConfig(t, exampleYAML(t))
	s3Store.BlobStore.File.Root = " "
	if err := s3Store.Validate(); err == nil {
		t.Fatal("config validation accepted a non-empty inactive file root beside an s3 blob store")
	}
}

func TestConfigValidationBoundsAdmissionRouteFields(t *testing.T) {
	loaded := mustLoadConfig(t, exampleYAML(t))
	model := loaded.Models["invoice-summarizer"]
	model.Routes[0].ID = strings.Repeat("r", 257)
	loaded.Models["invoice-summarizer"] = model
	if err := loaded.Validate(); err == nil {
		t.Fatal("config accepted an oversized route identifier")
	}

	loaded = mustLoadConfig(t, exampleYAML(t))
	model = loaded.Models["invoice-summarizer"]
	model.Routes[0].Model = strings.Repeat("m", 257)
	loaded.Models["invoice-summarizer"] = model
	if err := loaded.Validate(); err == nil {
		t.Fatal("config accepted an oversized route model")
	}

	loaded = mustLoadConfig(t, exampleYAML(t))
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
	compiled := configSchema(t)
	for _, root := range []string{"/", "relative/blobs", "/var/lib/../"} {
		t.Run(root, func(t *testing.T) {
			loaded := mustLoadConfig(t, developmentFileBlobYAML(t))
			loaded.BlobStore.File.Root = root
			if err := compiled.Validate(mustMarshalJSON(t, loaded)); err == nil {
				t.Fatalf("schema accepted unsafe file blob root %q", root)
			}
		})
	}
}

func TestConfigValidationRejectsUnsafeFileBlobRoots(t *testing.T) {
	for _, root := range []string{"/", "relative/blobs", "/var/lib/../"} {
		t.Run(root, func(t *testing.T) {
			loaded := mustLoadConfig(t, developmentFileBlobYAML(t))
			loaded.BlobStore.File.Root = root
			if err := loaded.Validate(); err == nil {
				t.Fatalf("config validation accepted unsafe file blob root %q", root)
			}
		})
	}
}

func TestConfigSchemaRejectsFourthServiceClass(t *testing.T) {
	compiled := configSchema(t)
	loaded := mustLoadConfig(t, exampleYAML(t))
	loaded.Endpoints["openai-prod"].ServiceClasses["turbo"] = loaded.Endpoints["openai-prod"].ServiceClasses["standard"]
	if err := compiled.Validate(mustMarshalJSON(t, loaded)); err == nil {
		t.Fatal("schema accepted a fourth public service class")
	}
}

func TestConfigSchemaRequiresClosedAnthropicAWSGatewayIdentity(t *testing.T) {
	compiled := configSchema(t)
	loaded := mustLoadConfig(t, exampleYAML(t))
	editEndpoint(&loaded, "anthropic-aws-us-east-1", func(endpoint *config.EndpointConfig) { endpoint.AWSWorkspaceID = "" })
	if err := compiled.Validate(mustMarshalJSON(t, loaded)); err == nil {
		t.Fatal("schema accepted an Anthropic AWS gateway endpoint without a workspace ID")
	}

	editEndpoint(&loaded, "anthropic-aws-us-east-1", func(endpoint *config.EndpointConfig) {
		endpoint.AWSWorkspaceID = "ws-example-123"
		endpoint.Auth.Kind = "bearer_env"
	})
	if err := compiled.Validate(mustMarshalJSON(t, loaded)); err == nil {
		t.Fatal("schema accepted secret auth for an Anthropic AWS gateway endpoint")
	}

}

func TestConfigSchemaAcceptsEveryBudgetMatcher(t *testing.T) {
	loaded := loadEveryBudgetMatcherExample(t)
	testassert.NoError(t, validateConfigSchema(t, loaded))
}

func TestConfigAcceptsAzureOpenAIChatFamily(t *testing.T) {
	loaded := loadExampleWith(t, "family: openai_responses", "family: azure_openai_chat")
	if got := loaded.Endpoints["openai-prod"].Family; got != "azure_openai_chat" {
		t.Fatalf("endpoint family = %q, want azure_openai_chat", got)
	}
	testassert.NoError(t, validateConfigSchema(t, loaded))
}

func TestConfigSchemaRequiresCloudAndRejectsSQL(t *testing.T) {
	loaded := mustLoadConfig(t, exampleYAML(t))
	encoded := mustMarshalJSON(t, loaded)
	compiled := configSchema(t)
	for _, variant := range []string{"missing-cloud", "obsolete-sql"} {
		t.Run(variant, func(t *testing.T) {
			var document map[string]any
			testassert.NoError(t, json.Unmarshal(encoded, &document))
			state := document["state"].(map[string]any)
			if variant == "missing-cloud" {
				delete(state, "requests")
			} else {
				state["postgres"] = map[string]any{"database": "old_worker"}
			}
			invalid := mustMarshalJSON(t, document)
			if err := compiled.Validate(invalid); err == nil {
				t.Fatal("schema accepted invalid durable storage")
			}
		})
	}
}

// environment is a free-form identifier. Production hardening applies to every
// value except the exact development name, so a near-miss cannot skip it.
func TestProductionHardeningAppliesToEveryNonDevelopmentEnvironment(t *testing.T) {
	compiled := configSchema(t)
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
		loaded := mustLoadConfig(t, exampleYAML(t))
		loaded.Environment, loaded.State.Redis.TLS.Enabled = environment, false
		for index := range loaded.Budgets.Policies {
			if loaded.Budgets.Policies[index].Match.Environment != "" {
				loaded.Budgets.Policies[index].Match.Environment = environment
			}
		}
		if err := compiled.Validate(mustMarshalJSON(t, loaded)); err == nil {
			t.Fatalf("schema accepted environment %q with Redis TLS disabled", environment)
		}
	}
	if config.IsProductionEnvironment(config.DevelopmentEnvironment) {
		t.Fatal("development is treated as production")
	}
}

func TestRedisServiceMeshSecurityContract(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*config.RedisConfig)
		valid  bool
	}{
		{"mesh", func(*config.RedisConfig) {}, true},
		{"unprotected production", func(r *config.RedisConfig) { r.ServiceMesh = false }, false},
		{"application TLS", func(r *config.RedisConfig) { r.TLS.Enabled = true }, false},
		{"password reference", func(r *config.RedisConfig) {
			r.Password = config.SecretRef{Kind: config.SecretEnv, Name: "REDIS_PASSWORD"}
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := mustLoadConfig(t, exampleYAML(t))
			value.State.Redis.ServiceMesh = true
			value.State.Redis.TLS = config.TLSConfig{}
			value.State.Redis.Username = config.SecretRef{}
			value.State.Redis.Password = config.SecretRef{}
			test.mutate(&value.State.Redis)
			if err := value.Validate(); (err == nil) != test.valid {
				t.Fatalf("validation = %v, valid = %v", err, test.valid)
			}
			data := mustMarshalJSON(t, value)
			compiled := configSchema(t)
			if err := compiled.Validate(data); (err == nil) != test.valid {
				t.Fatalf("schema validation = %v, valid = %v", err, test.valid)
			}
		})
	}
}

// validate-config must accept exactly the continuation handle key IDs that
// state.NewKeyring accepts at startup; payload-codec style IDs with dots or
// more than 32 characters are rejected up front instead of crash-looping.
func TestContinuationHandleKeyIDMatchesKeyring(t *testing.T) {
	const anchor = "    - id: key-2026-07\n"
	base := exampleContaining(t, anchor)
	loaded := mustLoadConfig(t, []byte(base))
	compiled := configSchema(t)
	for _, id := range []string{
		"key-2026-07",
		"key_2026",
		strings.Repeat("k", 32),
		"key.2026.07",
		strings.Repeat("k", 33),
		"key:1",
	} {
		_, loadErr := config.Load([]byte(strings.Replace(base, anchor, "    - id: \""+id+"\"\n", 1)))
		_, keyringErr := state.NewKeyring([]state.Key{{ID: id, Secret: make([]byte, 32), Primary: true}}, nil)
		if (loadErr == nil) != (keyringErr == nil) {
			t.Errorf("id %q: Load() = %v, NewKeyring() = %v", id, loadErr, keyringErr)
		}
		if loadErr != nil && !strings.Contains(loadErr.Error(), "continuation.handle_keys[0].id must be 1-32 characters") {
			t.Errorf("id %q: Load() = %v", id, loadErr)
		}
		value := loaded.Clone()
		value.Continuation.HandleKeys[0].ID = id
		encoded := mustMarshalJSON(t, value)
		if schemaErr := compiled.Validate(encoded); (schemaErr == nil) != (keyringErr == nil) {
			t.Errorf("id %q: schema validation = %v, NewKeyring() = %v", id, schemaErr, keyringErr)
		}
	}
}

// The worker builds an S3 blob store only from the AWS default credential
// chain, so validation refuses every other auth kind the generic auth check
// accepts rather than letting the worker fail each start (#1669).
func TestConfigValidationRejectsS3BlobAuthTheWorkerCannotBuild(t *testing.T) {
	compiled := configSchema(t)
	for kind, auth := range map[string]config.AuthConfig{
		"bearer_env":               {Kind: "bearer_env", Name: "S3_TOKEN"},
		"header_env":               {Kind: "header_env", Name: "S3_TOKEN"},
		"azure_default_credential": {Kind: "azure_default_credential"},
		"workload_identity":        {Kind: "workload_identity", Audience: "sts.amazonaws.com"},
		"aws_default_chain":        {Kind: "aws_default_chain"},
	} {
		t.Run(kind, func(t *testing.T) {
			if err := auth.Validate("blob_store.s3.auth"); err != nil {
				t.Fatalf("generic auth validation rejects %s: %v", kind, err)
			}
			loaded := mustLoadConfig(t, exampleYAML(t))
			loaded.BlobStore.S3.Auth = auth
			schemaErr := compiled.Validate(mustMarshalJSON(t, loaded))
			err := loaded.Validate()
			if kind == "aws_default_chain" {
				if err != nil || schemaErr != nil {
					t.Fatalf("Validate = %v, schema = %v", err, schemaErr)
				}
				return
			}
			if schemaErr == nil {
				t.Fatalf("the configuration schema accepts blob_store.s3.auth.kind %s", kind)
			}
			testassert.ErrorContains(t, err, "blob_store.s3.auth.kind")
		})
	}
}
