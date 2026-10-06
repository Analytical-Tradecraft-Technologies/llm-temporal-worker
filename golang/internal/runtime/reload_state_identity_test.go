package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
)

// Durable state identity is where in-flight requests, budgets and results
// live. A reload that moved it would orphan them, so each field is rejected
// with its path before any replacement client is built.
func TestRuntimeReplacementValidatorRejectsStateIdentityChanges(t *testing.T) {
	// An alias can only change to one the provider already maps, so the active
	// configuration carries a second alias for the same physical store.
	twoAliases := func(value *config.Config) {
		value.State.Requests.Provider.KeyValueStores = map[string]string{"requests": "physical-requests", "requests-b": "physical-requests"}
		value.State.Requests.Provider.BlobStores = map[string]string{"payloads": "physical-payloads", "payloads-b": "physical-payloads"}
	}
	tests := []struct {
		field  string
		base   func(*config.Config)
		mutate func(*config.Config)
	}{
		{field: "state.kind", mutate: func(value *config.Config) { value.State.Kind = config.StateKindRedis; value.State.Requests = nil }},
		{field: "state.redis.admission_hash_tag", mutate: func(value *config.Config) { value.State.Redis.AdmissionHashTag = "admission-b" }},
		{field: "state.redis.key_secret", mutate: func(value *config.Config) { value.State.Redis.KeySecret.Path = "/var/run/secrets/llmtw/other-key" }},
		{field: "state.requests.provider.aws.region", mutate: func(value *config.Config) { value.State.Requests.Provider.AWS.Region = "us-east-1" }},
		{field: "state.requests.provider.aws.profile", mutate: func(value *config.Config) { value.State.Requests.Provider.AWS.Profile = "other-account" }},
		{field: "state.requests.provider.aws.failover", base: func(value *config.Config) { value.State.Requests.Provider.AWS.AllowMRSC = true }, mutate: func(value *config.Config) {
			value.State.Requests.Provider.AWS.Failover = &config.CloudFailoverConfig{DynamoDBRegions: []string{"region-primary", "region-fallback-a"}, PayloadReplicas: []config.RegionalBucket{{Region: "region-fallback-a", Bucket: "fallback-payloads"}}, AttemptTimeout: config.Duration(time.Second)}
		}},
		{field: "state.requests.provider.aws.allow_mrsc", mutate: func(value *config.Config) { value.State.Requests.Provider.AWS.AllowMRSC = true }},
		{field: "state.requests.provider.aws.temp_directory", mutate: func(value *config.Config) { value.State.Requests.Provider.AWS.TempDirectory = "/var/tmp/other" }},
		{field: "state.requests.provider.key_value_stores", mutate: func(value *config.Config) {
			value.State.Requests.Provider.KeyValueStores = map[string]string{"requests": "other-physical-table"}
		}},
		{field: "state.requests.provider.blob_stores", mutate: func(value *config.Config) {
			value.State.Requests.Provider.BlobStores = map[string]string{"payloads": "other-physical-bucket"}
		}},
		{field: "state.requests.request_table", base: twoAliases, mutate: func(value *config.Config) { value.State.Requests.RequestTable = "requests-b" }},
		{field: "state.requests.payload_store", base: twoAliases, mutate: func(value *config.Config) { value.State.Requests.PayloadStore = "payloads-b" }},
		{field: "state.requests.namespace", mutate: func(value *config.Config) { value.State.Requests.Namespace = "worker-v2" }},
		{field: "state.requests.secret", mutate: func(value *config.Config) { value.State.Requests.Secret.Name = "OTHER_CLOUD_REQUEST_KEY" }},
		{field: "blob_store.kind", mutate: func(value *config.Config) {
			value.BlobStore.Kind, value.BlobStore.S3 = "file", config.S3Config{}
			value.BlobStore.File.Root = "/var/lib/llmtw/results"
		}},
		{field: "blob_store.s3.bucket", mutate: func(value *config.Config) { value.BlobStore.S3.Bucket = "other-results-bucket" }},
		{field: "blob_store.s3.region", mutate: func(value *config.Config) { value.BlobStore.S3.Region = "us-east-1" }},
		{field: "blob_store.s3.failover", mutate: func(value *config.Config) {
			value.BlobStore.S3.Failover = &config.BlobFailoverConfig{Replicas: []config.RegionalBucket{{Region: "region-fallback-a", Bucket: "fallback-results"}}, AttemptTimeout: config.Duration(time.Second)}
		}},
		{field: "blob_store.s3.prefix", mutate: func(value *config.Config) { value.BlobStore.S3.Prefix = "v2" }},
	}
	for _, test := range tests {
		t.Run(test.field, func(t *testing.T) {
			base := test.base
			if base == nil {
				base = func(*config.Config) {}
			}
			initial := replacementTestConfig(t, base)
			var clientBuilds atomic.Int32
			application, err := app.New(context.Background(), app.Options{
				InitialConfig: initial,
				Builder:       app.SnapshotBuilder{},
				Clients: func(context.Context, *config.Snapshot) (app.ClientSet, error) {
					clientBuilds.Add(1)
					return app.ClientSetFunc(func(context.Context) error { return nil }), nil
				},
				ReplacementValidator: validateRuntimeReplacement,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = application.Close(context.Background()) })
			before := application.Current()

			err = application.Reload(context.Background(), replacementTestConfig(t, func(value *config.Config) {
				base(value)
				test.mutate(value)
			}))
			var change *processLifetimeChangeError
			if !errors.Is(err, errProcessLifetimeConfigurationChanged) || !errors.As(err, &change) || change.field != test.field {
				t.Fatalf("Reload() error = %v, want process-lifetime rejection of %s", err, test.field)
			}
			if cause, field := classifyReloadFailure(err); cause != "process_lifetime" || field != test.field {
				t.Fatalf("classifyReloadFailure() = %q, %q; want process_lifetime, %s", cause, field, test.field)
			}
			if application.Current() != before || clientBuilds.Load() != 1 {
				t.Fatalf("rejected replacement was published or built clients (builds=%d)", clientBuilds.Load())
			}
		})
	}
}

func TestRuntimeReplacementValidatorRejectsFileResultRootChange(t *testing.T) {
	fileStore := func(root string) func(*config.Config) {
		return func(value *config.Config) {
			value.BlobStore.Kind, value.BlobStore.S3 = "file", config.S3Config{}
			value.BlobStore.File.Root = root
		}
	}
	current, err := config.Compile(context.Background(), replacementTestConfig(t, fileStore("/var/lib/llmtw/results")), nil)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := config.Compile(context.Background(), replacementTestConfig(t, fileStore("/var/lib/llmtw/other")), nil)
	if err != nil {
		t.Fatal(err)
	}
	var change *processLifetimeChangeError
	if err := validateRuntimeReplacement(current, replacement); !errors.As(err, &change) || change.field != "blob_store.file.root" {
		t.Fatalf("validateRuntimeReplacement() = %v, want blob_store.file.root", err)
	}
}

// How to reach the same Redis data is not identity: endpoints, credentials,
// TLS and timeouts are swapped with the snapshot's rebuilt clients.
func TestRuntimeReplacementValidatorAllowsStateConnectionChanges(t *testing.T) {
	current, err := config.Compile(context.Background(), replacementTestConfig(t, func(*config.Config) {}), nil)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := config.Compile(context.Background(), replacementTestConfig(t, func(value *config.Config) {
		value.State.Redis.Addresses = []string{"redis-2.example.internal:6379"}
		value.State.Redis.Username.Name = "REDIS_USERNAME_ROTATED"
		value.State.Redis.Password.Path = "/var/run/secrets/llmtw/redis-password-rotated"
		value.State.Redis.TLS.ServerName = "redis-2.example.internal"
		value.State.Redis.MaxConnections++
		value.State.Redis.DialTimeout += config.Duration(time.Second)
		value.State.Redis.OperationTimeout += config.Duration(time.Second)
		value.BlobStore.InlineBytes++
	}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeReplacement(current, replacement); err != nil {
		t.Fatalf("validateRuntimeReplacement() rejected a connection-only change: %v", err)
	}
}
