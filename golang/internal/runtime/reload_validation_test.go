package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
)

func TestRuntimeReplacementValidatorRejectsProcessLifetimeChanges(t *testing.T) {
	initial := replacementTestConfig(t, func(value *config.Config) {
		value.Environment = "development"
	})
	tests := []struct {
		name   string
		field  string
		mutate func(*config.Config)
	}{
		{name: "environment", field: "environment", mutate: func(value *config.Config) { value.Environment = "staging" }},
		{name: "health address", field: "server.health_address", mutate: func(value *config.Config) { value.Server.HealthAddress = "127.0.0.1:8081" }},
		{name: "metrics address", field: "server.metrics_address", mutate: func(value *config.Config) { value.Server.MetricsAddress = "127.0.0.1:9091" }},
		{name: "shutdown timeout", field: "server.shutdown_timeout", mutate: func(value *config.Config) { value.Server.ShutdownTimeout += config.Duration(time.Second) }},
		{name: "readiness interval", field: "server.readiness_probe_interval", mutate: func(value *config.Config) { value.Server.ReadinessProbeInterval += config.Duration(time.Second) }},
		{name: "readiness timeout", field: "server.readiness_probe_timeout", mutate: func(value *config.Config) { value.Server.ReadinessProbeTimeout += config.Duration(time.Second) }},
		{name: "inline payload bytes", field: "server.inline_payload_bytes", mutate: func(value *config.Config) { value.Server.InlinePayloadBytes++ }},
		{name: "Temporal target", field: "temporal.target", mutate: func(value *config.Config) { value.Temporal.Target = "temporal-2.example.internal:7233" }},
		{name: "Temporal namespace", field: "temporal.namespace", mutate: func(value *config.Config) { value.Temporal.Namespace = "production-2" }},
		{name: "Temporal task queue", field: "temporal.task_queue", mutate: func(value *config.Config) { value.Temporal.TaskQueue = "llm-inference-2" }},
		{name: "Temporal identity prefix", field: "temporal.identity_prefix", mutate: func(value *config.Config) { value.Temporal.IdentityPrefix = "llmtw-2" }},
		{name: "Temporal TLS enabled", field: "temporal.tls.enabled", mutate: func(value *config.Config) { value.Temporal.TLS.Enabled = false }},
		{name: "Temporal TLS server name", field: "temporal.tls.server_name", mutate: func(value *config.Config) { value.Temporal.TLS.ServerName = "temporal-2.example.internal" }},
		{name: "Temporal TLS CA file", field: "temporal.tls.ca_file", mutate: func(value *config.Config) { value.Temporal.TLS.CAFile = "/var/run/ca/temporal-2.pem" }},
		{name: "Temporal activity concurrency", field: "temporal.worker.max_concurrent_activities", mutate: func(value *config.Config) { value.Temporal.Worker.MaxConcurrentActivities++ }},
		{name: "Temporal poll concurrency", field: "temporal.worker.max_concurrent_activity_task_polls", mutate: func(value *config.Config) { value.Temporal.Worker.MaxConcurrentActivityTaskPolls++ }},
		{name: "Temporal graceful stop", field: "temporal.worker.graceful_stop_timeout", mutate: func(value *config.Config) { value.Temporal.Worker.GracefulStopTimeout += config.Duration(time.Second) }},
		{name: "heartbeat cadence", field: "temporal.worker.heartbeat_keepalive_interval", mutate: func(value *config.Config) {
			value.Temporal.Worker.HeartbeatKeepaliveInterval += config.Duration(time.Second)
		}},
		{name: "log format", field: "telemetry.logs.format", mutate: func(value *config.Config) { value.Telemetry.Logs.Format = "text" }},
		{name: "log level", field: "telemetry.logs.level", mutate: func(value *config.Config) { value.Telemetry.Logs.Level = "debug" }},
		{name: "metrics enabled", field: "telemetry.metrics.enabled", mutate: func(value *config.Config) { value.Telemetry.Metrics.Enabled = false }},
		{name: "tracing enabled", field: "telemetry.tracing.enabled", mutate: func(value *config.Config) { value.Telemetry.Tracing.Enabled = false }},
		{name: "tracing endpoint", field: "telemetry.tracing.otlp_endpoint", mutate: func(value *config.Config) { value.Telemetry.Tracing.OTLPEndpoint = "otel-2.example.internal:4317" }},
		{name: "tracing sample ratio", field: "telemetry.tracing.sample_ratio", mutate: func(value *config.Config) { value.Telemetry.Tracing.SampleRatio = "0.10" }},
		{name: "content logging", field: "telemetry.content_logging", mutate: func(value *config.Config) { value.Telemetry.ContentLogging = "redacted" }},
		{name: "Redis key prefix", field: "state.redis.key_prefix", mutate: func(value *config.Config) { value.State.Redis.KeyPrefix = "worker-b" }},
		{name: "PostgreSQL database", field: "state.postgres.database", mutate: func(value *config.Config) { value.State.Postgres.Database = "worker_b" }},
		{name: "PostgreSQL schema", field: "state.postgres.schema", mutate: func(value *config.Config) { value.State.Postgres.Schema = "worker_b" }},
		{name: "PostgreSQL table prefix", field: "state.postgres.table_prefix", mutate: func(value *config.Config) { value.State.Postgres.TablePrefix = "next_" }},
		{name: "endpoint membership", field: "endpoints.*.outbound_hosts", mutate: func(value *config.Config) {
			value.Endpoints["openai-canary"] = value.Endpoints["openai-prod"]
		}},
		{name: "endpoint outbound hosts", field: "endpoints.*.outbound_hosts", mutate: func(value *config.Config) {
			endpoint := value.Endpoints["openai-prod"]
			endpoint.BaseURL = "https://api2.example.com/v1"
			endpoint.OutboundHosts = []string{"api2.example.com"}
			value.Endpoints["openai-prod"] = endpoint
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
			beforeVersion := before.Config.ConfigVersion()

			err = application.Reload(context.Background(), replacementTestConfig(t, test.mutate))
			if !errors.Is(err, errProcessLifetimeConfigurationChanged) {
				t.Fatalf("Reload() error = %v, want process-lifetime rejection", err)
			}
			if !strings.Contains(err.Error(), test.field) {
				t.Fatalf("Reload() error = %v, want safe field %q", err, test.field)
			}
			if application.Current() != before || application.Current().Config.ConfigVersion() != beforeVersion {
				t.Fatal("rejected replacement changed the active snapshot or version")
			}
			if got := clientBuilds.Load(); got != 1 {
				t.Fatalf("client builds = %d, want initial build only", got)
			}
		})
	}
}

func TestRuntimeReplacementValidatorAllowsSnapshotScopedChanges(t *testing.T) {
	initial := replacementTestConfig(t, func(value *config.Config) {
		value.Environment = "development"
	})
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

	replacement := replacementTestConfig(t, func(value *config.Config) {
		value.Environment = "development"
		value.Limits.Items++
		value.Budgets.RequireMatch = false
		value.BlobStore.InlineBytes++
		value.State.Redis.Addresses = []string{"redis-2.example.internal:6379"}
		value.State.Postgres.Addresses = []string{"postgres-2.example.internal:5432"}
		endpoint := value.Endpoints["openai-prod"]
		endpoint.BaseURL = "https://api.openai.com/v2"
		endpoint.Timeout += config.Duration(time.Second)
		value.Endpoints["openai-prod"] = endpoint
		model := value.Models["invoice-summarizer"]
		model.Routes[0].Model = "gpt-example-2026-07-02"
		value.Models["invoice-summarizer"] = model
		value.Capabilities.Catalogs[0].SHA256 = strings.Repeat("1", 64)
		value.Pricing.Catalogs[0].SHA256 = strings.Repeat("2", 64)
	})
	if err := application.Reload(context.Background(), replacement); err != nil {
		t.Fatalf("Reload() rejected snapshot-scoped configuration: %v", err)
	}
	after := application.Current()
	if after == before || after.Config.ConfigVersion() == before.Config.ConfigVersion() {
		t.Fatal("allowed replacement did not publish a new snapshot and version")
	}
	if got := clientBuilds.Load(); got != 2 {
		t.Fatalf("client builds = %d, want initial and replacement", got)
	}
}

func replacementTestConfig(t *testing.T, mutate func(*config.Config)) []byte {
	t.Helper()
	value, err := config.Load(runtimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	value.Environment = "development"
	mutate(&value)
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
