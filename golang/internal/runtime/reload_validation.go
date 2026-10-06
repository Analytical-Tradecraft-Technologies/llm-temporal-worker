package runtime

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sync"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
)

var errProcessLifetimeConfigurationChanged = errors.New("process-lifetime configuration cannot change during reload")

var errBudgetWindowGeometryChanged = errors.New("budget window geometry cannot change behind an existing identity during reload")

// processLifetimeChangeError names the first changed process-lifetime field.
// The field is a fixed schema path from the table below, never a value.
type processLifetimeChangeError struct{ field string }

func (err *processLifetimeChangeError) Error() string {
	return fmt.Sprintf("%v: %s", errProcessLifetimeConfigurationChanged, err.field)
}

func (err *processLifetimeChangeError) Unwrap() error { return errProcessLifetimeConfigurationChanged }

// validateRuntimeReplacement rejects a replacement that changes configuration
// the process cannot swap safely. That is configuration captured by resources
// constructed once in New, and the identity of durable state. State clients
// are rebuilt per snapshot, so a reload could physically point them at another
// request namespace, table, payload store, key namespace or result bucket, but
// Temporal workflows in flight hold request IDs, operation keys, budget
// reservations and result references that exist only under the old identity.
// After such a swap they would read as absent, and a retried operation key
// would be dispatched and paid for again.
//
// Redis addresses, credentials, TLS and timeouts stay reloadable: they say how
// to reach the same data. The two key-material references are compared as
// references only; the material behind an unchanged reference must not change.
func validateRuntimeReplacement(current, replacement *config.Snapshot) error {
	return (&budgetWindowGeometries{}).validateReplacement(current, replacement)
}

// newRuntimeReplacementValidator returns the validator for one worker
// process. It remembers the geometry of every budget window identity an
// active snapshot has used, so an identity removed by one reload cannot come
// back with another geometry in a later one while its accounting may remain.
func newRuntimeReplacementValidator() func(current, replacement *config.Snapshot) error {
	return (&budgetWindowGeometries{}).validateReplacement
}

func (geometries *budgetWindowGeometries) validateReplacement(current, replacement *config.Snapshot) error {
	if current == nil || replacement == nil {
		return errProcessLifetimeConfigurationChanged
	}
	before := current.Config()
	after := replacement.Config()
	// Memory state ignores the Redis section entirely, so it names nothing
	// there. A change of state.kind itself is rejected first.
	usesRedis := before.State.Kind != config.StateKindMemory
	// A missing state.requests block compares as empty; its presence is a
	// separate entry in the table.
	var beforeRequests, afterRequests config.CloudRequestConfig
	if before.State.Requests != nil {
		beforeRequests = *before.State.Requests
	}
	if after.State.Requests != nil {
		afterRequests = *after.State.Requests
	}
	for _, field := range []struct {
		name    string
		changed bool
	}{
		{name: "environment", changed: before.Environment != after.Environment},
		{name: "server.health_address", changed: before.Server.HealthAddress != after.Server.HealthAddress},
		{name: "server.metrics_address", changed: before.Server.MetricsAddress != after.Server.MetricsAddress},
		{name: "server.shutdown_timeout", changed: before.Server.ShutdownTimeout != after.Server.ShutdownTimeout},
		{name: "server.readiness_probe_interval", changed: before.Server.ReadinessProbeInterval != after.Server.ReadinessProbeInterval},
		{name: "server.readiness_probe_timeout", changed: before.Server.ReadinessProbeTimeout != after.Server.ReadinessProbeTimeout},
		{name: "server.inline_payload_bytes", changed: before.Server.InlinePayloadBytes != after.Server.InlinePayloadBytes},
		{name: "temporal.target", changed: before.Temporal.Target != after.Temporal.Target},
		{name: "temporal.namespace", changed: before.Temporal.Namespace != after.Temporal.Namespace},
		{name: "temporal.task_queue", changed: before.Temporal.TaskQueue != after.Temporal.TaskQueue},
		{name: "temporal.identity_prefix", changed: before.Temporal.IdentityPrefix != after.Temporal.IdentityPrefix},
		{name: "temporal.tls.enabled", changed: before.Temporal.TLS.Enabled != after.Temporal.TLS.Enabled},
		{name: "temporal.tls.server_name", changed: before.Temporal.TLS.ServerName != after.Temporal.TLS.ServerName},
		{name: "temporal.tls.ca_file", changed: before.Temporal.TLS.CAFile != after.Temporal.TLS.CAFile},
		{name: "temporal.tls.cert_file", changed: before.Temporal.TLS.CertFile != after.Temporal.TLS.CertFile},
		{name: "temporal.tls.key_file", changed: before.Temporal.TLS.KeyFile != after.Temporal.TLS.KeyFile},
		{name: "temporal.api_key_file", changed: before.Temporal.APIKeyFile != after.Temporal.APIKeyFile},
		{name: "temporal.mesh_transport", changed: before.Temporal.MeshTransport != after.Temporal.MeshTransport},
		{name: "temporal.payload_codec", changed: !reflect.DeepEqual(before.Temporal.PayloadCodec, after.Temporal.PayloadCodec)},
		{name: "temporal.worker.max_concurrent_activities", changed: before.Temporal.Worker.MaxConcurrentActivities != after.Temporal.Worker.MaxConcurrentActivities},
		{name: "temporal.worker.max_concurrent_activity_task_polls", changed: before.Temporal.Worker.MaxConcurrentActivityTaskPolls != after.Temporal.Worker.MaxConcurrentActivityTaskPolls},
		{name: "temporal.worker.graceful_stop_timeout", changed: before.Temporal.Worker.GracefulStopTimeout != after.Temporal.Worker.GracefulStopTimeout},
		{name: "temporal.worker.heartbeat_keepalive_interval", changed: before.Temporal.Worker.HeartbeatKeepaliveInterval != after.Temporal.Worker.HeartbeatKeepaliveInterval},
		{name: "temporal.worker.versioning", changed: before.Temporal.Worker.Versioning != after.Temporal.Worker.Versioning},
		{name: "telemetry.logs.format", changed: before.Telemetry.Logs.Format != after.Telemetry.Logs.Format},
		{name: "telemetry.logs.level", changed: before.Telemetry.Logs.Level != after.Telemetry.Logs.Level},
		{name: "telemetry.metrics.enabled", changed: before.Telemetry.Metrics.Enabled != after.Telemetry.Metrics.Enabled},
		{name: "telemetry.tracing.enabled", changed: before.Telemetry.Tracing.Enabled != after.Telemetry.Tracing.Enabled},
		{name: "telemetry.tracing.otlp_endpoint", changed: before.Telemetry.Tracing.OTLPEndpoint != after.Telemetry.Tracing.OTLPEndpoint},
		{name: "telemetry.tracing.sample_ratio", changed: before.Telemetry.Tracing.SampleRatio != after.Telemetry.Tracing.SampleRatio},
		{name: "telemetry.content_logging", changed: before.Telemetry.ContentLogging != after.Telemetry.ContentLogging},
		{name: "state.kind", changed: before.State.Kind != after.State.Kind},
		{name: "state.redis.key_prefix", changed: before.State.Redis.KeyPrefix != after.State.Redis.KeyPrefix},
		{name: "state.redis.admission_hash_tag", changed: usesRedis && before.State.Redis.AdmissionHashTag != after.State.Redis.AdmissionHashTag},
		{name: "state.redis.key_secret", changed: usesRedis && before.State.Redis.KeySecret != after.State.Redis.KeySecret},
		{name: "state.requests", changed: (before.State.Requests == nil) != (after.State.Requests == nil)},
		{name: "state.requests.provider.type", changed: beforeRequests.Provider.Type != afterRequests.Provider.Type},
		{name: "state.requests.provider.aws.region", changed: beforeRequests.Provider.AWS.Region != afterRequests.Provider.AWS.Region},
		{name: "state.requests.provider.aws.profile", changed: beforeRequests.Provider.AWS.Profile != afterRequests.Provider.AWS.Profile},
		{name: "state.requests.provider.aws.failover", changed: !reflect.DeepEqual(beforeRequests.Provider.AWS.Failover, afterRequests.Provider.AWS.Failover)},
		{name: "state.requests.provider.aws.allow_mrsc", changed: beforeRequests.Provider.AWS.AllowMRSC != afterRequests.Provider.AWS.AllowMRSC},
		{name: "state.requests.provider.aws.temp_directory", changed: beforeRequests.Provider.AWS.TempDirectory != afterRequests.Provider.AWS.TempDirectory},
		{name: "state.requests.provider.key_value_stores", changed: !maps.Equal(beforeRequests.Provider.KeyValueStores, afterRequests.Provider.KeyValueStores)},
		{name: "state.requests.provider.blob_stores", changed: !maps.Equal(beforeRequests.Provider.BlobStores, afterRequests.Provider.BlobStores)},
		{name: "state.requests.request_table", changed: beforeRequests.RequestTable != afterRequests.RequestTable},
		{name: "state.requests.payload_store", changed: beforeRequests.PayloadStore != afterRequests.PayloadStore},
		{name: "state.requests.namespace", changed: beforeRequests.Namespace != afterRequests.Namespace},
		{name: "state.requests.secret", changed: beforeRequests.Secret != afterRequests.Secret},
		{name: "blob_store.kind", changed: before.BlobStore.Kind != after.BlobStore.Kind},
		{name: "blob_store.file.root", changed: before.BlobStore.File.Root != after.BlobStore.File.Root},
		{name: "blob_store.s3.bucket", changed: before.BlobStore.S3.Bucket != after.BlobStore.S3.Bucket},
		{name: "blob_store.s3.region", changed: before.BlobStore.S3.Region != after.BlobStore.S3.Region},
		{name: "blob_store.s3.failover", changed: !reflect.DeepEqual(before.BlobStore.S3.Failover, after.BlobStore.S3.Failover)},
		{name: "blob_store.s3.prefix", changed: before.BlobStore.S3.Prefix != after.BlobStore.S3.Prefix},
		{name: "endpoints.*.outbound_hosts", changed: !sameEndpointOutboundHosts(before.Endpoints, after.Endpoints)},
	} {
		if field.changed {
			return &processLifetimeChangeError{field: field.name}
		}
	}
	return geometries.validate(before.Budgets, after.Budgets)
}

type budgetWindowGeometry struct{ duration, bucket config.Duration }

// budgetWindowGeometries records the duration and bucket each budget window
// identity has been active with.
type budgetWindowGeometries struct {
	mu   sync.Mutex
	seen map[string]budgetWindowGeometry
}

// validate rejects a replacement that uses a known budget window identity
// with another duration or bucket. The identity selects the accounting hash,
// whose bucket layout and expiries were written for the old geometry. Only an
// explicit window id can do this: a derived identity changes with the
// geometry and starts separate accounting. The active policies are recorded
// on every call, so identities are remembered after they leave the snapshot.
func (geometries *budgetWindowGeometries) validate(before, after config.BudgetsConfig) error {
	geometries.mu.Lock()
	defer geometries.mu.Unlock()
	if geometries.seen == nil {
		geometries.seen = make(map[string]budgetWindowGeometry)
	}
	for _, policy := range before.Policies {
		for _, window := range policy.Windows {
			geometries.seen[policy.WindowIdentity(window)] = budgetWindowGeometry{window.Duration, window.Bucket}
		}
	}
	for policyIndex, policy := range after.Policies {
		for windowIndex, window := range policy.Windows {
			previous, exists := geometries.seen[policy.WindowIdentity(window)]
			if exists && previous != (budgetWindowGeometry{window.Duration, window.Bucket}) {
				return fmt.Errorf("%w: budgets.policies[%d].windows[%d] uses an id this worker has accounted with another duration or bucket; give the window a new id", errBudgetWindowGeometryChanged, policyIndex, windowIndex)
			}
		}
	}
	return nil
}

func sameEndpointOutboundHosts(before, after map[string]config.EndpointConfig) bool {
	if len(before) != len(after) {
		return false
	}
	for endpointID, beforeEndpoint := range before {
		afterEndpoint, ok := after[endpointID]
		if !ok || !sameStrings(beforeEndpoint.OutboundHosts, afterEndpoint.OutboundHosts) {
			return false
		}
	}
	return true
}

func sameStrings(before, after []string) bool {
	if len(before) != len(after) {
		return false
	}
	values := make(map[string]struct{}, len(before))
	for _, value := range before {
		values[value] = struct{}{}
	}
	for _, value := range after {
		if _, ok := values[value]; !ok {
			return false
		}
	}
	return true
}
