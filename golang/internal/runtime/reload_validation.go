package runtime

import (
	"errors"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/config"
)

var errProcessLifetimeConfigurationChanged = errors.New("process-lifetime configuration cannot change during reload")

var errBudgetWindowGeometryChanged = errors.New("budget window geometry cannot change behind an existing identity during reload")

// validateRuntimeReplacement compares only configuration captured by resources
// constructed once in New. Provider/state clients and request catalogs are
// rebuilt per snapshot and deliberately do not belong in this projection.
func validateRuntimeReplacement(current, replacement *config.Snapshot) error {
	if current == nil || replacement == nil {
		return errProcessLifetimeConfigurationChanged
	}
	before := current.Config()
	after := replacement.Config()
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
		{name: "temporal.worker.max_concurrent_activities", changed: before.Temporal.Worker.MaxConcurrentActivities != after.Temporal.Worker.MaxConcurrentActivities},
		{name: "temporal.worker.max_concurrent_activity_task_polls", changed: before.Temporal.Worker.MaxConcurrentActivityTaskPolls != after.Temporal.Worker.MaxConcurrentActivityTaskPolls},
		{name: "temporal.worker.graceful_stop_timeout", changed: before.Temporal.Worker.GracefulStopTimeout != after.Temporal.Worker.GracefulStopTimeout},
		{name: "temporal.worker.heartbeat_keepalive_interval", changed: before.Temporal.Worker.HeartbeatKeepaliveInterval != after.Temporal.Worker.HeartbeatKeepaliveInterval},
		{name: "telemetry.logs.format", changed: before.Telemetry.Logs.Format != after.Telemetry.Logs.Format},
		{name: "telemetry.logs.level", changed: before.Telemetry.Logs.Level != after.Telemetry.Logs.Level},
		{name: "telemetry.metrics.enabled", changed: before.Telemetry.Metrics.Enabled != after.Telemetry.Metrics.Enabled},
		{name: "telemetry.tracing.enabled", changed: before.Telemetry.Tracing.Enabled != after.Telemetry.Tracing.Enabled},
		{name: "telemetry.tracing.otlp_endpoint", changed: before.Telemetry.Tracing.OTLPEndpoint != after.Telemetry.Tracing.OTLPEndpoint},
		{name: "telemetry.tracing.sample_ratio", changed: before.Telemetry.Tracing.SampleRatio != after.Telemetry.Tracing.SampleRatio},
		{name: "telemetry.content_logging", changed: before.Telemetry.ContentLogging != after.Telemetry.ContentLogging},
		{name: "state.redis.key_prefix", changed: before.State.Redis.KeyPrefix != after.State.Redis.KeyPrefix},
		{name: "endpoints.*.outbound_hosts", changed: !sameEndpointOutboundHosts(before.Endpoints, after.Endpoints)},
	} {
		if field.changed {
			return fmt.Errorf("%w: %s", errProcessLifetimeConfigurationChanged, field.name)
		}
	}
	return validateBudgetWindowGeometry(before.Budgets, after.Budgets)
}

// validateBudgetWindowGeometry rejects a replacement that keeps a budget
// window identity but changes its duration or bucket. The identity selects the
// accounting hash, whose bucket layout and expiries were written for the old
// geometry. Only an explicit window id can do this: a derived identity changes
// with the geometry and starts separate accounting.
func validateBudgetWindowGeometry(before, after config.BudgetsConfig) error {
	type geometry struct{ duration, bucket config.Duration }
	current := make(map[string]geometry)
	for _, policy := range before.Policies {
		for _, window := range policy.Windows {
			current[policy.WindowIdentity(window)] = geometry{window.Duration, window.Bucket}
		}
	}
	for policyIndex, policy := range after.Policies {
		for windowIndex, window := range policy.Windows {
			previous, exists := current[policy.WindowIdentity(window)]
			if exists && previous != (geometry{window.Duration, window.Bucket}) {
				return fmt.Errorf("%w: budgets.policies[%d].windows[%d] keeps its id but changes duration or bucket; give the window a new id", errBudgetWindowGeometryChanged, policyIndex, windowIndex)
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
