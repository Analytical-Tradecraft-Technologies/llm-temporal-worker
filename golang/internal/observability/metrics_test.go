package observability_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

func TestMetricsBoundLabelsAndNeverExposeTenantText(t *testing.T) {
	metrics, err := observability.NewMetrics(observability.AllowedValues{
		Endpoints: []string{"endpoint-a"}, Models: []string{"model-a"},
		Outcomes: []string{"success"}, Methods: []string{"provider"},
	})
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordProviderAttempt("endpoint-a", "model-a", "priority", "success", time.Second)
	metrics.RecordProviderAttempt("secret-endpoint", "secret-model", "secret-class", "secret-outcome", time.Second)
	metrics.RecordActivityFailure("worker")
	metrics.RecordActivityFailure("secret-origin")
	metrics.RecordCost("endpoint-a", "model-a", "standard", "provider", 7)
	metrics.RecordCost("endpoint-a", "model-a", "standard", "tenant-secret", 3)

	response := &strings.Builder{}
	metricFamilies, err := metrics.Gather()
	if err != nil {
		t.Fatal(err)
	}
	encoder := expfmt.NewEncoder(response, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range metricFamilies {
		if err := encoder.Encode(family); err != nil {
			t.Fatal(err)
		}
	}
	output := response.String()
	for _, forbidden := range []string{"secret-endpoint", "secret-model", "secret-class", "secret-outcome", "secret-origin", "tenant-secret"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("metrics leaked %q: %s", forbidden, output)
		}
	}
	if !strings.Contains(output, `endpoint="endpoint-a"`) {
		t.Fatalf("configured endpoint was not recorded: %s", output)
	}
	if !strings.Contains(output, `endpoint="other"`) {
		t.Fatalf("unconfigured endpoint was not bounded: %s", output)
	}
	if !strings.Contains(output, `origin="worker"`) || !strings.Contains(output, `origin="other"`) {
		t.Fatalf("failure origins were not restricted to the fixed vocabulary: %s", output)
	}
}

func TestMetricsRecordersExposeEveryBoundedSignal(t *testing.T) {
	metrics, err := observability.NewMetrics(observability.AllowedValues{
		Endpoints:             []string{"endpoint-a"},
		Models:                []string{"model-a"},
		Policies:              []string{"policy-a"},
		Windows:               []string{"hour"},
		ErrorClasses:          []string{"none"},
		Phases:                []string{"total"},
		Statuses:              []string{"completed"},
		Outcomes:              []string{"success"},
		Methods:               []string{"provider"},
		OperationStates:       []string{"started"},
		ContinuationDecisions: []string{"continue"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := observability.WithMetrics(context.Background(), metrics)
	if got := observability.MetricsFromContext(ctx); got != metrics {
		t.Fatalf("metrics from context = %p, want %p", got, metrics)
	}

	metrics.RecordActivity("completed", "none", 2*time.Second, "total")
	metrics.RecordActivityFailure("provider")
	metrics.RecordProviderAttempt("endpoint-a", "model-a", "priority", "success", time.Second)
	metrics.RecordServiceClass("standard", "priority", "endpoint-a")
	metrics.RecordBudgetAdmission("policy-a", "success")
	metrics.SetBudgetReserved("policy-a", "hour", 12.5)
	metrics.RecordCost("endpoint-a", "model-a", "standard", "provider", 7.25)
	metrics.RecordExactCost("endpoint-a", "model-a", "standard", "provider")
	metrics.RecordCostStatus("endpoint-a", "model-a", "standard", "exact", "provider")
	metrics.RecordCostStatus("endpoint-a", "model-a", "standard", "unknown", "")
	metrics.RecordOperationState("started")
	metrics.RecordAmbiguous("endpoint-a")
	metrics.RecordContinuation("continue")
	metrics.RecordConfigReload("success")
	metrics.RecordMaintenance("cache", "eligible", 0, time.Second)
	metrics.RecordMaintenance("cache", "tombstoned", 2, time.Second)
	metrics.RecordMaintenance("cache", "skipped", 1, time.Second)
	metrics.RecordMaintenanceFailure("cache")
	metrics.RecordPostgresPool(4, 2, 2, 8)
	metrics.RecordPostgresLatency("query", 2*time.Millisecond)
	metrics.RecordPostgresLatency("lock", -time.Second)
	metrics.RecordPostgresTableTuples("cache", 20, 3)
	metrics.RecordCache("hit")
	metrics.RecordCache("use")
	metrics.RecordCache("fill")
	metrics.RecordPendingPoll("started")
	metrics.RecordPendingPoll("retry")
	metrics.SetWorkerPolling(true)
	metrics.SetWorkerPolling(false)
	metrics.SetHeartbeatAge(-time.Second)

	families, err := metrics.Gather()
	if err != nil {
		t.Fatal(err)
	}
	wantFamilies := []string{
		"llmtw_activity_total", "llmtw_activity_duration_seconds", "llmtw_activity_failure_total",
		"llmtw_provider_attempt_total", "llmtw_provider_duration_seconds", "llmtw_service_class_actual_total",
		"llmtw_budget_admission_total", "llmtw_budget_reserved_micro_usd", "llmtw_cost_micro_usd_total",
		"llmtw_cost_usd_total", "llmtw_operation_state_total", "llmtw_ambiguous_total",
		"llmtw_continuation_total", "llmtw_config_reload_total", "llmtw_worker_polling",
		"llmtw_heartbeat_age_seconds", "llmtw_maintenance_rows_total",
		"llmtw_maintenance_failures_total", "llmtw_maintenance_duration_seconds",
		"llmtw_postgres_pool_connections", "llmtw_postgres_latency_seconds", "llmtw_postgres_table_tuples",
		"llmtw_cache_events_total", "llmtw_provider_poll_total", "llmtw_cost_status_total",
	}
	seen := make(map[string]bool, len(families))
	for _, family := range families {
		seen[family.GetName()] = true
	}
	for _, name := range wantFamilies {
		if !seen[name] {
			t.Fatalf("metric family %q was not gathered; families = %v", name, seen)
		}
	}
	if got := metricValue(families, "llmtw_budget_reserved_micro_usd", nil); got != 12.5 {
		t.Fatalf("reserved budget = %v, want 12.5", got)
	}
	if got := metricValue(families, "llmtw_worker_polling", nil); got != 0 {
		t.Fatalf("worker polling = %v, want 0 after stop", got)
	}
	if got := metricValue(families, "llmtw_heartbeat_age_seconds", nil); got != 0 {
		t.Fatalf("heartbeat age = %v, want negative ages clamped to 0", got)
	}
	if got := metricValue(families, "llmtw_cost_usd_total", map[string]string{
		"endpoint": "endpoint-a", "model": "model-a", "class": "standard", "method": "provider",
	}); got != 1 {
		t.Fatalf("exact cost event count = %v, want 1", got)
	}
	if got := metricValue(families, "llmtw_maintenance_rows_total", map[string]string{"resource": "cache", "outcome": "tombstoned"}); got != 2 {
		t.Fatalf("maintenance tombstone count = %v, want 2", got)
	}
	if got := metricValue(families, "llmtw_maintenance_failures_total", map[string]string{"resource": "cache"}); got != 1 {
		t.Fatalf("maintenance failure count = %v, want 1", got)
	}
	if got := histogramCount(families, "llmtw_maintenance_duration_seconds", map[string]string{"resource": "cache"}); got != 1 {
		t.Fatalf("maintenance duration samples = %v, want one sample per pass", got)
	}
	if got := metricValue(families, "llmtw_postgres_pool_connections", map[string]string{"state": "max"}); got != 8 {
		t.Fatalf("postgres max pool gauge = %v, want 8", got)
	}
	if got := metricValue(families, "llmtw_postgres_latency_seconds", map[string]string{"kind": "lock"}); got < 0 {
		t.Fatalf("postgres lock latency = %v, want non-negative", got)
	}
	if got := metricValue(families, "llmtw_postgres_table_tuples", map[string]string{"resource": "cache", "state": "dead"}); got != 3 {
		t.Fatalf("postgres dead tuples = %v, want 3", got)
	}
	if got := metricValue(families, "llmtw_cache_events_total", map[string]string{"event": "use"}); got != 1 {
		t.Fatalf("cache use count = %v, want 1", got)
	}
	if got := metricValue(families, "llmtw_provider_poll_total", map[string]string{"outcome": "retry"}); got != 1 {
		t.Fatalf("provider poll retry count = %v, want 1", got)
	}
	if got := metricValue(families, "llmtw_cost_status_total", map[string]string{"status": "unknown"}); got != 1 {
		t.Fatalf("unknown cost count = %v, want 1", got)
	}

	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics handler status = %d, want 200", response.Code)
	}
	if !strings.Contains(response.Body.String(), "llmtw_provider_attempt_total") {
		t.Fatalf("metrics handler omitted provider counter: %s", response.Body.String())
	}
}

func TestMetricsNilBindingsAndDefaultBuiltInsAreSafe(t *testing.T) {
	var metrics *observability.Metrics
	if got := observability.MetricsFromContext(observability.WithMetrics(context.Background(), metrics)); got != nil {
		t.Fatalf("nil metrics binding = %p, want nil", got)
	}
	if got, err := metrics.Gather(); got != nil || err != nil {
		t.Fatalf("nil metrics Gather = (%v, %v), want (nil, nil)", got, err)
	}
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("nil metrics handler status = %d, want 404", response.Code)
	}
	metrics.RecordActivity("completed", "none", time.Second, "total")
	metrics.RecordActivityFailure("worker")
	metrics.RecordProviderAttempt("endpoint", "model", "economy", "success", time.Second)
	metrics.RecordServiceClass("standard", "priority", "endpoint")
	metrics.RecordBudgetAdmission("policy", "success")
	metrics.SetBudgetReserved("policy", "window", 1)
	metrics.RecordCost("endpoint", "model", "standard", "provider", 1)
	metrics.RecordExactCost("endpoint", "model", "standard", "provider")
	metrics.RecordCostStatus("endpoint", "model", "standard", "unknown", "secret-method")
	metrics.RecordOperationState("started")
	metrics.RecordAmbiguous("endpoint")
	metrics.RecordContinuation("continue")
	metrics.RecordConfigReload("success")
	metrics.RecordMaintenance("secret-resource", "secret-outcome", 1, time.Second)
	metrics.RecordMaintenanceFailure("secret-resource")
	metrics.RecordPostgresPool(-1, -2, -3, -4)
	metrics.RecordPostgresLatency("secret-kind", time.Second)
	metrics.RecordPostgresTableTuples("secret-resource", -1, -2)
	metrics.RecordCache("secret-event")
	metrics.RecordPendingPoll("secret-outcome")
	metrics.SetWorkerPolling(true)
	metrics.SetHeartbeatAge(time.Second)

	defaults, err := observability.NewMetrics(observability.AllowedValues{})
	if err != nil {
		t.Fatal(err)
	}
	defaults.RecordActivity("completed", "none", time.Second, "total")
	families, err := defaults.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if got := metricValue(families, "llmtw_activity_total", map[string]string{
		"status": "completed", "error_class": "none",
	}); got != 1 {
		t.Fatalf("default built-in labels were not retained: %v", got)
	}
}

func histogramCount(families []*dto.MetricFamily, name string, labels map[string]string) uint64 {
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			matched := true
			for key, want := range labels {
				found := false
				for _, label := range metric.GetLabel() {
					if label.GetName() == key && label.GetValue() == want {
						found = true
						break
					}
				}
				if !found {
					matched = false
					break
				}
			}
			if matched && metric.Histogram != nil {
				return metric.Histogram.GetSampleCount()
			}
		}
	}
	return 0
}

func metricValue(families []*dto.MetricFamily, name string, labels map[string]string) float64 {
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			matched := true
			for key, want := range labels {
				found := false
				for _, label := range metric.GetLabel() {
					if label.GetName() == key && label.GetValue() == want {
						found = true
						break
					}
				}
				if !found {
					matched = false
					break
				}
			}
			if matched {
				if metric.Gauge != nil {
					return metric.GetGauge().GetValue()
				}
				if metric.Counter != nil {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
