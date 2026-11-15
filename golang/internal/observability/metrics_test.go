package observability_test

import (
	"context"
	"iter"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

func TestMetricsBoundLabelsAndNeverExposeTenantText(t *testing.T) {
	metrics := newMetrics(t, observability.AllowedValues{
		Endpoints: []string{"endpoint-a"}, Models: []string{"model-a"},
		Outcomes: []string{"success"}, Methods: []string{"provider"},
	})
	metrics.RecordProviderAttempt("endpoint-a", "model-a", "priority", "success", time.Second)
	metrics.RecordProviderAttempt("secret-endpoint", "secret-model", "secret-class", "secret-outcome", time.Second)
	metrics.RecordActivityFailure("worker")
	metrics.RecordActivityFailure("secret-origin")
	metrics.RecordExactCost("endpoint-a", "model-a", "standard", "provider")
	metrics.RecordCostStatus("endpoint-a", "model-a", "standard", "exact", "provider")

	response := &strings.Builder{}
	metricFamilies := gather(t, metrics)
	encoder := expfmt.NewEncoder(response, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range metricFamilies {
		testassert.NoError(t, encoder.Encode(family))
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
	metrics := newMetrics(t, observability.AllowedValues{
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
	ctx := observability.WithMetrics(context.Background(), metrics)
	if got := observability.MetricsFromContext(ctx); got != metrics {
		t.Fatalf("metrics from context = %p, want %p", got, metrics)
	}

	metrics.RecordActivity("completed", "none", 2*time.Second, "total")
	metrics.RecordActivityFailure("provider")
	metrics.RecordProviderAttempt("endpoint-a", "model-a", "priority", "success", time.Second)
	metrics.RecordServiceClass("standard", "priority", "endpoint-a")
	metrics.RecordBudgetAdmission("policy-a", "success")
	metrics.RecordExactCost("endpoint-a", "model-a", "standard", "provider")
	metrics.RecordCostStatus("endpoint-a", "model-a", "standard", "exact", "provider")
	metrics.RecordCostStatus("endpoint-a", "model-a", "standard", "unknown", "")
	metrics.RecordOperationState("started")
	metrics.RecordAmbiguous("endpoint-a")
	metrics.RecordRouteHeld("endpoint-a", observability.RouteHeldCircuitOpen)
	metrics.RecordRouteHeld("endpoint-a", "unbounded-reason")
	metrics.RecordRouteHealthTransition("endpoint-a", observability.RouteHealthCircuitOpen)
	metrics.RecordRouteHealthTransition("unconfigured-endpoint", "unbounded-transition")
	expiry := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	metrics.SetCatalogPriceExpirySource(func(time.Time) time.Time { return expiry })
	metrics.RecordContinuation("continue")
	metrics.RecordConfigReload("success")
	metrics.RecordMaintenance("cache", "eligible", 0, time.Second)
	metrics.RecordMaintenance("cache", "tombstoned", 2, time.Second)
	metrics.RecordMaintenance("cache", "skipped", 1, time.Second)
	metrics.RecordMaintenanceFailure("cache")
	metrics.RecordMaintenance("budget", "deleted", 3, time.Second)
	metrics.RecordMaintenanceFailure("budget")
	metrics.RecordCache("hit")
	metrics.RecordCache("use")
	metrics.RecordCache("fill")
	metrics.RecordPendingPoll("started")
	metrics.RecordPendingPoll("retry")
	metrics.SetWorkerPolling(true)
	metrics.SetWorkerPolling(false)
	metrics.SetHeartbeatAge(-time.Second)

	families := gather(t, metrics)
	wantFamilies := []string{
		"llmtw_activity_total", "llmtw_activity_duration_seconds", "llmtw_activity_failure_total",
		"llmtw_provider_attempt_total", "llmtw_provider_duration_seconds", "llmtw_service_class_actual_total",
		"llmtw_budget_admission_total",
		"llmtw_cost_usd_total", "llmtw_operation_state_total", "llmtw_ambiguous_total",
		"llmtw_continuation_total", "llmtw_config_reload_total", "llmtw_worker_polling",
		"llmtw_heartbeat_age_seconds", "llmtw_maintenance_rows_total",
		"llmtw_maintenance_failures_total", "llmtw_maintenance_duration_seconds",
		"llmtw_cache_events_total", "llmtw_provider_poll_total", "llmtw_cost_status_total",
		"llmtw_route_held_total", "llmtw_route_health_transition_total",
		"llmtw_catalog_price_expiry_timestamp_seconds",
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
	requireMetricValues(t, families, []metricExpectation{
		{"llmtw_route_held_total", map[string]string{"endpoint": "endpoint-a", "reason": "circuit_open"}, 1, "route held = %v, want 1"},
		{"llmtw_route_held_total", map[string]string{"endpoint": "endpoint-a", "reason": "other"}, 1, "route held with an unknown reason = %v, want it bounded to other"},
		{"llmtw_route_health_transition_total", map[string]string{"endpoint": "endpoint-a", "to": "circuit_open"}, 1, "route circuit openings = %v, want 1"},
		{"llmtw_route_health_transition_total", map[string]string{"endpoint": "endpoint-a", "to": "degraded"}, 0, "pre-registered degraded transitions = %v, want 0"},
		{"llmtw_route_health_transition_total", map[string]string{"endpoint": "other", "to": "other"}, 1, "unknown endpoint and transition = %v, want both bounded to other"},
	})
	if got := metricValue(families, "llmtw_catalog_price_expiry_timestamp_seconds", nil); got != float64(expiry.Unix()) {
		t.Fatalf("catalog price expiry = %v, want %v", got, expiry.Unix())
	}
	metrics.SetCatalogPriceExpirySource(func(time.Time) time.Time { return time.Time{} })
	families = gather(t, metrics)
	if got := metricValue(families, "llmtw_catalog_price_expiry_timestamp_seconds", nil); !math.IsInf(got, 1) {
		t.Fatalf("catalog price expiry without a bound = %v, want +Inf", got)
	}
	requireMetricValues(t, families, []metricExpectation{
		{"llmtw_worker_polling", nil, 0, "worker polling = %v, want 0 after stop"},
		{"llmtw_heartbeat_age_seconds", nil, 0, "heartbeat age = %v, want negative ages clamped to 0"},
		{"llmtw_cost_usd_total", map[string]string{"endpoint": "endpoint-a", "model": "model-a", "class": "standard", "method": "provider"}, 1, "exact cost event count = %v, want 1"},
		{"llmtw_maintenance_rows_total", map[string]string{"resource": "cache", "outcome": "tombstoned"}, 2, "maintenance tombstone count = %v, want 2"},
		{"llmtw_maintenance_failures_total", map[string]string{"resource": "cache"}, 1, "maintenance failure count = %v, want 1"},
		{"llmtw_maintenance_rows_total", map[string]string{"resource": "budget", "outcome": "deleted"}, 3, "budget maintenance delete count = %v, want 3"},
		{"llmtw_maintenance_failures_total", map[string]string{"resource": "budget"}, 1, "budget maintenance failure count = %v, want 1"},
		{"llmtw_cache_events_total", map[string]string{"event": "use"}, 1, "cache use count = %v, want 1"},
		{"llmtw_provider_poll_total", map[string]string{"outcome": "retry"}, 1, "provider poll retry count = %v, want 1"},
		{"llmtw_cost_status_total", map[string]string{"status": "unknown"}, 1, "unknown cost count = %v, want 1"},
	})
	if got := histogramCount(families, "llmtw_maintenance_duration_seconds", map[string]string{"resource": "cache"}); got != 1 {
		t.Fatalf("maintenance duration samples = %v, want one sample per pass", got)
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
	metrics.RecordExactCost("endpoint", "model", "standard", "provider")
	metrics.RecordCostStatus("endpoint", "model", "standard", "unknown", "secret-method")
	metrics.RecordOperationState("started")
	metrics.RecordAmbiguous("endpoint")
	metrics.RecordContinuation("continue")
	metrics.RecordConfigReload("success")
	metrics.RecordMaintenance("secret-resource", "secret-outcome", 1, time.Second)
	metrics.RecordMaintenanceFailure("secret-resource")
	metrics.RecordCache("secret-event")
	metrics.RecordPendingPoll("secret-outcome")
	metrics.SetWorkerPolling(true)
	metrics.SetHeartbeatAge(time.Second)

	defaults := newMetrics(t, observability.AllowedValues{})
	defaults.RecordActivity("completed", "none", time.Second, "total")
	families := gather(t, defaults)
	if got := metricValue(families, "llmtw_activity_total", map[string]string{
		"status": "completed", "error_class": "none",
	}); got != 1 {
		t.Fatalf("default built-in labels were not retained: %v", got)
	}
}

func histogramCount(families []*dto.MetricFamily, name string, labels map[string]string) uint64 {
	for metric := range seriesWithLabels(families, name, labels) {
		if metric.Histogram != nil {
			return metric.Histogram.GetSampleCount()
		}
	}
	return 0
}

func metricValue(families []*dto.MetricFamily, name string, labels map[string]string) float64 {
	for metric := range seriesWithLabels(families, name, labels) {
		if metric.Gauge != nil {
			return metric.GetGauge().GetValue()
		}
		if metric.Counter != nil {
			return metric.GetCounter().GetValue()
		}
	}
	return 0
}

// seriesWithLabels yields, in gather order, every series of the named family
// that carries all of labels (extra labels are allowed).
func seriesWithLabels(families []*dto.MetricFamily, name string, labels map[string]string) iter.Seq[*dto.Metric] {
	return func(yield func(*dto.Metric) bool) {
		for _, family := range families {
			if family.GetName() != name {
				continue
			}
			for _, metric := range family.GetMetric() {
				if !hasLabels(metric, labels) {
					continue
				}
				if !yield(metric) {
					return
				}
			}
		}
	}
}

func hasLabels(metric *dto.Metric, labels map[string]string) bool {
	for key, want := range labels {
		found := false
		for _, label := range metric.GetLabel() {
			if label.GetName() == key && label.GetValue() == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

type metricExpectation struct {
	name    string
	labels  map[string]string
	want    float64
	message string // format with one %v for the observed value
}

func requireMetricValues(t *testing.T, families []*dto.MetricFamily, expectations []metricExpectation) {
	t.Helper()
	for _, expectation := range expectations {
		if got := metricValue(families, expectation.name, expectation.labels); got != expectation.want {
			t.Fatalf(expectation.message, got)
		}
	}
}

func newMetrics(t *testing.T, allowed observability.AllowedValues) *observability.Metrics {
	t.Helper()
	metrics, err := observability.NewMetrics(allowed)
	testassert.NoError(t, err)
	return metrics
}

func gather(t *testing.T, metrics *observability.Metrics) []*dto.MetricFamily {
	t.Helper()
	families, err := metrics.Gather()
	testassert.NoError(t, err)
	return families
}

func TestExtendAllowedLabelsIdentifiersIntroducedByReload(t *testing.T) {
	metrics := newMetrics(t, observability.AllowedValues{Endpoints: []string{"endpoint-a"}, Models: []string{"model-a"}, Policies: []string{"policy-a"}, Outcomes: []string{"success", "accepted"}})
	metrics.RecordBudgetAdmission("policy-b", "accepted")
	metrics.ExtendAllowed(observability.AllowedValues{Endpoints: []string{"endpoint-b"}, Models: []string{"model-b"}, Policies: []string{"policy-b"}})
	metrics.RecordBudgetAdmission("policy-b", "accepted")
	metrics.RecordBudgetAdmission("policy-a", "accepted")
	families := gather(t, metrics)
	for policy, want := range map[string]float64{"other": 1, "policy-b": 1, "policy-a": 1} {
		if got := metricValue(families, "llmtw_budget_admission_total", map[string]string{"policy": policy, "outcome": "accepted"}); got != want {
			t.Errorf("policy %q admissions = %v, want %v", policy, got, want)
		}
	}
	// A reloaded endpoint's route health transitions are exported before its
	// first flip.
	if got := metricValue(families, "llmtw_route_health_transition_total", map[string]string{"endpoint": "endpoint-b", "to": "circuit_open"}); got != 0 || !hasSeries(families, "llmtw_route_health_transition_total", map[string]string{"endpoint": "endpoint-b", "to": "circuit_open"}) {
		t.Errorf("reloaded endpoint transition series missing (value %v)", got)
	}
	var disabled *observability.Metrics
	disabled.ExtendAllowed(observability.AllowedValues{Policies: []string{"policy-c"}})
}

// Provider calls and v1 Activities run for minutes; their latency histograms
// must resolve observations past the Prometheus default 10 s ceiling (#1359).
func TestLatencyHistogramsResolveMinuteLongObservations(t *testing.T) {
	metrics := newMetrics(t, observability.AllowedValues{Endpoints: []string{"endpoint"}, Models: []string{"model"}, Outcomes: []string{"success"}})
	for _, duration := range []time.Duration{30 * time.Second, 240 * time.Second} {
		metrics.RecordProviderAttempt("endpoint", "model", "standard", "success", duration)
		metrics.RecordActivity("completed", "none", duration, "generate")
	}
	families := gather(t, metrics)
	for _, name := range []string{"llmtw_provider_duration_seconds", "llmtw_activity_duration_seconds"} {
		var histogram *dto.Histogram
		for _, family := range families {
			if family.GetName() == name && len(family.GetMetric()) == 1 {
				histogram = family.GetMetric()[0].GetHistogram()
			}
		}
		if histogram == nil {
			t.Fatalf("%s has no single series", name)
		}
		// The smallest finite bucket holding 30 s must not hold 240 s, and a
		// finite bucket must hold both.
		first, both := -1.0, -1.0
		for _, bucket := range histogram.GetBucket() {
			if first < 0 && bucket.GetCumulativeCount() == 1 {
				first = bucket.GetUpperBound()
			}
			if both < 0 && bucket.GetCumulativeCount() == 2 {
				both = bucket.GetUpperBound()
			}
		}
		if first < 30 || first >= 240 || both < 240 || both > 600 {
			t.Fatalf("%s buckets: 30 s first counted at le=%v, 240 s at le=%v", name, first, both)
		}
	}
}

// The admission-and-compilation objectives are a 25 ms p99 with memory state
// and 75 ms with same-region Redis. Every latency histogram, including the
// per-step v1 execution histogram, must place those targets on exact bucket
// bounds so production metrics can show whether a step met them (#1497).
func TestLatencyHistogramsResolveAdmissionObjectives(t *testing.T) {
	metrics := newMetrics(t, observability.AllowedValues{Endpoints: []string{"endpoint"}, Models: []string{"model"}, Outcomes: []string{"success"}})
	for _, duration := range []time.Duration{20 * time.Millisecond, 60 * time.Millisecond, 90 * time.Millisecond} {
		metrics.RecordProviderAttempt("endpoint", "model", "standard", "success", duration)
		metrics.RecordActivity("completed", "none", duration, "total")
		metrics.RecordExecutionStep(observability.ExecutionStepPrepare, duration)
	}
	metrics.RecordExecutionStep("unbounded-step", time.Millisecond)
	metrics.RecordExecutionStep(observability.ExecutionStepAcquire, -time.Second)
	families := gather(t, metrics)
	for name, labels := range map[string]map[string]string{
		"llmtw_provider_duration_seconds":       {"endpoint": "endpoint", "model": "model", "class": "standard"},
		"llmtw_activity_duration_seconds":       {"phase": "total"},
		"llmtw_execution_step_duration_seconds": {"step": observability.ExecutionStepPrepare},
	} {
		var histogram *dto.Histogram
		for _, family := range families {
			for _, metric := range family.GetMetric() {
				if family.GetName() == name && len(metric.GetLabel()) == len(labels) && labelsMatch(metric, labels) {
					histogram = metric.GetHistogram()
				}
			}
		}
		if histogram == nil {
			t.Fatalf("%s%v has no series", name, labels)
		}
		counts := map[float64]uint64{}
		for _, bucket := range histogram.GetBucket() {
			counts[bucket.GetUpperBound()] = bucket.GetCumulativeCount()
		}
		if counts[0.025] != 1 || counts[0.075] != 2 || counts[0.1] != 3 {
			t.Fatalf("%s cumulative counts at le=0.025/0.075/0.1 = %d/%d/%d, want 1/2/3", name, counts[0.025], counts[0.075], counts[0.1])
		}
	}
	if got := histogramCount(families, "llmtw_execution_step_duration_seconds", map[string]string{"step": "other"}); got != 1 {
		t.Fatalf("unknown execution step count = %d, want it bounded to other", got)
	}
	if got := histogramCount(families, "llmtw_execution_step_duration_seconds", map[string]string{"step": observability.ExecutionStepAcquire}); got != 1 {
		t.Fatalf("acquire step count = %d, want 1", got)
	}
}

func labelsMatch(metric *dto.Metric, labels map[string]string) bool {
	for _, label := range metric.GetLabel() {
		if labels[label.GetName()] != label.GetValue() {
			return false
		}
	}
	return true
}

func hasSeries(families []*dto.MetricFamily, name string, labels map[string]string) bool {
	for range seriesWithLabels(families, name, labels) {
		return true
	}
	return false
}
