package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

func TestGenerateRecordsMetricsForSuccessfulProviderDispatch(t *testing.T) {
	actual := llm.ServiceClassStandard
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	adapter.response.Service.Actual = &actual
	metrics, err := generateWithMetrics(t, adapter, "metrics-success")
	testassert.NoError(t, err)

	testkit.MetricCounter(t, metrics, "llmtw_provider_attempt_total", standardProviderLabels("outcome", "success"), 1)
	testkit.MetricHistogramCount(t, metrics, "llmtw_provider_duration_seconds", standardProviderLabels(), 1)
	for _, state := range []string{"reserved", "dispatching", "completed"} {
		testkit.MetricCounter(t, metrics, "llmtw_operation_state_total", map[string]string{"state": state}, 1)
	}
	testkit.MetricCounter(t, metrics, "llmtw_service_class_actual_total", map[string]string{"requested": "standard", "actual": "standard", "endpoint": "endpoint-1"}, 1)
	testkit.MetricCounter(t, metrics, "llmtw_cost_usd_total", standardProviderLabels("method", "catalog_usage"), 1)
	testkit.MetricCounter(t, metrics, "llmtw_cost_status_total", standardProviderLabels("status", "exact", "method", "catalog_usage"), 1)
}

func TestGenerateRecordsMetricsForAmbiguousProviderDispatch(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse(), ambiguous: true}
	metrics, err := generateWithMetrics(t, adapter, "metrics-ambiguous")
	if err == nil {
		t.Fatal("Generate unexpectedly succeeded")
	}

	testkit.MetricCounter(t, metrics, "llmtw_provider_attempt_total", standardProviderLabels("outcome", "failure"), 1)
	for _, state := range []string{"reserved", "dispatching", "ambiguous"} {
		testkit.MetricCounter(t, metrics, "llmtw_operation_state_total", map[string]string{"state": state}, 1)
	}
	testkit.MetricCounter(t, metrics, "llmtw_ambiguous_total", map[string]string{"endpoint": "endpoint-1"}, 1)
	testkit.MetricCounter(t, metrics, "llmtw_cost_usd_total", standardProviderLabels("method", "catalog_usage"), 0)
}

func TestOperationMetricsMapRawFailedStatesToBoundedFailedLabel(t *testing.T) {
	metrics := newEngineTestMetrics(t)
	ctx := observability.WithMetrics(context.Background(), metrics)
	recordOperationState(ctx, admission.StateDefiniteFailed)
	recordOperationState(ctx, admission.StateCanceled)

	testkit.MetricCounter(t, metrics, "llmtw_operation_state_total", map[string]string{"state": "failed"}, 2)
	testkit.MetricCounter(t, metrics, "llmtw_operation_state_total", map[string]string{"state": "other"}, 0)
}

func TestHandleCancellationRecordsDurablePreDispatchTransitions(t *testing.T) {
	harness := newHarness(t, &fakeAdapter{name: "fake", response: successfulResponse()})
	operation, err := harness.admission.Begin(context.Background(), admission.BeginRequest{ID: "metrics-canceled", ScopeKey: "tenant-1\x00metrics-canceled", LeaseUntil: harness.clock.Add(time.Minute), ExpiresAt: harness.clock.Add(time.Hour)})
	testassert.NoError(t, err)
	metrics := newEngineTestMetrics(t)
	ctx, cancel := context.WithCancel(observability.WithMetrics(context.Background(), metrics))
	cancel()
	if err := harness.engine.handleCancellation(ctx, operation.Operation, routing.Candidate{RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1"}, context.Canceled); err == nil {
		t.Fatal("handleCancellation unexpectedly succeeded")
	}
	testkit.MetricCounter(t, metrics, "llmtw_operation_state_total", map[string]string{"state": "dispatching"}, 1)
	testkit.MetricCounter(t, metrics, "llmtw_operation_state_total", map[string]string{"state": "failed"}, 1)
}

// generateWithMetrics runs one Generate for key and returns the metrics it recorded.
func generateWithMetrics(t *testing.T, adapter *fakeAdapter, key string) (*observability.Metrics, error) {
	t.Helper()
	harness := newHarness(t, adapter)
	metrics := newEngineTestMetrics(t)
	_, err := harness.engine.Generate(observability.WithMetrics(context.Background(), metrics), baseRequest(key))
	return metrics, err
}

// standardProviderLabels returns the endpoint-1/provider-model/standard labels plus extra key/value pairs.
func standardProviderLabels(extra ...string) map[string]string {
	labels := map[string]string{"endpoint": "endpoint-1", "model": "provider-model", "class": "standard"}
	for index := 0; index+1 < len(extra); index += 2 {
		labels[extra[index]] = extra[index+1]
	}
	return labels
}

func newEngineTestMetrics(t *testing.T) *observability.Metrics {
	t.Helper()
	metrics, err := observability.NewMetrics(observability.AllowedValues{
		Endpoints: []string{"endpoint-1"}, Models: []string{"provider-model"},
		Outcomes:        []string{"success", "failure", "accepted", "denied"},
		Methods:         []string{"provider_reported", "catalog_usage", "reconstructed_usage", "retained_reservation"},
		OperationStates: []string{"reserved", "dispatching", "completed", "failed", "ambiguous"},
	})
	testassert.NoError(t, err)
	return metrics
}

func TestGenerateRecordsUnreportedActualServiceClassAsUnknown(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	adapter.response.Service.Actual = nil
	metrics, err := generateWithMetrics(t, adapter, "metrics-unknown-actual")
	testassert.NoError(t, err)
	testkit.MetricCounter(t, metrics, "llmtw_service_class_actual_total", map[string]string{"requested": "standard", "actual": "unknown", "endpoint": "endpoint-1"}, 1)
	testkit.MetricCounter(t, metrics, "llmtw_service_class_actual_total", map[string]string{"requested": "standard", "actual": "standard", "endpoint": "endpoint-1"}, 0)
}

// A response that reports no actual class did not report a mappable tier, so
// the request tier must not be recorded as the provider's own label.
func TestGenerateKeepsProviderValueEmptyWhenNoTierWasReported(t *testing.T) {
	unreported := &fakeAdapter{name: "fake", response: successfulResponse()}
	response, err := newHarness(t, unreported).engine.Generate(context.Background(), baseRequest("provider-value-unreported"))
	testassert.NoError(t, err)
	if response.Service.Actual != nil || response.Service.ProviderValue != "" {
		t.Fatalf("service facts = %+v, want no actual class and no provider value", response.Service)
	}

	standard := llm.ServiceClassStandard
	classified := &fakeAdapter{name: "fake", response: successfulResponse()}
	classified.response.Service.Actual = &standard
	response, err = newHarness(t, classified).engine.Generate(context.Background(), baseRequest("provider-value-classified"))
	testassert.NoError(t, err)
	if response.Service.Actual == nil || response.Service.ProviderValue == "" {
		t.Fatalf("service facts = %+v, want the request tier as the classified response's label", response.Service)
	}
}
