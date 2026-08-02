package engine

import (
	"context"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

func TestGenerateRecordsMetricsForSuccessfulProviderDispatch(t *testing.T) {
	actual := llm.ServiceClassStandard
	adapter := &fakeAdapter{name: "fake", response: successfulResponse()}
	adapter.response.Service.Actual = &actual
	harness := newHarness(t, adapter)
	metrics := newEngineTestMetrics(t)

	if _, err := harness.engine.Generate(observability.WithMetrics(context.Background(), metrics), baseRequest("metrics-success")); err != nil {
		t.Fatal(err)
	}

	assertMetricCounter(t, metrics, "llmtw_provider_attempt_total", map[string]string{"endpoint": "endpoint-1", "model": "provider-model", "class": "standard", "outcome": "success"}, 1)
	assertMetricHistogramCount(t, metrics, "llmtw_provider_duration_seconds", map[string]string{"endpoint": "endpoint-1", "model": "provider-model", "class": "standard"}, 1)
	for _, state := range []string{"reserved", "dispatching", "completed"} {
		assertMetricCounter(t, metrics, "llmtw_operation_state_total", map[string]string{"state": state}, 1)
	}
	assertMetricCounter(t, metrics, "llmtw_service_class_actual_total", map[string]string{"requested": "standard", "actual": "standard", "endpoint": "endpoint-1"}, 1)
	assertMetricCounter(t, metrics, "llmtw_cost_usd_total", map[string]string{"endpoint": "endpoint-1", "model": "provider-model", "class": "standard", "method": "catalog_usage"}, 1)
	assertMetricCounter(t, metrics, "llmtw_cost_status_total", map[string]string{"endpoint": "endpoint-1", "model": "provider-model", "class": "standard", "status": "exact", "method": "catalog_usage"}, 1)
}

func TestGenerateRecordsMetricsForAmbiguousProviderDispatch(t *testing.T) {
	adapter := &fakeAdapter{name: "fake", response: successfulResponse(), ambiguous: true}
	harness := newHarness(t, adapter)
	metrics := newEngineTestMetrics(t)

	if _, err := harness.engine.Generate(observability.WithMetrics(context.Background(), metrics), baseRequest("metrics-ambiguous")); err == nil {
		t.Fatal("Generate unexpectedly succeeded")
	}

	assertMetricCounter(t, metrics, "llmtw_provider_attempt_total", map[string]string{"endpoint": "endpoint-1", "model": "provider-model", "class": "standard", "outcome": "failure"}, 1)
	for _, state := range []string{"reserved", "dispatching", "ambiguous"} {
		assertMetricCounter(t, metrics, "llmtw_operation_state_total", map[string]string{"state": state}, 1)
	}
	assertMetricCounter(t, metrics, "llmtw_ambiguous_total", map[string]string{"endpoint": "endpoint-1"}, 1)
	assertMetricCounter(t, metrics, "llmtw_cost_usd_total", map[string]string{"endpoint": "endpoint-1", "model": "provider-model", "class": "standard", "method": "catalog_usage"}, 0)
}

func TestOperationMetricsMapRawFailedStatesToBoundedFailedLabel(t *testing.T) {
	metrics := newEngineTestMetrics(t)
	ctx := observability.WithMetrics(context.Background(), metrics)
	recordOperationState(ctx, admission.StateDefiniteFailed)
	recordOperationState(ctx, admission.StateCanceled)

	assertMetricCounter(t, metrics, "llmtw_operation_state_total", map[string]string{"state": "failed"}, 2)
	assertMetricCounter(t, metrics, "llmtw_operation_state_total", map[string]string{"state": "other"}, 0)
}

func TestHandleCancellationRecordsDurablePreDispatchTransitions(t *testing.T) {
	harness := newHarness(t, &fakeAdapter{name: "fake", response: successfulResponse()})
	operation, err := harness.admission.Begin(context.Background(), admission.BeginRequest{ID: "metrics-canceled", ScopeKey: "tenant-1\x00metrics-canceled", LeaseUntil: harness.clock.Add(time.Minute), ExpiresAt: harness.clock.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	metrics := newEngineTestMetrics(t)
	ctx, cancel := context.WithCancel(observability.WithMetrics(context.Background(), metrics))
	cancel()
	if err := harness.engine.handleCancellation(ctx, operation.Operation, routing.Candidate{RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1"}, context.Canceled); err == nil {
		t.Fatal("handleCancellation unexpectedly succeeded")
	}
	assertMetricCounter(t, metrics, "llmtw_operation_state_total", map[string]string{"state": "dispatching"}, 1)
	assertMetricCounter(t, metrics, "llmtw_operation_state_total", map[string]string{"state": "failed"}, 1)
}

func newEngineTestMetrics(t *testing.T) *observability.Metrics {
	t.Helper()
	metrics, err := observability.NewMetrics(observability.AllowedValues{
		Endpoints: []string{"endpoint-1"}, Models: []string{"provider-model"},
		Outcomes:        []string{"success", "failure", "accepted", "denied"},
		Methods:         []string{"provider_reported", "catalog_usage", "reconstructed_usage", "retained_reservation"},
		OperationStates: []string{"reserved", "dispatching", "completed", "failed", "ambiguous"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return metrics
}
