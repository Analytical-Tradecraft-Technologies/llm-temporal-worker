package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func cloudMetricTotal(t *testing.T, metrics *observability.Metrics, name string) float64 {
	t.Helper()
	families, err := metrics.Gather()
	if err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if counter := metric.GetCounter(); counter != nil {
				total += counter.GetValue()
			}
			if histogram := metric.GetHistogram(); histogram != nil {
				total += float64(histogram.GetSampleCount())
			}
		}
	}
	return total
}

// The cloud v1 runtime records the request series the documentation
// promises: budget admissions, provider attempts and latency, service class,
// cost status and ambiguous dispatch (#988).
func TestCloudRuntimeRecordsRequestMetrics(t *testing.T) {
	metrics, err := observability.NewMetrics(observability.AllowedValues{Endpoints: []string{"endpoint"}, Outcomes: []string{"success", "failure", "accepted", "denied"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := observability.WithMetrics(context.Background(), metrics)
	f := boundedCloud(t, false)
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	for _, name := range []string{"llmtw_budget_admission_total", "llmtw_provider_attempt_total", "llmtw_provider_duration_seconds", "llmtw_service_class_actual_total", "llmtw_cost_status_total"} {
		if cloudMetricTotal(t, metrics, name) == 0 {
			t.Errorf("%s was not recorded on the cloud path", name)
		}
	}
	if cloudMetricTotal(t, metrics, "llmtw_ambiguous_total") != 0 {
		t.Fatal("a completed request was counted as ambiguous")
	}
	// A lost paid response is ambiguous.
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, errors.New("lost paid response")
	}
	f.request.OperationKey = "ambiguous"
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	if cloudMetricTotal(t, metrics, "llmtw_ambiguous_total") != 1 {
		t.Fatal("ambiguous dispatch was not recorded")
	}
}
