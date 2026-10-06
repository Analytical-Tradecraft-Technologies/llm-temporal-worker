package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func cloudMetricTotal(t *testing.T, metrics *observability.Metrics, name string, labels ...string) float64 {
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
	metrics:
		for _, metric := range family.GetMetric() {
			// labels are name/value pairs the series must carry.
			for i := 0; i+1 < len(labels); i += 2 {
				found := false
				for _, pair := range metric.GetLabel() {
					found = found || (pair.GetName() == labels[i] && pair.GetValue() == labels[i+1])
				}
				if !found {
					continue metrics
				}
			}
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
	if cloudMetricTotal(t, metrics, "llmtw_operation_state_total") == 0 {
		t.Fatal("operation states were not recorded on the cloud path")
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

// Outcomes are counted on the saved transition, whichever path saved it, so a
// recovery-only move to unknown counts as ambiguous and a re-save of the same
// stage counts nothing. A provider attempt is classified by the provider's
// answer, not by whether saving it succeeded.
func TestCloudMetricsCountTransitionsAndProviderAnswers(t *testing.T) {
	metrics, err := observability.NewMetrics(observability.AllowedValues{Endpoints: []string{"endpoint"}, Outcomes: []string{"success", "failure"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := observability.WithMetrics(context.Background(), metrics)
	plan := cloudstate.BudgetPlan{AttemptedClass: llm.ServiceClassStandard, Route: durable.RoutePlan{EndpointID: "endpoint", Model: "model"}}
	recordCloudExecutionOutcome(ctx, plan, cloudstate.ExecutionSubmitting, cloudstate.ProviderExecution{Stage: cloudstate.ExecutionUnknown})
	recordCloudExecutionOutcome(ctx, plan, cloudstate.ExecutionUnknown, cloudstate.ProviderExecution{Stage: cloudstate.ExecutionUnknown})
	if got := cloudMetricTotal(t, metrics, "llmtw_ambiguous_total"); got != 1 {
		t.Fatalf("ambiguous = %v, want one count for the recovery transition", got)
	}
	call := provider.Call{EndpointID: "endpoint", Model: "model", ServiceClass: llm.ServiceClassStandard}
	recordCloudProviderAttempt(ctx, call, true, time.Millisecond)
	families, err := metrics.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "llmtw_provider_attempt_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "outcome" && label.GetValue() != "success" {
					t.Fatalf("accepted provider answer recorded as %q", label.GetValue())
				}
			}
		}
	}
}

// Polls of provider-owned jobs and cache outcomes are counted on the cloud
// path (#1217).
func TestCloudRuntimeRecordsPollAndCacheMetrics(t *testing.T) {
	metrics, err := observability.NewMetrics(observability.AllowedValues{Endpoints: []string{"endpoint"}, Outcomes: []string{"success", "failure", "accepted", "denied"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := observability.WithMetrics(context.Background(), metrics)
	f := boundedCloud(t, true)
	f.request.Cache = &llm.CachePolicyV1{}
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	f.now = f.now.Add(2 * time.Second)
	v, err = f.runtime.PollExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	if got := cloudMetricTotal(t, metrics, "llmtw_provider_poll_total"); got != 2 {
		t.Fatalf("poll events = %v, want started and completed", got)
	}
	if got := cloudMetricTotal(t, metrics, "llmtw_provider_poll_total", "outcome", "completed"); got != 1 {
		t.Fatalf("completed polls = %v, want 1", got)
	}
	// An identical request under another key is served from the cache.
	f.request.OperationKey = "cached-repeat"
	v, err = f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	if err != nil || v.State != llm.ExecutionCompleted {
		ref = llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
		v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	}
	boundedState(t, v, err, llm.ExecutionCompleted)
	if got := cloudMetricTotal(t, metrics, "llmtw_continuation_total"); got < 2 {
		t.Fatalf("continuation events = %v, want a created checkpoint per completed request", got)
	}
	if got := cloudMetricTotal(t, metrics, "llmtw_cache_events_total"); got < 4 {
		t.Fatalf("cache events = %v, want a miss and fill, then a hit and use", got)
	}
}

// A completed poll whose result fails validation counts as failed, not
// completed.
func TestCloudRuntimeCountsInvalidPollAsFailed(t *testing.T) {
	metrics, err := observability.NewMetrics(observability.AllowedValues{Endpoints: []string{"endpoint"}, Outcomes: []string{"success", "failure", "accepted", "denied"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := observability.WithMetrics(context.Background(), metrics)
	f := boundedCloud(t, true)
	f.adapter.poll = func(ctx context.Context, call provider.Call, id string, o provider.Observer) (provider.ResumableResult, error) {
		v := executionResponse(call)
		v.ProviderOperationID = "another-job"
		return v, nil
	}
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	f.now = f.now.Add(2 * time.Second)
	if v, err = f.runtime.PollExecutionV1(ctx, ref); err == nil && v.State == llm.ExecutionProviderCompleted {
		t.Fatal("invalid poll result completed")
	}
	if got := cloudMetricTotal(t, metrics, "llmtw_provider_poll_total", "outcome", "completed"); got != 0 {
		t.Fatalf("completed polls = %v, want none", got)
	}
	if got := cloudMetricTotal(t, metrics, "llmtw_provider_poll_total", "outcome", "failed"); got != 1 {
		t.Fatalf("failed polls = %v, want 1", got)
	}
}

// Each durable step and each provider attempt is a span on the cloud path
// (#1217).
func TestCloudRuntimeRecordsStepAndAttemptSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracer := observability.NewTracer(observability.TraceOptions{Enabled: true, Exporter: exporter})
	ctx := observability.WithTracer(context.Background(), tracer)
	f := boundedCloud(t, false)
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	names := map[string]bool{}
	for _, span := range exporter.GetSpans() {
		names[span.Name] = true
	}
	if !names["llmtw.cloud.generate"] || !names["llmtw.provider_attempt"] {
		t.Fatalf("spans = %v, want the generate step and the provider attempt", names)
	}
}
