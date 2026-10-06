package activity

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"go.temporal.io/sdk/temporal"
)

func TestGenerateActivityRecordsTerminalMetrics(t *testing.T) {
	tests := []struct {
		name       string
		engineErr  error
		status     string
		errorClass string
		origin     string
	}{
		{name: "completed", status: "completed", errorClass: "none"},
		{name: "provider transient", engineErr: provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "unavailable"), status: "failed", errorClass: "provider_unavailable", origin: "provider"},
		{name: "configuration", engineErr: provider.NewError(provider.CodeConfiguration, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "configuration unavailable"), status: "failed", errorClass: "internal", origin: "worker"},
		{name: "provider authentication", engineErr: provider.NewError(provider.CodeAuthentication, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNever, "authentication failed"), status: "failed", errorClass: "internal", origin: "provider"},
		{name: "pricing no route", engineErr: provider.NewError(provider.CodeNoRoute, provider.PhasePrice, provider.DispatchNotDispatched, provider.RetryNever, "no eligible price"), status: "failed", errorClass: "internal", origin: "worker"},
		{name: "plan no route", engineErr: provider.NewError(provider.CodeNoRoute, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "unknown model"), status: "failed", errorClass: "internal", origin: "caller"},
		{name: "operation conflict", engineErr: provider.NewError(provider.CodeOperationConflict, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryNever, "operation conflict"), status: "failed", errorClass: "internal", origin: "caller"},
		{name: "budget denied", engineErr: provider.NewError(provider.CodeBudgetDenied, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryAfter, "budget denied"), status: "failed", errorClass: "budget_denied", origin: "budget"},
		{name: "unknown engine error", engineErr: errors.New("unexpected engine failure"), status: "failed", errorClass: "internal", origin: "worker"},
		{name: "canceled", engineErr: provider.NewError(provider.CodeCanceled, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "canceled"), status: "canceled", errorClass: "none"},
		{name: "Temporal canceled", engineErr: temporal.NewCanceledError(), status: "canceled", errorClass: "none"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metrics := newActivityTestMetrics(t)
			activities := Activities{Metrics: metrics, Engine: &fakeEngine{err: test.engineErr, response: llm.Response{OperationKey: "operation-1", OperationID: "operation-id", Status: llm.ResponseStatusCompleted, Service: llm.ServiceFacts{Requested: llm.ServiceClassStandard, Attempted: llm.ServiceClassStandard}}}}
			_, _ = activities.Generate(context.Background(), validGeneratePayload())
			assertActivityMetricCounter(t, metrics, "llmtw_activity_total", map[string]string{"status": test.status, "error_class": test.errorClass}, 1)
			assertActivityMetricHistogramCount(t, metrics, "llmtw_activity_duration_seconds", map[string]string{"phase": "total"}, 1)
			if test.origin != "" {
				assertActivityMetricCounter(t, metrics, "llmtw_activity_failure_total", map[string]string{"origin": test.origin}, 1)
			} else {
				assertActivityFailureCountZero(t, metrics, "llmtw_activity_failure_total", map[string]string{"origin": "worker"})
			}
		})
	}
}

func TestActivityFailureOriginFailsClosed(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantOrigin string
		wantFailed bool
	}{
		{name: "missing details", err: temporal.NewNonRetryableApplicationError("unknown failure", "unknown", nil), wantOrigin: "worker", wantFailed: true},
		{name: "unknown code", err: temporal.NewNonRetryableApplicationError("unknown failure", "unknown", nil, SafeErrorDetails{Code: "future_code"}), wantOrigin: "worker", wantFailed: true},
		{name: "canceled", err: context.Canceled},
		{name: "Temporal canceled", err: temporal.NewCanceledError()},
		{name: "pre-dispatch canceled", err: temporal.NewNonRetryableApplicationError("canceled", ErrorTypeInvalidArgument, nil, SafeErrorDetails{Code: string(provider.CodeCanceled)})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			origin, failed := activityFailureOrigin(test.err)
			if origin != test.wantOrigin || failed != test.wantFailed {
				t.Fatalf("activityFailureOrigin() = (%q, %t), want (%q, %t)", origin, failed, test.wantOrigin, test.wantFailed)
			}
		})
	}
}

func TestGenerateActivityDoesNotRecordFailureForPreDispatchCancellation(t *testing.T) {
	metrics := newActivityTestMetrics(t)
	activities := Activities{
		Metrics: metrics,
		Engine:  &fakeEngine{err: provider.NewPreDispatchContextError(context.Canceled)},
	}
	_, _ = activities.Generate(context.Background(), validGeneratePayload())
	for _, origin := range []string{"worker", "caller"} {
		assertActivityFailureCountZero(t, metrics, "llmtw_activity_failure_total", map[string]string{"origin": origin})
	}
}

func TestGenerateActivityRecordsWorkerFailureForOversizedResponse(t *testing.T) {
	metrics := newActivityTestMetrics(t)
	activities := Activities{
		Metrics:       metrics,
		PayloadLimits: PayloadLimits{MaxInlineBytes: 512},
		Engine: &fakeEngine{response: llm.Response{
			OperationKey: "operation-1",
			OperationID:  "operation-id",
			Status:       llm.ResponseStatusCompleted,
			Output: []llm.Item{llm.Message{
				Actor:   llm.ActorModel,
				Content: []llm.Part{llm.TextPart{Text: strings.Repeat("x", 1024)}},
			}},
			Service: llm.ServiceFacts{Requested: llm.ServiceClassStandard, Attempted: llm.ServiceClassStandard},
		}},
	}
	_, _ = activities.Generate(context.Background(), validGeneratePayload())
	assertActivityMetricCounter(t, metrics, "llmtw_activity_failure_total", map[string]string{"origin": "worker"}, 1)
	assertActivityFailureCountZero(t, metrics, "llmtw_activity_failure_total", map[string]string{"origin": "caller"})
}

func validGeneratePayload() GenerateRequest {
	return GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Model: "model-1", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}}}
}

func newActivityTestMetrics(t *testing.T) *observability.Metrics {
	t.Helper()
	metrics, err := observability.NewMetrics(observability.AllowedValues{Statuses: []string{"completed", "failed", "canceled"}, ErrorClasses: []string{"none", "provider_unavailable", "budget_denied", "internal"}, Phases: []string{"total"}})
	if err != nil {
		t.Fatal(err)
	}
	return metrics
}

func assertActivityMetricCounter(t *testing.T, metrics *observability.Metrics, name string, labels map[string]string, want float64) {
	t.Helper()
	got, found := activityMetricValue(t, metrics, name, labels, false)
	if !found || got != want {
		t.Fatalf("%s%v = %v, found=%v; want %v", name, labels, got, found, want)
	}
}

// assertActivityFailureCountZero accepts the zero series that the metrics
// exporter creates up front for every bounded failure origin.
func assertActivityFailureCountZero(t *testing.T, metrics *observability.Metrics, name string, labels map[string]string) {
	t.Helper()
	if got, found := activityMetricValue(t, metrics, name, labels, false); found && got != 0 {
		t.Fatalf("%s%v = %v; want no failures", name, labels, got)
	}
}

func assertActivityMetricHistogramCount(t *testing.T, metrics *observability.Metrics, name string, labels map[string]string, want uint64) {
	t.Helper()
	got, found := activityMetricValue(t, metrics, name, labels, true)
	if !found || uint64(got) != want {
		t.Fatalf("%s%v count = %v, found=%v; want %v", name, labels, got, found, want)
	}
}

func activityMetricValue(t *testing.T, metrics *observability.Metrics, name string, labels map[string]string, histogram bool) (float64, bool) {
	t.Helper()
	families, err := metrics.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			matches := len(metric.Label) == len(labels)
			for _, label := range metric.Label {
				if labels[label.GetName()] != label.GetValue() {
					matches = false
					break
				}
			}
			if matches {
				if histogram {
					return float64(metric.GetHistogram().GetSampleCount()), true
				}
				return metric.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}
