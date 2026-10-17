package activity

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/langfuse"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

type langfuseRuntimeStub struct {
	v1RuntimeStub
	err error
}

func (s *langfuseRuntimeStub) ExportLangfuseV1(context.Context, llm.ExecutionReferenceV1) error {
	return s.err
}

func TestExportLangfuseRetriesOnlyDeliveryFailures(t *testing.T) {
	payload, err := json.Marshal(llm.ExecutionReferenceV1{RequestID: executionTestID, Context: validGenerateV1Request().Context})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		err          error
		kind         string
		nonRetryable bool
	}{
		{langfuse.ErrExport, "langfuse_export_failed", false},
		{langfuse.ErrRejected, "langfuse_export_rejected", true},
		{langfuse.ErrUnavailable, "langfuse_export_unavailable", true},
		{langfuse.ErrSaturated, "langfuse_export_saturated", false},
	} {
		activities := &Activities{V1Runtime: &langfuseRuntimeStub{err: tc.err}}
		var suite testsuite.WorkflowTestSuite
		environment := suite.NewTestActivityEnvironment()
		environment.RegisterActivityWithOptions(activities.exportLangfuseTemporal, sdkactivity.RegisterOptions{Name: ExportLangfuseActivityName})
		_, err := environment.ExecuteActivity(ExportLangfuseActivityName, rawJSONArgument(t, payload))
		var applicationErr *temporal.ApplicationError
		if !errors.As(err, &applicationErr) || applicationErr.Type() != tc.kind || applicationErr.NonRetryable() != tc.nonRetryable {
			t.Fatalf("%v: got %v, want %s nonRetryable=%v", tc.err, err, tc.kind, tc.nonRetryable)
		}
	}
}

func TestExportLangfuseFailureIsVisibleOnMetrics(t *testing.T) {
	payload, err := json.Marshal(llm.ExecutionReferenceV1{RequestID: executionTestID, Context: validGenerateV1Request().Context})
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := observability.NewMetrics(observability.AllowedValues{})
	if err != nil {
		t.Fatal(err)
	}
	scrape := func() string {
		recorder := httptest.NewRecorder()
		metrics.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
		body, err := io.ReadAll(recorder.Result().Body)
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	// A healthy worker already exports every reason at zero, so alerts can
	// match the series before the first failure.
	if body := scrape(); !strings.Contains(body, `llmtw_langfuse_export_failures_total{reason="rejected"} 0`) {
		t.Fatalf("missing zero series:\n%s", body)
	}
	for _, exportErr := range []error{nil, langfuse.ErrRejected, langfuse.ErrUnavailable, langfuse.ErrSaturated, langfuse.ErrExport} {
		activities := &Activities{Metrics: metrics, V1Runtime: &langfuseRuntimeStub{err: exportErr}}
		var suite testsuite.WorkflowTestSuite
		environment := suite.NewTestActivityEnvironment()
		environment.RegisterActivityWithOptions(activities.exportLangfuseTemporal, sdkactivity.RegisterOptions{Name: ExportLangfuseActivityName})
		_, _ = environment.ExecuteActivity(ExportLangfuseActivityName, rawJSONArgument(t, payload))
	}
	body := scrape()
	for _, reason := range []string{"rejected", "unavailable", "saturated", "failed"} {
		if want := `llmtw_langfuse_export_failures_total{reason="` + reason + `"} 1`; !strings.Contains(body, want) {
			t.Fatalf("missing %s:\n%s", want, body)
		}
	}
}

// A saturated export backs off with jitter that grows per attempt and stays
// within the export workflow's retry window.
func TestExportLangfuseSaturatedRetryDelay(t *testing.T) {
	for _, tc := range []struct {
		attempt  int32
		min, max time.Duration
	}{{1, 500 * time.Millisecond, time.Second}, {2, time.Second, 2 * time.Second}, {4, 4 * time.Second, 8 * time.Second}, {9, 4 * time.Second, 8 * time.Second}, {0, 4 * time.Second, 8 * time.Second}} {
		for i := 0; i < 100; i++ {
			if delay := saturatedRetryDelay(tc.attempt); delay < tc.min || delay > tc.max {
				t.Fatalf("attempt %d: delay %v outside [%v, %v]", tc.attempt, delay, tc.min, tc.max)
			}
		}
	}
}
