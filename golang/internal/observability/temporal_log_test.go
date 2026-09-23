package observability_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	temporallog "go.temporal.io/sdk/log"
)

func TestTemporalLoggerUsesSlogAndFiltersSDKFields(t *testing.T) {
	var output bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Output: &output, Level: "debug"})
	if err != nil {
		t.Fatal(err)
	}
	adapter := logger.TemporalLogger()
	bound := temporallog.With(adapter, "TaskQueue", "llm", "WorkflowID", "workflow-1", "payload", "private-content")
	bound.Info("activity completed", "RunID", "run-1", "ActivityID", "activity-1", "Error", errors.New("private-content"), "ignored-odd-key")
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]any{"level": "INFO", "task_queue": "llm", "temporal_workflow_id": "workflow-1", "temporal_run_id": "run-1", "activity_id": "activity-1", "error_code": "internal"} {
		if record[key] != want {
			t.Errorf("%s=%v, want %v", key, record[key], want)
		}
	}
	if strings.Contains(output.String(), "private-content") {
		t.Fatal("raw SDK content leaked")
	}
	output.Reset()
	adapter.Debug("base event", "WorkflowID", map[string]string{"secret": "private-content"}, 42, "private-content")
	if strings.Contains(output.String(), "workflow-1") || strings.Contains(output.String(), "private-content") {
		t.Fatalf("bound context or arbitrary value leaked: %s", output.String())
	}
	if !strings.Contains(output.String(), `"level":"DEBUG"`) {
		t.Fatalf("debug log missing: %s", output.String())
	}
	output.Reset()
	failure := provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "private-content")
	bound.Error("activity failed", "Error", failure)
	if !strings.Contains(output.String(), `"error_code":"provider_unavailable"`) || strings.Contains(output.String(), "private-content") {
		t.Fatalf("classified error lost or leaked: %s", output.String())
	}
	output.Reset()
	bound.Warn("prompt: private-content")
	if strings.Contains(output.String(), "private-content") || !strings.Contains(output.String(), `"msg":"event"`) {
		t.Fatalf("message filtering bypassed: %s", output.String())
	}
}

func TestTemporalLoggerRespectsConfiguredLevelAndTextFormat(t *testing.T) {
	var output bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Output: &output, Level: "warn", Format: "text"})
	if err != nil {
		t.Fatal(err)
	}
	adapter := logger.TemporalLogger()
	adapter.Debug("filtered debug")
	adapter.Info("filtered info")
	if output.Len() != 0 {
		t.Fatal("SDK ignored log level")
	}
	adapter.Warn("worker warning", "TaskQueue", "llm")
	if !strings.Contains(output.String(), "level=WARN") || !strings.Contains(output.String(), "task_queue=llm") {
		t.Fatalf("SDK ignored text format: %s", output.String())
	}
}
