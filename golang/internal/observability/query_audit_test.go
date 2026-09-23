package observability_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestQueryAuditLogsMetadataWithoutPayloadsOrRawIdentifiers(t *testing.T) {
	var output bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Output: &output})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 9, 23, 12, 0, 0, 0, time.FixedZone("Sydney", 10*60*60))
	cost := "0"
	record := control.QueryAuditRecord{
		Tenant: "private-tenant", Project: "private-project", OperationKey: "private-operation-key",
		APIVersion: llm.QueryAPIVersion, Kind: llm.QueryBudgetStatus,
		RequestJSON: []byte(`{"prompt":"private-request"}`), ResponseJSON: []byte(`{"secret":"private-response"}`),
		RequestFingerprint: sha256.Sum256([]byte("request")), ResponseDigest: sha256.Sum256([]byte("response")),
		Source: "redis_budget_generation", ActualCostUSD: &cost, CostStatus: "exact", CostMethod: "control_query_zero",
		StartedAt: started, CompletedAt: started.Add(50 * time.Millisecond),
	}
	if err := logger.QueryAudit(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{record.Tenant, record.Project, record.OperationKey, "private-request", "private-response", "RequestJSON", "ResponseJSON"} {
		if strings.Contains(output.String(), private) {
			t.Fatalf("private data %q in audit log", private)
		}
	}
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]any{"msg": "control query completed", "query_kind": "budget_status", "actual_cost_usd": "0", "duration_ms": float64(50), "started_at": "2026-09-23T02:00:00Z"} {
		if entry[key] != want {
			t.Errorf("%s=%v, want %v", key, entry[key], want)
		}
	}
	for _, key := range []string{"tenant_hash", "project_hash", "operation_key_hash", "request_fingerprint", "response_digest"} {
		if entry[key] == nil || entry[key] == "" {
			t.Errorf("missing %s", key)
		}
	}
	output.Reset()
	record.ActualCostUSD = nil
	record.CostStatus, record.CostMethod, record.CostUnknownReasonCode = "unknown", "", "usage_missing"
	if err := logger.QueryAudit(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "actual_cost_usd") || !strings.Contains(output.String(), `"cost_status":"unknown"`) {
		t.Fatalf("unknown cost became zero: %s", output.String())
	}
}

type failingAuditWriter struct{ calls int }

func (writer *failingAuditWriter) Write([]byte) (int, error) {
	writer.calls++
	return 0, errors.New("log destination unavailable")
}

func TestQueryAuditIgnoresLogWriteFailureAndRespectsLevel(t *testing.T) {
	writer := &failingAuditWriter{}
	logger, err := observability.NewLogger(observability.LogOptions{Output: writer})
	if err != nil {
		t.Fatal(err)
	}
	if err := logger.QueryAudit(context.Background(), control.QueryAuditRecord{}); err != nil || writer.calls != 1 {
		t.Fatalf("audit err=%v, writes=%d", err, writer.calls)
	}
	logger, err = observability.NewLogger(observability.LogOptions{Output: writer, Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	if err := logger.QueryAudit(context.Background(), control.QueryAuditRecord{}); err != nil || writer.calls != 1 {
		t.Fatalf("filtered audit err=%v, writes=%d", err, writer.calls)
	}
}
