package observability_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestQueryAuditLogsMetadataWithoutPayloadsOrRawIdentifiers(t *testing.T) {
	var output bytes.Buffer
	logger := newLogger(t, observability.LogOptions{Output: &output})
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
	logger := newLogger(t, observability.LogOptions{Output: writer})
	if err := logger.QueryAudit(context.Background(), control.QueryAuditRecord{}); err != nil || writer.calls != 1 {
		t.Fatalf("audit err=%v, writes=%d", err, writer.calls)
	}
	logger = newLogger(t, observability.LogOptions{Output: writer, Level: "error"})
	if err := logger.QueryAudit(context.Background(), control.QueryAuditRecord{}); err != nil || writer.calls != 1 {
		t.Fatalf("filtered audit err=%v, writes=%d", err, writer.calls)
	}
}

// A scope denial keeps its Activity type through the attribute allowlist and
// never logs the raw tenant or project (#1535).
func TestScopeAccessDeniedLogsActivityTypeAndHashesOnly(t *testing.T) {
	var output bytes.Buffer
	logger := newLogger(t, observability.LogOptions{Format: "json", Output: &output})
	logger.ScopeAccessDenied(context.Background(), "raw-tenant", "raw-project", "llm.PrepareExecution.v1")
	var line map[string]any
	if err := json.Unmarshal(output.Bytes(), &line); err != nil {
		t.Fatalf("log %q: %v", output.String(), err)
	}
	if line["msg"] != "request scope access decision" || line["level"] != "WARN" || line["outcome"] != "denied" ||
		line["activity_type"] != "llm.PrepareExecution.v1" || line["tenant_hash"] != scopeHash("raw-tenant") || line["project_hash"] != scopeHash("raw-project") {
		t.Fatalf("log line = %v", line)
	}
	if strings.Contains(output.String(), "raw-tenant") || strings.Contains(output.String(), "raw-project") {
		t.Fatalf("log %q leaks a raw identifier", output.String())
	}
}

// A request access event logs only hashes and bounded values. A prepared
// event shares the scope denial's message with outcome allowed; a completed
// event adds the published checkpoint, route and terminal status (#1541).
func TestRequestAccessLogsHashesAndBoundedFieldsOnly(t *testing.T) {
	var output bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Format: "json", Output: &output})
	if err != nil {
		t.Fatal(err)
	}
	record := observability.RequestAccessRecord{
		Phase: observability.RequestAccessPrepared, Kind: "compact", ActivityType: "llm.PrepareExecution.v1",
		Tenant: "raw-tenant", Project: "raw-project", Actor: "raw-actor", OperationKey: "raw-operation-key",
		OperationID: "llmtw_req_raw", ParentCheckpoint: "ctn_v1.raw-parent",
	}
	if err := logger.RequestAccess(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	record.Phase, record.Checkpoint, record.RouteID, record.Status, record.ErrorCode = observability.RequestAccessCompleted, "ctn_v1.raw-child", "route-a", "failed", "invalid_argument"
	if err := logger.RequestAccess(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"raw-tenant", "raw-project", "raw-actor", "raw-operation-key", "llmtw_req_raw", "raw-parent", "raw-child"} {
		if strings.Contains(output.String(), private) {
			t.Fatalf("log %q leaks %q", output.String(), private)
		}
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines = %q", lines)
	}
	common := map[string]any{
		"level": "INFO", "outcome": "allowed", "request_kind": "compact", "activity_type": "llm.PrepareExecution.v1",
		"tenant_hash": scopeHash("raw-tenant"), "project_hash": scopeHash("raw-project"), "actor_hash": scopeHash("raw-actor"),
		"operation_key_hash": scopeHash("raw-operation-key"), "operation_id_hash": scopeHash("llmtw_req_raw"),
		"parent_checkpoint_hash": scopeHash("ctn_v1.raw-parent"),
	}
	for i, want := range []map[string]any{
		{"msg": "request scope access decision"},
		{"msg": "request access completed", "checkpoint_hash": scopeHash("ctn_v1.raw-child"), "route_id": "route-a", "status": "failed", "error_code": "invalid_argument"},
	} {
		for key, value := range common {
			want[key] = value
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &entry); err != nil {
			t.Fatal(err)
		}
		delete(entry, "time")
		if len(entry) != len(want) {
			t.Errorf("line %d fields = %v, want %v", i, entry, want)
		}
		for key, value := range want {
			if entry[key] != value {
				t.Errorf("line %d %s=%v, want %v", i, key, entry[key], value)
			}
		}
	}

	// Empty optional identifiers are omitted, and the level filter applies.
	output.Reset()
	if err := logger.RequestAccess(context.Background(), observability.RequestAccessRecord{Phase: observability.RequestAccessPrepared, Kind: "generate"}); err != nil {
		t.Fatal(err)
	}
	for _, absent := range []string{"actor_hash", "operation_id_hash", "parent_checkpoint_hash", "checkpoint_hash", "route_id", "status", "error_code", "activity_type"} {
		if strings.Contains(output.String(), absent) {
			t.Errorf("empty record logged %s: %s", absent, output.String())
		}
	}
	writer := &failingAuditWriter{}
	quiet, err := observability.NewLogger(observability.LogOptions{Output: writer, Level: "warn"})
	if err != nil {
		t.Fatal(err)
	}
	if err := quiet.RequestAccess(context.Background(), record); err != nil || writer.calls != 0 {
		t.Fatalf("warn-level access audit err=%v, writes=%d", err, writer.calls)
	}
	if err := (*observability.Logger)(nil).RequestAccess(context.Background(), record); err != nil {
		t.Fatal(err)
	}
}

func scopeHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", digest[:8])
}
