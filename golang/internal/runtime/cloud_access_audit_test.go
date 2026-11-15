package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

// accessAuditFields are every key a request access log line may hold.
var accessAuditFields = map[string]bool{
	"time": true, "level": true, "msg": true, "outcome": true, "request_kind": true,
	"tenant_hash": true, "project_hash": true, "actor_hash": true, "operation_key_hash": true,
	"operation_id_hash": true, "parent_checkpoint_hash": true, "checkpoint_hash": true,
	"activity_type": true, "route_id": true, "status": true, "error_code": true,
}

// A durable Generate and a Compact of its checkpoint each report an allowed
// access when prepared and when their terminal result is returned, with
// identifiers only. A failing sink does not fail any step, and the logged
// events hold no request content or raw identifier (#1541).
func TestCloudExecutionRuntimeAuditsRequestAccess(t *testing.T) {
	f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
		b.source.value.Routes.Models["alias"].Routes[0].ID = "audit-route"
	})
	// The fixture budget policy matches actors with the prefix "act".
	f.request.Context.Actor = "actor-private-7f3a"
	f.request.OperationKey = "generate-key-private-91c2"
	// A compaction with work to do runs every step, like a Generate.
	keepNoRecentTurns(&f.request)
	var output bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Format: "json", Output: &output})
	if err != nil {
		t.Fatal(err)
	}
	var records []observability.RequestAccessRecord
	f.options.AccessAudit = func(ctx context.Context, record observability.RequestAccessRecord) error {
		records = append(records, record)
		_ = logger.RequestAccess(ctx, record)
		return errors.New("audit sink unavailable")
	}
	f.restart(t)
	ctx := context.Background()

	prepared := f.prepareGenerate(t, ctx)
	parent := f.acquireGenerateComplete(t, ctx, f.ref(prepared))
	request := f.compactRequest(parent, &llm.CachePolicyV1{})
	request.OperationKey = "compact-key-private-4d1e"
	compacted := f.runCompact(t, ctx, request, false)

	// Prepare and the Generate (or Compact) step each prepare the request;
	// Acquire returns no terminal result; Complete returns the result.
	phases := []observability.RequestAccessPhase{
		observability.RequestAccessPrepared, observability.RequestAccessPrepared, observability.RequestAccessCompleted,
		observability.RequestAccessPrepared, observability.RequestAccessPrepared, observability.RequestAccessCompleted,
	}
	if len(records) != len(phases) {
		t.Fatalf("records = %+v, want %d", records, len(phases))
	}
	for i, record := range records {
		kind, key, id, parentHandle := "generate", f.request.OperationKey, parent.RequestID, ""
		if i >= 3 {
			kind, key, id, parentHandle = "compact", request.OperationKey, compacted.RequestID, string(parent.Generate.Checkpoint.Handle)
		}
		if record.Phase != phases[i] || record.Kind != kind || record.Tenant != "tenant" || record.Project != "project" || record.Actor != "actor-private-7f3a" ||
			record.OperationKey != key || record.OperationID != id || record.ParentCheckpoint != parentHandle {
			t.Fatalf("record %d = %+v", i, record)
		}
		if record.Phase == observability.RequestAccessPrepared && (record.Status != "" || record.Checkpoint != "" || record.RouteID != "") {
			t.Fatalf("prepared record %d has result fields: %+v", i, record)
		}
	}
	if done := records[2]; done.Status != "completed" || done.ErrorCode != "" || done.Checkpoint != string(parent.Generate.Checkpoint.Handle) ||
		done.RouteID != "audit-route" {
		t.Fatalf("generate completion = %+v", done)
	}
	if done := records[5]; done.Status != "completed" || done.Checkpoint != string(compacted.Compact.Checkpoint.Handle) {
		t.Fatalf("compact completion = %+v", done)
	}

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != len(records) {
		t.Fatalf("logged %d lines, want %d: %s", len(lines), len(records), output.String())
	}
	for i, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("line %d %q: %v", i, line, err)
		}
		for key := range entry {
			if !accessAuditFields[key] {
				t.Errorf("line %d has unexpected field %q", i, key)
			}
		}
		want := "request scope access decision"
		if records[i].Phase == observability.RequestAccessCompleted {
			want = "request access completed"
		}
		if entry["msg"] != want || entry["level"] != "INFO" || entry["outcome"] != "allowed" || entry["request_kind"] != records[i].Kind ||
			entry["actor_hash"] == nil || entry["operation_id_hash"] == nil || entry["operation_key_hash"] == nil {
			t.Errorf("line %d = %v", i, entry)
		}
	}
	for _, private := range []string{
		"actor-private-7f3a", "sensitive prompt", "answer", f.request.OperationKey, request.OperationKey,
		parent.RequestID, compacted.RequestID, prepared.Capability, compacted.Capability,
		string(parent.Generate.Checkpoint.Handle), string(compacted.Compact.Checkpoint.Handle),
	} {
		if private != "" && strings.Contains(output.String(), private) {
			t.Errorf("access log leaks %q: %s", private, output.String())
		}
	}
}

// Without a configured sink the events go to the Activity's logger, and a
// denied scope reports no allowed access (the resolver logs denials, #1535).
func TestCloudExecutionRuntimeAccessAuditDefaultsToContextLogger(t *testing.T) {
	f := boundedCloud(t, false)
	var output bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Format: "json", Output: &output})
	if err != nil {
		t.Fatal(err)
	}
	ctx := observability.WithLogger(context.Background(), logger)

	denied := f.request
	denied.Context.Tenant = "other-tenant"
	if _, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &denied}); err == nil {
		t.Fatal("denied scope prepared")
	}
	if output.Len() != 0 {
		t.Fatalf("denied scope logged an allowed access: %s", output.String())
	}

	f.prepareGenerate(t, ctx)
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("log %q: %v", output.String(), err)
	}
	if entry["msg"] != "request scope access decision" || entry["outcome"] != "allowed" || entry["request_kind"] != "generate" || entry["actor_hash"] == nil {
		t.Fatalf("log line = %v", entry)
	}
}

// Only a terminal result is an access completion: a completed response or a
// failure that will not be retried. A failure reports its stable error code.
func TestCloudAccessAuditCompletionStates(t *testing.T) {
	var records []observability.RequestAccessRecord
	r := &CloudExecutionRuntime{options: CloudExecutionOptions{AccessAudit: func(_ context.Context, record observability.RequestAccessRecord) error {
		records = append(records, record)
		return nil
	}}}
	request := llm.GenerateRequestV1{OperationKey: "key", Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}}
	p := PreparedCloudRequest{Record: cloudstate.Record{Request: cloudstate.CreateRequest{ID: "llmtw_req_1", Scope: testRequestScope, Kind: "generate"}}, Generate: &request}
	ctx := withAccessRoute(context.Background())
	for _, quiet := range []struct {
		result llm.ExecutionResultV1
		err    error
	}{
		{result: llm.ExecutionResultV1{State: llm.ExecutionPending}},
		{result: llm.ExecutionResultV1{State: llm.ExecutionProviderCompleted}},
		{result: llm.ExecutionResultV1{State: llm.ExecutionFailed, Retryable: true, ErrorCode: "rate_limited"}},
		{result: llm.ExecutionResultV1{State: llm.ExecutionCompleted}, err: errors.New("step failed")},
	} {
		r.auditCompleted(ctx, request.Context, p, quiet.result, quiet.err)
	}
	if len(records) != 0 {
		t.Fatalf("non-terminal results audited: %+v", records)
	}
	noteAccessRoute(ctx, "route-a")
	r.auditCompleted(ctx, request.Context, p, llm.ExecutionResultV1{State: llm.ExecutionFailed, ErrorCode: "invalid_argument"}, nil)
	if len(records) != 1 || records[0].Phase != observability.RequestAccessCompleted || records[0].Status != "failed" ||
		records[0].ErrorCode != "invalid_argument" || records[0].RouteID != "route-a" || records[0].OperationID != "llmtw_req_1" || records[0].Checkpoint != "" {
		t.Fatalf("terminal failure record = %+v", records)
	}
}

// A permanent provider failure on the initial Generate submission is
// terminal in that step, which never resumes a saved attempt; its access
// event still names the route. A later replay of the stored failure runs no
// provider execution and omits route_id.
func TestCloudAccessAuditRouteOnInitialSubmissionFailure(t *testing.T) {
	f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
		b.source.value.Routes.Models["alias"].Routes[0].ID = "audit-route"
	})
	var records []observability.RequestAccessRecord
	f.options.AccessAudit = func(_ context.Context, record observability.RequestAccessRecord) error {
		records = append(records, record)
		return nil
	}
	f.restart(t)
	f.adapter.invoke = invokeFailing(&f.submits, provider.NewError(provider.CodePermissionDenied, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNever, "private"))
	ctx := context.Background()
	f.step(t, ctx, f.request, llm.ExecutionFailed)
	if len(records) != 2 || records[1].Phase != observability.RequestAccessCompleted || records[1].Status != "failed" ||
		records[1].ErrorCode != string(provider.CodePermissionDenied) || records[1].RouteID != "audit-route" {
		t.Fatalf("submission failure records = %+v", records)
	}
	f.prepare(t, ctx, llm.PrepareExecutionV1{Generate: &f.request}, llm.ExecutionFailed)
	if len(records) != 4 || records[3].Phase != observability.RequestAccessCompleted || records[3].Status != "failed" || records[3].RouteID != "" {
		t.Fatalf("replayed failure records = %+v", records)
	}
}
