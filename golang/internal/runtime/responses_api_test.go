package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/responsesapi"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

type testOperationLookup struct {
	record cloudstate.Record
	err    error
	got    cloudstate.Operation
}

func (lookup *testOperationLookup) LookupOperation(_ context.Context, operation cloudstate.Operation) (cloudstate.Record, error) {
	lookup.got = operation
	return lookup.record, lookup.err
}

func TestLookupGenerateResultDecodesTheOperationRecord(t *testing.T) {
	created := time.Unix(1700000000, 0).UTC()
	response := llm.GenerateResponseV1{
		OperationKey: "resp_a", OperationID: "op", Status: llm.ResponseStatusCompleted,
		Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint", Kind: "generation", Depth: 1},
		Cache:      llm.CacheDispositionV1{Disposition: "disabled"},
		Cost:       llm.CostV1{Status: "unknown", UnknownReason: "provider_did_not_report_cost"},
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	completed, _ := json.Marshal(cloudRuntimeResult{Version: 1, Response: encoded})
	failed := json.RawMessage(`{"execution_failure":{"request_id":"llmtw_req_5f0c6b2e-8a1d-4c3b-9e7f-2a4d6c8b0e1f","kind":"generate","state":"failed","failure_code":"provider_error","error_code":"budget_denied"}}`)
	request := cloudstate.CreateRequest{CreatedAt: created}

	lookup := &testOperationLookup{record: cloudstate.Record{Request: request, Status: cloudstate.StatusCompleted, Progress: completed}}
	result, err := lookupGenerateResult(context.Background(), lookup, "acme", "search", "resp_a")
	if err != nil || result.State != responsesapi.StoredCompleted || result.Response.Checkpoint.Handle != "checkpoint" || !result.CreatedAt.Equal(created) {
		t.Fatalf("completed record = %+v, %v", result, err)
	}
	if lookup.got.Scope != (cloudstate.Scope{Tenant: "acme", Project: "search"}) || lookup.got.Kind != "generate" || lookup.got.Key != "resp_a" {
		t.Fatalf("lookup used %+v", lookup.got)
	}
	if _, err := lookupGenerateResult(context.Background(), lookup, "acme", "search", "resp_other"); !errors.Is(err, responsesapi.ErrResultStoreUnavailable) {
		t.Fatalf("a record for another key was accepted: %v", err)
	}

	lookup.record = cloudstate.Record{Request: request, Status: cloudstate.StatusFailed, Progress: failed}
	result, err = lookupGenerateResult(context.Background(), lookup, "acme", "search", "resp_a")
	if err != nil || result.State != responsesapi.StoredFailed || result.Failure.ErrorCode != "budget_denied" {
		t.Fatalf("failed record = %+v, %v", result, err)
	}

	for _, status := range []cloudstate.Status{cloudstate.StatusRunning, cloudstate.StatusProviderPending, cloudstate.StatusOutcomeUnknown} {
		lookup.record = cloudstate.Record{Request: request, Status: status, Progress: json.RawMessage(`{"version":1}`)}
		if result, err = lookupGenerateResult(context.Background(), lookup, "acme", "search", "resp_a"); err != nil || result.State != responsesapi.StoredRunning {
			t.Fatalf("%s record = %+v, %v", status, result, err)
		}
	}

	lookup.err = contracts.ErrNotFound
	if _, err := lookupGenerateResult(context.Background(), lookup, "acme", "search", "resp_a"); !errors.Is(err, responsesapi.ErrResultNotFound) {
		t.Fatalf("missing record = %v", err)
	}
	lookup.err = errors.New("throttled")
	if _, err := lookupGenerateResult(context.Background(), lookup, "acme", "search", "resp_a"); !errors.Is(err, responsesapi.ErrResultStoreUnavailable) {
		t.Fatalf("read failure = %v", err)
	}
}

func TestRuntimeServesTheResponsesAPIOnlyWhenConfigured(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	runtime, err := New(context.Background(), runtimeConfig(t), testRuntimeOptions(t, controller, &closed))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.ResponsesServer != nil {
		t.Fatal("the responses API listener exists without configuration")
	}
	_ = runtime.Shutdown(context.Background())

	configuration := strings.Replace(string(runtimeConfig(t)), "metrics_address: 127.0.0.1:0", "metrics_address: 127.0.0.1:0\n  responses_api_listen_address: 127.0.0.1:0", 1)
	closed.Store(false)
	runtime, err = New(context.Background(), []byte(configuration), testRuntimeOptions(t, &testWorker{}, &closed))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox does not permit loopback listeners: %v", err)
		}
		t.Fatal(err)
	}
	address := runtime.ResponsesServer.Addr()
	// An unauthenticated poll proves the listener routes to the API handler.
	if got := runtimeHealthStatus(t, address, "/v1/responses/resp_x"); got != http.StatusUnauthorized {
		t.Fatalf("GET without an API key = %d, want 401", got)
	}
	if got := runtimeHealthStatus(t, address, "/health/live"); got != http.StatusNotFound {
		t.Fatalf("the API listener serves probes: %d", got)
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := http.Get("http://" + address + "/v1/responses/resp_x"); err == nil {
		t.Fatal("the API listener is still accepting connections after shutdown")
	}
}
