package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/responsesapi"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
	goredis "github.com/redis/go-redis/v9"
)

type testOperationLookup struct {
	record cloudstate.Record
	err    error
	got    cloudstate.Operation
	calls  int
}

func (lookup *testOperationLookup) LookupOperation(_ context.Context, operation cloudstate.Operation) (cloudstate.Record, error) {
	lookup.got = operation
	lookup.calls++
	return lookup.record, lookup.err
}

type testResponseResultIndex struct {
	entries map[string]redisstore.ResponseResult
	getErr  error
	putErr  error
	puts    int
}

func (index *testResponseResultIndex) name(tenant, project, key string) string {
	return tenant + "/" + project + "/" + key
}

func (index *testResponseResultIndex) PutResponseResult(_ context.Context, tenant, project, key string, result redisstore.ResponseResult) error {
	index.puts++
	if index.putErr != nil {
		return index.putErr
	}
	if index.entries == nil {
		index.entries = map[string]redisstore.ResponseResult{}
	}
	index.entries[index.name(tenant, project, key)] = result
	return nil
}

func (index *testResponseResultIndex) GetResponseResult(_ context.Context, tenant, project, key string) (redisstore.ResponseResult, bool, error) {
	if index.getErr != nil {
		return redisstore.ResponseResult{}, false, index.getErr
	}
	result, found := index.entries[index.name(tenant, project, key)]
	return result, found, nil
}

func testCompletedRecord(t *testing.T, key string, created time.Time) cloudstate.Record {
	t.Helper()
	encoded, err := json.Marshal(testGenerateResponse(key))
	if err != nil {
		t.Fatal(err)
	}
	completed, _ := json.Marshal(cloudRuntimeResult{Version: 1, Response: encoded})
	return cloudstate.Record{Request: cloudstate.CreateRequest{CreatedAt: created}, Status: cloudstate.StatusCompleted, Progress: completed}
}

func testGenerateResponse(key string) llm.GenerateResponseV1 {
	return llm.GenerateResponseV1{
		OperationKey: key, OperationID: "op", Status: llm.ResponseStatusCompleted,
		Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint", Kind: "generation", Depth: 1},
		Cache:      llm.CacheDispositionV1{Disposition: "disabled"},
		Cost:       llm.CostV1{Status: "unknown", UnknownReason: "provider_did_not_report_cost"},
	}
}

func TestIndexedLookupAnswersRecentlyFinishedResultsFromRedisAlone(t *testing.T) {
	created := time.Unix(1700000000, 0).UTC()
	response := testGenerateResponse("resp_a")
	failure := llm.ExecutionResultV1{RequestID: "llmtw_req_5f0c6b2e-8a1d-4c3b-9e7f-2a4d6c8b0e1f", Kind: "generate", State: llm.ExecutionFailed, FailureCode: "provider_error", ErrorCode: "budget_denied"}
	index := &testResponseResultIndex{entries: map[string]redisstore.ResponseResult{
		"acme/search/resp_a": {CreatedAt: created, Response: &response},
		"acme/search/resp_f": {CreatedAt: created, Failure: &failure},
	}}
	lookup := &testOperationLookup{err: errors.New("durable storage must not be read")}

	result, err := lookupIndexedGenerateResult(context.Background(), index, lookup, "acme", "search", "resp_a")
	if err != nil || result.State != responsesapi.StoredCompleted || result.Response.Checkpoint.Handle != "checkpoint" || !result.CreatedAt.Equal(created) {
		t.Fatalf("indexed completion = %+v, %v", result, err)
	}
	result, err = lookupIndexedGenerateResult(context.Background(), index, lookup, "acme", "search", "resp_f")
	if err != nil || result.State != responsesapi.StoredFailed || result.Failure.ErrorCode != "budget_denied" {
		t.Fatalf("indexed failure = %+v, %v", result, err)
	}
	if lookup.calls != 0 {
		t.Fatalf("durable storage was read %d times for indexed results", lookup.calls)
	}
}

func TestIndexedLookupFallsBackToDurableStorageAndBackfillsTerminalResults(t *testing.T) {
	created := time.Unix(1700000000, 0).UTC()
	index := &testResponseResultIndex{}
	lookup := &testOperationLookup{record: testCompletedRecord(t, "resp_a", created)}

	result, err := lookupIndexedGenerateResult(context.Background(), index, lookup, "acme", "search", "resp_a")
	if err != nil || result.State != responsesapi.StoredCompleted || lookup.calls != 1 {
		t.Fatalf("durable fallback = %+v, %v after %d reads", result, err, lookup.calls)
	}
	// The durable answer is copied so the next poll needs only Redis.
	if _, err = lookupIndexedGenerateResult(context.Background(), index, lookup, "acme", "search", "resp_a"); err != nil || lookup.calls != 1 || index.puts != 1 {
		t.Fatalf("second poll = %v after %d durable reads and %d copies", err, lookup.calls, index.puts)
	}

	// A running record is never copied: only a terminal result may answer
	// without the durable record or Temporal.
	lookup.record = cloudstate.Record{Request: cloudstate.CreateRequest{CreatedAt: created}, Status: cloudstate.StatusRunning, Progress: json.RawMessage(`{"version":1}`)}
	if result, err = lookupIndexedGenerateResult(context.Background(), index, lookup, "acme", "search", "resp_running"); err != nil || result.State != responsesapi.StoredRunning || index.puts != 1 {
		t.Fatalf("running record = %+v, %v with %d copies", result, err, index.puts)
	}

	// Redis being unreadable or unwritable degrades to the durable answer.
	index.getErr, index.putErr = errors.New("redis down"), errors.New("redis down")
	lookup.record = testCompletedRecord(t, "resp_b", created)
	if result, err = lookupIndexedGenerateResult(context.Background(), index, lookup, "acme", "search", "resp_b"); err != nil || result.State != responsesapi.StoredCompleted {
		t.Fatalf("Redis failure = %+v, %v", result, err)
	}

	// Without a durable repository a Redis miss is not an answer.
	if _, err = lookupIndexedGenerateResult(context.Background(), &testResponseResultIndex{}, nil, "acme", "search", "resp_a"); !errors.Is(err, responsesapi.ErrResultStoreUnavailable) {
		t.Fatalf("miss without durable storage = %v", err)
	}
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

func TestCloudExecutionRuntimeIndexesTerminalResultsForResponsesAPIPolls(t *testing.T) {
	ctx := context.Background()
	f := boundedCloud(t, false)
	index := &testResponseResultIndex{}
	f.cap.ResponseResults = index
	f.restart(t)
	result := f.finish(t)
	name := index.name("tenant", "project", f.request.OperationKey)
	entry, found := index.entries[name]
	if !found || entry.Response == nil || entry.Response.Checkpoint.Handle != result.Generate.Checkpoint.Handle || entry.CreatedAt.IsZero() {
		t.Fatalf("completed result was not indexed: %+v (found %v)", entry, found)
	}
	// Budget and provider steps are not terminal and write nothing.
	if index.puts != 1 {
		t.Fatalf("index written %d times for one completion", index.puts)
	}
	lookup := &testOperationLookup{err: errors.New("durable storage must not be read")}
	stored, err := lookupIndexedGenerateResult(ctx, index, lookup, "tenant", "project", f.request.OperationKey)
	if err != nil || stored.State != responsesapi.StoredCompleted || stored.Response.Checkpoint.Handle != result.Generate.Checkpoint.Handle || lookup.calls != 0 {
		t.Fatalf("poll after completion = %+v, %v after %d durable reads", stored, err, lookup.calls)
	}

	// Redis is a copy: failing to write it never fails the committed result.
	down := boundedCloud(t, false)
	down.cap.ResponseResults = &testResponseResultIndex{putErr: errors.New("redis down")}
	down.restart(t)
	down.finish(t)

	failed := boundedCloud(t, false)
	failedIndex := &testResponseResultIndex{}
	failed.cap.ResponseResults = failedIndex
	failed.restart(t)
	failed.adapter.invoke = func(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
		return provider.Result{}, provider.NewError(provider.CodeCanceled, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "rejected")
	}
	v, err := failed.runtime.GenerateStepV1(ctx, failed.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	entry, found = failedIndex.entries[failedIndex.name("tenant", "project", failed.request.OperationKey)]
	if !found || entry.Failure == nil || entry.Failure.FailureCode != v.FailureCode || entry.Failure.Retryable {
		t.Fatalf("permanent failure was not indexed: %+v (found %v)", entry, found)
	}
}

func TestResponseResultIndexOnlyServesConfiguredResponsesAPI(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	keys, err := redisstore.NewKeyOptions("worker", "admission", bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	var value config.Config
	repository := &recordingCloudRequests{}
	if index, err := responseResultIndex(value, repository, client, keys); err != nil || index != nil {
		t.Fatalf("index without the Responses API = %v, %v", index, err)
	}
	value.Server.ResponsesAPIListenAddress = "127.0.0.1:8000"
	if index, err := responseResultIndex(value, nil, client, keys); err != nil || index != nil {
		t.Fatalf("index without cloud request storage = %v, %v", index, err)
	}
	if index, err := responseResultIndex(value, repository, client, keys); err != nil || index == nil {
		t.Fatalf("index with the Responses API = %v, %v", index, err)
	}
}
