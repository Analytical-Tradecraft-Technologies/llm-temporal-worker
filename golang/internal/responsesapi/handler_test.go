package responsesapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
)

const testKey = "Bearer acme/search/batch-job"

var testIdentity = llm.RequestContext{Tenant: "acme", Project: "search", Actor: "batch-job"}

type fakeExecution struct {
	status   enums.WorkflowExecutionStatus
	response *llm.GenerateResponseV1
	err      error
	// block makes Get wait until the context ends.
	block bool
}

type fakeClient struct {
	mu         sync.Mutex
	executions map[string]*fakeExecution
	started    []client.StartWorkflowOptions
	args       []llm.GenerateRequestV1
	describes  int
	onStart    func(id string) *fakeExecution
}

func newFakeClient() *fakeClient {
	return &fakeClient{executions: map[string]*fakeExecution{}}
}

func (fake *fakeClient) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if workflow != workflows.GenerateWorkflowName || len(args) != 1 {
		return nil, errors.New("unexpected workflow")
	}
	fake.started = append(fake.started, options)
	fake.args = append(fake.args, args[0].(llm.GenerateRequestV1))
	execution, ok := fake.executions[options.ID]
	if !ok {
		execution = &fakeExecution{status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING}
		if fake.onStart != nil {
			execution = fake.onStart(options.ID)
		}
		fake.executions[options.ID] = execution
	}
	return &fakeRun{id: options.ID, execution: execution}, nil
}

func (fake *fakeClient) GetWorkflow(_ context.Context, workflowID string, _ string) client.WorkflowRun {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return &fakeRun{id: workflowID, execution: fake.executions[workflowID]}
}

func (fake *fakeClient) DescribeWorkflowExecution(_ context.Context, workflowID, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.describes++
	execution, ok := fake.executions[workflowID]
	if !ok {
		return nil, serviceerror.NewNotFound("workflow not found")
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Status: execution.status, StartTime: timestamppb.New(time.Unix(1700000000, 0)),
	}}, nil
}

type fakeRun struct {
	id        string
	execution *fakeExecution
}

func (run *fakeRun) GetID() string                  { return run.id }
func (run *fakeRun) GetRunID() string               { return "run" }
func (run *fakeRun) GetFirstExecutionRunID() string { return "run" }
func (run *fakeRun) Get(ctx context.Context, valuePtr any) error {
	if run.execution == nil {
		return errors.New("workflow not found")
	}
	if run.execution.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if run.execution.err != nil {
		return run.execution.err
	}
	*valuePtr.(*llm.GenerateResponseV1) = *run.execution.response
	return nil
}
func (run *fakeRun) GetWithOptions(ctx context.Context, valuePtr any, _ client.WorkflowRunGetOptions) error {
	return run.Get(ctx, valuePtr)
}

type fakeStore struct {
	results map[string]StoredResult
	err     error
	calls   int
}

func (store *fakeStore) LookupGenerate(_ context.Context, tenant, project, key string) (StoredResult, error) {
	store.calls++
	if store.err != nil {
		return StoredResult{}, store.err
	}
	result, ok := store.results[tenant+"/"+project+"/"+key]
	if !ok {
		return StoredResult{}, ErrResultNotFound
	}
	return result, nil
}

func generateResponse(key, text string) *llm.GenerateResponseV1 {
	actual := llm.ServiceClassEconomy
	return &llm.GenerateResponseV1{
		OperationKey: key, OperationID: "op-1", Status: llm.ResponseStatusCompleted,
		Output:     []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: text}}}},
		Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint-" + llm.CheckpointHandle(key), Kind: "generation", Depth: 1},
		Route:      &llm.RouteFacts{RequestedModel: "fast", ResolvedModel: "gpt-test"},
		Service:    &llm.ServiceFacts{Requested: llm.ServiceClassEconomy, Actual: &actual},
		Usage:      &llm.Usage{InputTokens: 10, CacheReadTokens: 5, OutputTokens: 7, ReasoningTokens: 2},
	}
}

func newTestHandler(t *testing.T, fake *fakeClient, store ResultStore) *Handler {
	t.Helper()
	options := Options{Client: fake, TaskQueue: "llm", PayloadLimits: activity.PayloadLimits{MaxInlineBytes: 1 << 20},
		Clock: func() time.Time { return time.Unix(1700000100, 0) }, Random: bytes.NewReader(bytes.Repeat([]byte{0xab}, 64))}
	if store != nil {
		options.Results = store
	}
	handler, err := NewHandler(options)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func serve(handler http.Handler, method, path, key, body string, headers ...string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		request.Header.Set("Authorization", key)
	}
	for index := 0; index+1 < len(headers); index += 2 {
		request.Header.Set(headers[index], headers[index+1])
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func decodeBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %q: %v", recorder.Body.String(), err)
	}
	return body
}

func TestCreateWaitsForTheGenerateWorkflow(t *testing.T) {
	fake := newFakeClient()
	fake.onStart = func(id string) *fakeExecution {
		key := id[strings.LastIndex(id, "/")+1:]
		return &fakeExecution{status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, response: generateResponse(key, "hello")}
	}
	handler := newTestHandler(t, fake, nil)
	recorder := serve(handler, http.MethodPost, "/v1/responses", testKey, `{"model":"fast","input":"hi","service_tier":"flex"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	body := decodeBody(t, recorder)
	if body["status"] != "completed" || body["model"] != "gpt-test" || body["service_tier"] != "flex" {
		t.Fatalf("unexpected response: %v", body)
	}
	id := body["id"].(string)
	if !validResponseID(id) || recorder.Header().Get("X-Response-Id") != id {
		t.Fatalf("response ID %q is not exposed", id)
	}
	output := body["output"].([]any)[0].(map[string]any)
	content := output["content"].([]any)[0].(map[string]any)
	if output["type"] != "message" || content["text"] != "hello" {
		t.Fatalf("unexpected output: %v", output)
	}
	usage := body["usage"].(map[string]any)
	if usage["input_tokens"] != float64(15) || usage["total_tokens"] != float64(22) {
		t.Fatalf("unexpected usage: %v", usage)
	}
	if len(fake.started) != 1 {
		t.Fatalf("started %d workflows", len(fake.started))
	}
	options, args := fake.started[0], fake.args[0]
	if options.TaskQueue != "llm" || options.ID != workflowID(testIdentity, id) || !options.WorkflowExecutionErrorWhenAlreadyStarted {
		t.Fatalf("unexpected start options: %+v", options)
	}
	if args.OperationKey != id || !reflect.DeepEqual(args.Context, testIdentity) || *args.SettingsPatch.ServiceClass.Set != llm.ServiceClassEconomy {
		t.Fatalf("unexpected workflow input: %+v", args)
	}
}

func TestCreateInBackgroundReturnsQueuedImmediately(t *testing.T) {
	fake := newFakeClient()
	fake.onStart = func(string) *fakeExecution {
		return &fakeExecution{status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING, block: true}
	}
	handler := newTestHandler(t, fake, nil)
	recorder := serve(handler, http.MethodPost, "/v1/responses", testKey, `{"model":"fast","input":"hi","background":true}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	body := decodeBody(t, recorder)
	if body["status"] != "queued" || body["background"] != true || body["model"] != "fast" {
		t.Fatalf("unexpected response: %v", body)
	}
}

func TestIdempotencyKeyReattachesToTheSameWorkflow(t *testing.T) {
	fake := newFakeClient()
	handler := newTestHandler(t, fake, nil)
	for attempt := 0; attempt < 2; attempt++ {
		recorder := serve(handler, http.MethodPost, "/v1/responses", testKey, `{"model":"fast","input":"hi","background":true}`, "Idempotency-Key", "job-42")
		if recorder.Code != http.StatusOK || decodeBody(t, recorder)["id"] != "resp_job-42" {
			t.Fatalf("attempt %d: %d %s", attempt, recorder.Code, recorder.Body.String())
		}
	}
	for _, options := range fake.started {
		if options.ID != workflowID(testIdentity, "resp_job-42") ||
			options.WorkflowIDReusePolicy != enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE ||
			options.WorkflowIDConflictPolicy != enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING ||
			options.WorkflowExecutionErrorWhenAlreadyStarted {
			t.Fatalf("unexpected idempotent start options: %+v", options)
		}
	}
	if recorder := serve(handler, http.MethodPost, "/v1/responses", testKey, `{"model":"fast","input":"hi"}`, "Idempotency-Key", "bad key"); recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid key accepted: %d", recorder.Code)
	}
}

func TestCreateMapsWorkflowFailures(t *testing.T) {
	failure := temporal.GetDefaultFailureConverter().FailureToError(temporal.GetDefaultFailureConverter().ErrorToFailure(
		temporal.NewNonRetryableApplicationError("model request failed", "provider_error", nil, workflows.ExecutionFailureDetails{ErrorCode: "budget_denied"})))
	fake := newFakeClient()
	fake.onStart = func(string) *fakeExecution {
		return &fakeExecution{status: enums.WORKFLOW_EXECUTION_STATUS_FAILED, err: failure}
	}
	recorder := serve(newTestHandler(t, fake, nil), http.MethodPost, "/v1/responses", testKey, `{"model":"fast","input":"hi"}`)
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	if code := decodeBody(t, recorder)["error"].(map[string]any)["code"]; code != "insufficient_quota" {
		t.Fatalf("unexpected code %v", code)
	}
}

func TestCreateRejectsRequestsBeforeStarting(t *testing.T) {
	for name, test := range map[string]struct {
		key, body string
		status    int
	}{
		"missing key":      {key: "", body: `{"model":"fast","input":"hi"}`, status: http.StatusUnauthorized},
		"two-part key":     {key: "Bearer acme/search", body: `{"model":"fast","input":"hi"}`, status: http.StatusUnauthorized},
		"stream":           {key: testKey, body: `{"model":"fast","input":"hi","stream":true}`, status: http.StatusBadRequest},
		"unknown field":    {key: testKey, body: `{"model":"fast","input":"hi","frobnicate":1}`, status: http.StatusBadRequest},
		"missing model":    {key: testKey, body: `{"input":"hi"}`, status: http.StatusBadRequest},
		"scale tier":       {key: testKey, body: `{"model":"fast","input":"hi","service_tier":"scale"}`, status: http.StatusBadRequest},
		"hosted tool":      {key: testKey, body: `{"model":"fast","input":"hi","tools":[{"type":"web_search"}]}`, status: http.StatusBadRequest},
		"empty input":      {key: testKey, body: `{"model":"fast","input":""}`, status: http.StatusBadRequest},
		"unknown previous": {key: testKey, body: `{"input":"hi","previous_response_id":"resp_missing"}`, status: http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeClient()
			recorder := serve(newTestHandler(t, fake, nil), http.MethodPost, "/v1/responses", test.key, test.body)
			if recorder.Code != test.status {
				t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
			}
			if len(fake.started) != 0 {
				t.Fatal("a rejected request started a workflow")
			}
		})
	}
}

func TestPreviousResponseResumesItsCheckpoint(t *testing.T) {
	store := &fakeStore{results: map[string]StoredResult{
		"acme/search/resp_first": {State: StoredCompleted, Response: *generateResponse("resp_first", "one")},
	}}
	fake := newFakeClient()
	handler := newTestHandler(t, fake, store)
	recorder := serve(handler, http.MethodPost, "/v1/responses", testKey, `{"input":"again","previous_response_id":"resp_first","background":true}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	args := fake.args[0]
	if args.Parent == nil || *args.Parent != "checkpoint-resp_first" || !args.SettingsPatch.Instructions.Clear || args.SettingsPatch.Model.Set != nil {
		t.Fatalf("unexpected continuation: %+v", args)
	}
	if decodeBody(t, recorder)["previous_response_id"] != "resp_first" {
		t.Fatal("previous_response_id is not echoed")
	}
}

func TestRetrieveReadsTheDurableRecordBeforeTemporal(t *testing.T) {
	store := &fakeStore{results: map[string]StoredResult{
		"acme/search/resp_done":   {State: StoredCompleted, CreatedAt: time.Unix(1700000000, 0), Response: *generateResponse("resp_done", "stored")},
		"acme/search/resp_failed": {State: StoredFailed, Failure: llm.ExecutionResultV1{State: llm.ExecutionFailed, FailureCode: "provider_error", ErrorCode: "provider_rate_limited"}},
	}}
	fake := newFakeClient()
	handler := newTestHandler(t, fake, store)

	body := decodeBody(t, serve(handler, http.MethodGet, "/v1/responses/resp_done", testKey, ""))
	if body["status"] != "completed" || body["created_at"] != float64(1700000000) {
		t.Fatalf("unexpected response: %v", body)
	}
	body = decodeBody(t, serve(handler, http.MethodGet, "/v1/responses/resp_failed", testKey, ""))
	if body["status"] != "failed" || body["error"].(map[string]any)["code"] != "rate_limit_exceeded" {
		t.Fatalf("unexpected response: %v", body)
	}
	if fake.describes != 0 {
		t.Fatalf("terminal records consulted Temporal %d times", fake.describes)
	}
}

func TestRetrieveFallsBackToTemporal(t *testing.T) {
	store := &fakeStore{results: map[string]StoredResult{
		"acme/search/resp_running": {State: StoredRunning},
		"acme/search/resp_crashed": {State: StoredRunning},
	}}
	fake := newFakeClient()
	fake.executions[workflowID(testIdentity, "resp_queued")] = &fakeExecution{status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING}
	fake.executions[workflowID(testIdentity, "resp_running")] = &fakeExecution{status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING}
	fake.executions[workflowID(testIdentity, "resp_crashed")] = &fakeExecution{status: enums.WORKFLOW_EXECUTION_STATUS_TERMINATED}
	fake.executions[workflowID(testIdentity, "resp_memory")] = &fakeExecution{status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, response: generateResponse("resp_memory", "from temporal")}
	handler := newTestHandler(t, fake, store)

	for id, want := range map[string]string{
		"resp_queued": "queued", "resp_running": "in_progress", "resp_crashed": "failed", "resp_memory": "completed",
	} {
		recorder := serve(handler, http.MethodGet, "/v1/responses/"+id, testKey, "")
		if recorder.Code != http.StatusOK || decodeBody(t, recorder)["status"] != want {
			t.Fatalf("%s: %d %s", id, recorder.Code, recorder.Body.String())
		}
	}
	if recorder := serve(handler, http.MethodGet, "/v1/responses/resp_unknown", testKey, ""); recorder.Code != http.StatusNotFound {
		t.Fatalf("unknown ID: %d", recorder.Code)
	}
}

func TestRetrieveIsScopedToTheCallersProject(t *testing.T) {
	fake := newFakeClient()
	fake.executions[workflowID(testIdentity, "resp_private")] = &fakeExecution{status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, response: generateResponse("resp_private", "secret")}
	store := &fakeStore{results: map[string]StoredResult{"acme/search/resp_private": {State: StoredCompleted, Response: *generateResponse("resp_private", "secret")}}}
	handler := newTestHandler(t, fake, store)
	if recorder := serve(handler, http.MethodGet, "/v1/responses/resp_private", "Bearer acme/other/batch-job", ""); recorder.Code != http.StatusNotFound {
		t.Fatalf("another project read the response: %d %s", recorder.Code, recorder.Body.String())
	}
	if recorder := serve(handler, http.MethodGet, "/v1/responses/resp_private", "Bearer acme/search/someone-else", ""); recorder.Code != http.StatusOK {
		t.Fatalf("another actor in the same project could not read the response: %d", recorder.Code)
	}
}

func TestRetrieveUsesTemporalWhenTheStoreFails(t *testing.T) {
	fake := newFakeClient()
	fake.executions[workflowID(testIdentity, "resp_x")] = &fakeExecution{status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING}
	handler := newTestHandler(t, fake, &fakeStore{err: ErrResultStoreUnavailable})
	if body := decodeBody(t, serve(handler, http.MethodGet, "/v1/responses/resp_x", testKey, "")); body["status"] != "queued" {
		t.Fatalf("unexpected response: %v", body)
	}
}

func TestDrainEndsWaitingCreateRequests(t *testing.T) {
	fake := newFakeClient()
	fake.onStart = func(string) *fakeExecution {
		return &fakeExecution{status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING, block: true}
	}
	handler := newTestHandler(t, fake, nil)
	done := make(chan *httptest.ResponseRecorder)
	go func() {
		done <- serve(handler, http.MethodPost, "/v1/responses", testKey, `{"model":"fast","input":"hi"}`)
	}()
	for {
		fake.mu.Lock()
		started := len(fake.started)
		fake.mu.Unlock()
		if started == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	handler.Drain()
	select {
	case recorder := <-done:
		if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("X-Response-Id") == "" {
			t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not end the wait")
	}
}
