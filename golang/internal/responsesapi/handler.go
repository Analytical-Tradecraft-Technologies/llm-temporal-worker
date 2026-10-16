// Package responsesapi serves a subset of the OpenAI Responses API as a thin
// wrapper over the public Generate workflow. A create request starts the
// workflow and either waits for it or returns immediately (background); a
// poll reads the durable operation record first and asks Temporal only when
// that record cannot answer.
package responsesapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
)

const (
	ResponsesPath    = "/v1/responses"
	responsePath     = "/v1/responses/{id}"
	startTimeout     = 30 * time.Second
	lookupTimeout    = 10 * time.Second
	defaultBodyLimit = 4 << 20
)

// WorkflowClient is the subset of client.Client the API uses.
type WorkflowClient interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
	GetWorkflow(ctx context.Context, workflowID string, runID string) client.WorkflowRun
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
}

// StoredState is the durable record's view of an operation.
type StoredState uint8

const (
	// StoredRunning covers every non-terminal record state, including an
	// unknown provider outcome that the workflow may still resolve.
	StoredRunning StoredState = iota
	StoredCompleted
	StoredFailed
)

type StoredResult struct {
	State     StoredState
	CreatedAt time.Time
	Response  llm.GenerateResponseV1
	Failure   llm.ExecutionResultV1
}

var (
	// ErrResultNotFound means the store has no record for the operation yet.
	ErrResultNotFound = errors.New("response result not found")
	// ErrResultStoreUnavailable means the store is not configured or cannot be
	// read; the API then answers from Temporal alone.
	ErrResultStoreUnavailable = errors.New("response result store unavailable")
)

// ResultStore reads Generate operation records by caller scope and key.
type ResultStore interface {
	LookupGenerate(ctx context.Context, tenant, project, operationKey string) (StoredResult, error)
}

type Options struct {
	Client    WorkflowClient
	TaskQueue string
	// Results is optional. Without it every poll goes to Temporal.
	Results       ResultStore
	PayloadLimits activity.PayloadLimits
	MaxBodyBytes  int64
	Clock         func() time.Time
	Random        io.Reader
}

type Handler struct {
	options  Options
	mux      *http.ServeMux
	draining chan struct{}
	drain    sync.Once
}

func NewHandler(options Options) (*Handler, error) {
	if options.Client == nil {
		return nil, errors.New("responses API requires a Temporal client")
	}
	if options.TaskQueue == "" {
		return nil, errors.New("responses API requires a task queue")
	}
	if options.MaxBodyBytes <= 0 {
		options.MaxBodyBytes = defaultBodyLimit
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.Random == nil {
		options.Random = defaultRandom
	}
	handler := &Handler{options: options, mux: http.NewServeMux(), draining: make(chan struct{})}
	handler.mux.HandleFunc("POST "+ResponsesPath, handler.create)
	handler.mux.HandleFunc("GET "+responsePath, handler.retrieve)
	return handler, nil
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	handler.mux.ServeHTTP(writer, request)
}

// Drain ends every in-progress wait so the listener can shut down. Waiting
// callers get 503 with the response ID; their workflows keep running and the
// result can be polled from any worker.
func (handler *Handler) Drain() {
	handler.drain.Do(func() { close(handler.draining) })
}

func (handler *Handler) create(writer http.ResponseWriter, request *http.Request) {
	identity, ok := identityFromRequest(request)
	if !ok {
		writeError(writer, unauthenticated())
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, handler.options.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(writer, requestTooLarge())
			return
		}
		writeError(writer, invalidRequest("the request body could not be read"))
		return
	}
	decoded, apiErr := decodeCreateRequest(body)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	responseID, idempotent, apiErr := handler.responseID(request)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	mapped, apiErr := mapCreateRequest(decoded, identity, responseID)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if mapped.PreviousResponseID != "" {
		parent, apiErr := handler.parentCheckpoint(request.Context(), identity, mapped.PreviousResponseID)
		if apiErr != nil {
			writeError(writer, apiErr)
			return
		}
		mapped.Generate.Parent = &parent
	}
	if _, err := mapped.Generate.MarshalJSON(); err != nil {
		writeError(writer, invalidRequest("the request is not valid: "+err.Error()))
		return
	}
	if !activity.FitsInlinePayload(handler.options.PayloadLimits, mapped.Generate) {
		writeError(writer, requestTooLarge())
		return
	}
	meta := responseMeta{ID: responseID, CreatedAt: handler.options.Clock(), Background: mapped.Background, PreviousResponseID: mapped.PreviousResponseID}
	if mapped.Generate.SettingsPatch.Model.Set != nil {
		meta.Model = *mapped.Generate.SettingsPatch.Model.Set
	}
	// The ID is exposed before the start: a start that times out may still
	// have created the workflow, and the caller must be able to poll it rather
	// than retry under a new ID and pay twice.
	writer.Header().Set("X-Response-Id", responseID)
	run, apiErr := handler.start(request.Context(), identity, responseID, idempotent, mapped.Generate)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if mapped.Background {
		writeJSON(writer, http.StatusOK, newResponseObject(meta, "queued"))
		return
	}
	handler.wait(writer, request, run, meta)
}

func requestTooLarge() *apiError {
	return &apiError{Status: http.StatusRequestEntityTooLarge, Type: "invalid_request_error", Code: "request_too_large", Message: "the request exceeds the worker's payload limit"}
}

func (handler *Handler) responseID(request *http.Request) (string, bool, *apiError) {
	if key := request.Header.Get("Idempotency-Key"); key != "" {
		id, ok := idempotentResponseID(key)
		if !ok {
			return "", false, invalidRequest("Idempotency-Key must be 1 to 200 characters of letters, digits, '-', '_', '.' or ':'")
		}
		return id, true, nil
	}
	id, err := newResponseID(handler.options.Random)
	if err != nil {
		return "", false, serverError("a response ID could not be generated")
	}
	return id, false, nil
}

// start begins the Generate workflow. With an Idempotency-Key a repeated
// request attaches to the existing workflow, running or closed, instead of
// starting new paid work. The repeat's body is not compared with the original.
// The start outlives a disconnecting caller so it is never left ambiguous.
func (handler *Handler) start(parent context.Context, identity llm.RequestContext, responseID string, idempotent bool, generate llm.GenerateRequestV1) (client.WorkflowRun, *apiError) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), startTimeout)
	defer cancel()
	options := client.StartWorkflowOptions{ID: workflowID(identity, responseID), TaskQueue: handler.options.TaskQueue}
	if idempotent {
		options.WorkflowIDReusePolicy = enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		options.WorkflowIDConflictPolicy = enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	} else {
		options.WorkflowExecutionErrorWhenAlreadyStarted = true
	}
	run, err := handler.options.Client.ExecuteWorkflow(ctx, options, workflows.GenerateWorkflowName, generate)
	if err != nil {
		return nil, unavailable("the request may not have been submitted; poll GET /v1/responses/" + responseID + " before retrying")
	}
	return run, nil
}

func (handler *Handler) wait(writer http.ResponseWriter, request *http.Request, run client.WorkflowRun, meta responseMeta) {
	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	go func() {
		select {
		case <-handler.draining:
			cancel()
		case <-ctx.Done():
		}
	}()
	var response llm.GenerateResponseV1
	err := run.Get(ctx, &response)
	if request.Context().Err() != nil {
		// The caller disconnected; the workflow continues and can be polled.
		return
	}
	if ctx.Err() != nil {
		writer.Header().Set("Retry-After", "1")
		writeError(writer, unavailable("the worker is shutting down; poll GET /v1/responses/"+meta.ID+" for the result"))
		return
	}
	if err != nil {
		writeError(writer, workflowFailure(err))
		return
	}
	writeJSON(writer, http.StatusOK, completedResponse(meta, response))
}

func (handler *Handler) retrieve(writer http.ResponseWriter, request *http.Request) {
	identity, ok := identityFromRequest(request)
	if !ok {
		writeError(writer, unauthenticated())
		return
	}
	id := request.PathValue("id")
	if !validResponseID(id) {
		writeError(writer, notFound(id))
		return
	}
	object, apiErr := handler.lookup(request.Context(), identity, id)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	writeJSON(writer, http.StatusOK, object)
}

// lookup answers from the durable record when it holds a terminal result and
// otherwise from the workflow: a missing record may be a workflow that has not
// reached its first Activity, and a running record may belong to a workflow
// that failed without writing a result.
func (handler *Handler) lookup(parent context.Context, identity llm.RequestContext, id string) (responseObject, *apiError) {
	ctx, cancel := context.WithTimeout(parent, lookupTimeout)
	defer cancel()
	meta := responseMeta{ID: id}
	recorded := false
	if handler.options.Results != nil {
		stored, err := handler.options.Results.LookupGenerate(ctx, identity.Tenant, identity.Project, id)
		switch {
		case err == nil:
			meta.CreatedAt = stored.CreatedAt
			switch stored.State {
			case StoredCompleted:
				return completedResponse(meta, stored.Response), nil
			case StoredFailed:
				return failedResponse(meta, storedFailure(stored.Failure)), nil
			}
			recorded = true
		case errors.Is(err, ErrResultNotFound), errors.Is(err, ErrResultStoreUnavailable):
		default:
			// A store read error is not an answer; Temporal still is.
		}
	}
	return handler.lookupWorkflow(ctx, identity, meta, recorded)
}

func (handler *Handler) lookupWorkflow(ctx context.Context, identity llm.RequestContext, meta responseMeta, recorded bool) (responseObject, *apiError) {
	workflow := workflowID(identity, meta.ID)
	described, err := handler.options.Client.DescribeWorkflowExecution(ctx, workflow, "")
	if err != nil {
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			return responseObject{}, notFound(meta.ID)
		}
		return responseObject{}, unavailable("the response status could not be read")
	}
	info := described.GetWorkflowExecutionInfo()
	if meta.CreatedAt.IsZero() && info.GetStartTime() != nil {
		meta.CreatedAt = info.GetStartTime().AsTime()
	}
	switch info.GetStatus() {
	case enums.WORKFLOW_EXECUTION_STATUS_RUNNING:
		if recorded {
			return newResponseObject(meta, "in_progress"), nil
		}
		return newResponseObject(meta, "queued"), nil
	case enums.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return cancelledResponse(meta), nil
	case enums.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return failedResponse(meta, serverError("the request was terminated")), nil
	case enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return failedResponse(meta, failureFromCode("deadline_exceeded")), nil
	}
	var response llm.GenerateResponseV1
	if err := handler.options.Client.GetWorkflow(ctx, workflow, "").Get(ctx, &response); err != nil {
		if ctx.Err() != nil {
			return responseObject{}, unavailable("the response status could not be read")
		}
		return failedResponse(meta, workflowFailure(err)), nil
	}
	return completedResponse(meta, response), nil
}

// parentCheckpoint resolves previous_response_id to the checkpoint its result
// published. Only a completed response in the caller's scope can be resumed.
func (handler *Handler) parentCheckpoint(ctx context.Context, identity llm.RequestContext, id string) (llm.CheckpointHandle, *apiError) {
	previous, apiErr := handler.lookupResult(ctx, identity, id)
	if apiErr != nil {
		return "", apiErr
	}
	if previous == nil {
		return "", invalidParam("previous_response_id", "the previous response has not completed")
	}
	if previous.Checkpoint.Handle == "" {
		return "", invalidParam("previous_response_id", "the previous response has no checkpoint")
	}
	return previous.Checkpoint.Handle, nil
}

// lookupResult returns the completed result, nil while it is not complete, or
// an error for a missing or failed response.
func (handler *Handler) lookupResult(parent context.Context, identity llm.RequestContext, id string) (*llm.GenerateResponseV1, *apiError) {
	ctx, cancel := context.WithTimeout(parent, lookupTimeout)
	defer cancel()
	if handler.options.Results != nil {
		stored, err := handler.options.Results.LookupGenerate(ctx, identity.Tenant, identity.Project, id)
		if err == nil && stored.State == StoredCompleted {
			return &stored.Response, nil
		}
		if err == nil && stored.State == StoredFailed {
			return nil, invalidParam("previous_response_id", "the previous response failed")
		}
	}
	workflow := workflowID(identity, id)
	described, err := handler.options.Client.DescribeWorkflowExecution(ctx, workflow, "")
	if err != nil {
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			return nil, invalidParam("previous_response_id", "no response with id "+id)
		}
		return nil, unavailable("the previous response could not be read")
	}
	switch described.GetWorkflowExecutionInfo().GetStatus() {
	case enums.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return nil, nil
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
	default:
		return nil, invalidParam("previous_response_id", "the previous response failed")
	}
	var response llm.GenerateResponseV1
	if err := handler.options.Client.GetWorkflow(ctx, workflow, "").Get(ctx, &response); err != nil {
		return nil, unavailable("the previous response could not be read")
	}
	return &response, nil
}
