package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

type queryHandlerFunc func(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error)

func (handler queryHandlerFunc) ExecuteQuery(ctx context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return handler(ctx, request)
}

// echoQueryHandler answers every request with queryResponse.
func echoQueryHandler(_ context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return queryResponse(request), nil
}

// failingQueryHandler fails every request with err.
func failingQueryHandler(err error) queryHandlerFunc {
	return func(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
		return llm.QueryResponseV1{}, err
	}
}

// allowQuery authorizes every query.
func allowQuery(context.Context, Authorization) error { return nil }

type typedQueryHandlerFunc func(context.Context, QueryRequest, *BoundCursorClaims) (QueryResponse, error)

func (handler typedQueryHandlerFunc) ExecuteTypedQuery(ctx context.Context, request QueryRequest, claims *BoundCursorClaims) (QueryResponse, error) {
	return handler(ctx, request, claims)
}

func queryRequest() llm.QueryRequestV1 {
	return llm.QueryRequestV1{
		APIVersion: llm.QueryAPIVersion, OperationKey: "query-1", Kind: llm.QueryProviderStatus,
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "workflow"},
		Query:   json.RawMessage(`{"page_size":10}`),
	}
}

func queryResponse(request llm.QueryRequestV1) llm.QueryResponseV1 {
	return llm.QueryResponseV1{
		APIVersion: llm.QueryAPIVersion, OperationKey: request.OperationKey, QueryExecutionID: "query-execution-1",
		Kind: request.Kind, ObservedAt: "2026-07-20T00:00:00Z", Source: "persisted", Freshness: "current", Complete: true,
		Result: llm.ProviderStatusPage{Routes: []json.RawMessage{}},
		Cost:   llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "control_query_zero"},
	}
}

func queryResponseForKind(request llm.QueryRequestV1) llm.QueryResponseV1 {
	response := queryResponse(request)
	switch request.Kind {
	case llm.QueryModelInventory:
		response.Result = llm.ModelInventoryPage{Models: []json.RawMessage{}}
	case llm.QueryCreditStatus:
		response.Result = llm.CreditStatusPage{Endpoints: []json.RawMessage{}}
	case llm.QueryBudgetStatus:
		response.Result = llm.BudgetStatus{
			ActiveAt: "2026-07-20T00:00:00Z", GenerationID: "generation-1",
			ManifestDigest: strings.Repeat("a", 64), StreamHighWaterMark: "stream-1",
			Windows: []json.RawMessage{},
		}
	case llm.QuerySpendSummary:
		response.Result = llm.SpendSummary{
			StartTime: "2026-07-19T00:00:00Z", EndTime: "2026-07-20T00:00:00Z",
			Buckets: []json.RawMessage{},
		}
	}
	return response
}

func stringPointer(value string) *string { return &value }

// numberedRows formats count JSON rows from format, whose one %d verb is the
// row index.
func numberedRows(count int, format string) []json.RawMessage {
	rows := make([]json.RawMessage, count)
	for index := range rows {
		rows[index] = json.RawMessage(fmt.Sprintf(format, index))
	}
	return rows
}

func providerRouteRows(count int) []json.RawMessage {
	return numberedRows(count, `{"route_id":"route-%d","provider":"provider","endpoint":"endpoint","availability":"available","observed_at":"2026-07-20T00:00:00Z","stale_after":"2026-07-21T00:00:00Z"}`)
}

func modelInventoryRows(count int) []json.RawMessage {
	return numberedRows(count, `{"provider":"provider","endpoint":"endpoint","provider_model_id":"model-%d","lifecycle":"available","capabilities":[],"complete_snapshot":true}`)
}

func creditStatusRows(count int) []json.RawMessage {
	return numberedRows(count, `{"provider":"provider","endpoint":"endpoint-%d","credit_state":"ok","billing_state":"ok","evidence_source":"unknown"}`)
}

// typedRequest returns the request of kind under the workflow scope that
// queryRequest uses on the wire.
func typedRequest(key OperationKey, kind llm.QueryKind, filter QueryFilter) QueryRequest {
	return QueryRequest{OperationKey: key, Scope: QueryScope{Tenant: "tenant", Project: "project", Actor: "workflow"}, Kind: kind, Filter: filter}
}

func typedProviderRequest() QueryRequest {
	return typedRequest("query-1", llm.QueryProviderStatus, ProviderStatusQuery{Page: QueryPage{Size: 10}})
}

func typedModelRequest() QueryRequest {
	request := typedProviderRequest()
	request.Kind = llm.QueryModelInventory
	request.Filter = ModelInventoryQuery{Page: QueryPage{Size: 10}}
	return request
}

func typedBudgetRequest() QueryRequest {
	activeAt := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	policy := PolicyKey("daily")
	return typedRequest("budget-query", llm.QueryBudgetStatus, BudgetStatusQuery{PolicyKey: &policy, ActiveAt: &activeAt, IncludeWindows: boolPointer(true)})
}

func typedSpendRequest() QueryRequest {
	start := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	return typedRequest("spend-query", llm.QuerySpendSummary, SpendSummaryQuery{StartTime: start, EndTime: start.Add(24 * time.Hour), GroupBy: []SpendDimension{SpendByProvider}, OperationKinds: []OperationKind{OperationGenerate}})
}

func allTypedQueryRequests() []QueryRequest {
	credit := typedRequest("credit-query", llm.QueryCreditStatus, CreditStatusQuery{Page: QueryPage{Size: 10}})
	return []QueryRequest{typedProviderRequest(), typedModelRequest(), credit, typedBudgetRequest(), typedSpendRequest()}
}

func typedRequestWire(t *testing.T, request QueryRequest) llm.QueryRequestV1 {
	t.Helper()
	wire, err := EncodeQueryRequest(request)
	if err != nil {
		t.Fatalf("EncodeQueryRequest() error = %v", err)
	}
	return wire
}

func typedProviderResponse(request QueryRequest, next *QueryCursor) QueryResponse {
	response := persistedQueryResponse(request.OperationKey, request.Kind, time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC), ProviderStatusResult{Routes: []ProviderStatusRow{}})
	response.ExecutionID, response.Complete, response.NextCursor = "query-execution-typed", next == nil, next
	return response
}

func testQueryService(handler Handler, authorize AuthorizeFunc) *QueryService {
	return &QueryService{Handler: handler, Authorize: authorize, CursorKey: []byte("query-test-key"), Clock: func() time.Time { return time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC) }}
}

// typedQueryService returns a service that runs handler with every query
// authorized, cursors bound by codec, and the clock fixed at base.
func typedQueryService(handler typedQueryHandlerFunc, codec *CursorCodec, base time.Time) *QueryService {
	return &QueryService{TypedHandler: handler, Authorize: allowQuery, CursorCodec: codec, Clock: func() time.Time { return base }}
}

// queryHandlerWith answers every request with queryResponseForKind after
// applying mutate.
func queryHandlerWith(mutate func(*llm.QueryResponseV1)) queryHandlerFunc {
	return func(_ context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
		response := queryResponseForKind(request)
		mutate(&response)
		return response, nil
	}
}

// requireProviderError fails unless err wraps a *provider.Error, which it
// returns.
func requireProviderError(t *testing.T, err error) *provider.Error {
	t.Helper()
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) {
		t.Fatalf("error = %T %v, want provider.Error", err, err)
	}
	return providerErr
}

// mustSignServiceCursor signs position for request at issuedAt and fails the
// test on error.
func mustSignServiceCursor(t *testing.T, service *QueryService, request llm.QueryRequestV1, position string, issuedAt time.Time) string {
	t.Helper()
	token, err := service.SignCursor(request, position, issuedAt)
	if err != nil {
		t.Fatalf("SignCursor() error = %v", err)
	}
	return token
}

func TestQueryServiceAuthorizesAndValidatesResponse(t *testing.T) {
	request := queryRequest()
	var seen Authorization
	service := testQueryService(queryHandlerFunc(echoQueryHandler), func(_ context.Context, authorization Authorization) error {
		seen = authorization
		return nil
	})
	response, err := service.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if response.QueryExecutionID == "" || seen.Tenant != "tenant" || seen.Project != "project" || seen.Actor != "workflow" || seen.Kind != llm.QueryProviderStatus {
		t.Fatalf("response/authorization = %#v / %#v", response, seen)
	}
}

func TestQueryServiceRejectsInconsistentPaginationCompletion(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*llm.QueryResponseV1)
	}{
		{name: "complete page with cursor", mutate: func(response *llm.QueryResponseV1) {
			response.NextCursor = stringPointer("page-2")
		}},
		{name: "incomplete page without cursor", mutate: func(response *llm.QueryResponseV1) {
			response.Complete = false
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := queryRequest()
			service := testQueryService(queryHandlerWith(test.mutate), allowQuery)
			if _, err := service.Execute(context.Background(), request); err == nil {
				t.Fatal("inconsistent pagination response was accepted")
			}
		})
	}

	for _, kind := range []llm.QueryKind{llm.QueryBudgetStatus, llm.QuerySpendSummary} {
		t.Run(string(kind)+" incomplete", func(t *testing.T) {
			request := queryRequest()
			request.Kind = kind
			if kind == llm.QueryBudgetStatus {
				request.Query = json.RawMessage(`{"active_at":"2026-07-20T00:00:00Z"}`)
			} else {
				request.Query = json.RawMessage(`{"start_time":"2026-07-19T00:00:00Z","end_time":"2026-07-20T00:00:00Z"}`)
			}
			service := testQueryService(queryHandlerWith(func(response *llm.QueryResponseV1) { response.Complete = false }), allowQuery)
			if _, err := service.Execute(context.Background(), request); err == nil {
				t.Fatal("incomplete snapshot response was accepted")
			}
		})
	}
}

func TestQueryServiceAuditsValidatedResponseBeforeReturning(t *testing.T) {
	request := queryRequest()
	var audit QueryAuditRecord
	service := testQueryService(queryHandlerFunc(echoQueryHandler), allowQuery)
	service.Audit = func(_ context.Context, got QueryAuditRecord) error {
		audit = got
		return nil
	}

	response, err := service.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if audit.Tenant != request.Context.Tenant || audit.Project != request.Context.Project || audit.OperationKey != request.OperationKey || audit.Kind != request.Kind {
		t.Fatalf("audit identity = %#v", audit)
	}
	if audit.CostStatus != "exact" || audit.CostMethod != "control_query_zero" || audit.ActualCostUSD == nil || *audit.ActualCostUSD != "0" {
		t.Fatalf("audit cost = %#v", audit)
	}
	if len(audit.RequestJSON) == 0 || len(audit.ResponseJSON) == 0 || audit.StartedAt.IsZero() || audit.CompletedAt.IsZero() || audit.CompletedAt.Before(audit.StartedAt) {
		t.Fatalf("audit record is incomplete = %#v", audit)
	}
	if audit.RequestFingerprint != sha256.Sum256(audit.RequestJSON) {
		t.Fatal("audit request fingerprint does not match canonical request")
	}
	if audit.ResponseDigest != sha256.Sum256(audit.ResponseJSON) {
		t.Fatal("audit response digest does not match canonical response")
	}
	var decoded map[string]any
	if err := json.Unmarshal(audit.ResponseJSON, &decoded); err != nil || decoded["query_execution_id"] != response.QueryExecutionID {
		t.Fatalf("audit response JSON = %s, err = %v", audit.ResponseJSON, err)
	}
	var requestEnvelope map[string]any
	if err := json.Unmarshal(audit.RequestJSON, &requestEnvelope); err != nil || requestEnvelope["operation_key"] != request.OperationKey {
		t.Fatalf("audit request JSON = %s, err = %v", audit.RequestJSON, err)
	}
}

func TestQueryServiceAuditFailureDoesNotFailSuccessfulRead(t *testing.T) {
	for _, auditErr := range []error{errors.New("audit unavailable"), context.DeadlineExceeded} {
		service := testQueryService(queryHandlerFunc(echoQueryHandler), allowQuery)
		calls := 0
		service.Audit = func(context.Context, QueryAuditRecord) error { calls++; return auditErr }

		response, err := service.Execute(context.Background(), queryRequest())
		if err != nil || response.OperationKey != queryRequest().OperationKey || calls != 1 {
			t.Fatalf("audit failure changed result: response=%+v err=%v calls=%d", response, err, calls)
		}
	}
}

func TestQueryServiceDoesNotAuditInvalidResponse(t *testing.T) {
	service := testQueryService(queryHandlerWith(func(response *llm.QueryResponseV1) { response.OperationKey = "different-operation" }), allowQuery)
	called := false
	service.Audit = func(context.Context, QueryAuditRecord) error {
		called = true
		return nil
	}

	if _, err := service.Execute(context.Background(), queryRequest()); err == nil {
		t.Fatal("Execute() unexpectedly accepted an invalid response")
	}
	if called {
		t.Fatal("audit callback ran for an invalid response")
	}
}

func TestQueryServiceRejectsResponseRowsAboveRequestedPageSize(t *testing.T) {
	tests := []struct {
		kind   llm.QueryKind
		result llm.QueryResult
	}{
		{kind: llm.QueryProviderStatus, result: llm.ProviderStatusPage{Routes: providerRouteRows(2)}},
		{kind: llm.QueryModelInventory, result: llm.ModelInventoryPage{Models: modelInventoryRows(2)}},
		{kind: llm.QueryCreditStatus, result: llm.CreditStatusPage{Endpoints: creditStatusRows(2)}},
	}

	for _, test := range tests {
		t.Run(string(test.kind), func(t *testing.T) {
			request := queryRequest()
			request.Kind = test.kind
			request.Query = json.RawMessage(`{"page_size":1}`)
			service := testQueryService(queryHandlerWith(func(response *llm.QueryResponseV1) { response.Result = test.result }), allowQuery)

			if _, err := service.Execute(context.Background(), request); err == nil || !strings.Contains(err.Error(), "page contains 2 rows; limit is 1") {
				t.Fatalf("Execute() error = %v, want response page-size bound", err)
			}
		})
	}
}

func TestQueryServiceAppliesDefaultResponsePageSize(t *testing.T) {
	request := queryRequest()
	request.Query = json.RawMessage(`{}`)
	service := testQueryService(queryHandlerWith(func(response *llm.QueryResponseV1) {
		response.Result = llm.ProviderStatusPage{Routes: providerRouteRows(101)}
	}), allowQuery)

	if _, err := service.Execute(context.Background(), request); err == nil || !strings.Contains(err.Error(), "page contains 101 rows; limit is 100") {
		t.Fatalf("Execute() error = %v, want default response page-size bound", err)
	}
}

func TestQueryServiceRawHandlerAdmitsAllQueryKinds(t *testing.T) {
	service := testQueryService(queryHandlerWith(func(*llm.QueryResponseV1) {}), allowQuery)

	for _, typedRequest := range allTypedQueryRequests() {
		t.Run(string(typedRequest.Kind), func(t *testing.T) {
			request := typedRequestWire(t, typedRequest)
			response, err := service.Execute(context.Background(), request)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if response.Kind != typedRequest.Kind {
				t.Fatalf("response kind = %q, want %q", response.Kind, typedRequest.Kind)
			}
		})
	}
}

func TestQueryServiceTypedHandlerAdmitsAllQueryKinds(t *testing.T) {
	base := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	service := typedQueryService(func(_ context.Context, request QueryRequest, claims *BoundCursorClaims) (QueryResponse, error) {
		if claims != nil {
			t.Fatalf("non-paginated query received cursor claims: %#v", claims)
		}
		response := typedProviderResponse(request, nil)
		switch request.Kind {
		case llm.QueryModelInventory:
			response.Result = ModelInventoryResult{Models: []ModelInventoryRow{}}
		case llm.QueryCreditStatus:
			response.Result = CreditStatusResult{Endpoints: []CreditStatusRow{}}
		case llm.QueryBudgetStatus:
			response.Result = BudgetStatusResult{ActiveAt: base, GenerationID: "generation-1", ManifestDigest: ManifestDigest(strings.Repeat("a", 64)), StreamHighWaterMark: "stream-1", Windows: []BudgetWindow{}}
		case llm.QuerySpendSummary:
			response.Result = SpendSummaryResult{StartTime: base.Add(-24 * time.Hour), EndTime: base, Buckets: []SpendBucket{}}
		}
		return response, nil
	}, &CursorCodec{Key: []byte("all-five-query-key")}, base)

	for _, typedRequest := range allTypedQueryRequests() {
		t.Run(string(typedRequest.Kind), func(t *testing.T) {
			response, err := service.Execute(context.Background(), typedRequestWire(t, typedRequest))
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			decoded, err := DecodeQueryResponse(response)
			if err != nil {
				t.Fatalf("DecodeQueryResponse() error = %v", err)
			}
			if decoded.Kind != typedRequest.Kind || decoded.NextCursor != nil {
				t.Fatalf("decoded response = %#v, want kind %q without cursor", decoded, typedRequest.Kind)
			}
		})
	}
}

func TestQueryServiceRejectsUnsupportedKindAndAuthorization(t *testing.T) {
	request := queryRequest()
	request.Kind = llm.QueryKind("future_query")
	service := testQueryService(queryHandlerFunc(echoQueryHandler), allowQuery)
	if _, err := service.Execute(context.Background(), request); !errors.Is(err, ErrQueryUnsupported) {
		t.Fatalf("unsupported kind error = %v", err)
	}
	request = queryRequest()
	service.Authorize = func(context.Context, Authorization) error { return errors.New("missing role") }
	if _, err := service.Execute(context.Background(), request); !errors.Is(err, ErrQueryAuthorization) {
		t.Fatalf("authorization error = %v", err)
	}
	request = queryRequest()
	request.Query = json.RawMessage(`{"page_size":1001}`)
	service.Authorize = allowQuery
	if _, err := service.Execute(context.Background(), request); err == nil || !strings.Contains(err.Error(), "page_size") {
		t.Fatalf("page-size bound error = %v", err)
	}
}

func TestQueryServiceCursorsAreBoundToScopeAndFilter(t *testing.T) {
	service := testQueryService(queryHandlerFunc(echoQueryHandler), allowQuery)
	request := queryRequest()
	issued := service.now()
	token := mustSignServiceCursor(t, service, request, "route-2", issued)
	if err := service.ValidateCursor(request, token, issued.Add(time.Minute)); err != nil {
		t.Fatalf("ValidateCursor() error = %v", err)
	}
	request.Query = json.RawMessage(`{"page_size":20}`)
	if err := service.ValidateCursor(request, token, issued.Add(time.Minute)); !errors.Is(err, ErrQueryCursor) {
		t.Fatalf("filter-bound cursor error = %v", err)
	}
	request = queryRequest()
	request.Context.Tenant = "other-tenant"
	if err := service.ValidateCursor(request, token, issued.Add(time.Minute)); !errors.Is(err, ErrQueryCursor) {
		t.Fatalf("scope-bound cursor error = %v", err)
	}
	if err := service.ValidateCursor(queryRequest(), token, issued.Add(16*time.Minute)); !errors.Is(err, ErrQueryCursor) {
		t.Fatalf("expired cursor error = %v", err)
	}
}

func TestQueryServiceDecodeCursorReturnsAuthenticatedPosition(t *testing.T) {
	service := testQueryService(queryHandlerFunc(echoQueryHandler), allowQuery)
	request := queryRequest()
	issued := service.now()
	token := mustSignServiceCursor(t, service, request, `{"after":"provider\u0000endpoint","horizon":"2026-07-20T00:00:00Z"}`, issued)
	claims, err := service.DecodeCursor(request, token, issued.Add(time.Minute))
	if err != nil {
		t.Fatalf("DecodeCursor() error = %v", err)
	}
	if claims.Kind != request.Kind || claims.Position == "" || !claims.IssuedAt.Equal(issued) || !claims.ExpiresAt.After(issued) {
		t.Fatalf("claims = %#v", claims)
	}
}

func TestQueryServiceValidatesOutgoingCursorAgainstFreshTime(t *testing.T) {
	request := queryRequest()
	base := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	clockCalls := 0
	var service *QueryService
	service = &QueryService{
		Authorize: allowQuery,
		CursorKey: []byte("query-test-key"),
		Clock: func() time.Time {
			clockCalls++
			if clockCalls == 1 {
				return base
			}
			return base.Add(3 * time.Minute)
		},
		Handler: queryHandlerFunc(func(_ context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
			cursor, err := service.SignCursor(request, "route-2", base.Add(3*time.Minute))
			if err != nil {
				return llm.QueryResponseV1{}, err
			}
			response := queryResponse(request)
			response.Complete = false
			response.NextCursor = &cursor
			return response, nil
		}),
	}
	if _, err := service.Execute(context.Background(), request); err != nil {
		t.Fatalf("Execute() rejected a cursor signed after a slow handler: %v", err)
	}
	if clockCalls != 2 {
		t.Fatalf("clock calls = %d, want pre-handler and post-handler samples", clockCalls)
	}
}

func TestQueryServiceRejectsResponseMismatchAndUnsafeScope(t *testing.T) {
	request := queryRequest()
	service := testQueryService(queryHandlerWith(func(response *llm.QueryResponseV1) { response.OperationKey = "other" }), allowQuery)
	if _, err := service.Execute(context.Background(), request); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("response mismatch error = %v", err)
	}
	request = queryRequest()
	request.Context.Actor = " workflow"
	if _, err := service.Execute(context.Background(), request); err == nil || !strings.Contains(err.Error(), "unsafe") {
		t.Fatalf("unsafe scope error = %v", err)
	}
}

func TestQueryServiceClassifiesRawHandlerFailureAsRetryableStateUnavailable(t *testing.T) {
	request := queryRequest()
	backendErr := errors.New("postgres unavailable: dsn=postgres://secret")
	service := testQueryService(failingQueryHandler(backendErr), allowQuery)

	_, err := service.Execute(context.Background(), request)
	providerErr := requireProviderError(t, err)
	if providerErr.Code != provider.CodeStateUnavailable || providerErr.Phase != provider.PhaseStateLoad || providerErr.Dispatch != provider.DispatchNotDispatched || providerErr.Retry != provider.RetrySameOperation {
		t.Fatalf("provider error = %#v", providerErr)
	}
	if !errors.Is(err, backendErr) {
		t.Fatalf("wrapped error = %v, want original cause for local diagnostics", err)
	}
	if strings.Contains(err.Error(), "postgres") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("error leaked backend details: %v", err)
	}
}

func TestQueryServiceMapsExpiredProviderViewToPermanentCursorError(t *testing.T) {
	service := testQueryService(failingQueryHandler(ErrProviderViewExpired), allowQuery)
	_, err := service.Execute(context.Background(), queryRequest())
	providerErr := requireProviderError(t, err)
	if providerErr.Code != provider.CodeInvalidArgument || providerErr.Phase != provider.PhaseStateLoad || providerErr.Dispatch != provider.DispatchNotDispatched || providerErr.Retry != provider.RetryNever {
		t.Fatalf("provider error = %#v", providerErr)
	}
	if !strings.Contains(providerErr.SafeMessage, "restart pagination") {
		t.Fatalf("safe message = %q, want restart pagination guidance", providerErr.SafeMessage)
	}
	if !errors.Is(err, ErrQueryCursor) || !errors.Is(err, ErrProviderViewExpired) {
		t.Fatalf("error = %v, want cursor and expired-view causes", err)
	}
}

func TestQueryServicePreservesPreclassifiedProviderFailure(t *testing.T) {
	request := queryRequest()
	providerErr := provider.NewError(provider.CodePermissionDenied, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "query access denied")
	service := testQueryService(failingQueryHandler(providerErr), allowQuery)

	_, err := service.Execute(context.Background(), request)
	if err != providerErr {
		t.Fatalf("error = %p, want original provider error %p", err, providerErr)
	}
}

func TestQueryServicePreservesCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	service := testQueryService(queryHandlerFunc(func(handlerCtx context.Context, _ llm.QueryRequestV1) (llm.QueryResponseV1, error) {
		cancel()
		return llm.QueryResponseV1{}, handlerCtx.Err()
	}), allowQuery)
	_, err := service.Execute(ctx, queryRequest())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want caller cancellation", err)
	}
	var providerErr *provider.Error
	if errors.As(err, &providerErr) {
		t.Fatalf("error = %#v, caller cancellation must not be remapped", providerErr)
	}
}

func TestQueryServiceMapsChildDeadlineToRetryableStateUnavailable(t *testing.T) {
	service := testQueryService(failingQueryHandler(context.DeadlineExceeded), allowQuery)
	_, err := service.Execute(context.Background(), queryRequest())
	providerErr := requireProviderError(t, err)
	if providerErr.Code != provider.CodeStateUnavailable || providerErr.Phase != provider.PhaseStateLoad || providerErr.Retry != provider.RetrySameOperation {
		t.Fatalf("provider error = %#v", providerErr)
	}
}

func TestQueryServiceTypedHandlerBindsCursorClaimsAndValidatesOutgoingCursor(t *testing.T) {
	base := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	horizon := base.Add(-time.Minute)
	codec := &CursorCodec{Key: []byte("typed-query-key"), TTL: 15 * time.Minute}
	request := typedProviderRequest()
	var incoming *BoundCursorClaims
	service := typedQueryService(func(_ context.Context, got QueryRequest, claims *BoundCursorClaims) (QueryResponse, error) {
		incoming = claims
		next := mustSignCursor(t, codec, got, "route-2", horizon, base)
		return typedProviderResponse(got, &next), nil
	}, codec, base)
	first, err := service.Execute(context.Background(), typedRequestWire(t, request))
	if err != nil {
		t.Fatalf("first typed Execute() error = %v", err)
	}
	if first.NextCursor == nil {
		t.Fatal("first typed response did not contain a cursor")
	}
	claims, err := codec.Decode(request, QueryCursor(*first.NextCursor), base)
	if err != nil || claims.Position != "route-2" || !claims.Horizon.Equal(horizon) {
		t.Fatalf("outgoing claims = %#v, error = %v", claims, err)
	}

	request.Filter = ProviderStatusQuery{Page: QueryPage{Size: 10, Cursor: (*QueryCursor)(first.NextCursor)}}
	second, err := service.Execute(context.Background(), typedRequestWire(t, request))
	if err != nil {
		t.Fatalf("second typed Execute() error = %v", err)
	}
	if second.NextCursor == nil || incoming == nil || incoming.Position != "route-2" || !incoming.Horizon.Equal(horizon) {
		t.Fatalf("incoming claims = %#v, response = %#v", incoming, second)
	}
}

func TestQueryServiceTypedHandlerRejectsChangedContinuationHorizon(t *testing.T) {
	base := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	horizon := base.Add(-time.Minute)
	codec := &CursorCodec{Key: []byte("typed-query-key"), TTL: 15 * time.Minute}
	request := typedProviderRequest()
	service := typedQueryService(func(_ context.Context, got QueryRequest, claims *BoundCursorClaims) (QueryResponse, error) {
		nextHorizon := horizon
		if claims != nil {
			nextHorizon = horizon.Add(time.Minute)
		}
		next := mustSignCursor(t, codec, got, "route-2", nextHorizon, base)
		return typedProviderResponse(got, &next), nil
	}, codec, base)
	first, err := service.Execute(context.Background(), typedRequestWire(t, request))
	if err != nil || first.NextCursor == nil {
		t.Fatalf("first typed Execute() response = %#v, error = %v", first, err)
	}
	request.Filter = ProviderStatusQuery{Page: QueryPage{Size: 10, Cursor: (*QueryCursor)(first.NextCursor)}}
	if _, err := service.Execute(context.Background(), typedRequestWire(t, request)); !errors.Is(err, ErrQueryCursor) {
		t.Fatalf("changed continuation horizon error = %v, want %v", err, ErrQueryCursor)
	}
}

func TestQueryServiceRawHandlerAcceptsTypedCursorCodec(t *testing.T) {
	base := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	codec := &CursorCodec{Key: []byte("typed-query-key")}
	typed := typedProviderRequest()
	token := mustSignCursor(t, codec, typed, "route-2", base.Add(-time.Minute), base)
	typed.Filter = ProviderStatusQuery{Page: QueryPage{Size: 10, Cursor: &token}}
	request := typedRequestWire(t, typed)
	service := &QueryService{
		Handler:     queryHandlerFunc(echoQueryHandler),
		Authorize:   allowQuery,
		CursorCodec: codec,
		Clock:       func() time.Time { return base },
	}
	if _, err := service.Execute(context.Background(), request); err != nil {
		t.Fatalf("raw Handler with typed cursor error = %v", err)
	}
}

func TestQueryServiceTypedCursorBridgeRejectsInvalidClaims(t *testing.T) {
	base := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	codec := &CursorCodec{Key: []byte("typed-query-key"), TTL: time.Minute}
	service := typedQueryService(func(_ context.Context, request QueryRequest, _ *BoundCursorClaims) (QueryResponse, error) {
		return typedProviderResponse(request, nil), nil
	}, codec, base)
	valid := typedProviderRequest()
	validToken := mustSignCursor(t, codec, valid, "route-2", base.Add(-time.Minute), base)
	withCursor := func(request QueryRequest, token QueryCursor) llm.QueryRequestV1 {
		switch request.Kind {
		case llm.QueryProviderStatus:
			filter := request.Filter.(ProviderStatusQuery)
			filter.Page.Cursor = &token
			request.Filter = filter
		case llm.QueryModelInventory:
			filter := request.Filter.(ModelInventoryQuery)
			filter.Page.Cursor = &token
			request.Filter = filter
		case llm.QueryCreditStatus:
			filter := request.Filter.(CreditStatusQuery)
			filter.Page.Cursor = &token
			request.Filter = filter
		}
		return typedRequestWire(t, request)
	}
	mutateToken := func(token QueryCursor) QueryCursor {
		value := string(token)
		replacement := byte('A')
		if value[0] == replacement {
			replacement = 'B'
		}
		return QueryCursor(string(replacement) + value[1:])
	}
	mutated := func(mutate func(*QueryRequest)) QueryRequest {
		copy := valid
		mutate(&copy)
		return copy
	}
	tests := []struct {
		name    string
		request llm.QueryRequestV1
		wantErr error
	}{
		{name: "tamper", request: withCursor(valid, mutateToken(validToken)), wantErr: ErrQueryCursor},
		{name: "scope", request: withCursor(mutated(func(copy *QueryRequest) { copy.Scope.Tenant = "other" }), validToken), wantErr: ErrQueryCursor},
		{name: "actor", request: withCursor(mutated(func(copy *QueryRequest) { copy.Scope.Actor = "other" }), validToken), wantErr: ErrQueryCursor},
		{name: "filter", request: withCursor(mutated(func(copy *QueryRequest) { copy.Filter = ProviderStatusQuery{Page: QueryPage{Size: 11}} }), validToken), wantErr: ErrQueryCursor},
		{name: "cross-kind", request: withCursor(typedModelRequest(), validToken), wantErr: ErrQueryCursor},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := service.Execute(context.Background(), test.request); !errors.Is(err, test.wantErr) {
				t.Fatalf("Execute() error = %v, want %v", err, test.wantErr)
			}
		})
	}

	expired, err := codec.Sign(valid, "route-2", base.Add(-2*time.Minute), base.Add(-2*time.Minute))
	if err != nil {
		t.Fatalf("expired CursorCodec.Sign() error = %v", err)
	}
	if _, err := service.Execute(context.Background(), withCursor(valid, expired)); !errors.Is(err, ErrQueryCursor) {
		t.Fatalf("expired cursor error = %v", err)
	}
	future, err := codec.Sign(valid, "route-2", base, base.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("future CursorCodec.Sign() error = %v", err)
	}
	if _, err := service.Execute(context.Background(), withCursor(valid, future)); !errors.Is(err, ErrQueryCursor) {
		t.Fatalf("future cursor error = %v", err)
	}
	if _, err := codec.Sign(valid, "route-2", time.Time{}, base); !errors.Is(err, ErrQueryCursor) {
		t.Fatalf("zero-horizon sign error = %v", err)
	}
	limited := *codec
	limited.MaxPosition = 4
	if _, err := limited.Sign(valid, "route-2", base, base); !errors.Is(err, ErrQueryCursor) {
		t.Fatalf("oversized position sign error = %v", err)
	}
}

func TestQueryServiceTypedCursorBridgeRejectsInvalidOutgoingCursor(t *testing.T) {
	base := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	service := typedQueryService(func(_ context.Context, request QueryRequest, _ *BoundCursorClaims) (QueryResponse, error) {
		invalid := QueryCursor("not-a-signed-cursor")
		return typedProviderResponse(request, &invalid), nil
	}, &CursorCodec{Key: []byte("typed-query-key")}, base)
	if _, err := service.Execute(context.Background(), typedRequestWire(t, typedProviderRequest())); !errors.Is(err, ErrQueryCursor) {
		t.Fatalf("invalid outgoing cursor error = %v", err)
	}
}

func TestQueryServiceTypedHandlerResponseContractErrorIsNotMappedToStateUnavailable(t *testing.T) {
	base := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	service := typedQueryService(func(_ context.Context, request QueryRequest, _ *BoundCursorClaims) (QueryResponse, error) {
		response := typedProviderResponse(request, nil)
		response.OperationKey = ""
		return response, nil
	}, &CursorCodec{Key: []byte("typed-query-key")}, base)
	_, err := service.Execute(context.Background(), typedRequestWire(t, typedProviderRequest()))
	if err == nil || !strings.Contains(err.Error(), "typed query response") {
		t.Fatalf("typed response contract error = %v", err)
	}
	var providerErr *provider.Error
	if errors.As(err, &providerErr) {
		t.Fatalf("typed response contract error was mapped to provider error: %#v", providerErr)
	}
}
