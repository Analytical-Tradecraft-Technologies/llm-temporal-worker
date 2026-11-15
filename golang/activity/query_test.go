package activity

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

type queryServiceStub struct {
	called bool
	err    error
}

type queryResponseServiceStub struct {
	response llm.QueryResponseV1
}

func (stub queryResponseServiceStub) Execute(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return stub.response, nil
}

type queryBackendHandler struct {
	err error
}

func (handler queryBackendHandler) ExecuteQuery(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return llm.QueryResponseV1{}, handler.err
}

func (stub *queryServiceStub) Execute(_ context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	stub.called = true
	if stub.err != nil {
		return llm.QueryResponseV1{}, stub.err
	}
	return providerStatusQueryResponse(request, "query-execution-1"), nil
}

// providerStatusQueryResponse is a complete, zero-cost provider status page
// answering request.
func providerStatusQueryResponse(request llm.QueryRequestV1, executionID string) llm.QueryResponseV1 {
	return llm.QueryResponseV1{
		APIVersion: llm.QueryAPIVersion, OperationKey: request.OperationKey, QueryExecutionID: executionID, Kind: request.Kind,
		ObservedAt: "2026-07-20T00:00:00Z", Source: "persisted", Freshness: "current", Complete: true,
		Result: llm.ProviderStatusPage{Routes: []json.RawMessage{}},
		Cost:   llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "control_query_zero"},
	}
}

func TestQueryV1UsesIndependentQueryService(t *testing.T) {
	service := &queryServiceStub{}
	activities := &Activities{QueryService: service}
	response, err := activities.QueryV1(context.Background(), validQueryV1Request())
	testassert.NoError(t, err, "QueryV1 error")
	if !service.called || response == nil || response.QueryExecutionID != "query-execution-1" {
		t.Fatalf("service called = %v, response = %#v", service.called, response)
	}
	if _, err := activities.GenerateV1(context.Background(), validGenerateV1Request()); err == nil {
		t.Fatal("GenerateV1 unexpectedly succeeded without a durable V1 runtime")
	}
}

func TestQueryV1RejectsInconsistentPaginationCompletion(t *testing.T) {
	request := validQueryV1Request()
	cursor := "page-2"
	response := providerStatusQueryResponse(request, "query-execution-1")
	response.NextCursor = &cursor
	activities := &Activities{QueryService: queryResponseServiceStub{response: response}}
	if result, err := activities.QueryV1(context.Background(), request); result != nil || err == nil {
		t.Fatalf("QueryV1 result = %#v, error = %v; want contract rejection", result, err)
	}
}

func TestRegisterIncludesQueryActivityForQueryOnlyService(t *testing.T) {
	registry := &v1Registry{}
	activities := &Activities{QueryService: &queryServiceStub{}}
	activities.Register(registry)
	if len(registry.names) != 10 || registry.names[2] != QueryActivityName {
		t.Fatalf("registered names = %v, want v1 activities including %q", registry.names, QueryActivityName)
	}
}

func TestQueryV1MapsStateOutageToRetryableSanitizedTemporalError(t *testing.T) {
	backendErr := errors.New("postgres unavailable: password=secret")
	service := &control.QueryService{
		Handler: queryBackendHandler{err: backendErr},
		Authorize: func(context.Context, control.Authorization) error {
			return nil
		},
	}
	activities := &Activities{QueryService: service}

	response, err := activities.QueryV1(context.Background(), validQueryV1Request())
	if response != nil {
		t.Fatalf("response = %#v, want nil on error", response)
	}
	applicationErr, details := safeApplicationError(t, err)
	assertApplicationType(t, applicationErr, ErrorTypeProviderTransient, false)
	if details.Code != string(provider.CodeStateUnavailable) || details.Phase != string(provider.PhaseStateLoad) || details.Dispatch != string(provider.DispatchNotDispatched) {
		t.Fatalf("safe details = %#v", details)
	}
	if strings.Contains(err.Error(), "postgres") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("error leaked backend details: %v", err)
	}
}
