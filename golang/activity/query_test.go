package activity

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"go.temporal.io/sdk/temporal"
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
	return llm.QueryResponseV1{
		APIVersion: llm.QueryAPIVersion, OperationKey: request.OperationKey, QueryExecutionID: "query-execution-1", Kind: request.Kind,
		ObservedAt: "2026-07-20T00:00:00Z", Source: "persisted", Freshness: "current", Complete: true,
		Result: llm.ProviderStatusPage{Routes: []json.RawMessage{}},
		Cost:   llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "control_query_zero"},
	}, nil
}

func TestQueryV1UsesIndependentQueryService(t *testing.T) {
	service := &queryServiceStub{}
	activities := &Activities{QueryService: service}
	response, err := activities.QueryV1(context.Background(), validQueryV1Request())
	if err != nil {
		t.Fatalf("QueryV1 error = %v", err)
	}
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
	response := llm.QueryResponseV1{
		APIVersion: llm.QueryAPIVersion, OperationKey: request.OperationKey, QueryExecutionID: "query-execution-1", Kind: request.Kind,
		ObservedAt: "2026-07-20T00:00:00Z", Source: "persisted", Freshness: "current", Complete: true, NextCursor: &cursor,
		Result: llm.ProviderStatusPage{Routes: []json.RawMessage{}},
		Cost:   llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "control_query_zero"},
	}
	activities := &Activities{QueryService: queryResponseServiceStub{response: response}}
	if result, err := activities.QueryV1(context.Background(), request); result != nil || err == nil {
		t.Fatalf("QueryV1 result = %#v, error = %v; want contract rejection", result, err)
	}
}

func TestRegisterIncludesQueryActivityForQueryOnlyService(t *testing.T) {
	registry := &v1Registry{}
	activities := &Activities{QueryService: &queryServiceStub{}}
	activities.Register(registry)
	if len(registry.names) != 3 || registry.names[2] != QueryActivityName {
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
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) {
		t.Fatalf("error = %T %v, want Temporal application error", err, err)
	}
	if applicationErr.Type() != ErrorTypeProviderTransient || applicationErr.NonRetryable() {
		t.Fatalf("application error = type %q non_retryable=%v", applicationErr.Type(), applicationErr.NonRetryable())
	}
	var details SafeErrorDetails
	if detailsErr := applicationErr.Details(&details); detailsErr != nil {
		t.Fatalf("details error = %v", detailsErr)
	}
	if details.Code != string(provider.CodeStateUnavailable) || details.Phase != string(provider.PhaseStateLoad) || details.Dispatch != string(provider.DispatchNotDispatched) {
		t.Fatalf("safe details = %#v", details)
	}
	if strings.Contains(err.Error(), "postgres") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("error leaked backend details: %v", err)
	}
}
