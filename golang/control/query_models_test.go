package control

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestTypedQueryRequestRoundTrip(t *testing.T) {
	provider := ProviderID("openai")
	endpoint := EndpointID("primary")
	refresh := 30 * time.Second
	pageCursor := QueryCursor("cursor")
	request := QueryRequest{
		OperationKey: "query-op",
		Scope:        QueryScope{Tenant: "tenant-a", Project: "project-a", Actor: "actor-a"},
		Kind:         llm.QueryProviderStatus,
		Filter:       ProviderStatusQuery{Provider: &provider, Endpoint: &endpoint, IncludeHealthy: boolPointer(true), RefreshIfOlderThan: refresh, Page: QueryPage{Size: 25, Cursor: &pageCursor}},
	}
	wire, err := EncodeQueryRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	data, _ := json.Marshal(wire)
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["api_version"] != llm.QueryAPIVersion {
		t.Fatalf("wrong version: %#v", fields["api_version"])
	}
	decoded, err := DecodeQueryRequest(wire)
	if err != nil {
		t.Fatal(err)
	}
	filter, ok := decoded.Filter.(ProviderStatusQuery)
	if !ok || filter.Page.Size != 25 || filter.RefreshIfOlderThan != refresh || filter.Provider == nil || *filter.Provider != provider || filter.Page.Cursor == nil || *filter.Page.Cursor != pageCursor {
		t.Fatalf("decoded filter lost typed values: %#v", decoded.Filter)
	}
}

func TestTypedQueryRequestRejectsTags(t *testing.T) {
	request := QueryRequest{
		OperationKey: "query-op",
		Scope:        QueryScope{Tenant: "tenant", Project: "project", Actor: "actor", Tags: map[string]string{"region": "au"}},
		Kind:         llm.QueryProviderStatus,
		Filter:       ProviderStatusQuery{},
	}
	_, err := EncodeQueryRequest(request)
	if err == nil {
		t.Fatal("typed query request silently dropped context tags")
	}
	if message := err.Error(); !strings.Contains(message, "context") || !strings.Contains(message, "tags") {
		t.Fatalf("error = %q, want clear context tags rejection", message)
	}
}

func TestTypedQueryRequestAcceptsPointerFilters(t *testing.T) {
	filter := &ProviderStatusQuery{}
	wire, err := EncodeQueryRequest(QueryRequest{OperationKey: "op", Scope: testScope(), Kind: llm.QueryProviderStatus, Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeQueryRequest(wire); err != nil {
		t.Fatal(err)
	}
}

func TestTypedQueryAPIsRejectTypedNilPointers(t *testing.T) {
	var filter *ProviderStatusQuery
	if _, err := EncodeQueryRequest(QueryRequest{OperationKey: "op", Scope: testScope(), Kind: llm.QueryProviderStatus, Filter: filter}); err == nil {
		t.Fatal("typed nil filter was accepted")
	}
	var result *ProviderStatusResult
	if _, err := EncodeQueryResponse(QueryResponse{OperationKey: "op", ExecutionID: "exec", Kind: llm.QueryProviderStatus, Provenance: QueryProvenance{ObservedAt: time.Now().UTC()}, Result: result}); err == nil {
		t.Fatal("typed nil result was accepted")
	}
}

func TestTypedQueryRequestSupportsAllWireKinds(t *testing.T) {
	start := time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	policy := PolicyKey("daily")
	queries := []QueryRequest{
		{OperationKey: "provider", Scope: testScope(), Kind: llm.QueryProviderStatus, Filter: ProviderStatusQuery{}},
		{OperationKey: "inventory", Scope: testScope(), Kind: llm.QueryModelInventory, Filter: ModelInventoryQuery{}},
		{OperationKey: "credit", Scope: testScope(), Kind: llm.QueryCreditStatus, Filter: CreditStatusQuery{}},
		{OperationKey: "budget", Scope: testScope(), Kind: llm.QueryBudgetStatus, Filter: BudgetStatusQuery{PolicyKey: &policy, ActiveAt: &start, IncludeWindows: boolPointer(true)}},
		{OperationKey: "spend", Scope: testScope(), Kind: llm.QuerySpendSummary, Filter: SpendSummaryQuery{StartTime: start, EndTime: end, GroupBy: []SpendDimension{SpendByProvider, SpendByModel}, OperationKinds: []OperationKind{OperationGenerate}}},
	}
	for _, query := range queries {
		wire, err := EncodeQueryRequest(query)
		if err != nil {
			t.Fatalf("%s: %v", query.OperationKey, err)
		}
		if _, err := DecodeQueryRequest(wire); err != nil {
			t.Fatalf("%s decode: %v", query.OperationKey, err)
		}
	}
}

func TestTypedQueryRequestRejectsInvalidRefreshDuration(t *testing.T) {
	for _, refresh := range []time.Duration{500 * time.Millisecond, -time.Second, 86401 * time.Second} {
		_, err := EncodeQueryRequest(QueryRequest{OperationKey: "op", Scope: testScope(), Kind: llm.QueryProviderStatus, Filter: ProviderStatusQuery{RefreshIfOlderThan: refresh}})
		if err == nil {
			t.Fatalf("refresh %s was accepted", refresh)
		}
	}
}

func TestTypedQueryResponseRoundTrip(t *testing.T) {
	now := time.Date(2026, time.July, 21, 1, 2, 3, 0, time.UTC)
	credit := QueryCreditLow
	billing := QueryBillingBlocked
	response := QueryResponse{OperationKey: "credit-op", ExecutionID: "execution-1", Kind: llm.QueryCreditStatus, Provenance: QueryProvenance{Source: QuerySourcePersisted, Freshness: QueryFreshCurrent, ObservedAt: now}, Complete: true, Result: CreditStatusResult{Endpoints: []CreditStatusRow{{Provider: "openai", Endpoint: "primary", Credit: credit, Billing: billing, EvidenceSource: QueryEvidenceProviderAPI}}}, Cost: QueryCost{Status: QueryCostExact, ActualUSD: decimalPointer("0"), Method: QueryCostControlZero}}
	wire, err := EncodeQueryResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeQueryResponse(wire)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := decoded.Result.(CreditStatusResult)
	if !ok || len(result.Endpoints) != 1 || result.Endpoints[0].Credit != credit || result.Endpoints[0].Billing != billing {
		t.Fatalf("decoded result lost typed values: %#v", decoded.Result)
	}
}

func TestTypedQueryResponseRejectsUnknownWireRow(t *testing.T) {
	result := llm.ProviderStatusPage{Routes: []json.RawMessage{json.RawMessage(`{"route_id":"r","provider":"p","endpoint":"e","availability":"available","observed_at":"2026-07-21T00:00:00Z","stale_after":"2026-07-21T01:00:00Z","unexpected":true`)}}
	wire := llm.QueryResponseV1{APIVersion: llm.QueryAPIVersion, OperationKey: "op", QueryExecutionID: "exec", Kind: llm.QueryProviderStatus, ObservedAt: "2026-07-21T00:00:00Z", Source: "persisted", Freshness: "current", Complete: true, Result: result, Cost: llm.CostV1{Status: "unknown", UnknownReason: "not_metered"}}
	if _, err := DecodeQueryResponse(wire); err == nil {
		t.Fatal("expected unknown row field to be rejected")
	}
}

func TestTypedQueryResponseRejectsNonPaginatedCursor(t *testing.T) {
	now := time.Date(2026, time.July, 21, 1, 2, 3, 0, time.UTC)
	for _, test := range []struct {
		name   string
		kind   llm.QueryKind
		result QueryResult
	}{
		{name: "budget", kind: llm.QueryBudgetStatus, result: BudgetStatusResult{ActiveAt: now, GenerationID: "generation", ManifestDigest: ManifestDigest(strings.Repeat("0", 64)), StreamHighWaterMark: "1-0", Windows: []BudgetWindow{}}},
		{name: "spend", kind: llm.QuerySpendSummary, result: SpendSummaryResult{StartTime: now.Add(-time.Hour), EndTime: now, Buckets: []SpendBucket{}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cursor := QueryCursor("unexpected-page-2")
			_, err := EncodeQueryResponse(QueryResponse{OperationKey: OperationKey(test.name), ExecutionID: "execution", Kind: test.kind, Provenance: QueryProvenance{Source: QuerySourcePersisted, Freshness: QueryFreshCurrent, ObservedAt: now}, Complete: true, NextCursor: &cursor, Result: test.result, Cost: QueryCost{Status: QueryCostExact, ActualUSD: decimalPointer("0"), Method: QueryCostControlZero}})
			if err == nil {
				t.Fatal("non-paginated query response with a cursor was accepted")
			}
		})
	}
}

func TestTypedQueryResponseRequiresStableResultOrdering(t *testing.T) {
	now := time.Date(2026, time.July, 21, 1, 2, 3, 0, time.UTC)
	tests := []struct {
		name   string
		kind   llm.QueryKind
		result QueryResult
	}{
		{
			name: "provider status",
			kind: llm.QueryProviderStatus,
			result: ProviderStatusResult{Routes: []ProviderStatusRow{
				{RouteID: "route-b", Provider: "provider", Endpoint: "endpoint", Availability: QueryAvailable, ObservedAt: now, StaleAfter: now.Add(time.Hour)},
				{RouteID: "route-a", Provider: "provider", Endpoint: "endpoint", Availability: QueryAvailable, ObservedAt: now, StaleAfter: now.Add(time.Hour)},
			}},
		},
		{
			name: "model inventory",
			kind: llm.QueryModelInventory,
			result: ModelInventoryResult{Models: []ModelInventoryRow{
				{Provider: "provider", Endpoint: "endpoint", ProviderModelID: "model-b", Lifecycle: QueryLifecycleAvailable, Capabilities: []string{}, CompleteSnapshot: true},
				{Provider: "provider", Endpoint: "endpoint", ProviderModelID: "model-a", Lifecycle: QueryLifecycleAvailable, Capabilities: []string{}, CompleteSnapshot: true},
			}},
		},
		{
			name: "credit status",
			kind: llm.QueryCreditStatus,
			result: CreditStatusResult{Endpoints: []CreditStatusRow{
				{Provider: "provider", Endpoint: "endpoint-b", Credit: QueryCreditOK, Billing: QueryBillingOK, EvidenceSource: QueryEvidenceUnknown},
				{Provider: "provider", Endpoint: "endpoint-a", Credit: QueryCreditOK, Billing: QueryBillingOK, EvidenceSource: QueryEvidenceUnknown},
			}},
		},
		{
			name: "budget status",
			kind: llm.QueryBudgetStatus,
			result: BudgetStatusResult{ActiveAt: now, GenerationID: "generation", ManifestDigest: ManifestDigest(strings.Repeat("0", 64)), StreamHighWaterMark: "1-0", Windows: []BudgetWindow{
				{PolicyKey: "policy", WindowKey: "window-b", CoverageStart: now, CoverageEnd: now.Add(time.Hour), LimitUSD: "10", ReservedCostUSD: "1", AccountedCostUSD: "2", AvailableUSD: "7"},
				{PolicyKey: "policy", WindowKey: "window-a", CoverageStart: now, CoverageEnd: now.Add(time.Hour), LimitUSD: "10", ReservedCostUSD: "1", AccountedCostUSD: "2", AvailableUSD: "7"},
			}},
		},
		{
			name: "spend summary",
			kind: llm.QuerySpendSummary,
			result: SpendSummaryResult{StartTime: now, EndTime: now.Add(time.Hour), Buckets: []SpendBucket{
				{Group: &SpendGroup{Provider: providerIDPointer("provider-b")}, KnownActualCostUSD: "1", ExactOperationCount: 1, Completeness: "complete"},
				{Group: &SpendGroup{Provider: providerIDPointer("provider-a")}, KnownActualCostUSD: "1", ExactOperationCount: 1, Completeness: "complete"},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := EncodeQueryResponse(QueryResponse{OperationKey: "op", ExecutionID: "execution", Kind: test.kind, Provenance: QueryProvenance{Source: QuerySourcePersisted, Freshness: QueryFreshCurrent, ObservedAt: now}, Complete: true, Result: test.result, Cost: QueryCost{Status: QueryCostExact, ActualUSD: decimalPointer("0"), Method: QueryCostControlZero}})
			if err == nil || !strings.Contains(err.Error(), "sorted and unique") {
				t.Fatalf("unordered result error = %v, want stable-order rejection", err)
			}
		})
	}
}

func TestTypedQueryResponseAcceptsStableSpendGroupingWithNilDimensions(t *testing.T) {
	now := time.Date(2026, time.July, 21, 1, 2, 3, 0, time.UTC)
	provider := ProviderID("provider")
	response := QueryResponse{OperationKey: "op", ExecutionID: "execution", Kind: llm.QuerySpendSummary, Provenance: QueryProvenance{Source: QuerySourcePersisted, Freshness: QueryFreshCurrent, ObservedAt: now}, Complete: true, Result: SpendSummaryResult{StartTime: now, EndTime: now.Add(time.Hour), Buckets: []SpendBucket{
		{Group: nil, KnownActualCostUSD: "0", ExactOperationCount: 0, Completeness: "complete"},
		{Group: &SpendGroup{Provider: &provider}, KnownActualCostUSD: "1", ExactOperationCount: 1, Completeness: "complete"},
	}}, Cost: QueryCost{Status: QueryCostExact, ActualUSD: decimalPointer("0"), Method: QueryCostControlZero}}
	if _, err := EncodeQueryResponse(response); err != nil {
		t.Fatalf("stable spend result with nil dimensions rejected: %v", err)
	}
}

func TestTypedQueryResponseRejectsDuplicateResultKey(t *testing.T) {
	now := time.Date(2026, time.July, 21, 1, 2, 3, 0, time.UTC)
	response := QueryResponse{
		OperationKey: "op", ExecutionID: "execution", Kind: llm.QueryProviderStatus,
		Provenance: QueryProvenance{Source: QuerySourcePersisted, Freshness: QueryFreshCurrent, ObservedAt: now},
		Complete:   true,
		Result: ProviderStatusResult{Routes: []ProviderStatusRow{
			{RouteID: "route-a", Provider: "provider", Endpoint: "endpoint", Availability: QueryAvailable, ObservedAt: now, StaleAfter: now.Add(time.Hour)},
			{RouteID: "route-a", Provider: "provider", Endpoint: "endpoint", Availability: QueryAvailable, ObservedAt: now, StaleAfter: now.Add(time.Hour)},
		}},
		Cost: QueryCost{Status: QueryCostExact, ActualUSD: decimalPointer("0"), Method: QueryCostControlZero},
	}
	if _, err := EncodeQueryResponse(response); err == nil || !strings.Contains(err.Error(), "sorted and unique") {
		t.Fatalf("duplicate result key error = %v, want stable-order rejection", err)
	}
}

func TestBudgetWindowRetryDurationUsesWireSeconds(t *testing.T) {
	window := BudgetWindow{PolicyKey: "daily", WindowKey: "hour", CoverageStart: time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC), CoverageEnd: time.Date(2026, time.July, 21, 1, 0, 0, 0, time.UTC), LimitUSD: "10", ReservedCostUSD: "1", AccountedCostUSD: "2", AvailableUSD: "7", RetryAfter: durationPointer(30 * time.Second)}
	data, err := json.Marshal(window)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) == "" || !json.Valid(data) || !containsJSONNumber(data, "retry_after_seconds", 30) {
		t.Fatalf("retry duration was not encoded as seconds: %s", data)
	}
	var decoded BudgetWindow
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.RetryAfter == nil || *decoded.RetryAfter != 30*time.Second {
		t.Fatalf("retry duration round-trip mismatch: %#v", decoded.RetryAfter)
	}
}

func testScope() QueryScope                              { return QueryScope{Tenant: "tenant", Project: "project", Actor: "actor"} }
func boolPointer(value bool) *bool                       { return &value }
func providerIDPointer(value string) *ProviderID         { typed := ProviderID(value); return &typed }
func decimalPointer(value DecimalUSD) *DecimalUSD        { return &value }
func durationPointer(value time.Duration) *time.Duration { return &value }
func containsJSONNumber(data []byte, key string, want int64) bool {
	var value map[string]json.RawMessage
	if json.Unmarshal(data, &value) != nil {
		return false
	}
	var got int64
	if json.Unmarshal(value[key], &got) != nil {
		return false
	}
	return got == want
}
