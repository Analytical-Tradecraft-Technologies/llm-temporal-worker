package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

type fakePersistedProvider struct {
	mu       sync.Mutex
	status   postgresstore.ProviderStatusPage
	credit   control.CreditStatusPage
	lastOpts postgresstore.ProviderStatusListOptions
}

type fakeSpendSummary struct {
	result   control.SpendSummaryResult
	lastOpts postgresstore.SpendSummaryListOptions
}

type fakeBudgetStatus struct {
	result   control.BudgetStatusResult
	query    control.BudgetStatusQuery
	activeAt time.Time
	calls    int
}

func (fake *fakeBudgetStatus) ReadBudgetStatus(_ context.Context, query control.BudgetStatusQuery, activeAt time.Time) (control.BudgetStatusResult, error) {
	fake.query = query
	fake.activeAt = activeAt
	fake.calls++
	return fake.result, nil
}

func (fake *fakeSpendSummary) ListSpendSummary(_ context.Context, options postgresstore.SpendSummaryListOptions) (control.SpendSummaryResult, error) {
	fake.lastOpts = options
	return fake.result, nil
}

func (fake *fakePersistedProvider) ListRouteStatuses(_ context.Context, options postgresstore.ProviderStatusListOptions) (postgresstore.ProviderStatusPage, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.lastOpts = options
	return fake.status, nil
}

func (fake *fakePersistedProvider) ListCreditStatuses(_ context.Context, _ postgresstore.CreditStatusListOptions) (control.CreditStatusPage, error) {
	return fake.credit, nil
}

func persistedQueryTestService(t *testing.T, providerReader *fakePersistedProvider, audit control.AuditFunc) *control.QueryService {
	t.Helper()
	now := time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC)
	codec := &control.CursorCodec{Key: []byte("query-test-key"), TTL: time.Hour, MaxPosition: 128}
	handler := &persistedQueryHandler{configDigest: sha256.Sum256([]byte("snapshot")), provider: providerReader, cursor: codec, clock: func() time.Time { return now }}
	if audit == nil {
		audit = func(context.Context, control.QueryAuditRecord) error { return nil }
	}
	return &control.QueryService{TypedHandler: handler, Authorize: func(context.Context, control.Authorization) error { return nil }, Audit: audit, CursorCodec: codec, Clock: func() time.Time { return now }}
}

func providerQueryRequest(t *testing.T, cursor *control.QueryCursor) llm.QueryRequestV1 {
	t.Helper()
	request, err := control.EncodeQueryRequest(control.QueryRequest{OperationKey: "query-op", Scope: control.QueryScope{Tenant: "tenant", Project: "project", Actor: "actor"}, Kind: llm.QueryProviderStatus, Filter: control.ProviderStatusQuery{IncludeHealthy: boolPtr(true), Page: control.QueryPage{Size: 1, Cursor: cursor}}})
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func boolPtr(value bool) *bool { return &value }

func TestPersistedQueryProviderStatusIsAuditedAndCursorBound(t *testing.T) {
	observed := time.Date(2026, time.July, 21, 23, 0, 0, 0, time.UTC)
	providerReader := &fakePersistedProvider{status: postgresstore.ProviderStatusPage{Routes: []control.RouteStatus{{RouteID: "route-a", EndpointID: "endpoint-a", Provider: "provider-a", Availability: control.AvailabilityAvailable, Credit: control.CreditOK, Billing: control.BillingOK, Circuit: control.CircuitClosed, ObservedAt: observed, StaleAfter: observed.Add(time.Hour)}}}}
	var audited control.QueryAuditRecord
	service := persistedQueryTestService(t, providerReader, func(_ context.Context, record control.QueryAuditRecord) error { audited = record; return nil })
	first, err := service.Execute(context.Background(), providerQueryRequest(t, nil))
	if err != nil {
		t.Fatalf("first query: %v", err)
	}
	if first.NextCursor != nil || !first.Complete || audited.Kind != llm.QueryProviderStatus || audited.Source != string(control.QuerySourcePersisted) || audited.ActualCostUSD == nil || *audited.ActualCostUSD != "0" {
		t.Fatalf("unexpected first response/audit: response=%+v audit=%+v", first, audited)
	}
	if providerReader.lastOpts.SnapshotHorizon.IsZero() || providerReader.lastOpts.ConfigDigest == ([32]byte{}) {
		t.Fatalf("handler did not bind snapshot options: %+v", providerReader.lastOpts)
	}

	providerReader.status.NextRouteID = "route-b"
	first, err = service.Execute(context.Background(), providerQueryRequest(t, nil))
	if err != nil || first.NextCursor == nil || first.Complete {
		t.Fatalf("cursor page: response=%+v err=%v", first, err)
	}
	providerReader.status.NextRouteID = ""
	next := control.QueryCursor(*first.NextCursor)
	second, err := service.Execute(context.Background(), providerQueryRequest(t, &next))
	if err != nil {
		t.Fatalf("continuation query: %v", err)
	}
	if second.NextCursor != nil || providerReader.lastOpts.AfterRouteID != "route-b" {
		t.Fatalf("continuation was not bound: response=%+v options=%+v", second, providerReader.lastOpts)
	}
}

func TestPersistedQueryBudgetStatusUsesRedisReaderContract(t *testing.T) {
	activeAt := time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC)
	policy := control.PolicyKey("daily")
	includeWindows := true
	reader := &fakeBudgetStatus{result: control.BudgetStatusResult{
		ActiveAt:            activeAt,
		GenerationID:        control.BudgetGenerationID("generation-1"),
		ManifestDigest:      control.ManifestDigest(strings.Repeat("a", 64)),
		StreamHighWaterMark: control.StreamHighWaterMark("42-0"),
		Windows: []control.BudgetWindow{{
			PolicyKey:        policy,
			WindowKey:        control.WindowKey("hour"),
			CoverageStart:    activeAt.Add(-time.Hour),
			CoverageEnd:      activeAt.Add(time.Hour),
			LimitUSD:         "10",
			ReservedCostUSD:  "1",
			AccountedCostUSD: "2",
			AvailableUSD:     "7",
		}},
	}}
	codec := &control.CursorCodec{Key: []byte("query-test-key"), TTL: time.Hour, MaxPosition: 128}
	handler := &persistedQueryHandler{budget: reader, cursor: codec, clock: func() time.Time { return activeAt.Add(5 * time.Minute) }}
	var audited control.QueryAuditRecord
	service := &control.QueryService{TypedHandler: handler, Authorize: func(context.Context, control.Authorization) error { return nil }, Audit: func(_ context.Context, record control.QueryAuditRecord) error { audited = record; return nil }, CursorCodec: codec, Clock: func() time.Time { return activeAt.Add(5 * time.Minute) }}
	request, err := control.EncodeQueryRequest(control.QueryRequest{
		OperationKey: "budget-op",
		Scope:        control.QueryScope{Tenant: "tenant", Project: "project", Actor: "actor"},
		Kind:         llm.QueryBudgetStatus,
		Filter:       control.BudgetStatusQuery{PolicyKey: &policy, ActiveAt: &activeAt, IncludeWindows: &includeWindows},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("budget status query: %v", err)
	}
	decoded, err := control.DecodeQueryResponse(response)
	if err != nil {
		t.Fatalf("decode budget status response: %v", err)
	}
	result, ok := decoded.Result.(control.BudgetStatusResult)
	if !ok || result.GenerationID != "generation-1" || len(result.Windows) != 1 || result.Windows[0].AvailableUSD != "7" {
		t.Fatalf("unexpected budget status result: %#v", decoded.Result)
	}
	if decoded.Provenance.Source != control.QuerySourceRedisBudget {
		t.Fatalf("budget status provenance source = %q, want %q", decoded.Provenance.Source, control.QuerySourceRedisBudget)
	}
	if audited.Source != string(control.QuerySourceRedisBudget) {
		t.Fatalf("budget status audit source = %q, want %q", audited.Source, control.QuerySourceRedisBudget)
	}
	if reader.calls != 1 || !reader.activeAt.Equal(activeAt) || reader.query.PolicyKey == nil || *reader.query.PolicyKey != policy || reader.query.IncludeWindows == nil || !*reader.query.IncludeWindows {
		t.Fatalf("reader was not bound to query/instant: calls=%d active_at=%s query=%#v", reader.calls, reader.activeAt, reader.query)
	}
}

func TestPersistedQueryBudgetStatusRejectsMismatchedReaderInstant(t *testing.T) {
	requested := time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC)
	reader := &fakeBudgetStatus{result: control.BudgetStatusResult{
		ActiveAt:            requested.Add(time.Second),
		GenerationID:        control.BudgetGenerationID("generation-1"),
		ManifestDigest:      control.ManifestDigest(strings.Repeat("a", 64)),
		StreamHighWaterMark: control.StreamHighWaterMark("42-0"),
	}}
	codec := &control.CursorCodec{Key: []byte("query-test-key"), TTL: time.Hour, MaxPosition: 128}
	handler := &persistedQueryHandler{budget: reader, cursor: codec, clock: func() time.Time { return requested }}
	service := &control.QueryService{TypedHandler: handler, Authorize: func(context.Context, control.Authorization) error { return nil }, CursorCodec: codec, Clock: func() time.Time { return requested }}
	request, err := control.EncodeQueryRequest(control.QueryRequest{
		OperationKey: "budget-op",
		Scope:        control.QueryScope{Tenant: "tenant", Project: "project", Actor: "actor"},
		Kind:         llm.QueryBudgetStatus,
		Filter:       control.BudgetStatusQuery{ActiveAt: &requested},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Execute(context.Background(), request); err == nil {
		t.Fatal("mismatched budget reader instant unexpectedly accepted")
	}
}

func TestNewPersistedQueryServiceBuilderBindsSnapshotRedisBudgetReader(t *testing.T) {
	authorize := func(context.Context, control.Authorization) error { return nil }
	reader := &fakeBudgetStatus{}
	audit := &postgresstore.QueryExecutionRepository{}
	builder, err := NewPersistedQueryServiceBuilder(PersistedQueryBuilderOptions{
		Authorize: authorize,
		Cursor:    &control.CursorCodec{Key: []byte("query-builder-key"), TTL: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := builder(context.Background(), &config.Snapshot{}, PostgresQueryRepositories{
		QueryAudit:   audit,
		BudgetStatus: reader,
	})
	if err != nil {
		t.Fatalf("builder() error = %v", err)
	}
	queryService, ok := service.(*control.QueryService)
	if !ok {
		t.Fatalf("query service = %T, want *control.QueryService", service)
	}
	handler, ok := queryService.TypedHandler.(*persistedQueryHandler)
	if !ok {
		t.Fatalf("typed handler = %T, want *persistedQueryHandler", queryService.TypedHandler)
	}
	if handler.budget != reader {
		t.Fatalf("budget reader = %T (%p), want snapshot reader %p", handler.budget, handler.budget, reader)
	}
}

func TestPersistedQueryBudgetAndSpendFailClosed(t *testing.T) {
	service := persistedQueryTestService(t, &fakePersistedProvider{}, nil)
	for _, kind := range []llm.QueryKind{llm.QueryBudgetStatus, llm.QuerySpendSummary} {
		request, err := control.EncodeQueryRequest(control.QueryRequest{OperationKey: "query-op", Scope: control.QueryScope{Tenant: "tenant", Project: "project", Actor: "actor"}, Kind: kind, Filter: budgetOrSpendFilter(kind)})
		if err != nil {
			t.Fatal(err)
		}
		_, err = service.Execute(context.Background(), request)
		var providerErr *provider.Error
		if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeUnsupportedCapability || providerErr.Retry != provider.RetryNever {
			t.Fatalf("kind %s error=%v, want non-retryable unsupported capability", kind, err)
		}
	}
}

func TestPersistedQuerySpendSummaryUsesExplicitScopeResolver(t *testing.T) {
	const expectedTenant = "tenant"
	const expectedProject = "project"
	scopeID := uuid.MustParse("019c6e27-e55b-73d1-87d8-4e01f1f75043")
	start := time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	spend := &fakeSpendSummary{result: control.SpendSummaryResult{
		StartTime: start, EndTime: end,
		Buckets: []control.SpendBucket{{KnownActualCostUSD: "1.25", ExactOperationCount: 1, Completeness: "complete"}},
	}}
	codec := &control.CursorCodec{Key: []byte("query-test-key"), TTL: time.Hour, MaxPosition: 128}
	now := end.Add(time.Minute)
	var resolved control.QueryScope
	handler := &persistedQueryHandler{
		spend: spend,
		resolveScope: func(_ context.Context, scope control.QueryScope) (uuid.UUID, error) {
			resolved = scope
			return scopeID, nil
		},
		cursor: codec,
		clock:  func() time.Time { return now },
	}
	service := &control.QueryService{TypedHandler: handler, Authorize: func(context.Context, control.Authorization) error { return nil }, CursorCodec: codec, Clock: func() time.Time { return now }}
	request, err := control.EncodeQueryRequest(control.QueryRequest{
		OperationKey: "spend-op",
		Scope:        control.QueryScope{Tenant: expectedTenant, Project: expectedProject, Actor: "actor"},
		Kind:         llm.QuerySpendSummary,
		Filter:       control.SpendSummaryQuery{StartTime: start, EndTime: end, GroupBy: []control.SpendDimension{control.SpendByProvider}, OperationKinds: []control.OperationKind{control.OperationGenerate}},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("spend summary query: %v", err)
	}
	decoded, err := control.DecodeQueryResponse(response)
	if err != nil {
		t.Fatalf("decode spend summary response: %v", err)
	}
	result, ok := decoded.Result.(control.SpendSummaryResult)
	if !ok || len(result.Buckets) != 1 || result.Buckets[0].KnownActualCostUSD != "1.25" {
		t.Fatalf("unexpected spend summary result: %#v", decoded.Result)
	}
	if resolved.Tenant != expectedTenant || resolved.Project != expectedProject {
		t.Fatalf("resolver received wrong scope: %#v", resolved)
	}
	if spend.lastOpts.ScopeID != scopeID || !spend.lastOpts.StartTime.Equal(start) || !spend.lastOpts.EndTime.Equal(end) || len(spend.lastOpts.GroupBy) != 1 || spend.lastOpts.GroupBy[0] != control.SpendByProvider || len(spend.lastOpts.OperationKinds) != 1 || spend.lastOpts.OperationKinds[0] != control.OperationGenerate {
		t.Fatalf("repository options were not scope/filter bound: %#v", spend.lastOpts)
	}
}

func TestPersistedQuerySpendSummaryFailsClosedWithoutScopeResolver(t *testing.T) {
	spend := &fakeSpendSummary{}
	service := persistedQueryTestService(t, &fakePersistedProvider{}, nil)
	service.TypedHandler = &persistedQueryHandler{
		spend:  spend,
		cursor: service.CursorCodec,
		clock:  func() time.Time { return time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC) },
	}
	request, err := control.EncodeQueryRequest(control.QueryRequest{
		OperationKey: "spend-op",
		Scope:        control.QueryScope{Tenant: "tenant", Project: "project", Actor: "actor"},
		Kind:         llm.QuerySpendSummary,
		Filter:       control.SpendSummaryQuery{StartTime: time.Unix(1, 0).UTC(), EndTime: time.Unix(2, 0).UTC()},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Execute(context.Background(), request)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeUnsupportedCapability || providerErr.Retry != provider.RetryNever {
		t.Fatalf("error=%v, want non-retryable unsupported capability", err)
	}
}

func budgetOrSpendFilter(kind llm.QueryKind) control.QueryFilter {
	if kind == llm.QueryBudgetStatus {
		return control.BudgetStatusQuery{}
	}
	return control.SpendSummaryQuery{StartTime: time.Unix(1, 0).UTC(), EndTime: time.Unix(2, 0).UTC()}
}

func TestNewPersistedQueryServiceRequiresSecuritySeams(t *testing.T) {
	if _, err := NewPersistedQueryService(nil, PostgresQueryRepositories{}, PersistedQueryOptions{}); err == nil {
		t.Fatal("nil snapshot unexpectedly accepted")
	}
}

func TestNewPersistedQueryServiceNormalizesMissingOptionalCapabilities(t *testing.T) {
	var budget *fakeBudgetStatus
	codec := &control.CursorCodec{Key: []byte("query-test-key"), TTL: time.Hour}
	service, err := NewPersistedQueryService(&config.Snapshot{}, PostgresQueryRepositories{}, PersistedQueryOptions{
		Authorize:    func(context.Context, control.Authorization) error { return nil },
		Cursor:       codec,
		Audit:        func(context.Context, control.QueryAuditRecord) error { return nil },
		BudgetStatus: budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := service.(*control.QueryService).TypedHandler.(*persistedQueryHandler)
	if handler.provider != nil || handler.inventory != nil || handler.spend != nil || handler.budget != nil {
		t.Fatalf("missing optional capabilities were not normalized: %#v", handler)
	}
	request, err := control.EncodeQueryRequest(control.QueryRequest{
		OperationKey: "budget-op",
		Scope:        control.QueryScope{Tenant: "tenant", Project: "project", Actor: "actor"},
		Kind:         llm.QueryBudgetStatus,
		Filter:       control.BudgetStatusQuery{},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Execute(context.Background(), request)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeUnsupportedCapability || providerErr.Retry != provider.RetryNever {
		t.Fatalf("typed-nil budget reader error = %v, want non-retryable unsupported capability", err)
	}
}

func TestNewPersistedQueryServiceBuilderRequiresDeploymentSecurityInputs(t *testing.T) {
	authorize := func(context.Context, control.Authorization) error { return nil }
	codec := &control.CursorCodec{Key: []byte("query-builder-key"), TTL: time.Hour}
	for _, test := range []struct {
		name    string
		options PersistedQueryBuilderOptions
	}{
		{name: "authorization", options: PersistedQueryBuilderOptions{Cursor: codec}},
		{name: "cursor", options: PersistedQueryBuilderOptions{Authorize: authorize}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewPersistedQueryServiceBuilder(test.options); err == nil || !strings.Contains(err.Error(), test.name) {
				t.Fatalf("NewPersistedQueryServiceBuilder() error = %v, want missing %s", err, test.name)
			}
		})
	}
}

func TestPersistedQueryServiceBuilderRequiresAndBindsSnapshotAuditRepository(t *testing.T) {
	authorize := func(context.Context, control.Authorization) error { return nil }
	cursorKey := []byte("query-builder-key")
	builder, err := NewPersistedQueryServiceBuilder(PersistedQueryBuilderOptions{
		Authorize: authorize,
		Cursor:    &control.CursorCodec{Key: cursorKey, TTL: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	cursorKey[0] = 'X'

	if _, err := builder(context.Background(), &config.Snapshot{}, PostgresQueryRepositories{}); err == nil || !strings.Contains(err.Error(), "audit repository") {
		t.Fatalf("builder without audit repository error = %v", err)
	}
	var nilAudit *postgresstore.QueryExecutionRepository
	if _, err := builder(context.Background(), &config.Snapshot{}, PostgresQueryRepositories{QueryAudit: nilAudit}); err == nil || !strings.Contains(err.Error(), "audit repository") {
		t.Fatalf("builder with typed-nil audit repository error = %v", err)
	}

	audit := &postgresstore.QueryExecutionRepository{}
	scopeID := uuid.MustParse("019c9aaf-77f7-7d7f-92c0-b53eb2ed3c47")
	service, err := builder(context.Background(), &config.Snapshot{}, PostgresQueryRepositories{
		QueryAudit: audit,
		ScopeResolver: func(context.Context, control.QueryScope) (uuid.UUID, error) {
			return scopeID, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	queryService, ok := service.(*control.QueryService)
	if !ok {
		t.Fatalf("query service = %T, want *control.QueryService", service)
	}
	if err := queryService.Audit(context.Background(), control.QueryAuditRecord{}); err == nil || !strings.Contains(err.Error(), "query audit request JSON") {
		t.Fatalf("bound PostgreSQL audit adapter error = %v, want request validation", err)
	}
	handler := queryService.TypedHandler.(*persistedQueryHandler)
	if got, err := handler.resolveScope(context.Background(), control.QueryScope{Tenant: "tenant", Project: "project"}); err != nil || got != scopeID {
		t.Fatalf("snapshot scope resolver = %s, %v; want %s", got, err, scopeID)
	}
	if got := string(queryService.CursorCodec.Key); got != "query-builder-key" {
		t.Fatalf("snapshot cursor key = %q, want immutable builder copy", got)
	}
	queryService.CursorCodec.Key[0] = 'Y'
	nextService, err := builder(context.Background(), &config.Snapshot{}, PostgresQueryRepositories{QueryAudit: audit})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(nextService.(*control.QueryService).CursorCodec.Key); got != "query-builder-key" {
		t.Fatalf("reloaded snapshot cursor key = %q, want independent builder copy", got)
	}
}
