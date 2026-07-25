package runtime

// This file composes the storage-neutral typed query contract with the
// PostgreSQL read pages. Budget status is exposed only when a snapshot-scoped
// Redis generation reader is supplied. Spend is read from the PostgreSQL
// ledgers when an authenticated deployment supplies the scope-ID resolver.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

// QueryServiceBuilder composes a QueryService for one immutable config
// snapshot. Deployments use this seam to provide authorization, cursor keys,
// and the query audit callback without the production factory inventing
// security material or silently accepting unaudited reads.
type QueryServiceBuilder func(context.Context, *config.Snapshot, PostgresQueryRepositories) (activity.QueryService, error)

type providerStatusReader interface {
	ListRouteStatuses(context.Context, postgresstore.ProviderStatusListOptions) (postgresstore.ProviderStatusPage, error)
	ListCreditStatuses(context.Context, postgresstore.CreditStatusListOptions) (control.CreditStatusPage, error)
}

type inventoryReader interface {
	ListInventoryModels(context.Context, postgresstore.InventoryModelListOptions) (postgresstore.InventoryModelPage, error)
}

type spendSummaryReader interface {
	ListSpendSummary(context.Context, postgresstore.SpendSummaryListOptions) (control.SpendSummaryResult, error)
}

// BudgetStatusReader is the snapshot-scoped Redis read seam for the budget
// status query. The reader owns the Redis generation/manifest/window
// validation: it must reject a requested instant outside the active coverage,
// bind the result to the active generation and Stream high-water mark, and
// never consult PostgreSQL budget tables. Keeping this contract above the
// storage package avoids inventing a Redis window-hash field layout before
// the generation materializer publishes that format.
type BudgetStatusReader interface {
	ReadBudgetStatus(context.Context, control.BudgetStatusQuery, time.Time) (control.BudgetStatusResult, error)
}

// QueryScopeResolver maps an already-authorized tenant/project scope to its
// opaque PostgreSQL scope ID. The runtime never derives or guesses this ID:
// deployments must supply the same keyed resolver used by their durable
// repositories. Returning uuid.Nil or an error fails the query closed.
type QueryScopeResolver func(context.Context, control.QueryScope) (uuid.UUID, error)

// PersistedQueryOptions are the mandatory security and observability seams
// for NewPersistedQueryService. No default authorization, cursor key, or
// audit sink is safe to infer from a config snapshot.
type PersistedQueryOptions struct {
	Authorize    control.AuthorizeFunc
	Cursor       *control.CursorCodec
	Audit        control.AuditFunc
	Clock        func() time.Time
	ResolveScope QueryScopeResolver
	// BudgetStatus is required to expose budget_status. It must be backed by
	// the active Redis generation and must not fall back to PostgreSQL.
	BudgetStatus BudgetStatusReader
}

// NewPersistedQueryService builds the persisted query families against
// one snapshot digest. Missing repository capabilities remain fail-closed;
// callers must not receive an empty answer that could be mistaken for state.
func NewPersistedQueryService(snapshot *config.Snapshot, repositories PostgresQueryRepositories, options PersistedQueryOptions) (activity.QueryService, error) {
	if snapshot == nil {
		return nil, errors.New("persisted query snapshot is nil")
	}
	if options.Authorize == nil {
		return nil, errors.New("persisted query authorization is required")
	}
	if options.Cursor == nil || len(options.Cursor.Key) == 0 {
		return nil, errors.New("persisted query cursor codec is required")
	}
	if options.Audit == nil {
		return nil, errors.New("persisted query audit sink is required")
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	return &control.QueryService{
		TypedHandler: &persistedQueryHandler{
			configDigest: snapshot.Digest(),
			provider:     repositories.ProviderStatus,
			inventory:    repositories.Inventory,
			spend:        repositories.SpendSummary,
			budget:       options.BudgetStatus,
			resolveScope: options.ResolveScope,
			cursor:       options.Cursor,
			clock:        clock,
		},
		Authorize:   options.Authorize,
		Audit:       options.Audit,
		CursorCodec: options.Cursor,
		Clock:       clock,
	}, nil
}

type persistedQueryHandler struct {
	configDigest [32]byte
	provider     providerStatusReader
	inventory    inventoryReader
	spend        spendSummaryReader
	budget       BudgetStatusReader
	resolveScope QueryScopeResolver
	cursor       *control.CursorCodec
	clock        func() time.Time
}

var _ control.TypedHandler = (*persistedQueryHandler)(nil)

func (handler *persistedQueryHandler) ExecuteTypedQuery(ctx context.Context, request control.QueryRequest, claims *control.BoundCursorClaims) (control.QueryResponse, error) {
	if handler == nil || handler.clock == nil {
		return control.QueryResponse{}, errors.New("persisted query handler is not configured")
	}
	now := handler.clock().UTC()
	if now.IsZero() {
		return control.QueryResponse{}, errors.New("persisted query clock returned zero")
	}
	if refreshRequested(request.Filter) {
		return control.QueryResponse{}, unsupportedQuery(request.Kind, "refresh is not configured for the persisted-only composition")
	}
	switch request.Kind {
	case llm.QueryProviderStatus:
		return handler.providerStatus(ctx, request, claims, now)
	case llm.QueryModelInventory:
		return handler.modelInventory(ctx, request, claims, now)
	case llm.QueryCreditStatus:
		return handler.creditStatus(ctx, request, claims, now)
	case llm.QuerySpendSummary:
		return handler.spendSummary(ctx, request, now)
	case llm.QueryBudgetStatus:
		return handler.budgetStatus(ctx, request, now)
	default:
		return control.QueryResponse{}, unsupportedQuery(request.Kind, "query kind is not configured")
	}
}

func (handler *persistedQueryHandler) budgetStatus(ctx context.Context, request control.QueryRequest, now time.Time) (control.QueryResponse, error) {
	if handler.budget == nil {
		return control.QueryResponse{}, unsupportedQuery(request.Kind, "Redis budget status reader is not configured")
	}
	query, ok := request.Filter.(control.BudgetStatusQuery)
	if !ok {
		return control.QueryResponse{}, fmt.Errorf("budget status filter has unexpected type")
	}
	activeAt := now
	if query.ActiveAt != nil {
		activeAt = query.ActiveAt.UTC()
		if activeAt.IsZero() {
			return control.QueryResponse{}, fmt.Errorf("budget status active_at is invalid")
		}
	}
	result, err := handler.budget.ReadBudgetStatus(ctx, query, activeAt)
	if err != nil {
		return control.QueryResponse{}, err
	}
	if result.ActiveAt.IsZero() || !result.ActiveAt.UTC().Equal(activeAt) {
		return control.QueryResponse{}, fmt.Errorf("budget status reader returned a mismatched active_at")
	}
	return handler.response(request, result, control.QueryFreshCurrent, activeAt, nil), nil
}

func (handler *persistedQueryHandler) spendSummary(ctx context.Context, request control.QueryRequest, now time.Time) (control.QueryResponse, error) {
	if handler.spend == nil {
		return control.QueryResponse{}, unsupportedQuery(request.Kind, "spend summary repository is not configured")
	}
	if handler.resolveScope == nil {
		return control.QueryResponse{}, unsupportedQuery(request.Kind, "spend summary scope resolver is not configured")
	}
	query, ok := request.Filter.(control.SpendSummaryQuery)
	if !ok {
		return control.QueryResponse{}, fmt.Errorf("spend summary filter has unexpected type")
	}
	scopeID, err := handler.resolveScope(ctx, request.Scope)
	if err != nil {
		return control.QueryResponse{}, fmt.Errorf("resolve spend summary scope: %w", err)
	}
	if scopeID == uuid.Nil {
		return control.QueryResponse{}, errors.New("resolve spend summary scope: resolver returned nil scope id")
	}
	result, err := handler.spend.ListSpendSummary(ctx, postgresstore.SpendSummaryListOptions{
		ScopeID: scopeID, StartTime: query.StartTime, EndTime: query.EndTime,
		GroupBy:        append([]control.SpendDimension(nil), query.GroupBy...),
		OperationKinds: append([]control.OperationKind(nil), query.OperationKinds...),
	})
	if err != nil {
		return control.QueryResponse{}, err
	}
	return handler.response(request, result, control.QueryFreshCurrent, now, nil), nil
}

func (handler *persistedQueryHandler) providerStatus(ctx context.Context, request control.QueryRequest, claims *control.BoundCursorClaims, now time.Time) (control.QueryResponse, error) {
	if handler.provider == nil {
		return control.QueryResponse{}, unsupportedQuery(request.Kind, "provider status repository is not configured")
	}
	query, ok := request.Filter.(control.ProviderStatusQuery)
	if !ok {
		return control.QueryResponse{}, fmt.Errorf("provider status filter has unexpected type")
	}
	horizon := now
	after := ""
	if claims != nil {
		horizon, after = claims.Horizon, claims.Position
	}
	var availability control.Availability
	if query.Availability != nil {
		availability = control.Availability(*query.Availability)
	}
	page, err := handler.provider.ListRouteStatuses(ctx, postgresstore.ProviderStatusListOptions{
		ConfigDigest: handler.configDigest, Provider: stringValue(query.Provider), EndpointID: stringValue(query.Endpoint),
		Availability: availability, IncludeHealthy: boolValue(query.IncludeHealthy), SnapshotHorizon: horizon,
		AfterRouteID: after, Limit: query.Page.Size,
	})
	if err != nil {
		return control.QueryResponse{}, err
	}
	rows := make([]control.ProviderStatusRow, 0, len(page.Routes))
	freshness := control.QueryFreshCurrent
	for _, status := range page.Routes {
		row := control.ProviderStatusRow{RouteID: control.RouteID(status.RouteID), Provider: control.ProviderID(status.Provider), Endpoint: control.EndpointID(status.EndpointID), Availability: control.QueryAvailability(status.Availability), ObservedAt: status.ObservedAt.UTC(), StaleAfter: status.StaleAfter.UTC()}
		credit := control.QueryCreditState(status.Credit)
		billing := control.QueryBillingState(status.Billing)
		circuit := control.QueryCircuitState(status.Circuit)
		row.Credit, row.Billing, row.Circuit = &credit, &billing, &circuit
		rows = append(rows, row)
		if staleAt(now, status.StaleAfter) {
			freshness = control.QueryFreshStale
		}
	}
	var cursor *control.QueryCursor
	if page.NextRouteID != "" {
		signed, signErr := handler.sign(request, page.NextRouteID, horizon, now)
		if signErr != nil {
			return control.QueryResponse{}, signErr
		}
		cursor = &signed
	}
	return handler.response(request, control.ProviderStatusResult{Routes: rows}, freshness, horizon, cursor), nil
}

func (handler *persistedQueryHandler) modelInventory(ctx context.Context, request control.QueryRequest, claims *control.BoundCursorClaims, now time.Time) (control.QueryResponse, error) {
	if handler.inventory == nil {
		return control.QueryResponse{}, unsupportedQuery(request.Kind, "model inventory repository is not configured")
	}
	query, ok := request.Filter.(control.ModelInventoryQuery)
	if !ok {
		return control.QueryResponse{}, fmt.Errorf("model inventory filter has unexpected type")
	}
	horizon := time.Time{}
	position := postgresstore.InventoryModelPosition{}
	if claims != nil {
		horizon = claims.Horizon
		var err error
		position, err = decodeInventoryPosition(claims.Position)
		if err != nil {
			return control.QueryResponse{}, fmt.Errorf("inventory cursor: %w", err)
		}
	}
	var lifecycle control.Lifecycle
	if query.Lifecycle != nil {
		lifecycle = control.Lifecycle(*query.Lifecycle)
	}
	page, err := handler.inventory.ListInventoryModels(ctx, postgresstore.InventoryModelListOptions{
		ConfigDigest: handler.configDigest, Provider: stringValue(query.Provider), EndpointID: stringValue(query.Endpoint), ModelPrefix: stringValue(query.ModelPrefix), Lifecycle: lifecycle,
		SnapshotHorizon: horizon, After: position, Limit: query.Page.Size,
	})
	if err != nil {
		return control.QueryResponse{}, err
	}
	if page.SnapshotHorizon.IsZero() {
		// Empty inventories still need a non-zero horizon if a future storage
		// implementation ever emits a continuation for the same request.
		page.SnapshotHorizon = now
	}
	freshness := control.QueryFreshCurrent
	rows := make([]control.ModelInventoryRow, 0, len(page.Models))
	for _, record := range page.Models {
		if record.Snapshot.ProvenanceAt(now) != control.ProvenanceCurrent {
			freshness = control.QueryFreshStale
		}
		var display *control.ModelDisplayName
		if record.Model.DisplayName != "" {
			value := control.ModelDisplayName(record.Model.DisplayName)
			display = &value
		}
		lifecycle := control.QueryLifecycle(record.Model.Lifecycle)
		rows = append(rows, control.ModelInventoryRow{Provider: control.ProviderID(record.Snapshot.Provider), Endpoint: control.EndpointID(record.Snapshot.EndpointID), ProviderModelID: control.ProviderModelID(record.Model.ProviderModelID), DisplayName: display, Lifecycle: lifecycle, Capabilities: []string{}, CompleteSnapshot: record.Snapshot.Complete})
	}
	var cursor *control.QueryCursor
	if page.Next != nil {
		position := encodeInventoryPosition(*page.Next)
		signed, signErr := handler.sign(request, position, page.SnapshotHorizon, now)
		if signErr != nil {
			return control.QueryResponse{}, signErr
		}
		cursor = &signed
	}
	return handler.response(request, control.ModelInventoryResult{Models: rows}, freshness, page.SnapshotHorizon, cursor), nil
}

func (handler *persistedQueryHandler) creditStatus(ctx context.Context, request control.QueryRequest, claims *control.BoundCursorClaims, now time.Time) (control.QueryResponse, error) {
	if handler.provider == nil {
		return control.QueryResponse{}, unsupportedQuery(request.Kind, "credit status repository is not configured")
	}
	query, ok := request.Filter.(control.CreditStatusQuery)
	if !ok {
		return control.QueryResponse{}, fmt.Errorf("credit status filter has unexpected type")
	}
	horizon, after := now, ""
	if claims != nil {
		horizon, after = claims.Horizon, claims.Position
	}
	page, err := handler.provider.ListCreditStatuses(ctx, postgresstore.CreditStatusListOptions{ConfigDigest: handler.configDigest, Provider: stringValue(query.Provider), EndpointID: stringValue(query.Endpoint), IncludeOK: boolValue(query.IncludeOK), SnapshotHorizon: horizon, AfterEndpointKey: after, Limit: query.Page.Size})
	if err != nil {
		return control.QueryResponse{}, err
	}
	freshness := control.QueryFreshCurrent
	rows := make([]control.CreditStatusRow, 0, len(page.Endpoints))
	for _, status := range page.Endpoints {
		credit := control.QueryCreditState(status.Credit)
		billing := control.QueryBillingState(status.BillingWireState())
		source := control.QueryEvidenceSource(status.EvidenceSource)
		var confirmed *time.Time
		if !status.ConfirmedAt.IsZero() {
			value := status.ConfirmedAt.UTC()
			confirmed = &value
		}
		var safe *control.SafeCode
		if status.SafeEvidenceCode != "" {
			value := control.SafeCode(status.SafeEvidenceCode)
			safe = &value
		}
		rows = append(rows, control.CreditStatusRow{Provider: control.ProviderID(status.Provider), Endpoint: control.EndpointID(status.EndpointID), Credit: credit, Billing: billing, ConfirmedAt: confirmed, EvidenceSource: source, SafeEvidenceCode: safe})
		if staleAt(now, status.StaleAfter) {
			freshness = control.QueryFreshStale
		}
	}
	var cursor *control.QueryCursor
	if page.NextEndpointKey != "" {
		signed, signErr := handler.sign(request, page.NextEndpointKey, horizon, now)
		if signErr != nil {
			return control.QueryResponse{}, signErr
		}
		cursor = &signed
	}
	return handler.response(request, control.CreditStatusResult{Endpoints: rows}, freshness, horizon, cursor), nil
}

func (handler *persistedQueryHandler) response(request control.QueryRequest, result control.QueryResult, freshness control.QueryFreshness, observed time.Time, cursor *control.QueryCursor) control.QueryResponse {
	zero := control.DecimalUSD("0")
	return control.QueryResponse{OperationKey: request.OperationKey, ExecutionID: executionID(request), Kind: request.Kind, Provenance: control.QueryProvenance{Source: control.QuerySourcePersisted, Freshness: freshness, ObservedAt: observed.UTC()}, Complete: cursor == nil, NextCursor: cursor, Result: result, Cost: control.QueryCost{Status: control.QueryCostExact, ActualUSD: &zero, Method: control.QueryCostControlZero}}
}

func (handler *persistedQueryHandler) sign(request control.QueryRequest, position string, horizon, issuedAt time.Time) (control.QueryCursor, error) {
	if handler == nil || handler.cursor == nil {
		return "", control.ErrQueryCursor
	}
	return handler.cursor.Sign(request, position, horizon, issuedAt)
}

func unsupportedQuery(kind llm.QueryKind, reason string) error {
	return provider.NewError(provider.CodeUnsupportedCapability, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, fmt.Sprintf("%s: %s", kind, reason))
}

func refreshRequested(filter control.QueryFilter) bool {
	switch value := filter.(type) {
	case control.ProviderStatusQuery:
		return value.RefreshIfOlderThan > 0
	case control.ModelInventoryQuery:
		return value.RefreshIfOlderThan > 0
	case control.CreditStatusQuery:
		return value.RefreshIfOlderThan > 0
	default:
		return false
	}
}

func staleAt(now, staleAfter time.Time) bool { return !staleAfter.IsZero() && !now.Before(staleAfter) }

func stringValue[T ~string](value *T) string {
	if value == nil {
		return ""
	}
	return string(*value)
}

func boolValue(value *bool) bool { return value != nil && *value }

func executionID(request control.QueryRequest) control.QueryExecutionID {
	wire, _ := control.EncodeQueryRequest(request)
	data, _ := json.Marshal(wire)
	digest := sha256.Sum256(append([]byte("llm-temporal-worker/query/v1/"), data...))
	return control.QueryExecutionID(hex.EncodeToString(digest[:]))
}

func encodeInventoryPosition(position postgresstore.InventoryModelPosition) string {
	return strings.Join([]string{position.Provider, position.EndpointID, position.SnapshotID.String(), position.ProviderModelID}, "\x00")
}

func decodeInventoryPosition(value string) (postgresstore.InventoryModelPosition, error) {
	parts := strings.Split(value, "\x00")
	if len(parts) != 4 || parts[0] == "" || parts[1] == "" || parts[3] == "" {
		return postgresstore.InventoryModelPosition{}, errors.New("position is incomplete")
	}
	snapshotID, err := uuid.Parse(parts[2])
	if err != nil || snapshotID == uuid.Nil {
		return postgresstore.InventoryModelPosition{}, errors.New("snapshot id is invalid")
	}
	return postgresstore.InventoryModelPosition{Provider: parts[0], EndpointID: parts[1], SnapshotID: snapshotID, ProviderModelID: parts[3]}, nil
}
