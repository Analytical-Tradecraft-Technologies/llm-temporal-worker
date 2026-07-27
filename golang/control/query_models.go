package control

// This file contains the typed control-plane view of llm.QueryRequestV1 and
// llm.QueryResponseV1.  The llm package remains the wire boundary: these
// models intentionally contain no json.RawMessage and no storage/provider
// implementation.  Encode/Decode are the only conversion points.

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

type TenantID string
type ProjectID string
type ActorID string
type OperationKey string
type QueryExecutionID string
type ProviderID string
type EndpointID string
type RouteID string
type ProviderModelID string
type ModelID string
type ModelDisplayName string
type BudgetGenerationID string
type StreamHighWaterMark string
type ModelPrefix string
type PolicyKey string
type WindowKey string
type DecimalUSD string
type ManifestDigest string
type SafeCode string
type QueryCursor string

const maxDurationSeconds int64 = 9223372036

type QueryAvailability string

const (
	QueryAvailable   QueryAvailability = "available"
	QueryDegraded    QueryAvailability = "degraded"
	QueryUnavailable QueryAvailability = "unavailable"
)

type QueryLifecycle string

const (
	QueryLifecycleAvailable   QueryLifecycle = "available"
	QueryLifecycleDeprecated  QueryLifecycle = "deprecated"
	QueryLifecycleUnavailable QueryLifecycle = "unavailable"
	QueryLifecycleUnknown     QueryLifecycle = "unknown"
)

type QueryCreditState string

const (
	QueryCreditOK        QueryCreditState = "ok"
	QueryCreditLow       QueryCreditState = "low"
	QueryCreditExhausted QueryCreditState = "exhausted"
	QueryCreditUnknown   QueryCreditState = "unknown"
)

// QueryBillingState follows the wire contract.  It deliberately does not
// alias control.BillingState, whose incident value is "issue".
type QueryBillingState string

const (
	QueryBillingOK      QueryBillingState = "ok"
	QueryBillingBlocked QueryBillingState = "blocked"
	QueryBillingUnknown QueryBillingState = "unknown"
)

type QueryCircuitState string

const (
	QueryCircuitClosed   QueryCircuitState = "closed"
	QueryCircuitOpen     QueryCircuitState = "open"
	QueryCircuitHalfOpen QueryCircuitState = "half_open"
)

type QueryEvidenceSource string

const (
	QueryEvidenceProviderAPI QueryEvidenceSource = "provider_api"
	QueryEvidenceOperator    QueryEvidenceSource = "operator"
	QueryEvidenceUnknown     QueryEvidenceSource = "unknown"
)

type QuerySource string

const (
	QuerySourcePersisted          QuerySource = "persisted"
	QuerySourcePersistedRefreshed QuerySource = "persisted_and_refreshed"
	QuerySourceRedisBudget        QuerySource = "redis_budget_generation"
)

type QueryFreshness string

const (
	QueryFreshCurrent QueryFreshness = "current"
	QueryFreshStale   QueryFreshness = "stale"
	QueryFreshUnknown QueryFreshness = "unknown"
)

type QueryCostStatus string

const (
	QueryCostExact   QueryCostStatus = "exact"
	QueryCostUnknown QueryCostStatus = "unknown"
)

type QueryCostMethod string

const (
	QueryCostControlZero QueryCostMethod = "control_query_zero"
	QueryCostProvider    QueryCostMethod = "provider_reported"
	QueryCostCatalog     QueryCostMethod = "catalog_usage"
)

type OperationKind string

const (
	OperationGenerate OperationKind = "generate"
	OperationCompact  OperationKind = "compact"
	OperationQuery    OperationKind = "query"
)

type SpendDimension string

const (
	SpendByOperation SpendDimension = "operation_kind"
	SpendByProvider  SpendDimension = "provider"
	SpendByModel     SpendDimension = "model"
)

type QueryScope struct {
	Tenant  TenantID
	Project ProjectID
	Actor   ActorID
	Tags    map[string]string
}

type QueryPage struct {
	Size   int
	Cursor *QueryCursor
}

// QueryFilter is a closed union of the five wire filter models. Its
// unexported method prevents callers from inventing a filter that the wire
// validator cannot safely encode.
type QueryFilter interface{ queryKind() llm.QueryKind }
type QueryResult interface{ queryKind() llm.QueryKind }

type ProviderStatusQuery struct {
	Provider           *ProviderID
	Endpoint           *EndpointID
	Availability       *QueryAvailability
	IncludeHealthy     *bool
	RefreshIfOlderThan time.Duration
	Page               QueryPage
}

func (ProviderStatusQuery) queryKind() llm.QueryKind { return llm.QueryProviderStatus }

type ModelInventoryQuery struct {
	Provider           *ProviderID
	Endpoint           *EndpointID
	ModelPrefix        *ModelPrefix
	Lifecycle          *QueryLifecycle
	RefreshIfOlderThan time.Duration
	Page               QueryPage
}

func (ModelInventoryQuery) queryKind() llm.QueryKind { return llm.QueryModelInventory }

type CreditStatusQuery struct {
	Provider           *ProviderID
	Endpoint           *EndpointID
	IncludeOK          *bool
	RefreshIfOlderThan time.Duration
	Page               QueryPage
}

func (CreditStatusQuery) queryKind() llm.QueryKind { return llm.QueryCreditStatus }

type BudgetStatusQuery struct {
	PolicyKey      *PolicyKey
	ActiveAt       *time.Time
	IncludeWindows *bool
}

func (BudgetStatusQuery) queryKind() llm.QueryKind { return llm.QueryBudgetStatus }

type SpendSummaryQuery struct {
	StartTime      time.Time
	EndTime        time.Time
	GroupBy        []SpendDimension
	OperationKinds []OperationKind
}

func (SpendSummaryQuery) queryKind() llm.QueryKind { return llm.QuerySpendSummary }

type QueryRequest struct {
	OperationKey OperationKey
	Scope        QueryScope
	Kind         llm.QueryKind
	Filter       QueryFilter
}

type ProviderStatusRow struct {
	RouteID      RouteID            `json:"route_id"`
	Provider     ProviderID         `json:"provider"`
	Endpoint     EndpointID         `json:"endpoint"`
	Availability QueryAvailability  `json:"availability"`
	Credit       *QueryCreditState  `json:"credit_state,omitempty"`
	Billing      *QueryBillingState `json:"billing_state,omitempty"`
	Circuit      *QueryCircuitState `json:"circuit_state,omitempty"`
	ObservedAt   time.Time          `json:"observed_at"`
	StaleAfter   time.Time          `json:"stale_after"`
	SafeCode     *SafeCode          `json:"safe_code,omitempty"`
}

type ProviderStatusResult struct{ Routes []ProviderStatusRow }

func (ProviderStatusResult) queryKind() llm.QueryKind { return llm.QueryProviderStatus }

type ModelInventoryRow struct {
	Provider         ProviderID        `json:"provider"`
	Endpoint         EndpointID        `json:"endpoint"`
	ProviderModelID  ProviderModelID   `json:"provider_model_id"`
	DisplayName      *ModelDisplayName `json:"display_name,omitempty"`
	Lifecycle        QueryLifecycle    `json:"lifecycle"`
	Capabilities     []string          `json:"capabilities"`
	CompleteSnapshot bool              `json:"complete_snapshot"`
}
type ModelInventoryResult struct{ Models []ModelInventoryRow }

func (ModelInventoryResult) queryKind() llm.QueryKind { return llm.QueryModelInventory }

type CreditStatusRow struct {
	Provider         ProviderID          `json:"provider"`
	Endpoint         EndpointID          `json:"endpoint"`
	Credit           QueryCreditState    `json:"credit_state"`
	Billing          QueryBillingState   `json:"billing_state"`
	ConfirmedAt      *time.Time          `json:"confirmed_at,omitempty"`
	EvidenceSource   QueryEvidenceSource `json:"evidence_source"`
	SafeEvidenceCode *SafeCode           `json:"safe_evidence_code,omitempty"`
}
type CreditStatusResult struct{ Endpoints []CreditStatusRow }

func (CreditStatusResult) queryKind() llm.QueryKind { return llm.QueryCreditStatus }

type BudgetWindow struct {
	PolicyKey        PolicyKey      `json:"policy_key"`
	WindowKey        WindowKey      `json:"window_key"`
	CoverageStart    time.Time      `json:"coverage_start"`
	CoverageEnd      time.Time      `json:"coverage_end"`
	LimitUSD         DecimalUSD     `json:"limit_usd"`
	ReservedCostUSD  DecimalUSD     `json:"reserved_cost_usd"`
	AccountedCostUSD DecimalUSD     `json:"accounted_cost_usd"`
	AvailableUSD     DecimalUSD     `json:"available_usd"`
	RetryAfter       *time.Duration `json:"retry_after_seconds,omitempty"`
}

func (window BudgetWindow) MarshalJSON() ([]byte, error) {
	type wire struct {
		PolicyKey        PolicyKey  `json:"policy_key"`
		WindowKey        WindowKey  `json:"window_key"`
		CoverageStart    time.Time  `json:"coverage_start"`
		CoverageEnd      time.Time  `json:"coverage_end"`
		LimitUSD         DecimalUSD `json:"limit_usd"`
		ReservedCostUSD  DecimalUSD `json:"reserved_cost_usd"`
		AccountedCostUSD DecimalUSD `json:"accounted_cost_usd"`
		AvailableUSD     DecimalUSD `json:"available_usd"`
		RetryAfter       *int64     `json:"retry_after_seconds,omitempty"`
	}
	var retry *int64
	if window.RetryAfter != nil {
		if int64(*window.RetryAfter/time.Second) > maxDurationSeconds {
			return nil, fmt.Errorf("retry_after is too large")
		}
		seconds := int64(*window.RetryAfter / time.Second)
		if seconds < 0 || time.Duration(seconds)*time.Second != *window.RetryAfter {
			return nil, fmt.Errorf("retry_after must be a nonnegative whole number of seconds")
		}
		retry = &seconds
	}
	return json.Marshal(wire{window.PolicyKey, window.WindowKey, window.CoverageStart, window.CoverageEnd, window.LimitUSD, window.ReservedCostUSD, window.AccountedCostUSD, window.AvailableUSD, retry})
}

func (window *BudgetWindow) UnmarshalJSON(data []byte) error {
	var value struct {
		PolicyKey        PolicyKey  `json:"policy_key"`
		WindowKey        WindowKey  `json:"window_key"`
		CoverageStart    time.Time  `json:"coverage_start"`
		CoverageEnd      time.Time  `json:"coverage_end"`
		LimitUSD         DecimalUSD `json:"limit_usd"`
		ReservedCostUSD  DecimalUSD `json:"reserved_cost_usd"`
		AccountedCostUSD DecimalUSD `json:"accounted_cost_usd"`
		AvailableUSD     DecimalUSD `json:"available_usd"`
		RetryAfter       *int64     `json:"retry_after_seconds"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	var retry *time.Duration
	if value.RetryAfter != nil {
		if *value.RetryAfter < 0 {
			return fmt.Errorf("retry_after_seconds must be nonnegative")
		}
		if *value.RetryAfter > maxDurationSeconds {
			return fmt.Errorf("retry_after_seconds is too large")
		}
		d := time.Duration(*value.RetryAfter) * time.Second
		retry = &d
	}
	*window = BudgetWindow{PolicyKey: value.PolicyKey, WindowKey: value.WindowKey, CoverageStart: value.CoverageStart, CoverageEnd: value.CoverageEnd, LimitUSD: value.LimitUSD, ReservedCostUSD: value.ReservedCostUSD, AccountedCostUSD: value.AccountedCostUSD, AvailableUSD: value.AvailableUSD, RetryAfter: retry}
	return nil
}

type BudgetStatusResult struct {
	ActiveAt            time.Time           `json:"active_at"`
	GenerationID        BudgetGenerationID  `json:"generation_id"`
	ManifestDigest      ManifestDigest      `json:"manifest_digest"`
	StreamHighWaterMark StreamHighWaterMark `json:"stream_high_water_mark"`
	Windows             []BudgetWindow      `json:"windows"`
}

func (BudgetStatusResult) queryKind() llm.QueryKind { return llm.QueryBudgetStatus }

type SpendGroup struct {
	OperationKind *OperationKind   `json:"operation_kind,omitempty"`
	Provider      *ProviderID      `json:"provider,omitempty"`
	Model         *ProviderModelID `json:"model,omitempty"`
}
type SpendBucket struct {
	Group                 *SpendGroup `json:"group,omitempty"`
	KnownActualCostUSD    DecimalUSD  `json:"known_actual_cost_usd"`
	ExactOperationCount   int64       `json:"exact_operation_count"`
	UnknownOperationCount int64       `json:"unknown_operation_count"`
	Completeness          string      `json:"completeness"`
}
type SpendSummaryResult struct {
	StartTime time.Time     `json:"start_time"`
	EndTime   time.Time     `json:"end_time"`
	Buckets   []SpendBucket `json:"buckets"`
}

func (SpendSummaryResult) queryKind() llm.QueryKind { return llm.QuerySpendSummary }

type QueryProvenance struct {
	Source     QuerySource
	Freshness  QueryFreshness
	ObservedAt time.Time
}
type QueryCost struct {
	Status        QueryCostStatus
	ActualUSD     *DecimalUSD
	Method        QueryCostMethod
	UnknownReason string
}
type QueryResponse struct {
	OperationKey OperationKey
	ExecutionID  QueryExecutionID
	Kind         llm.QueryKind
	Provenance   QueryProvenance
	Complete     bool
	NextCursor   *QueryCursor
	Result       QueryResult
	Cost         QueryCost
}

type requestWire struct {
	Provider       string             `json:"provider,omitempty"`
	Endpoint       string             `json:"endpoint,omitempty"`
	Availability   *QueryAvailability `json:"availability,omitempty"`
	IncludeHealthy *bool              `json:"include_healthy,omitempty"`
	IncludeOK      *bool              `json:"include_ok,omitempty"`
	ModelPrefix    *ModelPrefix       `json:"model_prefix,omitempty"`
	Lifecycle      *QueryLifecycle    `json:"lifecycle,omitempty"`
	Refresh        *int64             `json:"refresh_if_older_than_seconds,omitempty"`
	PolicyKey      *PolicyKey         `json:"policy_key,omitempty"`
	ActiveAt       *time.Time         `json:"active_at,omitempty"`
	IncludeWindows *bool              `json:"include_windows,omitempty"`
	PageSize       *int               `json:"page_size,omitempty"`
	Cursor         *QueryCursor       `json:"cursor,omitempty"`
	StartTime      *time.Time         `json:"start_time,omitempty"`
	EndTime        *time.Time         `json:"end_time,omitempty"`
	GroupBy        []SpendDimension   `json:"group_by,omitempty"`
	OperationKinds []OperationKind    `json:"operation_kinds,omitempty"`
}

func EncodeQueryRequest(request QueryRequest) (llm.QueryRequestV1, error) {
	kind, ok := filterKind(request.Filter)
	if !ok || kind != request.Kind || request.OperationKey == "" {
		return llm.QueryRequestV1{}, fmt.Errorf("typed query request is incomplete")
	}
	filter, err := encodeFilter(request.Filter)
	if err != nil {
		return llm.QueryRequestV1{}, err
	}
	wire := llm.QueryRequestV1{APIVersion: llm.QueryAPIVersion, OperationKey: string(request.OperationKey), Context: llm.RequestContext{Tenant: string(request.Scope.Tenant), Project: string(request.Scope.Project), Actor: string(request.Scope.Actor), Tags: cloneTags(request.Scope.Tags)}, Kind: request.Kind, Query: filter}
	if _, err := json.Marshal(wire); err != nil {
		return llm.QueryRequestV1{}, fmt.Errorf("typed query request: %w", err)
	}
	return wire, nil
}

func DecodeQueryRequest(wire llm.QueryRequestV1) (QueryRequest, error) {
	if wire.APIVersion != llm.QueryAPIVersion {
		return QueryRequest{}, fmt.Errorf("wire query request api version %q is unsupported", wire.APIVersion)
	}
	if _, err := json.Marshal(wire); err != nil {
		return QueryRequest{}, fmt.Errorf("wire query request: %w", err)
	}
	filter, err := decodeFilter(wire.Kind, wire.Query)
	if err != nil {
		return QueryRequest{}, err
	}
	return QueryRequest{OperationKey: OperationKey(wire.OperationKey), Scope: QueryScope{Tenant: TenantID(wire.Context.Tenant), Project: ProjectID(wire.Context.Project), Actor: ActorID(wire.Context.Actor), Tags: cloneTags(wire.Context.Tags)}, Kind: wire.Kind, Filter: filter}, nil
}

func encodeFilter(filter QueryFilter) ([]byte, error) {
	var value any
	switch typed := filter.(type) {
	case ProviderStatusQuery:
		if err := validateRefreshDuration(typed.RefreshIfOlderThan); err != nil {
			return nil, err
		}
		value = providerRequestWire(typed)
	case ModelInventoryQuery:
		if err := validateRefreshDuration(typed.RefreshIfOlderThan); err != nil {
			return nil, err
		}
		value = modelRequestWire(typed)
	case CreditStatusQuery:
		if err := validateRefreshDuration(typed.RefreshIfOlderThan); err != nil {
			return nil, err
		}
		value = creditRequestWire(typed)
	case BudgetStatusQuery:
		value = budgetRequestWire(typed)
	case SpendSummaryQuery:
		value = spendRequestWire(typed)
	case *ProviderStatusQuery:
		if typed == nil {
			return nil, fmt.Errorf("nil typed query filter")
		}
		if err := validateRefreshDuration(typed.RefreshIfOlderThan); err != nil {
			return nil, err
		}
		value = providerRequestWire(*typed)
	case *ModelInventoryQuery:
		if typed == nil {
			return nil, fmt.Errorf("nil typed query filter")
		}
		if err := validateRefreshDuration(typed.RefreshIfOlderThan); err != nil {
			return nil, err
		}
		value = modelRequestWire(*typed)
	case *CreditStatusQuery:
		if typed == nil {
			return nil, fmt.Errorf("nil typed query filter")
		}
		if err := validateRefreshDuration(typed.RefreshIfOlderThan); err != nil {
			return nil, err
		}
		value = creditRequestWire(*typed)
	case *BudgetStatusQuery:
		if typed == nil {
			return nil, fmt.Errorf("nil typed query filter")
		}
		value = budgetRequestWire(*typed)
	case *SpendSummaryQuery:
		if typed == nil {
			return nil, fmt.Errorf("nil typed query filter")
		}
		value = spendRequestWire(*typed)
	default:
		return nil, fmt.Errorf("unknown typed query filter")
	}
	return json.Marshal(value)
}

func providerRequestWire(value ProviderStatusQuery) requestWire {
	return requestWire{Provider: stringValue(value.Provider), Endpoint: stringValue(value.Endpoint), Availability: value.Availability, IncludeHealthy: value.IncludeHealthy, Refresh: seconds(value.RefreshIfOlderThan), PageSize: pageSize(value.Page), Cursor: value.Page.Cursor}
}
func modelRequestWire(value ModelInventoryQuery) requestWire {
	return requestWire{Provider: stringValue(value.Provider), Endpoint: stringValue(value.Endpoint), ModelPrefix: value.ModelPrefix, Lifecycle: value.Lifecycle, Refresh: seconds(value.RefreshIfOlderThan), PageSize: pageSize(value.Page), Cursor: value.Page.Cursor}
}
func creditRequestWire(value CreditStatusQuery) requestWire {
	return requestWire{Provider: stringValue(value.Provider), Endpoint: stringValue(value.Endpoint), IncludeOK: value.IncludeOK, Refresh: seconds(value.RefreshIfOlderThan), PageSize: pageSize(value.Page), Cursor: value.Page.Cursor}
}
func budgetRequestWire(value BudgetStatusQuery) requestWire {
	return requestWire{PolicyKey: value.PolicyKey, ActiveAt: value.ActiveAt, IncludeWindows: value.IncludeWindows}
}
func spendRequestWire(value SpendSummaryQuery) requestWire {
	return requestWire{StartTime: &value.StartTime, EndTime: &value.EndTime, GroupBy: value.GroupBy, OperationKinds: value.OperationKinds}
}

func validateRefreshDuration(value time.Duration) error {
	if value == 0 {
		return nil
	}
	if value < time.Second || value%time.Second != 0 || value > 86400*time.Second {
		return fmt.Errorf("refresh_if_older_than must be a whole number of seconds between 1 and 86400")
	}
	return nil
}

func decodeFilter(kind llm.QueryKind, raw json.RawMessage) (QueryFilter, error) {
	var value requestWire
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("typed query filter: %w", err)
	}
	switch kind {
	case llm.QueryProviderStatus:
		return ProviderStatusQuery{Provider: stringID[ProviderID](value.Provider), Endpoint: stringID[EndpointID](value.Endpoint), Availability: value.Availability, IncludeHealthy: value.IncludeHealthy, RefreshIfOlderThan: duration(value.Refresh), Page: QueryPage{Size: valueOrZero(value.PageSize), Cursor: value.Cursor}}, nil
	case llm.QueryModelInventory:
		return ModelInventoryQuery{Provider: stringID[ProviderID](value.Provider), Endpoint: stringID[EndpointID](value.Endpoint), ModelPrefix: value.ModelPrefix, Lifecycle: value.Lifecycle, RefreshIfOlderThan: duration(value.Refresh), Page: QueryPage{Size: valueOrZero(value.PageSize), Cursor: value.Cursor}}, nil
	case llm.QueryCreditStatus:
		return CreditStatusQuery{Provider: stringID[ProviderID](value.Provider), Endpoint: stringID[EndpointID](value.Endpoint), IncludeOK: value.IncludeOK, RefreshIfOlderThan: duration(value.Refresh), Page: QueryPage{Size: valueOrZero(value.PageSize), Cursor: value.Cursor}}, nil
	case llm.QueryBudgetStatus:
		return BudgetStatusQuery{PolicyKey: value.PolicyKey, ActiveAt: value.ActiveAt, IncludeWindows: value.IncludeWindows}, nil
	case llm.QuerySpendSummary:
		return SpendSummaryQuery{StartTime: valueOrZeroTime(value.StartTime), EndTime: valueOrZeroTime(value.EndTime), GroupBy: value.GroupBy, OperationKinds: value.OperationKinds}, nil
	default:
		return nil, fmt.Errorf("unknown query kind %q", kind)
	}
}

// The response conversion uses json as a strict, already-validated wire
// boundary.  llm.QueryResponseV1.UnmarshalJSON validates every row before we
// decode it into typed values.
func DecodeQueryResponse(wire llm.QueryResponseV1) (QueryResponse, error) {
	if wire.APIVersion != llm.QueryAPIVersion {
		return QueryResponse{}, fmt.Errorf("wire query response api version %q is unsupported", wire.APIVersion)
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return QueryResponse{}, fmt.Errorf("wire query response: %w", err)
	}
	var fields struct {
		APIVersion       string          `json:"api_version"`
		OperationKey     string          `json:"operation_key"`
		QueryExecutionID string          `json:"query_execution_id"`
		Kind             llm.QueryKind   `json:"kind"`
		ObservedAt       string          `json:"observed_at"`
		Source           QuerySource     `json:"source"`
		Freshness        QueryFreshness  `json:"freshness"`
		Complete         bool            `json:"complete"`
		NextCursor       *QueryCursor    `json:"next_cursor"`
		Result           json.RawMessage `json:"result"`
		CostStatus       QueryCostStatus `json:"cost_status"`
		ActualCostUSD    *DecimalUSD     `json:"actual_cost_usd"`
		CostMethod       QueryCostMethod `json:"cost_method"`
		UnknownReason    string          `json:"cost_unknown_reason_code"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		return QueryResponse{}, err
	}
	result, err := decodeResult(fields.Kind, fields.Result)
	if err != nil {
		return QueryResponse{}, err
	}
	observed, err := time.Parse(time.RFC3339Nano, fields.ObservedAt)
	if err != nil {
		return QueryResponse{}, err
	}
	return QueryResponse{OperationKey: OperationKey(fields.OperationKey), ExecutionID: QueryExecutionID(fields.QueryExecutionID), Kind: fields.Kind, Provenance: QueryProvenance{Source: fields.Source, Freshness: fields.Freshness, ObservedAt: observed}, Complete: fields.Complete, NextCursor: fields.NextCursor, Result: result, Cost: QueryCost{Status: fields.CostStatus, ActualUSD: fields.ActualCostUSD, Method: fields.CostMethod, UnknownReason: fields.UnknownReason}}, nil
}

func EncodeQueryResponse(response QueryResponse) (llm.QueryResponseV1, error) {
	kind, ok := resultKind(response.Result)
	if !ok || kind != response.Kind || response.OperationKey == "" || response.ExecutionID == "" || response.Provenance.ObservedAt.IsZero() {
		return llm.QueryResponseV1{}, fmt.Errorf("typed query response is incomplete")
	}
	if err := validateQueryResultOrdering(response.Kind, response.Result); err != nil {
		return llm.QueryResponseV1{}, err
	}
	result, err := encodeResult(response.Kind, response.Result)
	if err != nil {
		return llm.QueryResponseV1{}, err
	}
	wire := llm.QueryResponseV1{APIVersion: llm.QueryAPIVersion, OperationKey: string(response.OperationKey), QueryExecutionID: string(response.ExecutionID), Kind: response.Kind, ObservedAt: response.Provenance.ObservedAt.UTC().Format(time.RFC3339Nano), Source: string(response.Provenance.Source), Freshness: string(response.Provenance.Freshness), Complete: response.Complete, NextCursor: stringPtr(response.NextCursor), Result: result, Cost: llm.CostV1{Status: string(response.Cost.Status), ActualCostUSD: decimalPtr(response.Cost.ActualUSD), Method: string(response.Cost.Method), UnknownReason: string(response.Cost.UnknownReason)}}
	if _, err := json.Marshal(wire); err != nil {
		return llm.QueryResponseV1{}, fmt.Errorf("typed query response: %w", err)
	}
	return wire, nil
}

func encodeResult(kind llm.QueryKind, value QueryResult) (llm.QueryResult, error) {
	if _, ok := resultKind(value); !ok {
		return nil, fmt.Errorf("nil typed query result")
	}
	// Pointers are accepted for ergonomic parity with the request models; the
	// closed wire value is still reconstructed below.
	switch typed := value.(type) {
	case *ProviderStatusResult:
		value = *typed
	case *ModelInventoryResult:
		value = *typed
	case *CreditStatusResult:
		value = *typed
	case *BudgetStatusResult:
		value = *typed
	case *SpendSummaryResult:
		value = *typed
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	switch kind {
	case llm.QueryProviderStatus:
		var rows struct {
			Routes []json.RawMessage `json:"routes"`
		}
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, err
		}
		return llm.ProviderStatusPage{Routes: rows.Routes}, nil
	case llm.QueryModelInventory:
		var rows struct {
			Models []json.RawMessage `json:"models"`
		}
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, err
		}
		return llm.ModelInventoryPage{Models: rows.Models}, nil
	case llm.QueryCreditStatus:
		var rows struct {
			Endpoints []json.RawMessage `json:"endpoints"`
		}
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, err
		}
		return llm.CreditStatusPage{Endpoints: rows.Endpoints}, nil
	case llm.QueryBudgetStatus:
		var value llm.BudgetStatus
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		return value, nil
	case llm.QuerySpendSummary:
		var value llm.SpendSummary
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unknown query result kind %q", kind)
	}
}

func decodeResult(kind llm.QueryKind, raw json.RawMessage) (QueryResult, error) {
	switch kind {
	case llm.QueryProviderStatus:
		var page struct {
			Routes []json.RawMessage `json:"routes"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, err
		}
		rows := make([]ProviderStatusRow, len(page.Routes))
		for i, r := range page.Routes {
			if err := json.Unmarshal(r, &rows[i]); err != nil {
				return nil, fmt.Errorf("route %d: %w", i, err)
			}
		}
		return ProviderStatusResult{Routes: rows}, nil
	case llm.QueryModelInventory:
		var page struct {
			Models []json.RawMessage `json:"models"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, err
		}
		rows := make([]ModelInventoryRow, len(page.Models))
		for i, r := range page.Models {
			if err := json.Unmarshal(r, &rows[i]); err != nil {
				return nil, fmt.Errorf("model %d: %w", i, err)
			}
		}
		return ModelInventoryResult{Models: rows}, nil
	case llm.QueryCreditStatus:
		var page struct {
			Endpoints []json.RawMessage `json:"endpoints"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, err
		}
		rows := make([]CreditStatusRow, len(page.Endpoints))
		for i, r := range page.Endpoints {
			if err := json.Unmarshal(r, &rows[i]); err != nil {
				return nil, fmt.Errorf("endpoint %d: %w", i, err)
			}
		}
		return CreditStatusResult{Endpoints: rows}, nil
	case llm.QueryBudgetStatus:
		var value struct {
			ActiveAt            time.Time           `json:"active_at"`
			GenerationID        BudgetGenerationID  `json:"generation_id"`
			ManifestDigest      ManifestDigest      `json:"manifest_digest"`
			StreamHighWaterMark StreamHighWaterMark `json:"stream_high_water_mark"`
			Windows             []json.RawMessage   `json:"windows"`
		}
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		windows := make([]BudgetWindow, len(value.Windows))
		for i, r := range value.Windows {
			if err := json.Unmarshal(r, &windows[i]); err != nil {
				return nil, fmt.Errorf("window %d: %w", i, err)
			}
		}
		return BudgetStatusResult{ActiveAt: value.ActiveAt, GenerationID: value.GenerationID, ManifestDigest: value.ManifestDigest, StreamHighWaterMark: value.StreamHighWaterMark, Windows: windows}, nil
	case llm.QuerySpendSummary:
		var value struct {
			StartTime time.Time         `json:"start_time"`
			EndTime   time.Time         `json:"end_time"`
			Buckets   []json.RawMessage `json:"buckets"`
		}
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		buckets := make([]SpendBucket, len(value.Buckets))
		for i, r := range value.Buckets {
			if err := json.Unmarshal(r, &buckets[i]); err != nil {
				return nil, fmt.Errorf("bucket %d: %w", i, err)
			}
		}
		return SpendSummaryResult{StartTime: value.StartTime, EndTime: value.EndTime, Buckets: buckets}, nil
	default:
		return nil, fmt.Errorf("unknown query result kind %q", kind)
	}
}

func cloneTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
}
func stringValue[T ~string](value *T) string {
	if value == nil {
		return ""
	}
	return string(*value)
}
func stringID[T ~string](value string) *T {
	if value == "" {
		return nil
	}
	v := T(value)
	return &v
}
func seconds(value time.Duration) *int64 {
	if value == 0 {
		return nil
	}
	v := int64(value / time.Second)
	return &v
}
func duration(value *int64) time.Duration {
	if value == nil {
		return 0
	}
	return time.Duration(*value) * time.Second
}
func pageSize(page QueryPage) *int {
	if page.Size == 0 {
		return nil
	}
	v := page.Size
	return &v
}
func valueOrZero(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}
func valueOrZeroTime(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return *value
}

func filterKind(filter QueryFilter) (llm.QueryKind, bool) {
	switch value := filter.(type) {
	case nil:
		return "", false
	case *ProviderStatusQuery:
		if value == nil {
			return "", false
		}
		return value.queryKind(), true
	case *ModelInventoryQuery:
		if value == nil {
			return "", false
		}
		return value.queryKind(), true
	case *CreditStatusQuery:
		if value == nil {
			return "", false
		}
		return value.queryKind(), true
	case *BudgetStatusQuery:
		if value == nil {
			return "", false
		}
		return value.queryKind(), true
	case *SpendSummaryQuery:
		if value == nil {
			return "", false
		}
		return value.queryKind(), true
	default:
		return filter.queryKind(), true
	}
}

func resultKind(result QueryResult) (llm.QueryKind, bool) {
	switch value := result.(type) {
	case nil:
		return "", false
	case *ProviderStatusResult:
		if value == nil {
			return "", false
		}
		return value.queryKind(), true
	case *ModelInventoryResult:
		if value == nil {
			return "", false
		}
		return value.queryKind(), true
	case *CreditStatusResult:
		if value == nil {
			return "", false
		}
		return value.queryKind(), true
	case *BudgetStatusResult:
		if value == nil {
			return "", false
		}
		return value.queryKind(), true
	case *SpendSummaryResult:
		if value == nil {
			return "", false
		}
		return value.queryKind(), true
	default:
		return result.queryKind(), true
	}
}
func stringPtr(value *QueryCursor) *string {
	if value == nil {
		return nil
	}
	v := string(*value)
	return &v
}
func decimalPtr(value *DecimalUSD) *string {
	if value == nil {
		return nil
	}
	v := string(*value)
	return &v
}
func canonicalStrings(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
