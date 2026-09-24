package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// MaxQueryAuditJSONBytes bounds the redacted control JSON handed to an audit
// sink. Query payloads are already bounded by the Activity contract; this
// second limit keeps a misconfigured sink from receiving an unbounded record.
const MaxQueryAuditJSONBytes = 256 * 1024

// QueryAuditRecord is the storage-neutral record emitted after a query result
// has passed response and cursor validation. RequestJSON and ResponseJSON are
// canonical closed control envelopes; they contain no provider prompt/body or
// credential material. A PostgreSQL adapter can map these fields to
// storage.QueryExecutionRequest at the composition boundary.
type QueryAuditRecord struct {
	Tenant                string
	Project               string
	OperationKey          string
	RequestFingerprint    [32]byte
	APIVersion            string
	Kind                  llm.QueryKind
	RequestJSON           []byte
	ResponseJSON          []byte
	ResponseDigest        [32]byte
	Source                string
	ActualCostUSD         *string
	CostStatus            string
	CostMethod            string
	CostUnknownReasonCode string
	StartedAt             time.Time
	CompletedAt           time.Time
}

// AuditFunc observes a validated query on a best-effort basis. An audit failure
// must never fail or retry an otherwise successful query. Implementations should
// log bounded metadata and return promptly; they must not require durable storage.
// The error result is retained for adapters, but Execute deliberately ignores it.
type AuditFunc func(context.Context, QueryAuditRecord) error

func buildQueryAudit(request llm.QueryRequestV1, requestJSON []byte, response llm.QueryResponseV1, startedAt, completedAt time.Time) (QueryAuditRecord, error) {
	canonicalRequest, err := canonicalRawQueryAuditJSON("request", requestJSON)
	if err != nil {
		return QueryAuditRecord{}, err
	}
	responseJSON, err := canonicalQueryAuditJSON("response", response)
	if err != nil {
		return QueryAuditRecord{}, err
	}
	var actualCost *string
	if response.Cost.ActualCostUSD != nil {
		value := *response.Cost.ActualCostUSD
		actualCost = &value
	}
	return QueryAuditRecord{
		Tenant: request.Context.Tenant, Project: request.Context.Project,
		OperationKey: request.OperationKey, RequestFingerprint: sha256.Sum256(canonicalRequest),
		APIVersion: request.APIVersion, Kind: request.Kind, RequestJSON: canonicalRequest,
		ResponseJSON: responseJSON, ResponseDigest: sha256.Sum256(responseJSON),
		Source: response.Source, ActualCostUSD: actualCost,
		CostStatus: response.Cost.Status, CostMethod: response.Cost.Method,
		CostUnknownReasonCode: response.Cost.UnknownReason,
		StartedAt:             startedAt.UTC(), CompletedAt: completedAt.UTC(),
	}, nil
}

func canonicalQueryAuditJSON(name string, value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("query audit %s JSON: %w", name, err)
	}
	return canonicalRawQueryAuditJSON(name, raw)
}

func canonicalRawQueryAuditJSON(name string, raw []byte) ([]byte, error) {
	canonical, err := llm.CanonicalJSONWithLimits(raw, MaxQueryAuditJSONBytes, 64)
	if err != nil {
		return nil, fmt.Errorf("query audit %s JSON: %w", name, err)
	}
	return canonical, nil
}
