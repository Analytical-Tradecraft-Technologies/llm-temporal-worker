package postgres

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestQueryExecutionPersistenceIntegration(t *testing.T) {
	ctx, namespace, pool, cleanup := providerControlIntegrationPool(t)
	defer cleanup()
	keys := Keyring{Active: "query-v1", Keys: map[string][]byte{"query-v1": []byte("01234567890123456789012345678901")}}
	scopeKeys := ScopeKeyring{ActiveVersion: "scope-v1", Keys: map[string][]byte{"scope-v1": []byte("abcdefghijklmnopqrstuvwxyz123456")}}
	scopes := DefaultScopeRepository(pool, namespace, scopeKeys)
	repository := DefaultQueryExecutionRepository(pool, namespace, keys, scopes)
	repository.Now = func() time.Time { return time.Date(2026, time.July, 20, 8, 0, 0, 0, time.UTC) }
	request := validQueryExecutionRequest(repository.Now())
	request.Tenant = "query-ledger-" + time.Now().UTC().Format("150405.000000000")
	request.ResponseJSON = []byte(`{"routes": [], "kind": "provider_status", "api_version": "llm.temporal/query/v1"}`)
	request.ResponseDigest = sha256.Sum256([]byte(canonicalQueryJSON(request.ResponseJSON)))
	record, err := repository.Record(ctx, request)
	if err != nil {
		t.Fatalf("record query execution: %v", err)
	}
	if record.ID == [16]byte{} || record.ScopeID == [16]byte{} {
		t.Fatalf("record identity was empty: %#v", record)
	}
	if record.CostStatus != "exact" || record.CostMethod != "control_query_zero" || record.ActualCostUSD == nil || !record.ActualCostUSD.IsZero() {
		t.Fatalf("record cost = %#v", record)
	}

	replayRequest := request
	replayRequest.ResponseJSON = []byte(`{"api_version":"llm.temporal/query/v1","kind":"provider_status","routes":[{"route_id":"stored"}]}`)
	replayRequest.ResponseDigest = sha256.Sum256(replayRequest.ResponseJSON)
	replay, err := repository.Record(ctx, replayRequest)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if replay.ID != record.ID || canonicalQueryJSON(replay.ResponseJSON) != canonicalQueryJSON(record.ResponseJSON) || sha256.Sum256(replay.ResponseJSON) != replay.ResponseDigest {
		t.Fatalf("replay changed stored result: first=%#v replay=%#v", record, replay)
	}

	conflict := request
	conflict.RequestJSON = []byte(`{"api_version":"llm.temporal/query/v1","kind":"provider_status","query":{"include_healthy":true}}`)
	conflict.RequestFingerprint = sha256.Sum256(conflict.RequestJSON)
	if _, err := repository.Record(ctx, conflict); !errors.Is(err, ErrQueryExecutionConflict) {
		t.Fatalf("request fingerprint conflict error = %v", err)
	}

	unknown := request
	unknown.OperationKey = "unknown-cost"
	unknown.CostStatus = "unknown"
	unknown.ActualCostUSD = nil
	unknown.CostMethod = ""
	unknown.CostUnknownReasonCode = "provider_charge_unavailable"
	unknownRecord, err := repository.Record(ctx, unknown)
	if err != nil {
		t.Fatalf("record unknown-cost query: %v", err)
	}
	if unknownRecord.ActualCostUSD != nil || unknownRecord.CostMethod != "" || unknownRecord.CostUnknownReasonCode != "provider_charge_unavailable" {
		t.Fatalf("unknown cost record = %#v", unknownRecord)
	}

	executions, err := namespace.Render("query_executions")
	if err != nil {
		t.Fatal(err)
	}
	var storedRequest, storedResponse, storedActual, storedStatus, storedMethod string
	var operationHMAC, fingerprintHMAC, responseDigest []byte
	if err := pool.QueryRow(ctx, "SELECT operation_key_hmac, request_fingerprint_hmac, request_jsonb::text, response_jsonb::text, response_digest, COALESCE(actual_cost_usd::text,''), cost_status, COALESCE(cost_method,'') FROM "+executions+" WHERE query_execution_id=$1", record.ID).Scan(&operationHMAC, &fingerprintHMAC, &storedRequest, &storedResponse, &responseDigest, &storedActual, &storedStatus, &storedMethod); err != nil {
		t.Fatal(err)
	}
	if len(operationHMAC) != 32 || len(fingerprintHMAC) != 32 || len(responseDigest) != 32 {
		t.Fatalf("stored digest lengths = %d, %d, %d", len(operationHMAC), len(fingerprintHMAC), len(responseDigest))
	}
	if storedStatus != "exact" || storedMethod != "control_query_zero" || storedActual != "0.000000000000000000" {
		t.Fatalf("stored exact cost = %q, %q, %q", storedActual, storedStatus, storedMethod)
	}
	if storedRequest == "" || storedResponse == "" {
		t.Fatal("stored control JSON or response digest is empty")
	}
	mutatedStoredRequest := []byte(`{"api_version":"llm.temporal/query/v1","kind":"provider_status","query":{"include_healthy":true}}`)
	if _, err := pool.Exec(ctx, "UPDATE "+executions+" SET request_jsonb=$1::jsonb WHERE query_execution_id=$2", mutatedStoredRequest, record.ID); err != nil {
		t.Fatalf("mutate stored request fixture: %v", err)
	}
	if _, err := repository.Record(ctx, request); err == nil || !strings.Contains(err.Error(), "does not match stored request JSON") {
		t.Fatalf("mutated stored request replay error = %v, want persisted fingerprint mismatch", err)
	}
	if len(unknownRecord.RequestJSON) == 0 {
		t.Fatal("unknown-cost record lost request JSON")
	}
}

func canonicalQueryJSON(data []byte) string {
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return string(data)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return string(data)
	}
	return string(canonical)
}

func TestQueryExecutionExactUSDPrecisionIntegration(t *testing.T) {
	ctx, namespace, pool, cleanup := providerControlIntegrationPool(t)
	defer cleanup()
	keys := Keyring{Active: "query-v1", Keys: map[string][]byte{"query-v1": []byte("01234567890123456789012345678901")}}
	scopeKeys := ScopeKeyring{ActiveVersion: "scope-v1", Keys: map[string][]byte{"scope-v1": []byte("abcdefghijklmnopqrstuvwxyz123456")}}
	repository := DefaultQueryExecutionRepository(pool, namespace, keys, DefaultScopeRepository(pool, namespace, scopeKeys))
	repository.Now = func() time.Time { return time.Date(2026, time.July, 20, 8, 0, 0, 0, time.UTC) }
	request := validQueryExecutionRequest(repository.Now())
	request.Tenant = "query-ledger-precision-" + time.Now().UTC().Format("150405.000000000")
	request.OperationKey = "precision"
	request.CostMethod = "catalog_usage"
	request.ActualCostUSD = usdPointer(pricing.MustUSD("12345678901234567890.123456789012345678"))
	record, err := repository.Record(ctx, request)
	if err != nil {
		t.Fatalf("record precise query cost: %v", err)
	}
	if record.ActualCostUSD == nil || record.ActualCostUSD.String() != "12345678901234567890.123456789012345678" {
		t.Fatalf("precise query cost = %v", record.ActualCostUSD)
	}
}

func TestQueryExecutionRetentionCannotPrecedeCompletionIntegration(t *testing.T) {
	ctx, namespace, pool, cleanup := providerControlIntegrationPool(t)
	defer cleanup()
	keys := Keyring{Active: "query-v1", Keys: map[string][]byte{"query-v1": []byte("01234567890123456789012345678901")}}
	scopeKeys := ScopeKeyring{ActiveVersion: "scope-v1", Keys: map[string][]byte{"scope-v1": []byte("abcdefghijklmnopqrstuvwxyz123456")}}
	repository := DefaultQueryExecutionRepository(pool, namespace, keys, DefaultScopeRepository(pool, namespace, scopeKeys))
	repository.Now = func() time.Time { return time.Date(2026, time.July, 20, 8, 0, 0, 0, time.UTC) }
	request := validQueryExecutionRequest(repository.Now())
	request.Tenant = "query-ledger-retention-" + time.Now().UTC().Format("150405.000000000")
	record, err := repository.Record(ctx, request)
	if err != nil {
		t.Fatalf("record query execution: %v", err)
	}
	executions, err := namespace.Render("query_executions")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE "+executions+" SET retention_expires_at = completed_at WHERE query_execution_id = $1", record.ID); err == nil {
		t.Fatal("query execution retention was allowed to expire at completion")
	}
}
