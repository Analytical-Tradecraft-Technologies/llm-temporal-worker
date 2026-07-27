package postgres

import (
	"crypto/hmac"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func validQueryExecutionRequest(now time.Time) QueryExecutionRequest {
	requestJSON := []byte(`{"api_version":"llm.temporal/query/v1","kind":"provider_status","query":{"include_healthy":false}}`)
	responseJSON := []byte(`{"api_version":"llm.temporal/query/v1","kind":"provider_status","routes":[]}`)
	return QueryExecutionRequest{
		Tenant:             "tenant",
		Project:            "project",
		OperationKey:       "query-operation",
		RequestFingerprint: sha256.Sum256(requestJSON),
		APIVersion:         llm.QueryAPIVersion,
		Kind:               llm.QueryProviderStatus,
		RequestJSON:        requestJSON,
		ResponseJSON:       responseJSON,
		ResponseDigest:     sha256.Sum256(responseJSON),
		Source:             "persisted",
		ActualCostUSD:      usdPointer(pricing.MustUSD("0")),
		CostStatus:         "exact",
		CostMethod:         "control_query_zero",
		StartedAt:          now.Add(-time.Second),
		CompletedAt:        now,
		RetentionExpiresAt: now.Add(24 * time.Hour),
	}
}

func usdPointer(value pricing.USD) *pricing.USD { return &value }

func TestValidateQueryExecutionRequest(t *testing.T) {
	now := time.Date(2026, time.July, 20, 8, 0, 0, 0, time.UTC)
	if err := validateQueryExecutionRequest(validQueryExecutionRequest(now), now); err != nil {
		t.Fatalf("valid query execution rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*QueryExecutionRequest)
		want   string
	}{
		{"sensitive request field", func(request *QueryExecutionRequest) {
			request.RequestJSON = []byte(`{"query":{"prompt":"secret"}}`)
			request.RequestFingerprint = sha256.Sum256(request.RequestJSON)
		}, "not redacted"},
		{"response digest mismatch", func(request *QueryExecutionRequest) {
			request.ResponseDigest = sha256.Sum256([]byte(`{"different":true}`))
		}, "digest"},
		{"unknown kind", func(request *QueryExecutionRequest) { request.Kind = llm.QueryKind("future") }, "kind"},
		{"unknown source", func(request *QueryExecutionRequest) { request.Source = "provider_api" }, "source"},
		{"nonzero control cost", func(request *QueryExecutionRequest) { request.ActualCostUSD = usdPointer(pricing.MustUSD("0.01")) }, "control_query_zero"},
		{"unknown cost without reason", func(request *QueryExecutionRequest) {
			request.CostStatus = "unknown"
			request.ActualCostUSD = nil
			request.CostMethod = ""
		}, "unknown"},
		{"retention at completion", func(request *QueryExecutionRequest) {
			request.RetentionExpiresAt = request.CompletedAt
		}, "timestamps"},
		{"future completion", func(request *QueryExecutionRequest) { request.CompletedAt = now.Add(3 * time.Minute) }, "future"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := validQueryExecutionRequest(now)
			test.mutate(&request)
			if err := validateQueryExecutionRequest(request, now); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validate error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestValidateQueryExecutionRequestRejectsRequestFingerprintMismatch(t *testing.T) {
	now := time.Date(2026, time.July, 20, 8, 0, 0, 0, time.UTC)
	request := validQueryExecutionRequest(now)
	request.RequestJSON = []byte(`{"api_version":"llm.temporal/query/v1","kind":"provider_status","query":{"include_healthy":true}}`)
	if err := validateQueryExecutionRequest(request, now); err == nil || !strings.Contains(err.Error(), "request fingerprint") {
		t.Fatalf("validateQueryExecutionRequest() error = %v, want request fingerprint mismatch", err)
	}
}

func TestValidateStoredQueryExecutionRequestFingerprint(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	request := validQueryExecutionRequest(time.Date(2026, time.July, 20, 8, 0, 0, 0, time.UTC))
	digest := sha256.Sum256(request.RequestJSON)
	fingerprint := operationHMAC(key, "query-request-fingerprint", digest[:])
	if err := validateStoredQueryExecutionRequestFingerprint(key, request.RequestJSON, fingerprint[:]); err != nil {
		t.Fatalf("valid stored request fingerprint rejected: %v", err)
	}

	mutated := []byte(`{"api_version":"llm.temporal/query/v1","kind":"provider_status","query":{"include_healthy":true}}`)
	if err := validateStoredQueryExecutionRequestFingerprint(key, mutated, fingerprint[:]); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mutated stored request error = %v, want mismatch", err)
	}
	if err := validateStoredQueryExecutionRequestFingerprint(key, request.RequestJSON, make([]byte, 31)); err == nil || !strings.Contains(err.Error(), "length") {
		t.Fatalf("short stored fingerprint error = %v, want length", err)
	}
	wrongKey := []byte("abcdefghijklmnopqrstuvwxyz123456")
	wrong := operationHMAC(wrongKey, "query-request-fingerprint", digest[:])
	if hmac.Equal(wrong[:], fingerprint[:]) {
		t.Fatal("test keys unexpectedly produced equal HMACs")
	}
	if err := validateStoredQueryExecutionRequestFingerprint(key, request.RequestJSON, wrong[:]); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("wrong stored fingerprint error = %v, want mismatch", err)
	}
}

func TestValidateQueryExecutionCostVariants(t *testing.T) {
	now := time.Date(2026, time.July, 20, 8, 0, 0, 0, time.UTC)
	unknown := validQueryExecutionRequest(now)
	unknown.OperationKey = "unknown-cost"
	unknown.CostStatus = "unknown"
	unknown.ActualCostUSD = nil
	unknown.CostMethod = ""
	unknown.CostUnknownReasonCode = "provider_charge_unavailable"
	if err := validateQueryExecutionRequest(unknown, now); err != nil {
		t.Fatalf("valid unknown cost rejected: %v", err)
	}
	exact := validQueryExecutionRequest(now)
	exact.CostMethod = "catalog_usage"
	exact.ActualCostUSD = usdPointer(pricing.MustUSD("0.000000000000000001"))
	if err := validateQueryExecutionRequest(exact, now); err != nil {
		t.Fatalf("valid exact cost rejected: %v", err)
	}
}

func TestValidateQueryExecutionJSONRejectsNonObjectsAndOversize(t *testing.T) {
	now := time.Date(2026, time.July, 20, 8, 0, 0, 0, time.UTC)
	request := validQueryExecutionRequest(now)
	request.RequestJSON = []byte(`[1]`)
	request.RequestFingerprint = sha256.Sum256(request.RequestJSON)
	if err := validateQueryExecutionRequest(request, now); err == nil || !strings.Contains(err.Error(), "object") {
		t.Fatalf("array request JSON error = %v", err)
	}
	request = validQueryExecutionRequest(now)
	request.ResponseJSON = []byte(`{"value":"` + strings.Repeat("x", MaxQueryExecutionJSONBytes) + `"}`)
	request.ResponseDigest = sha256.Sum256(request.ResponseJSON)
	if err := validateQueryExecutionRequest(request, now); err == nil || !strings.Contains(err.Error(), "bytes") {
		t.Fatalf("oversize response JSON error = %v", err)
	}
}

func TestCanonicalizeQueryExecutionJSON(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"whitespace", `{ "api_version": "llm.temporal/query/v1", "kind": "provider_status" }`, `{"api_version":"llm.temporal/query/v1","kind":"provider_status"}`},
		{"key order", `{"kind":"provider_status","api_version":"llm.temporal/query/v1"}`, `{"api_version":"llm.temporal/query/v1","kind":"provider_status"}`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			canonical, err := canonicalQueryExecutionJSON("response", []byte(test.raw))
			if err != nil {
				t.Fatalf("canonicalize JSON: %v", err)
			}
			if string(canonical) != test.want {
				t.Fatalf("canonical JSON = %s, want %s", canonical, test.want)
			}
		})
	}
	if _, err := canonicalQueryExecutionJSON("response", []byte(`{"kind":"provider_status","kind":"provider_status"}`)); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate key error = %v", err)
	}
}
