package runtime

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func assertOriginCost(t *testing.T, diagnostics []llm.Diagnostic, operation, amount string) {
	t.Helper()
	expectedJSON, err := json.Marshal(llm.CostV1{Status: "exact", ActualCostUSD: &amount, Method: "catalog_usage"})
	if err != nil {
		t.Fatal(err)
	}
	var expected llm.CostV1
	if err := json.Unmarshal(expectedJSON, &expected); err != nil {
		t.Fatal(err)
	}
	var found []llm.Diagnostic
	for _, d := range diagnostics {
		if d.Code == "cache_origin_cost" {
			found = append(found, d)
		}
	}
	if len(found) != 1 || found[0].Details["origin_operation_id"] != operation || found[0].Details["actual_cost_usd"] != *expected.ActualCostUSD || found[0].Details["cost_status"] != "exact" {
		t.Fatalf("original cost provenance = %#v", found)
	}
}

func TestCacheOriginCostPreservesReceiptAndUnknowns(t *testing.T) {
	for _, cost := range []string{
		`{"status":"exact","actual_cost_usd":"0.000000000000000001","method":"catalog_usage","catalog_version":"prices-original"}`,
		`{"status":"exact","actual_cost_usd":"0","method":"provider_reported"}`,
		`{"status":"unknown","actual_cost_usd":null,"unknown_reason":"provider_did_not_report_cost"}`,
	} {
		origin := cache.ResponseEntry{OriginOperationID: "origin-public-operation", Response: []byte(`{"api_version":"llm.temporal/v1","operation_id":"origin-public-operation","cost":` + cost + `}`)}
		before := append([]byte(nil), origin.Response...)
		diagnostic, err := cacheOriginCost(origin)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, origin.Response) {
			t.Fatal("origin receipt mutated")
		}
		if diagnostic.Details["cost_status"] == "unknown" {
			if _, invented := diagnostic.Details["actual_cost_usd"]; invented || diagnostic.Details["unknown_reason"] != "provider_did_not_report_cost" {
				t.Fatal("unknown cost was invented")
			}
		}
		encoded, err := json.Marshal(diagnostic)
		if err != nil {
			t.Fatal(err)
		}
		var decoded llm.Diagnostic
		if err := json.Unmarshal(encoded, &decoded); err != nil || !reflect.DeepEqual(decoded, diagnostic) {
			t.Fatalf("diagnostic wire round trip: %v", err)
		}
		repeated, err := withCacheOriginCost([]llm.Diagnostic{diagnostic, diagnostic}, origin)
		if err != nil || len(repeated) != 1 {
			t.Fatal("provenance duplicated on replay")
		}
	}
}

func TestCacheOriginCostRejectsCorruptReceipt(t *testing.T) {
	for _, payload := range []string{
		`null`, `{}`, `{"api_version":"llm.temporal/v1","operation_id":"wrong","cost":{"status":"exact","actual_cost_usd":"1","method":"provider_reported"}}`,
		`{"api_version":"llm.temporal/v1","operation_id":"origin","cost":{"status":"exact","actual_cost_usd":"-1","method":"provider_reported"}}`,
	} {
		if _, err := cacheOriginCost(cache.ResponseEntry{OriginOperationID: "origin", Response: []byte(payload)}); err == nil {
			t.Fatal("accepted corrupt origin receipt")
		}
	}
}
