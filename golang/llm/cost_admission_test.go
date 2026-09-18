package llm

import (
	"encoding/json"
	"strings"
	"testing"
)

func testCostAdmission(operationKey string) CostAdmissionV1 {
	return CostAdmissionV1{
		APIVersion: CostAdmissionAPIVersion,
		BudgetID:   "budget-1", CustomerID: "customer-1", RunID: "run-1",
		OperationKey: operationKey, GatewayAttemptOrdinal: 7, BatchID: "batch-1", BatchSHA256: strings.Repeat("b", 64),
		GrantMaterializationRequestSHA256: strings.Repeat("e", 64),
		GrantID:                           "grant-1", GrantSHA256: strings.Repeat("c", 64), GrantKeyID: "key-1",
		GrantHMACSHA256: strings.Repeat("d", 64), PricingGenerationID: "prices-v1",
		PricingManifestSHA256: strings.Repeat("a", 64), RemainingMaxCostMicrounits: 250000,
	}
}

func TestCostAdmissionV1ClosedRoundTrip(t *testing.T) {
	request := GenerateRequestV1{
		APIVersion: APIVersion, OperationKey: "forecast-operation",
		Context:       RequestContext{Tenant: "tenant", Project: "forecast", Actor: "actor", Tags: map[string]string{CostAdmissionContextTag: CostAdmissionForecastV1}},
		CostAdmission: func() *CostAdmissionV1 { value := testCostAdmission("forecast-operation"); return &value }(),
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded GenerateRequestV1
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.CostAdmission == nil || *decoded.CostAdmission != *request.CostAdmission {
		t.Fatalf("cost admission round trip = %#v", decoded.CostAdmission)
	}

	var unknown GenerateRequestV1
	withUnknown := strings.Replace(string(encoded), `"budget_id":"budget-1"`, `"budget_id":"budget-1","provider":"forbidden"`, 1)
	if err := json.Unmarshal([]byte(withUnknown), &unknown); err == nil {
		t.Fatal("unknown cost_admission field accepted")
	}
}

func TestCostAdmissionRequiresMaterializationIdentityWithGrant(t *testing.T) {
	admission := testCostAdmission("forecast-operation")
	encoded, err := json.Marshal(admission)
	if err != nil {
		t.Fatal(err)
	}
	for _, identity := range []string{"", strings.Repeat("E", 64), strings.Repeat("e", 63)} {
		admission.GrantMaterializationRequestSHA256 = identity
		if _, err := json.Marshal(admission); err == nil {
			t.Fatalf("invalid materialization identity %q encoded", identity)
		}
		var decoded CostAdmissionV1
		changed := strings.Replace(string(encoded), strings.Repeat("e", 64), identity, 1)
		if err := json.Unmarshal([]byte(changed), &decoded); err == nil {
			t.Fatalf("invalid materialization identity %q decoded", identity)
		}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	delete(fields, "grant_materialization_request_sha256")
	missing, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	var decoded CostAdmissionV1
	if err := json.Unmarshal(missing, &decoded); err == nil {
		t.Fatal("grant without explicit materialization identity decoded")
	}
}

func TestCostAdmissionRequiresBoundedGatewayAttemptOrdinal(t *testing.T) {
	for _, ordinal := range []int64{0, -1, 1_000_001} {
		admission := testCostAdmission("forecast-operation")
		admission.GatewayAttemptOrdinal = ordinal
		if _, err := json.Marshal(admission); err == nil {
			t.Fatalf("gateway attempt ordinal %d was accepted", ordinal)
		}
	}
}

func TestForecastRequiresAdmissionWhileLegacyRemainsCompatible(t *testing.T) {
	legacy := GenerateRequestV1{APIVersion: APIVersion, OperationKey: "legacy", Context: RequestContext{Tenant: "tenant", Project: "legacy", Actor: "actor"}}
	if _, err := json.Marshal(legacy); err != nil {
		t.Fatalf("legacy request rejected: %v", err)
	}
	forecast := legacy
	forecast.Context.Tags = map[string]string{CostAdmissionContextTag: CostAdmissionForecastV1}
	if _, err := json.Marshal(forecast); err == nil {
		t.Fatal("forecast request without admission accepted")
	}
	forecast.Context.Tags[CostAdmissionContextTag] = "unknown/v1"
	if _, err := json.Marshal(forecast); err == nil {
		t.Fatal("unknown cost admission context tag accepted")
	}
	forecast.Context.Tags[CostAdmissionContextTag] = CostAdmissionForecastV1
	admission := testCostAdmission("other-operation")
	forecast.CostAdmission = &admission
	if _, err := json.Marshal(forecast); err == nil {
		t.Fatal("mismatched admission operation accepted")
	}
}

func TestResponseCostMustMatchAdmissionPricingGeneration(t *testing.T) {
	admission := testCostAdmission("forecast-operation")
	zero := "0"
	response := GenerateResponseV1{
		APIVersion: APIVersion, OperationKey: admission.OperationKey, OperationID: "operation-id", Status: ResponseStatusCompleted,
		Checkpoint: CheckpointMetadata{Handle: "checkpoint", Kind: "generation"}, Cache: CacheDispositionV1{Disposition: "disabled"},
		Cost: CostV1{Status: "exact", ActualCostUSD: &zero, Method: "catalog_usage", CatalogVersion: "other-prices"}, CostAdmission: &admission,
	}
	if _, err := json.Marshal(response); err == nil {
		t.Fatal("mismatched response pricing generation accepted")
	}
}
