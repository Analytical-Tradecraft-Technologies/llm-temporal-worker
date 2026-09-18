package llm

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func reserveBatchRequestFixture() ReserveBatchRequestV1 {
	return ReserveBatchRequestV1{
		APIVersion: ReserveBatchAPIVersion,
		Context:    RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
		CustomerID: "customer", RunID: "run", BudgetID: "budget", BatchKey: "panel-1",
		PricingGenerationID: "prices-1", PricingManifestSHA256: strings.Repeat("a", 64),
		RemainingMaxCostMicrounits: 1000,
		Operations:                 []ReserveBatchOperationV1{{OperationKey: "op-1", Model: "model-1", ServiceClass: ServiceClassPriority, ServiceClassFallbacks: []ServiceClass{ServiceClassStandard, ServiceClassEconomy}, MaxInputTokens: 100, MaxOutputTokens: 20, MaxReasoningTokens: 5, MaxCacheReadTokens: 10}},
	}
}

func TestReserveBatchRequestV1ClosedCanonicalRoundTrip(t *testing.T) {
	request := reserveBatchRequestFixture()
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"service_class_fallbacks":["standard","economy"]`) {
		t.Fatalf("non-canonical request: %s", encoded)
	}
	var decoded ReserveBatchRequestV1
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	gotDigest, err := decoded.RequestSHA256()
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := request.RequestSHA256()
	if err != nil {
		t.Fatal(err)
	}
	if gotDigest != wantDigest {
		t.Fatalf("request digest changed: %s != %s", gotDigest, wantDigest)
	}
	unknown := strings.Replace(string(encoded), `"model":"model-1"`, `"model":"model-1","provider":"forbidden"`, 1)
	if err := json.Unmarshal([]byte(unknown), &decoded); err == nil {
		t.Fatal("accepted unknown descriptor field")
	}
}

func TestReserveBatchRequestV1RejectsBounds(t *testing.T) {
	request := reserveBatchRequestFixture()
	request.Operations[0].MaxInputTokens = 0
	if _, err := json.Marshal(request); err == nil {
		t.Fatal("accepted non-positive input bound")
	}
	request = reserveBatchRequestFixture()
	request.Operations = make([]ReserveBatchOperationV1, MaxReserveBatchOperations+1)
	for index := range request.Operations {
		request.Operations[index] = reserveBatchRequestFixture().Operations[0]
		request.Operations[index].OperationKey += string(rune(index + 1))
	}
	if _, err := json.Marshal(request); err == nil {
		t.Fatal("accepted more than 1024 descriptors")
	}
}

func TestReserveBatchResponseV1ClosedRoundTripAndAtomicDenial(t *testing.T) {
	grant := ReserveBatchGrantV1{OperationKey: "op-1", OperationSHA256: strings.Repeat("1", 64), OperationID: "operation-1", GrantID: "grant-1", GrantSHA256: strings.Repeat("2", 64), GrantKeyID: "key-1", GrantHMACSHA256: strings.Repeat("3", 64), MaxCostMicrounits: 500, ReservationGenerationID: "generation-1", ReservationIncarnationID: "incarnation-1", ReservationExpiresAt: time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)}
	response := ReserveBatchResponseV1{APIVersion: ReserveBatchAPIVersion, BatchID: "batch-1", RequestSHA256: strings.Repeat("4", 64), BatchSHA256: strings.Repeat("5", 64), Status: ReserveBatchStatusReserved, ReservedCostMicrounits: 500, Grants: []ReserveBatchGrantV1{grant}}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ReserveBatchResponseV1
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Grants[0].GrantHMACSHA256 != grant.GrantHMACSHA256 {
		t.Fatalf("grant identity changed: %#v", decoded.Grants[0])
	}
	response.Status, response.ReservedCostMicrounits = ReserveBatchStatusDenied, 0
	if _, err := json.Marshal(response); err == nil {
		t.Fatal("accepted denial carrying grants")
	}
	response.Grants = nil
	if encoded, err = json.Marshal(response); err != nil || !strings.Contains(string(encoded), `"grants":[]`) {
		t.Fatalf("canonical denial = %s, %v", encoded, err)
	}
}

func TestReserveBatchTemplateAndAllocateClosedRoundTrip(t *testing.T) {
	request := reserveBatchRequestFixture()
	request.Operations = nil
	request.PhaseKey = "phase-1"
	request.PhaseExpiresAt = time.Date(2026, 8, 10, 13, 0, 0, 0, time.UTC)
	request.Templates = []ReserveBatchTemplateV1{{TemplateKey: "panel-member", Count: 3, Model: "model-1", ServiceClass: ServiceClassStandard, MaxInputTokens: 100, MaxOutputTokens: 20}}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ReserveBatchRequestV1
	if err := json.Unmarshal(encoded, &decoded); err != nil || len(decoded.Templates) != 1 {
		t.Fatalf("template round trip = %#v, %v", decoded, err)
	}
	request.Operations = reserveBatchRequestFixture().Operations
	if _, err := json.Marshal(request); err == nil {
		t.Fatal("accepted operations and templates together")
	}
	escrow := ReserveBatchResponseV1{APIVersion: ReserveBatchAPIVersion, BatchID: "batch-1", RequestSHA256: strings.Repeat("4", 64), BatchSHA256: strings.Repeat("5", 64), Status: ReserveBatchStatusEscrowed, EscrowID: "escrow-1", EscrowSHA256: strings.Repeat("6", 64), ReservedCostMicrounits: 1000}
	if _, err := json.Marshal(escrow); err != nil {
		t.Fatal(err)
	}
	allocation := AllocateBatchGrantsRequestV1{APIVersion: AllocateBatchGrantsAPIVersion, Context: request.Context, CustomerID: request.CustomerID, RunID: request.RunID, BudgetID: request.BudgetID, PhaseKey: "phase-1", WaveKey: "wave-1", AllocationSequence: 1, BatchID: escrow.BatchID, BatchSHA256: escrow.BatchSHA256, EscrowID: escrow.EscrowID, EscrowSHA256: escrow.EscrowSHA256, GrantExpiresAt: time.Date(2026, 8, 10, 12, 30, 0, 0, time.UTC), Operations: reserveBatchRequestFixture().Operations}
	encoded, err = json.Marshal(allocation)
	if err != nil {
		t.Fatal(err)
	}
	var allocated AllocateBatchGrantsRequestV1
	if err := json.Unmarshal(encoded, &allocated); err != nil || len(allocated.Operations) != 1 || allocated.AllocationSequence != 1 {
		t.Fatalf("allocation round trip = %#v, %v", allocated, err)
	}
}

func TestCloseBatchV1ClosedIdempotentIdentity(t *testing.T) {
	request := CloseBatchRequestV1{APIVersion: CloseBatchAPIVersion, Context: RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}, CustomerID: "customer", RunID: "run", BudgetID: "budget", PhaseKey: "phase-1", CloseKey: "close-1", Reason: CloseBatchReasonZeroOpUnchanged, BatchID: "batch-1", BatchSHA256: strings.Repeat("a", 64), EscrowID: "escrow-1", EscrowSHA256: strings.Repeat("b", 64)}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded CloseBatchRequestV1
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	got, err := decoded.RequestSHA256()
	if err != nil {
		t.Fatal(err)
	}
	want, err := request.RequestSHA256()
	if err != nil || got != want {
		t.Fatalf("close digest = %s, %v; want %s", got, err, want)
	}
	unknown := strings.Replace(string(encoded), `"close_key":"close-1"`, `"close_key":"close-1","provider":"forbidden"`, 1)
	if err := json.Unmarshal([]byte(unknown), &decoded); err == nil {
		t.Fatal("accepted unknown close field")
	}
	response := CloseBatchResponseV1{APIVersion: CloseBatchAPIVersion, BatchID: "batch-1", EscrowID: "escrow-1", Status: CloseBatchStatusAlreadyClosed, RefundedCostMicrounits: 1000}
	if _, err := json.Marshal(response); err != nil {
		t.Fatal(err)
	}
}
