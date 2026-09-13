package postgres

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func TestSpendSummaryExecutionIntegration(t *testing.T) {
	ctx, namespace, pool, cleanup := providerControlIntegrationPool(t)
	defer cleanup()
	key := []byte("01234567890123456789012345678901")
	scopes := DefaultScopeRepository(pool, namespace, ScopeKeyring{ActiveVersion: "scope-v1", Keys: map[string][]byte{"scope-v1": key}})
	scope, err := scopes.Ensure(ctx, "spend-integration", "project")
	if err != nil {
		t.Fatal(err)
	}
	operations := DefaultOperationRepository(pool, namespace, Keyring{Active: "operation-v1", Keys: map[string][]byte{"operation-v1": key}}, scopes)
	operationID := "spend-integration-" + time.Now().UTC().Format("150405.000000000")
	begin, err := operations.Begin(ctx, admission.BeginRequest{ID: operationID, OperationKey: operationID, Actor: "postgres-test", ScopeKey: "spend-integration/project", RequestDigest: admission.Digest([]byte(operationID)), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour), RequestManifest: []byte(`{"model":"fixture"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := operations.MarkDispatching(ctx, admission.DispatchRequest{OperationID: operationID, DispatchToken: begin.Operation.DispatchToken, Attempt: admission.AttemptFacts{EndpointID: "endpoint", Provider: "provider", ResolvedModel: "model-fixture"}}); err != nil {
		t.Fatal(err)
	}
	resultRef := &state.BlobRef{Digest: admission.Digest([]byte(operationID + ":result")), Size: 1, Media: "application/json"}
	if err := operations.Complete(ctx, admission.CompleteRequest{OperationID: operationID, DispatchToken: begin.Operation.DispatchToken, ResultRef: resultRef, ActualCostUSD: pricing.MustUSD("1.250000000000000000")}); err != nil {
		t.Fatal(err)
	}

	queryRepository := DefaultQueryExecutionRepository(pool, namespace, Keyring{Active: "query-v1", Keys: map[string][]byte{"query-v1": key}}, scopes)
	now := time.Now().UTC().Truncate(time.Microsecond)
	query := validQueryExecutionRequest(now)
	query.Tenant, query.Project, query.OperationKey = "spend-integration", "project", "spend-exact-"+operationID
	query.ActualCostUSD = usdPointer(pricing.MustUSD("0.050000000000000000"))
	query.CostMethod = "catalog_usage"
	query.RequestFingerprint = sha256.Sum256(query.RequestJSON)
	if _, err := queryRepository.Record(ctx, query); err != nil {
		t.Fatal(err)
	}
	unknown := query
	unknown.OperationKey = "spend-unknown-" + operationID
	unknown.CostStatus, unknown.CostMethod, unknown.ActualCostUSD = "unknown", "", nil
	unknown.CostUnknownReasonCode = "provider_charge_unavailable"
	unknown.StartedAt = now.Add(time.Second)
	unknown.CompletedAt = now.Add(time.Second)
	unknown.RetentionExpiresAt = unknown.CompletedAt.Add(time.Hour)
	unknown.RequestFingerprint = sha256.Sum256(unknown.RequestJSON)
	if _, err := queryRepository.Record(ctx, unknown); err != nil {
		t.Fatal(err)
	}

	repository := SpendSummaryRepository{Pool: pool, Namespace: namespace}
	window := SpendSummaryListOptions{ScopeID: scope.ID, StartTime: now.Add(-time.Minute), EndTime: now.Add(time.Minute), GroupBy: []control.SpendDimension{control.SpendByOperation}}
	grouped, err := repository.ListSpendSummary(ctx, window)
	if err != nil {
		t.Fatal(err)
	}
	if len(grouped.Buckets) != 2 {
		t.Fatalf("grouped buckets = %#v, want generate and query", grouped.Buckets)
	}
	if grouped.Buckets[0].Group == nil || grouped.Buckets[0].Group.OperationKind == nil || *grouped.Buckets[0].Group.OperationKind != control.OperationGenerate || grouped.Buckets[0].KnownActualCostUSD != "1.250000000000000000" || grouped.Buckets[0].UnknownOperationCount != 0 {
		t.Fatalf("generate bucket = %#v", grouped.Buckets[0])
	}
	if grouped.Buckets[1].Group == nil || grouped.Buckets[1].Group.OperationKind == nil || *grouped.Buckets[1].Group.OperationKind != control.OperationQuery || grouped.Buckets[1].KnownActualCostUSD != "0.050000000000000000" || grouped.Buckets[1].UnknownOperationCount != 1 || grouped.Buckets[1].Completeness != "partial" {
		t.Fatalf("query bucket = %#v", grouped.Buckets[1])
	}
	unknownOnly, err := repository.ListSpendSummary(ctx, SpendSummaryListOptions{
		ScopeID: scope.ID, StartTime: now.Add(500 * time.Millisecond), EndTime: now.Add(2 * time.Second),
		OperationKinds: []control.OperationKind{control.OperationQuery},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(unknownOnly.Buckets) != 1 || unknownOnly.Buckets[0].KnownActualCostUSD != "0.000000000000000000" || unknownOnly.Buckets[0].ExactOperationCount != 0 || unknownOnly.Buckets[0].UnknownOperationCount != 1 || unknownOnly.Buckets[0].Completeness != "partial" {
		t.Fatalf("unknown-only exact/unknown bucket = %#v", unknownOnly.Buckets)
	}
	byRoute, err := repository.ListSpendSummary(ctx, SpendSummaryListOptions{ScopeID: scope.ID, StartTime: now.Add(-time.Minute), EndTime: now.Add(time.Minute), GroupBy: []control.SpendDimension{control.SpendByProvider, control.SpendByModel}})
	if err != nil {
		t.Fatal(err)
	}
	if len(byRoute.Buckets) != 2 {
		t.Fatalf("provider/model buckets = %#v, want fixture provider plus query NULL group", byRoute.Buckets)
	}
	foundProvider := false
	for _, bucket := range byRoute.Buckets {
		if bucket.Group != nil && bucket.Group.Provider != nil && *bucket.Group.Provider == control.ProviderID("provider") {
			foundProvider = true
			if bucket.Group.Model == nil || *bucket.Group.Model != control.ProviderModelID("model-fixture") {
				t.Fatalf("final-attempt model attribution = %#v", bucket)
			}
		}
	}
	if !foundProvider {
		t.Fatalf("final-attempt provider group missing: %#v", byRoute.Buckets)
	}

	empty, err := repository.ListSpendSummary(ctx, SpendSummaryListOptions{ScopeID: scope.ID, StartTime: now.Add(2 * time.Hour), EndTime: now.Add(3 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Buckets) != 1 || empty.Buckets[0].KnownActualCostUSD != "0.000000000000000000" || empty.Buckets[0].ExactOperationCount != 0 || empty.Buckets[0].UnknownOperationCount != 0 {
		t.Fatalf("empty global bucket = %#v, want one exact zero bucket", empty.Buckets)
	}
}
