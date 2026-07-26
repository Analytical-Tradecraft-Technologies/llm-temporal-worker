package postgres

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestMaintenanceBudgetBucketRetentionFencesReservations(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()
	now := time.Now().UTC().Truncate(time.Microsecond)
	operationKey := "budget-bucket-retention-" + uuid.NewString()
	configDigest := sha256.Sum256([]byte(operationKey))
	started, err := repository.Begin(ctx, admission.BeginRequest{
		ID:              operationKey,
		ScopeKey:        "budget-retention/fixtures",
		RequestDigest:   admission.Digest([]byte(operationKey)),
		ReservationUSD:  pricing.MustUSD("0"),
		ConfigVersion:   operationKey,
		ConfigDigest:    configDigest,
		ExpiresAt:       now.Add(time.Hour),
		RequestManifest: []byte(`{"model":"fixture"}`),
	})
	if err != nil || started.Existing {
		t.Fatalf("begin operation = %#v, %v", started, err)
	}
	scope, err := repository.Scopes.Ensure(ctx, "budget-retention", "fixtures")
	if err != nil {
		t.Fatal(err)
	}
	generationID, policyID, windowID := uuid.New(), uuid.New(), uuid.New()
	selectorDigest := sha256.Sum256([]byte(operationKey + ":selector"))
	bucketStart := now.Add(-4 * time.Hour).Truncate(time.Hour)
	if err := insertBudgetJournalFixtures(ctx, repository, scope.ID, generationID, policyID, windowID, configDigest, selectorDigest, now); err != nil {
		t.Fatalf("insert budget fixtures: %v", err)
	}
	operationID := operationUUID(operationKey)
	amount := pricing.MustUSD("1.000000000000000000")
	reserve := budget.ReservationEvent{
		EventID: uuid.NewString(), GenerationID: generationID.String(), OperationID: operationID.String(),
		WindowID: windowID.String(), BucketStart: bucketStart, ReservationRevision: 1,
		AmountUSD: amount, OccurredAt: now,
	}
	journal := &BudgetJournalRepository{Pool: repository.Pool, Namespace: repository.Namespace}
	if _, err := journal.AppendReservation(ctx, reserve); err != nil {
		t.Fatalf("append reserve: %v", err)
	}
	zero := pricing.MustUSD("0")
	if _, err := journal.AppendCompletion(ctx, budget.CompletionEvent{
		EventID: uuid.NewString(), GenerationID: reserve.GenerationID, OperationID: reserve.OperationID,
		WindowID: reserve.WindowID, BucketStart: bucketStart, ReservationRevision: 2,
		Kind: budget.JournalRelease, ReservedDecreaseUSD: amount, ActualCostUSD: &zero,
		CostStatus: budget.CostExact, OccurredAt: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("append release: %v", err)
	}

	cutoff := now.Add(-2 * time.Hour)
	maintenance := MaintenanceRepository{Pool: repository.Pool, Namespace: repository.Namespace}
	if _, err := maintenance.PruneExpiredBudgetBuckets(ctx, now, now.Add(-time.Hour), 2*time.Hour, 10); err == nil {
		t.Fatal("budget retention accepted a cutoff inside the maximum window horizon")
	}
	first, err := maintenance.PruneExpiredBudgetBuckets(ctx, now, cutoff, 2*time.Hour, 10)
	if err != nil {
		t.Fatalf("fenced budget prune: %v", err)
	}
	if first.Examined != 0 || first.Deleted != 0 {
		t.Fatalf("retention removed a bucket whose reservation row remains: %+v", first)
	}
	reservations, err := repository.Namespace.Render("operation_budget_reservations")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Pool.Exec(ctx, "DELETE FROM "+reservations+" WHERE operation_id=$1 AND window_id=$2", operationID, windowID); err != nil {
		t.Fatalf("remove test reservation fence: %v", err)
	}
	second, err := maintenance.PruneExpiredBudgetBuckets(ctx, now, cutoff, 2*time.Hour, 10)
	if err != nil {
		t.Fatalf("empty budget prune: %v", err)
	}
	if second.Examined != 1 || second.Deleted != 1 {
		t.Fatalf("empty historical bucket was not deleted: %+v", second)
	}
}
