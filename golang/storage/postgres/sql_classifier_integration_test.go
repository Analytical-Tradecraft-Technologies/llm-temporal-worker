package postgres

import (
	"context"
	"crypto/sha256"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// TestBudgetJournalIntegrationHasNoBudgetReads runs the real durable journal
// append/finalize path through a traced PostgreSQL pool. The classifier is an
// execution allowlist: every statement that names a budget relation must be
// INSERT or UPDATE. This proves the write-only journal boundary, while
// deliberately leaving Redis admission and runtime composition to their own
// future integration gates.
func TestBudgetJournalIntegrationHasNoBudgetReads(t *testing.T) {
	recorder := &SQLTraceRecorder{}
	repository, ctx, cleanup := tracedOperationIntegrationRepository(t, recorder)
	defer cleanup()

	operationKey := "budget-sql-classifier-" + uuid.NewString()
	configDigest := sha256.Sum256([]byte(operationKey))
	started, err := repository.Begin(ctx, admission.BeginRequest{
		ID: operationKey, ScopeKey: "budget-sql-classifier/fixtures",
		RequestDigest:  admission.Digest([]byte(operationKey)),
		ReservationUSD: pricing.MustUSD("0"), ConfigVersion: operationKey,
		ConfigDigest: configDigest, ExpiresAt: time.Now().UTC().Add(time.Hour),
		RequestManifest: []byte(`{"model":"fixture"}`),
	})
	if err != nil {
		t.Fatalf("begin operation: %v", err)
	}
	if started.Existing {
		t.Fatal("new integration operation unexpectedly replayed")
	}
	scope, err := repository.Scopes.Ensure(ctx, "budget-sql-classifier", "fixtures")
	if err != nil {
		t.Fatalf("ensure scope: %v", err)
	}
	generationID, policyID, windowID := uuid.New(), uuid.New(), uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := insertBudgetJournalFixtures(ctx, repository, scope.ID, generationID, policyID, windowID, configDigest, sha256.Sum256([]byte(operationKey+":selector")), now); err != nil {
		t.Fatalf("insert fixtures: %v", err)
	}
	bucketStart := now.Truncate(time.Hour)
	reserve := budget.ReservationEvent{
		EventID: uuid.NewString(), GenerationID: generationID.String(),
		OperationID: operationUUID(operationKey).String(), WindowID: windowID.String(),
		BucketStart: bucketStart, ReservationRevision: 1,
		AmountUSD: pricing.MustUSD("1.250000000000000000"), OccurredAt: now,
	}
	journal := &BudgetJournalRepository{Pool: repository.Pool, Namespace: repository.Namespace}
	first, err := journal.AppendReservation(ctx, reserve)
	if err != nil {
		t.Fatalf("append reservation: %v", err)
	}
	if first.JournalID == 0 {
		t.Fatal("append reservation returned no journal ID")
	}
	actual := pricing.MustUSD("0.250000000000000000")
	completion := budget.CompletionEvent{
		EventID: uuid.NewString(), GenerationID: reserve.GenerationID,
		OperationID: reserve.OperationID, WindowID: reserve.WindowID,
		BucketStart: bucketStart, ReservationRevision: 2,
		Kind: budget.JournalFinalizeExact, ReservedDecreaseUSD: reserve.AmountUSD,
		AccountedIncreaseUSD: actual, ActualCostUSD: &actual,
		CostStatus: budget.CostExact, OccurredAt: now.Add(time.Second),
	}
	if _, err := journal.AppendCompletion(ctx, completion); err != nil {
		t.Fatalf("append completion: %v", err)
	}

	var budgetStatements []ClassifiedSQL
	for _, statement := range recorder.Snapshot() {
		if statement.BudgetTable {
			budgetStatements = append(budgetStatements, statement)
		}
	}
	if len(budgetStatements) == 0 {
		t.Fatal("tracer captured no budget statements")
	}
	for _, statement := range budgetStatements {
		if statement.BudgetRead || (statement.Kind != SQLStatementInsert && statement.Kind != SQLStatementUpdate) {
			t.Fatalf("budget statement was not an allowed write: kind=%d read=%v sql=%q", statement.Kind, statement.BudgetRead, statement.StatementSQL)
		}
	}
}

// TestOperationLifecycleIntegrationHasNoBudgetReads traces the normal durable
// operation admission/dispatch/finalization path. Budget admission is owned by
// Redis; this PostgreSQL path must never acquire a budget-table read as a
// fallback. The classifier deliberately rejects unknown statement shapes so a
// future CTE or stored procedure cannot silently weaken this contract.
func TestOperationLifecycleIntegrationHasNoBudgetReads(t *testing.T) {
	recorder := &SQLTraceRecorder{}
	repository, ctx, cleanup := tracedOperationIntegrationRepository(t, recorder)
	defer cleanup()

	operationKey := "operation-sql-classifier-" + uuid.NewString()
	configDigest := sha256.Sum256([]byte(operationKey))
	request := admission.BeginRequest{
		ID: operationKey, ScopeKey: "operation-sql-classifier/fixtures",
		RequestDigest:  admission.Digest([]byte(operationKey)),
		ReservationUSD: pricing.MustUSD("0"), ConfigVersion: operationKey,
		ConfigDigest: configDigest, ExpiresAt: time.Now().UTC().Add(time.Hour),
		RequestManifest: []byte(`{"model":"fixture"}`),
	}
	started, err := repository.Begin(ctx, request)
	if err != nil {
		t.Fatalf("begin operation: %v", err)
	}
	if started.Existing {
		t.Fatal("new integration operation unexpectedly replayed")
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{
		OperationID: operationKey, DispatchToken: started.Operation.DispatchToken,
		Attempt: admission.AttemptFacts{RouteID: "route", EndpointID: "endpoint", Provider: "fixture"},
	}); err != nil {
		t.Fatalf("mark dispatching: %v", err)
	}
	result := &state.BlobRef{Digest: admission.Digest([]byte(operationKey + ":result")), Size: 6, Media: "application/json"}
	if err := repository.Complete(ctx, admission.CompleteRequest{
		OperationID: operationKey, DispatchToken: started.Operation.DispatchToken,
		ResultRef: result, ActualCostUSD: pricing.MustUSD("0"),
	}); err != nil {
		t.Fatalf("complete operation: %v", err)
	}

	for _, statement := range recorder.Snapshot() {
		if !statement.BudgetTable {
			continue
		}
		if statement.BudgetRead || (statement.Kind != SQLStatementInsert && statement.Kind != SQLStatementUpdate) {
			t.Fatalf("operation lifecycle issued a non-write budget statement: kind=%d read=%v sql=%q", statement.Kind, statement.BudgetRead, statement.StatementSQL)
		}
	}
}

func tracedOperationIntegrationRepository(t *testing.T, tracer pgx.QueryTracer) (OperationRepository, context.Context, func()) {
	t.Helper()
	if os.Getenv("LLMTW_POSTGRES_ADDR") == "" {
		t.Skip("LLMTW_POSTGRES_ADDR is not configured; set it for PostgreSQL operation tests")
	}
	ns, err := NewNamespace(valueOr("LLMTW_POSTGRES_DATABASE", "llm_worker"), valueOr("LLMTW_POSTGRES_SCHEMA", "llm_worker"), os.Getenv("LLMTW_POSTGRES_TABLE_PREFIX"))
	if err != nil {
		t.Fatal(err)
	}
	config, err := BuildPoolConfig(PoolOptions{
		Namespace: ns, Addresses: []string{os.Getenv("LLMTW_POSTGRES_ADDR")},
		Username: valueOr("LLMTW_POSTGRES_USER", "llmtw"), Password: valueOr("LLMTW_POSTGRES_PASSWORD", "llmtw"),
		MaxConnections: 8, MinConnections: 1, DialTimeout: 5 * time.Second,
		StatementTimeout: 5 * time.Second, LockTimeout: time.Second, IdleTxTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if configurer, ok := tracer.(interface{ SetBudgetRelations(Namespace) error }); ok {
		if err := configurer.SetBudgetRelations(ns); err != nil {
			t.Fatal(err)
		}
	}
	config.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := Install(ctx, pool, ns); err != nil {
		cancel()
		pool.Close()
		t.Fatalf("install schema: %v", err)
	}
	// Installation includes role-grant DDL whose text mentions every budget
	// relation. The proof starts after setup so it covers only journal/runtime
	// execution, not schema bootstrap noise.
	if resetter, ok := tracer.(interface{ Reset() }); ok {
		resetter.Reset()
	}
	key := []byte("01234567890123456789012345678901")
	scopes := DefaultScopeRepository(pool, ns, ScopeKeyring{ActiveVersion: "scope-v1", Keys: map[string][]byte{"scope-v1": key}})
	repository := DefaultOperationRepository(pool, ns, Keyring{Active: "op-v1", Keys: map[string][]byte{"op-v1": key}}, scopes)
	return repository, ctx, func() { cancel(); pool.Close() }
}
