package cloudstate

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// The must* helpers run one repository call with a background context and
// fail the test on any error. Tests that expect an error call the repository
// directly.

func mustValue[T any](t testing.TB, value T, err error) T {
	t.Helper()
	testassert.NoError(t, err)
	return value
}

// requireErrorIsNot fails the test unless err matches want and does not match not.
func requireErrorIsNot(t testing.TB, err, want, not error, context string) {
	t.Helper()
	testassert.ErrorIsNot(t, err, not, context)
	testassert.ErrorIs(t, err, want, context)
}

func mustCreate(t testing.TB, r *Repository, request CreateRequest) Record {
	t.Helper()
	record, err := r.Create(context.Background(), request)
	return mustValue(t, record, err)
}

func mustRead(t testing.TB, r *Repository, scope Scope, id RequestID) Record {
	t.Helper()
	record, err := r.Read(context.Background(), scope, id)
	return mustValue(t, record, err)
}

func mustTryUpdate(t testing.TB, r *Repository, scope Scope, id RequestID, update Update) Record {
	t.Helper()
	record, err := r.TryUpdate(context.Background(), scope, id, update)
	return mustValue(t, record, err)
}

func mustBeginOperation(t testing.TB, r *Repository, operation Operation) Record {
	t.Helper()
	record, err := r.BeginOperation(context.Background(), operation)
	return mustValue(t, record, err)
}

func mustCompleteOperation(t testing.TB, r *Repository, scope Scope, id RequestID, progress json.RawMessage, now time.Time) Record {
	t.Helper()
	record, err := r.CompleteOperation(context.Background(), scope, id, progress, now)
	return mustValue(t, record, err)
}

func mustSavePreparation(t testing.TB, r *Repository, scope Scope, id RequestID, preparation RequestPreparation) {
	t.Helper()
	testassert.NoError(t, r.SaveRequestPreparation(context.Background(), scope, id, preparation))
}

// mustSaveBudgetPlan saves plan as of its own quote time.
func mustSaveBudgetPlan(t testing.TB, r *Repository, scope Scope, id RequestID, plan BudgetPlan) {
	t.Helper()
	testassert.NoError(t, r.SaveBudgetPlan(context.Background(), scope, id, plan, plan.QuotedAt))
}

func mustSaveCheckpointFinalization(t testing.TB, r *Repository, scope Scope, id RequestID, plan CheckpointFinalization, now time.Time) {
	t.Helper()
	testassert.NoError(t, r.SaveCheckpointFinalization(context.Background(), scope, id, plan, now))
}

func mustSaveHandoff(t testing.TB, r *Repository, scope Scope, id RequestID, handoff FinalizationHandoff, now time.Time) {
	t.Helper()
	testassert.NoError(t, r.SaveFinalizationHandoff(context.Background(), scope, id, handoff, now))
}

func mustBeginRequestAttempt(t testing.TB, r *Repository, scope Scope, rootID, previous RequestID, now time.Time) RequestAttempt {
	t.Helper()
	attempt, err := r.BeginRequestAttempt(context.Background(), scope, rootID, previous, now)
	return mustValue(t, attempt, err)
}

func mustFinishRequestFailure(t testing.TB, r *Repository, scope Scope, rootID, attemptID RequestID, failure llm.ExecutionResultV1, now time.Time) {
	t.Helper()
	testassert.NoError(t, r.FinishRequestFailure(context.Background(), scope, rootID, attemptID, failure, now))
}

func mustPublishCheckpoint(t testing.TB, store state.CheckpointRepository, checkpoint state.DurableCheckpoint) {
	t.Helper()
	testassert.NoError(t, publishCheckpoint(context.Background(), store, checkpoint))
}

func mustPublishResponse(t testing.TB, r *Repository, entry cache.ResponseEntry) {
	t.Helper()
	testassert.NoError(t, r.Responses().Publish(context.Background(), entry))
}

func mustRecordUse(t testing.TB, r *Repository, use cache.ResponseUse) {
	t.Helper()
	testassert.NoError(t, r.Responses().RecordUse(context.Background(), use))
}

func mustResponseCache(t testing.TB, responses cache.ResponseRepository, fills cache.FillRepository, now func() time.Time) *durable.ResponseCache {
	t.Helper()
	c, err := durable.NewResponseCache(responses, fills, now)
	return mustValue(t, c, err)
}
