package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
	redisclient "github.com/redis/go-redis/v9"
)

func mustNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// requireErrorIs fails the test with "<context>: <err>" unless err matches want.
func requireErrorIs(t testing.TB, err, want error, context string) {
	t.Helper()
	if errors.Is(err, want) {
		return
	}
	if context == "" {
		t.Fatal(err)
	}
	t.Fatalf("%s: %v", context, err)
}

// errOf drops the value of a (value, error) result so it can be asserted on.
func errOf[T any](_ T, err error) error { return err }

// mustCall returns f(arg), failing the test if f reports an error.
func mustCall[A, T any](t testing.TB, f func(A) (T, error), arg A) T {
	t.Helper()
	value, err := f(arg)
	mustNoError(t, err)
	return value
}

func mustPutChild(t testing.TB, store *ContinuationStore, request state.PutChildRequest) state.Handle {
	t.Helper()
	handle, err := store.PutChild(context.Background(), request)
	mustNoError(t, err)
	return handle
}

// The must* constructors below fail the test when construction fails.

func mustBudgetEventPort(t testing.TB, client BudgetEventRedisClient, keys BudgetKeySpace) *RedisBudgetEventPort {
	t.Helper()
	port, err := NewRedisBudgetEventPort(client, keys)
	mustNoError(t, err)
	return port
}

func mustBudgetInitializer(t testing.TB, client redisclient.UniversalClient, keys KeyOptions, mode AdmissionMode, version string) *RedisBudgetInitializer {
	t.Helper()
	initializer, err := NewRedisBudgetInitializer(client, keys, mode, version)
	mustNoError(t, err)
	return initializer
}

func mustBudgetWorkerLeaseStore(t testing.TB, client BudgetWorkerLeaseRedisClient, keys BudgetKeySpace, options BudgetWorkerLeaseOptions) *BudgetWorkerLeaseStore {
	t.Helper()
	store, err := NewBudgetWorkerLeaseStore(client, keys, options)
	mustNoError(t, err)
	return store
}

func mustCanonicalReservations(t testing.TB, operation durable.OperationID, generation durable.GenerationID, values []admission.WindowReservation, expiresAt, now time.Time) []durableReservation {
	t.Helper()
	reservations, err := canonicalDurableReservations(operation, generation, values, expiresAt, now)
	mustNoError(t, err)
	return reservations
}
