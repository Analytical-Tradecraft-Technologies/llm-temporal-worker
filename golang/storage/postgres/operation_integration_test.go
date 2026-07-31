package postgres

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func operationIntegrationRepository(t *testing.T) (OperationRepository, context.Context, func()) {
	t.Helper()
	if os.Getenv("LLMTW_POSTGRES_ADDR") == "" {
		t.Skip("LLMTW_POSTGRES_ADDR is not configured; set it for PostgreSQL operation tests")
	}
	ns, err := NewNamespace(valueOr("LLMTW_POSTGRES_DATABASE", "llm_worker"), valueOr("LLMTW_POSTGRES_SCHEMA", "llm_worker"), os.Getenv("LLMTW_POSTGRES_TABLE_PREFIX"))
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(context.Background(), PoolOptions{Namespace: ns, Addresses: []string{os.Getenv("LLMTW_POSTGRES_ADDR")}, Username: valueOr("LLMTW_POSTGRES_USER", "llmtw"), Password: valueOr("LLMTW_POSTGRES_PASSWORD", "llmtw"), MaxConnections: 8, MinConnections: 1, DialTimeout: 5 * time.Second, StatementTimeout: 5 * time.Second, LockTimeout: time.Second, IdleTxTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := Install(ctx, pool, ns); err != nil {
		cancel()
		pool.Close()
		t.Fatal(err)
	}
	key := []byte("01234567890123456789012345678901")
	scopes := DefaultScopeRepository(pool, ns, ScopeKeyring{ActiveVersion: "scope-v1", Keys: map[string][]byte{"scope-v1": key}})
	repository := DefaultOperationRepository(pool, ns, Keyring{Active: "op-v1", Keys: map[string][]byte{"op-v1": key}}, scopes)
	return repository, ctx, func() { cancel(); pool.Close() }
}

func TestOperationReplayConflictAndResult(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()
	id := "operation-integration-" + time.Now().UTC().Format("20060102150405.000000000")
	request := admission.BeginRequest{ID: id, ScopeKey: "integration/project", RequestDigest: admission.Digest([]byte("request")), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().Add(time.Hour), RequestManifest: []byte(`{"model":"test"}`)}
	first, err := repository.Begin(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := repository.Begin(ctx, request)
	if err != nil || !replay.Existing {
		t.Fatalf("replay=%#v err=%v", replay, err)
	}
	// PostgreSQL timestamptz stores microsecond precision, so compare against
	// the exact durable representation rather than the caller's nanoseconds.
	expectedExpiry := request.ExpiresAt.UTC().Truncate(time.Microsecond)
	if replay.Operation.RequestDigest != request.RequestDigest || replay.Operation.ExpiresAt.IsZero() || !replay.Operation.ExpiresAt.Equal(expectedExpiry) || replay.Operation.LeaseUntil.IsZero() || replay.Operation.ReservedCostUSD == nil || replay.Operation.ReservedCostUSD.Cmp(request.ReservationUSD) != 0 {
		t.Fatalf("replay metadata = %#v, want durable expiry, lease, digest, and reservation", replay.Operation)
	}
	request.RequestDigest = admission.Digest([]byte("different"))
	if _, err := repository.Begin(ctx, request); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("conflict=%v", err)
	}
	request.RequestDigest = admission.Digest([]byte("request"))
	request.ScopeKey = "other/project"
	if _, err := repository.Begin(ctx, request); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("operation-id conflict=%v", err)
	}
	request.ScopeKey = "integration/project"
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: id, DispatchToken: first.Operation.DispatchToken, Attempt: admission.AttemptFacts{RouteID: "primary", EndpointID: "test", Provider: "fixture"}}); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkProviderPending(ctx, admission.ProviderPendingRequest{OperationID: id, DispatchToken: first.Operation.DispatchToken, ProviderOperationID: "provider-operation-1", EndpointID: "test", Provider: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkProviderPending(ctx, admission.ProviderPendingRequest{OperationID: id, DispatchToken: first.Operation.DispatchToken, ProviderOperationID: "provider-operation-2", EndpointID: "test", Provider: "fixture"}); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("divergent provider operation = %v", err)
	}
	if providerID, err := repository.ProviderOperation(ctx, id); err != nil || providerID != "provider-operation-1" {
		t.Fatalf("provider operation reconciliation = %q, %v", providerID, err)
	}
	ref := &state.BlobRef{Digest: admission.Digest([]byte("result")), Size: 6, Media: "application/json"}
	if err := repository.Complete(ctx, admission.CompleteRequest{OperationID: id, DispatchToken: first.Operation.DispatchToken, ResultRef: ref, ActualCostUSD: pricing.MustUSD("0")}); err != nil {
		t.Fatal(err)
	}
	completed, err := repository.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if completed.ScopeKey != request.ScopeKey || completed.ExpiresAt.IsZero() || completed.ResultRef == nil || *completed.ResultRef != *ref {
		t.Fatalf("hydrated operation metadata = %#v", completed)
	}
	attempts, err := repository.Attempts(ctx, id)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts=%#v err=%v", attempts, err)
	}
}

func TestDispatchingGetHydratesEndpointForRestartRecovery(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()
	id := "operation-dispatching-restart-" + uuid.NewString()
	started, err := repository.Begin(ctx, admission.BeginRequest{
		ID: id, ScopeKey: "dispatching-restart/project", RequestDigest: admission.Digest([]byte(id)),
		ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{
		OperationID: id, DispatchToken: started.Operation.DispatchToken,
		Attempt: admission.AttemptFacts{RouteID: "restart-route", EndpointID: "restart-endpoint", Provider: "restart-provider"},
	}); err != nil {
		t.Fatal(err)
	}
	recovered, err := repository.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.State != admission.StateDispatching {
		t.Fatalf("state = %q, want dispatching", recovered.State)
	}
	if recovered.Attempt.EndpointID != "restart-endpoint" || recovered.Attempt.Provider != "restart-provider" {
		t.Fatalf("hydrated attempt = %#v, want endpoint/provider from dispatching row", recovered.Attempt)
	}
}

// TestProviderOperationTamperingFailsClosed proves the recovery boundary for
// persisted provider poll IDs.  The ID is envelope-encrypted and authenticated
// in PostgreSQL; the reconciliation loader must refuse to resume when an
// operator, bad backup, or storage fault changes either the ciphertext or its
// binding digest.
func TestProviderOperationTamperingFailsClosed(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	newPending := func(providerID string) string {
		id := "operation-provider-integrity-" + uuid.NewString()
		started, err := repository.Begin(ctx, admission.BeginRequest{
			ID: id, ScopeKey: "provider-integrity/project", RequestDigest: admission.Digest([]byte(id)),
			ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: id, DispatchToken: started.Operation.DispatchToken}); err != nil {
			t.Fatal(err)
		}
		if err := repository.MarkProviderPending(ctx, admission.ProviderPendingRequest{
			OperationID: id, DispatchToken: started.Operation.DispatchToken,
			ProviderOperationID: providerID, EndpointID: "integrity-endpoint", Provider: "fixture",
		}); err != nil {
			t.Fatal(err)
		}
		return id
	}

	operations, err := repository.Namespace.Render("operations")
	if err != nil {
		t.Fatal(err)
	}
	const providerID = "provider-operation-secret"
	ciphertextID := newPending(providerID)
	var ciphertext []byte
	if err := repository.Pool.QueryRow(ctx, "SELECT provider_operation_id_ciphertext FROM "+operations+" WHERE operation_id=$1", operationUUID(ciphertextID)).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if len(ciphertext) == 0 || bytes.Contains(ciphertext, []byte(providerID)) {
		t.Fatalf("provider operation ID is not encrypted at rest: %x", ciphertext)
	}
	if got, err := repository.ProviderOperation(ctx, ciphertextID); err != nil || got != providerID {
		t.Fatalf("untampered provider operation = %q, %v; want %q", got, err, providerID)
	}
	if _, err := repository.Pool.Exec(ctx, "UPDATE "+operations+" SET provider_operation_id_ciphertext = provider_operation_id_ciphertext || $2 WHERE operation_id=$1", operationUUID(ciphertextID), []byte{1}); err != nil {
		t.Fatal(err)
	}
	if got, err := repository.ProviderOperation(ctx, ciphertextID); err == nil {
		t.Fatalf("tampered provider ciphertext unexpectedly opened as %q", got)
	} else if strings.Contains(err.Error(), providerID) {
		t.Fatalf("tampered provider error leaked provider ID: %v", err)
	}

	// The provider-operation uniqueness index is endpoint-scoped, so use a
	// distinct fixture ID for the second independent corruption case.
	digestID := newPending(providerID + "-digest")
	if _, err := repository.Pool.Exec(ctx, "UPDATE "+operations+" SET provider_operation_id_hmac = decode(repeat('00', 32), 'hex') WHERE operation_id=$1", operationUUID(digestID)); err != nil {
		t.Fatal(err)
	}
	if got, err := repository.ProviderOperation(ctx, digestID); err == nil {
		t.Fatalf("tampered provider digest unexpectedly opened as %q", got)
	} else if strings.Contains(err.Error(), providerID) {
		t.Fatalf("tampered provider digest error leaked provider ID: %v", err)
	}
}

func TestOperationRetryPersistsEveryAttempt(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()
	id := "operation-retry-" + time.Now().UTC().Format("20060102150405.000000000")
	request := admission.BeginRequest{ID: id, ScopeKey: "retry/project", RequestDigest: admission.Digest([]byte("retry")), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().Add(time.Hour), RequestManifest: []byte(`{"model":"test"}`)}
	first, err := repository.Begin(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	dispatch := admission.DispatchRequest{OperationID: id, DispatchToken: first.Operation.DispatchToken, Attempt: admission.AttemptFacts{RouteID: "primary", EndpointID: "test", Provider: "fixture"}}
	if err := repository.MarkDispatching(ctx, dispatch); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Continue(ctx, admission.ContinueRequest{OperationID: id, DispatchToken: first.Operation.DispatchToken, RemainingUSD: pricing.MustUSD("0")}); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkDispatching(ctx, dispatch); err != nil {
		t.Fatal(err)
	}
	attempts, err := repository.Attempts(ctx, id)
	if err != nil || len(attempts) != 2 || attempts[0].AttemptNumber != 1 || attempts[1].AttemptNumber != 2 {
		t.Fatalf("retry attempts=%#v err=%v", attempts, err)
	}
}

func TestAcceptedFailurePersistsUnknownCost(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()
	id := "operation-accepted-failure-" + time.Now().UTC().Format("20060102150405.000000000")
	request := admission.BeginRequest{ID: id, ScopeKey: "failure/project", RequestDigest: admission.Digest([]byte("failure")), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().Add(time.Hour), RequestManifest: []byte(`{"model":"test"}`)}
	first, err := repository.Begin(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: id, DispatchToken: first.Operation.DispatchToken}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Fail(ctx, admission.FailRequest{OperationID: id, DispatchToken: first.Operation.DispatchToken, Certainty: admission.Accepted, Reason: "provider accepted"}); err != nil {
		t.Fatal(err)
	}
	failed, err := repository.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if failed.State != admission.StateAmbiguous || failed.ActualCostUSD != nil {
		t.Fatalf("accepted failure=%#v, want ambiguous with unknown cost", failed)
	}
}

func TestOperationValidationAndRetryGuards(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	future := time.Now().UTC().Add(time.Hour)
	invalid := []struct {
		name    string
		request admission.BeginRequest
	}{
		{name: "missing id", request: admission.BeginRequest{ScopeKey: "tenant/project", ExpiresAt: future}},
		{name: "expired", request: admission.BeginRequest{ID: "expired", ScopeKey: "tenant/project", ExpiresAt: time.Now().UTC().Add(-time.Minute)}},
		{name: "unsupported operation kind", request: admission.BeginRequest{ID: "unsupported", ScopeKey: "tenant/project", OperationKind: "query", ExpiresAt: future}},
		{name: "invalid manifest json", request: admission.BeginRequest{ID: "invalid-json", ScopeKey: "tenant/project", RequestManifest: []byte(`{"model":`), ExpiresAt: future}},
		{name: "non-object manifest", request: admission.BeginRequest{ID: "array-manifest", ScopeKey: "tenant/project", RequestManifest: []byte(`["model"]`), ExpiresAt: future}},
		{name: "empty scope component", request: admission.BeginRequest{ID: "empty-scope", ScopeKey: "tenant\x00", ExpiresAt: future}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if _, err := repository.Begin(ctx, test.request); err == nil {
				t.Fatal("invalid begin request unexpectedly succeeded")
			}
		})
	}

	id := "operation-guards-" + time.Now().UTC().Format("20060102150405.000000000")
	request := admission.BeginRequest{
		ID:             id,
		ScopeKey:       "compact-tenant",
		RequestDigest:  admission.Digest([]byte("compact-request")),
		ReservationUSD: pricing.MustUSD("1.25"),
		OperationKind:  "compact",
		ExpiresAt:      future,
	}
	started, err := repository.Begin(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if started.Existing || started.Operation.State != admission.StateReserved || started.Operation.ConfigVersion != "unknown" || started.Operation.ScopeKey != request.ScopeKey {
		t.Fatalf("begin defaults = %#v", started)
	}
	token := started.Operation.DispatchToken

	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: id, DispatchToken: "wrong"}); !errors.Is(err, admission.ErrInvalidToken) {
		t.Fatalf("invalid dispatch token = %v", err)
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: id, DispatchToken: token}); err != nil {
		t.Fatal(err)
	}
	attempts, err := repository.Attempts(ctx, id)
	if err != nil || len(attempts) != 1 || attempts[0].AttemptNumber != 1 || attempts[0].RouteID != "unknown" || attempts[0].EndpointID != "unknown" || attempts[0].Provider != "unknown" || attempts[0].Dispatch != admission.Accepted {
		t.Fatalf("default dispatch attempt = %#v, %v", attempts, err)
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: id, DispatchToken: token}); !errors.Is(err, admission.ErrInvalidTransition) {
		t.Fatalf("repeated dispatch = %v", err)
	}

	if _, err := repository.Continue(ctx, admission.ContinueRequest{OperationID: id, DispatchToken: "wrong", RemainingUSD: pricing.MustUSD("0.25")}); !errors.Is(err, admission.ErrInvalidToken) {
		t.Fatalf("invalid continue token = %v", err)
	}
	continued, err := repository.Continue(ctx, admission.ContinueRequest{OperationID: id, DispatchToken: token, RemainingUSD: pricing.MustUSD("0.25")})
	if err != nil || continued.Operation.State != admission.StateReserved {
		t.Fatalf("continue to reserved = %#v, %v", continued, err)
	}

	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: id, DispatchToken: token, Attempt: admission.AttemptFacts{RouteID: "retry", EndpointID: "endpoint", Provider: "fixture"}}); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkProviderPending(ctx, admission.ProviderPendingRequest{OperationID: id, DispatchToken: token, EndpointID: "endpoint"}); err == nil {
		t.Fatal("provider pending accepted without provider operation id")
	}
	if err := repository.MarkProviderPending(ctx, admission.ProviderPendingRequest{OperationID: id, DispatchToken: "wrong", ProviderOperationID: "provider-1", EndpointID: "endpoint"}); !errors.Is(err, admission.ErrInvalidToken) {
		t.Fatalf("invalid provider pending token = %v", err)
	}
	providerRequest := admission.ProviderPendingRequest{OperationID: id, DispatchToken: token, ProviderOperationID: "provider-1", EndpointID: "endpoint", Provider: "fixture"}
	if err := repository.MarkProviderPending(ctx, providerRequest); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkProviderPending(ctx, providerRequest); err != nil {
		t.Fatalf("idempotent provider pending = %v", err)
	}
	providerRequest.ProviderOperationID = "provider-2"
	if err := repository.MarkProviderPending(ctx, providerRequest); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("divergent provider pending = %v", err)
	}
	if providerID, err := repository.ProviderOperation(ctx, id); err != nil || providerID != "provider-1" {
		t.Fatalf("provider operation = %q, %v", providerID, err)
	}
	continued, err = repository.Continue(ctx, admission.ContinueRequest{OperationID: id, DispatchToken: token, RemainingUSD: pricing.MustUSD("0.10")})
	if err != nil || continued.Operation.State != admission.StateReserved {
		t.Fatalf("provider pending continue = %#v, %v", continued, err)
	}

	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: id, DispatchToken: token}); err != nil {
		t.Fatal(err)
	}
	result := &state.BlobRef{Digest: admission.Digest([]byte("result")), Size: 6, Media: "application/json"}
	if err := repository.Complete(ctx, admission.CompleteRequest{OperationID: id, DispatchToken: token, ResultRef: result, CostStatus: "invalid"}); err == nil {
		t.Fatal("invalid completion cost status unexpectedly succeeded")
	}
	if err := repository.Complete(ctx, admission.CompleteRequest{OperationID: id, DispatchToken: token, ResultRef: result, CostStatus: "unknown", UnknownReason: "provider timeout"}); err != nil {
		t.Fatal(err)
	}
	completed, err := repository.Get(ctx, id)
	if err != nil || completed.State != admission.StateCompleted || completed.ResultRef == nil || completed.ActualCostUSD != nil {
		t.Fatalf("unknown-cost completion = %#v, %v", completed, err)
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: id, DispatchToken: token}); !errors.Is(err, admission.ErrInvalidTransition) {
		t.Fatalf("dispatch after completion = %v", err)
	}
	if _, err := repository.Continue(ctx, admission.ContinueRequest{OperationID: id, DispatchToken: token, RemainingUSD: pricing.MustUSD("0")}); !errors.Is(err, admission.ErrInvalidTransition) {
		t.Fatalf("continue after completion = %v", err)
	}
}

func TestRejectedFailurePersistsExactCostAndSafeReason(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	id := "operation-rejected-failure-" + time.Now().UTC().Format("20060102150405.000000000")
	request := admission.BeginRequest{ID: id, ScopeKey: "failure/rejected", RequestDigest: admission.Digest([]byte("rejected")), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour)}
	started, err := repository.Begin(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: id, DispatchToken: started.Operation.DispatchToken}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Fail(ctx, admission.FailRequest{OperationID: id, DispatchToken: started.Operation.DispatchToken, Certainty: admission.Rejected, Reason: "Provider timeout: request/123"}); err != nil {
		t.Fatal(err)
	}
	failed, err := repository.Get(ctx, id)
	if err != nil || failed.State != admission.StateDefiniteFailed || failed.ActualCostUSD == nil || failed.ActualCostUSD.Cmp(pricing.MustUSD("0")) != 0 {
		t.Fatalf("rejected failure = %#v, %v", failed, err)
	}

	relation, err := repository.Namespace.Render("operations")
	if err != nil {
		t.Fatal(err)
	}
	var status, method, reason string
	if err := repository.Pool.QueryRow(ctx, "SELECT cost_status, COALESCE(cost_method,''), COALESCE(cost_unknown_reason_code,'') FROM "+relation+" WHERE operation_id=$1", operationUUID(id)).Scan(&status, &method, &reason); err != nil {
		t.Fatal(err)
	}
	if status != "exact" || method != "worker_cache_zero" || reason != "" {
		t.Fatalf("rejected failure metadata = %q, %q, %q", status, method, reason)
	}
}

func TestTerminalOperationClosesItsAttemptWithCostFacts(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	attempts, err := repository.Namespace.Render("operation_attempts")
	if err != nil {
		t.Fatal(err)
	}

	// A completed operation must not leave its provider attempt looking
	// submitted. The route-level exact cost is retained independently of the
	// operation projection so retry/reconciliation tooling can audit every
	// attempted route.
	completedID := "operation-attempt-complete-" + uuid.NewString()
	started, err := repository.Begin(ctx, admission.BeginRequest{
		ID: completedID, ScopeKey: "attempt-terminal/project", RequestDigest: admission.Digest([]byte(completedID)),
		ReservationUSD: pricing.MustUSD("1.25"), ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	attempt := admission.AttemptFacts{RouteID: "primary", EndpointID: "endpoint", Provider: "fixture", ResolvedModel: "fixture-model", Dispatch: admission.Accepted}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: completedID, DispatchToken: started.Operation.DispatchToken, Attempt: attempt}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Complete(ctx, admission.CompleteRequest{
		OperationID: completedID, DispatchToken: started.Operation.DispatchToken,
		ResultRef:     &state.BlobRef{Digest: admission.Digest([]byte("completed")), Size: 9, Media: "application/json"},
		ActualCostUSD: pricing.MustUSD("1.25"), CostStatus: "exact", CostMethod: "provider_reported", Attempt: attempt,
	}); err != nil {
		t.Fatal(err)
	}
	assertTerminalAttempt(t, ctx, repository, attempts, completedID, "completed", "accepted", "exact", "provider_reported", "1.25")

	// Accepted/ambiguous provider outcomes retain NULL actual cost and a safe
	// reason at the attempt level; zero is reserved for a proven free outcome.
	ambiguousID := "operation-attempt-ambiguous-" + uuid.NewString()
	started, err = repository.Begin(ctx, admission.BeginRequest{
		ID: ambiguousID, ScopeKey: "attempt-terminal/project", RequestDigest: admission.Digest([]byte(ambiguousID)),
		ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: ambiguousID, DispatchToken: started.Operation.DispatchToken, Attempt: attempt}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Fail(ctx, admission.FailRequest{OperationID: ambiguousID, DispatchToken: started.Operation.DispatchToken, Certainty: admission.Accepted, Reason: "Provider timeout: request/123", Attempt: attempt}); err != nil {
		t.Fatal(err)
	}
	assertTerminalAttempt(t, ctx, repository, attempts, ambiguousID, "ambiguous", "ambiguous", "unknown", "", "")
}

func assertTerminalAttempt(t *testing.T, ctx context.Context, repository OperationRepository, relation, operationID, wantState, wantDispatch, wantStatus, wantMethod, wantActual string) {
	t.Helper()
	var stateValue, dispatch, status string
	var method, actual, reason *string
	var finished time.Time
	if err := repository.Pool.QueryRow(ctx, "SELECT state, dispatch_disposition, cost_status, cost_method, actual_cost_usd::text, cost_unknown_reason_code, finished_at FROM "+relation+" WHERE operation_id=$1 AND attempt_number=1", operationUUID(operationID)).Scan(&stateValue, &dispatch, &status, &method, &actual, &reason, &finished); err != nil {
		t.Fatal(err)
	}
	if stateValue != wantState || dispatch != wantDispatch || status != wantStatus {
		t.Fatalf("terminal attempt state = %q/%q/%q, want %q/%q/%q", stateValue, dispatch, status, wantState, wantDispatch, wantStatus)
	}
	if !finished.After(time.Time{}) {
		t.Fatal("terminal attempt has no finished_at timestamp")
	}
	gotMethod := ""
	if method != nil {
		gotMethod = *method
	}
	if gotMethod != wantMethod {
		t.Fatalf("terminal attempt method = %q, want %q", gotMethod, wantMethod)
	}
	if wantActual == "" {
		if actual != nil {
			t.Fatalf("terminal attempt actual cost = %q, want SQL NULL", *actual)
		}
		if reason == nil || *reason != "providertimeoutrequest123" {
			t.Fatalf("terminal attempt unknown reason = %#v", reason)
		}
		return
	}
	if actual == nil {
		t.Fatal("terminal attempt exact cost is SQL NULL")
	}
	decoded, err := DecodeUSD(*actual)
	if err != nil || decoded.Cmp(pricing.MustUSD(wantActual)) != 0 {
		t.Fatalf("terminal attempt actual cost = %q (%v), want %s", *actual, err, wantActual)
	}
}
