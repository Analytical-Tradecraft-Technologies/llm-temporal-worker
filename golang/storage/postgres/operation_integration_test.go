package postgres

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
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
	request := admission.BeginRequest{ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: "integration/project", RequestDigest: admission.Digest([]byte("request")), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().Add(time.Hour), RequestManifest: []byte(`{"model":"test"}`)}
	first, err := repository.Begin(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	operations, err := repository.Namespace.Render("operations")
	if err != nil {
		t.Fatal(err)
	}
	var identityVersion int
	var actorHMAC, operationKeyHMAC []byte
	if err := repository.Pool.QueryRow(ctx, "SELECT operation_identity_version, operation_actor_hmac, operation_key_hmac FROM "+operations+" WHERE operation_id=$1", operationUUID(id)).Scan(&identityVersion, &actorHMAC, &operationKeyHMAC); err != nil {
		t.Fatal(err)
	}
	key := []byte("01234567890123456789012345678901")
	expectedActor := operationHMAC(key, "operation-actor", []byte(request.Actor))
	expectedKey := operationHMAC(key, "operation-key-v2", []byte(request.Actor+"\x00"+defaultAPIVersion+"\x00"+request.OperationKey))
	if identityVersion != 2 || !bytes.Equal(actorHMAC, expectedActor[:]) || !bytes.Equal(operationKeyHMAC, expectedKey[:]) {
		t.Fatalf("strict identity = version %d actor %x key %x", identityVersion, actorHMAC, operationKeyHMAC)
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
	otherActor := request
	otherActor.ID = id + "-other-actor"
	otherActor.Actor = "other-postgres-test"
	if distinct, err := repository.Begin(ctx, otherActor); err != nil || distinct.Existing {
		t.Fatalf("actor-scoped identity collided: %#v, %v", distinct, err)
	}
	otherAPI := request
	otherAPI.ID = id + "-other-api"
	otherAPI.APIVersion = "llm.generate.v2"
	if distinct, err := repository.Begin(ctx, otherAPI); err != nil || distinct.Existing {
		t.Fatalf("API-scoped identity collided: %#v, %v", distinct, err)
	}
	request.ImmutableFacts = []byte(`{"generation_id":"generation-1","incarnation_id":"incarnation-1","expires_at":"2026-08-10T13:00:00Z"}`)
	factsReplay, err := repository.Begin(ctx, request)
	if err != nil || string(factsReplay.Operation.ImmutableFacts) == "" {
		t.Fatalf("persist immutable reservation facts: %#v, %v", factsReplay.Operation, err)
	}
	request.ImmutableFacts = []byte(`{"generation_id":"generation-2"}`)
	if _, err := repository.Begin(ctx, request); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("immutable reservation facts conflict = %v", err)
	}
	request.ImmutableFacts = nil
	request.RequestDigest = admission.Digest([]byte("different"))
	request.ID = id + "-different-surrogate"
	if _, err := repository.Begin(ctx, request); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("logical identity accepted a different request digest: %v", err)
	}
	request.ID = id
	request.RequestDigest = admission.Digest([]byte("request"))
	request.ScopeKey = "other/project"
	if _, err := repository.Begin(ctx, request); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("operation-id conflict=%v", err)
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{
		OperationID: id, DispatchToken: first.Operation.DispatchToken,
		LeaseUntil: time.Now().UTC().Add(-time.Minute),
	}); !errors.Is(err, admission.ErrInvalidTransition) {
		t.Fatalf("expired dispatch lease was accepted: %v", err)
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

func TestOperationPersistsCatalogCostVersion(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	id := "operation-catalog-cost-" + time.Now().UTC().Format("20060102150405.000000000")
	started, err := repository.Begin(ctx, admission.BeginRequest{
		ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: "integration/catalog-cost",
		RequestDigest: admission.Digest([]byte("catalog-cost-request")), ReservationUSD: pricing.MustUSD("1"),
		ExpiresAt: time.Now().Add(time.Hour), RequestManifest: []byte(`{"model":"test"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	attempt := admission.AttemptFacts{
		RouteID: "catalog-route", EndpointID: "catalog-endpoint", Provider: "fixture",
		ResolvedModel: "catalog-model", Dispatch: admission.Accepted, AttemptNumber: 1,
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{
		OperationID: id, DispatchToken: started.Operation.DispatchToken, Attempt: attempt,
	}); err != nil {
		t.Fatal(err)
	}
	result := &state.BlobRef{Digest: admission.Digest([]byte("catalog-cost-result")), Size: 1, Media: "application/json"}
	complete := admission.CompleteRequest{
		OperationID: id, DispatchToken: started.Operation.DispatchToken, ResultRef: result,
		ActualCostUSD: pricing.MustUSD("0.125"), CostStatus: "exact", CostMethod: "catalog_usage",
		Attempt: attempt,
	}
	if err := repository.Complete(ctx, complete); err == nil || !strings.Contains(err.Error(), "requires a catalog version") {
		t.Fatalf("missing catalog version completion error = %v", err)
	}
	complete.CostCatalogVersion = "catalog-v1"
	if err := repository.Complete(ctx, complete); err != nil {
		t.Fatal(err)
	}

	operations, err := repository.Namespace.Render("operations")
	if err != nil {
		t.Fatal(err)
	}
	attempts, err := repository.Namespace.Render("operation_attempts")
	if err != nil {
		t.Fatal(err)
	}
	var operationCatalog, attemptCatalog string
	if err := repository.Pool.QueryRow(ctx, "SELECT cost_catalog_version FROM "+operations+" WHERE operation_id=$1", operationUUID(id)).Scan(&operationCatalog); err != nil {
		t.Fatal(err)
	}
	if err := repository.Pool.QueryRow(ctx, "SELECT cost_catalog_version FROM "+attempts+" WHERE operation_id=$1 AND attempt_number=1", operationUUID(id)).Scan(&attemptCatalog); err != nil {
		t.Fatal(err)
	}
	if operationCatalog != complete.CostCatalogVersion || attemptCatalog != complete.CostCatalogVersion {
		t.Fatalf("catalog versions = operation %q attempt %q, want %q", operationCatalog, attemptCatalog, complete.CostCatalogVersion)
	}

	failedID := id + "-failed"
	failed, err := repository.Begin(ctx, admission.BeginRequest{
		ID: failedID, OperationKey: failedID, Actor: "postgres-test", ScopeKey: "integration/catalog-cost",
		RequestDigest: admission.Digest([]byte("catalog-cost-failure")), ReservationUSD: pricing.MustUSD("1"),
		ExpiresAt: time.Now().Add(time.Hour), RequestManifest: []byte(`{"model":"test"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{
		OperationID: failedID, DispatchToken: failed.Operation.DispatchToken, Attempt: attempt,
	}); err != nil {
		t.Fatal(err)
	}
	failure := admission.FailRequest{
		OperationID: failedID, DispatchToken: failed.Operation.DispatchToken,
		Certainty: admission.Accepted, IncurredCostUSD: pricing.MustUSD("0.25"), PostResponse: true,
		CostStatus: "exact", CostMethod: "catalog_usage", Attempt: attempt, Reason: "post_response_validation_failed",
	}
	if err := repository.Fail(ctx, failure); err == nil || !strings.Contains(err.Error(), "requires a catalog version") {
		t.Fatalf("missing catalog version failure error = %v", err)
	}
	failure.CostCatalogVersion = "catalog-v2"
	if err := repository.Fail(ctx, failure); err != nil {
		t.Fatal(err)
	}
	if err := repository.Pool.QueryRow(ctx, "SELECT cost_catalog_version FROM "+operations+" WHERE operation_id=$1", operationUUID(failedID)).Scan(&operationCatalog); err != nil {
		t.Fatal(err)
	}
	if err := repository.Pool.QueryRow(ctx, "SELECT cost_catalog_version FROM "+attempts+" WHERE operation_id=$1 AND attempt_number=1", operationUUID(failedID)).Scan(&attemptCatalog); err != nil {
		t.Fatal(err)
	}
	if operationCatalog != failure.CostCatalogVersion || attemptCatalog != failure.CostCatalogVersion {
		t.Fatalf("failed catalog versions = operation %q attempt %q, want %q", operationCatalog, attemptCatalog, failure.CostCatalogVersion)
	}
}
func TestOperationStoresContentFreeManifestAndEncryptedCanonicalPayload(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	id := "operation-payload-" + time.Now().UTC().Format("20060102150405.000000000")
	payload := []byte(`{"append":[{"actor":"human","content":[{"text":"prompt-plaintext-secret","type":"text"}]}],"api_version":"llm.temporal/v1","context":{"actor":"actor-secret","project":"project-plaintext-secret","tenant":"tenant-plaintext-secret"},"operation_key":"operation-secret","settings_patch":{"model":{"set":"settings-model-secret"},"tools":{"set":[{"description":"tool-plaintext-secret","input_schema":{"type":"object"},"name":"secret_tool"}]}}}`)
	digest := admission.Digest(payload)
	manifest, err := canonicalOperationRequestManifest(payload, digest)
	if err != nil {
		t.Fatal(err)
	}
	request := admission.BeginRequest{ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: "tenant-plaintext-secret/project-plaintext-secret",
		RequestDigest: digest, ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().Add(time.Hour),
		OperationKind: "generate", APIVersion: "llm.temporal/v1", RequestSchemaVersion: 1,
		RequestManifest: manifest, RequestPayload: payload}
	if _, err := repository.Begin(ctx, request); err != nil {
		t.Fatal(err)
	}
	operations, err := repository.Namespace.Render("operations")
	if err != nil {
		t.Fatal(err)
	}
	var persistedManifest string
	var payloadSHA []byte
	var payloadBytes int64
	var payloadReference string
	var ciphertext []byte
	var keyID string
	var scopeID uuid.UUID
	if err := repository.Pool.QueryRow(ctx, "SELECT request_manifest_jsonb::text, request_payload_sha256, request_payload_byte_length, request_payload_reference, request_inline_ciphertext, request_key_id, scope_id FROM "+operations+" WHERE operation_id=$1", operationUUID(id)).Scan(&persistedManifest, &payloadSHA, &payloadBytes, &payloadReference, &ciphertext, &keyID, &scopeID); err != nil {
		t.Fatal(err)
	}
	for _, plaintext := range []string{"prompt-plaintext-secret", "settings-model-secret", "tool-plaintext-secret", "secret_tool", "tenant-plaintext-secret", "project-plaintext-secret"} {
		if strings.Contains(persistedManifest, plaintext) {
			t.Fatalf("request_manifest_jsonb leaked %q: %s", plaintext, persistedManifest)
		}
		if !bytes.Contains(payload, []byte(plaintext)) {
			t.Fatalf("fixture payload does not cover plaintext %q", plaintext)
		}
	}
	if !bytes.Equal(payloadSHA, digest[:]) || payloadBytes != int64(len(payload)) || payloadReference != "inline" {
		t.Fatalf("content-free request metadata = sha %x bytes %d ref %q", payloadSHA, payloadBytes, payloadReference)
	}
	envelopeContext := EnvelopeContext{ScopeID: scopeID, OperationID: operationUUID(id), PayloadKind: "operation-request", Digest: digest}
	contextHash, err := contextDigest(envelopeContext)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := repository.Keys.Open(envelopeContext, SealedValue{KeyID: keyID, Ciphertext: ciphertext, ContextHash: contextHash})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(replayed, payload) {
		t.Fatalf("decrypted canonical request changed:\n got %s\nwant %s", replayed, payload)
	}
}

func TestDispatchingGetHydratesEndpointForRestartRecovery(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()
	id := "operation-dispatching-restart-" + uuid.NewString()
	started, err := repository.Begin(ctx, admission.BeginRequest{ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: "dispatching-restart/project", RequestDigest: admission.Digest([]byte(id)),
		ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour)})
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
		started, err := repository.Begin(ctx, admission.BeginRequest{ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: "provider-integrity/project", RequestDigest: admission.Digest([]byte(id)),
			ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour)})
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
	request := admission.BeginRequest{ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: "retry/project", RequestDigest: admission.Digest([]byte("retry")), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().Add(time.Hour), RequestManifest: []byte(`{"model":"test"}`)}
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
	recovered, err := repository.Get(ctx, id)
	if err != nil || recovered.Attempt.AttemptNumber != 2 || recovered.Attempt.RouteID != "primary" {
		t.Fatalf("recovered latest attempt=%#v err=%v", recovered.Attempt, err)
	}
}

func TestAcceptedFailurePersistsUnknownCost(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()
	id := "operation-accepted-failure-" + time.Now().UTC().Format("20060102150405.000000000")
	request := admission.BeginRequest{ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: "failure/project", RequestDigest: admission.Digest([]byte("failure")), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().Add(time.Hour), RequestManifest: []byte(`{"model":"test"}`)}
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
		{name: "expired", request: admission.BeginRequest{ID: "expired", OperationKey: "expired", Actor: "postgres-test", ScopeKey: "tenant/project", ExpiresAt: time.Now().UTC().Add(-time.Minute)}},
		{name: "unsupported operation kind", request: admission.BeginRequest{ID: "unsupported", OperationKey: "unsupported", Actor: "postgres-test", ScopeKey: "tenant/project", OperationKind: "query", ExpiresAt: future}},
		{name: "invalid manifest json", request: admission.BeginRequest{ID: "invalid-json", OperationKey: "invalid-json", Actor: "postgres-test", ScopeKey: "tenant/project", RequestManifest: []byte(`{"model":`), ExpiresAt: future}},
		{name: "non-object manifest", request: admission.BeginRequest{ID: "array-manifest", OperationKey: "array-manifest", Actor: "postgres-test", ScopeKey: "tenant/project", RequestManifest: []byte(`["model"]`), ExpiresAt: future}},
		{name: "empty scope component", request: admission.BeginRequest{ID: "empty-scope", OperationKey: "empty-scope", Actor: "postgres-test", ScopeKey: "tenant\x00", ExpiresAt: future}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if _, err := repository.Begin(ctx, test.request); err == nil {
				t.Fatal("invalid begin request unexpectedly succeeded")
			}
		})
	}

	id := "operation-guards-" + time.Now().UTC().Format("20060102150405.000000000")
	request := admission.BeginRequest{ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: "compact-tenant",
		RequestDigest:  admission.Digest([]byte("compact-request")),
		ReservationUSD: pricing.MustUSD("1.25"),
		OperationKind:  "compact",
		ExpiresAt:      future}
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
	request := admission.BeginRequest{ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: "failure/rejected", RequestDigest: admission.Digest([]byte("rejected")), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour)}
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

func TestFailBeforeDispatchAtomicallyPersistsTerminalAttemptAndReplays(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	id := "operation-pre-write-failure-" + uuid.NewString()
	started, err := repository.Begin(ctx, admission.BeginRequest{ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: "failure/pre-write", RequestDigest: admission.Digest([]byte(id)),
		ReservationUSD: pricing.MustUSD("0.25"), ExpiresAt: time.Now().UTC().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	failure := admission.FailRequest{
		OperationID: id, DispatchToken: started.Operation.DispatchToken,
		Certainty: admission.NotDispatched, Reason: "provider_dispatch_failed",
		Attempt: admission.AttemptFacts{
			RouteID: "route-1", EndpointID: "endpoint-1", Provider: "fixture",
			ResolvedModel: "model-1", ServiceClass: "priority", Dispatch: admission.NotDispatched,
		},
	}
	invalidToken := failure
	invalidToken.DispatchToken = "wrong-token"
	if err := repository.FailBeforeDispatch(ctx, invalidToken); !errors.Is(err, admission.ErrInvalidToken) {
		t.Fatalf("invalid pre-write failure token = %v", err)
	}
	unchanged, err := repository.Get(ctx, id)
	if err != nil || unchanged.State != admission.StateReserved {
		t.Fatalf("invalid token changed operation = %#v, %v", unchanged, err)
	}
	if attempts, err := repository.Attempts(ctx, id); err != nil || len(attempts) != 0 {
		t.Fatalf("invalid token persisted attempt = %#v, %v", attempts, err)
	}
	if err := repository.FailBeforeDispatch(ctx, failure); err != nil {
		t.Fatal(err)
	}
	failed, err := repository.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if failed.State != admission.StateDefiniteFailed || failed.CompletedAt.IsZero() || failed.CostStatus != "exact" || failed.CostMethod != "worker_cache_zero" || failed.ActualCostUSD == nil || failed.ActualCostUSD.Cmp(pricing.MustUSD("0")) != 0 {
		t.Fatalf("atomic pre-write failure = %#v", failed)
	}
	persistedAttempts, err := repository.Attempts(ctx, id)
	if err != nil || len(persistedAttempts) != 1 || persistedAttempts[0].AttemptNumber != 1 || persistedAttempts[0].RouteID != failure.Attempt.RouteID || persistedAttempts[0].Dispatch != admission.NotDispatched {
		t.Fatalf("atomic pre-write attempt = %#v, %v", persistedAttempts, err)
	}
	if err := repository.FailBeforeDispatch(ctx, failure); err != nil {
		t.Fatalf("idempotent replay = %v", err)
	}
	replayedAttempts, err := repository.Attempts(ctx, id)
	if err != nil || len(replayedAttempts) != 1 {
		t.Fatalf("replay duplicated attempt = %#v, %v", replayedAttempts, err)
	}
	conflict := failure
	conflict.Attempt.EndpointID = "different-endpoint"
	if err := repository.FailBeforeDispatch(ctx, conflict); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("divergent replay = %v, want operation conflict", err)
	}

	attemptRelation, err := repository.Namespace.Render("operation_attempts")
	if err != nil {
		t.Fatal(err)
	}
	var attemptState, attemptMethod string
	if err := repository.Pool.QueryRow(ctx, "SELECT state, COALESCE(cost_method,'') FROM "+attemptRelation+" WHERE operation_id=$1 AND attempt_number=1", operationUUID(id)).Scan(&attemptState, &attemptMethod); err != nil {
		t.Fatal(err)
	}
	if attemptState != "pre_write_failed" || attemptMethod != "definite_uncharged_zero" {
		t.Fatalf("atomic attempt terminal facts = %q, %q", attemptState, attemptMethod)
	}

	noProviderID := "operation-no-provider-failure-" + uuid.NewString()
	noProvider, err := repository.Begin(ctx, admission.BeginRequest{
		ID: noProviderID, OperationKey: noProviderID, Actor: "postgres-test",
		ScopeKey: "failure/no-provider", RequestDigest: admission.Digest([]byte(noProviderID)),
		ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	noProviderFailure := admission.FailRequest{
		OperationID: noProviderID, DispatchToken: noProvider.Operation.DispatchToken,
		Certainty: admission.NotDispatched, Reason: "checkpoint_bounds_exceeded",
		Attempt: admission.AttemptFacts{Dispatch: admission.NotDispatched},
	}
	if err := repository.FailBeforeDispatch(ctx, noProviderFailure); err != nil {
		t.Fatal(err)
	}
	noProviderAttempts, err := repository.Attempts(ctx, noProviderID)
	if err != nil || len(noProviderAttempts) != 1 {
		t.Fatalf("no-provider attempt = %#v, %v", noProviderAttempts, err)
	}
	noProviderAttempt := noProviderAttempts[0]
	if noProviderAttempt.RouteID != "" || noProviderAttempt.EndpointID != "" ||
		noProviderAttempt.Provider != "" || noProviderAttempt.ResolvedModel != "" ||
		noProviderAttempt.ServiceClass != "" {
		t.Fatalf("pre-dispatch rejection persisted fallback provider identity: %#v", noProviderAttempt)
	}
	var endpointFamily, modelRevision string
	if err := repository.Pool.QueryRow(ctx,
		"SELECT endpoint_family, route_model_revision FROM "+attemptRelation+" WHERE operation_id=$1 AND attempt_number=1",
		operationUUID(noProviderID),
	).Scan(&endpointFamily, &modelRevision); err != nil {
		t.Fatal(err)
	}
	if endpointFamily != "" || modelRevision != "" {
		t.Fatalf("pre-dispatch rejection persisted fallback route provenance: %q/%q", endpointFamily, modelRevision)
	}
	if err := repository.FailBeforeDispatch(ctx, noProviderFailure); err != nil {
		t.Fatalf("idempotent no-provider replay = %v", err)
	}
	replayedNoProviderAttempts, err := repository.Attempts(ctx, noProviderID)
	if err != nil || !reflect.DeepEqual(replayedNoProviderAttempts, noProviderAttempts) {
		t.Fatalf("no-provider replay changed receipt: before=%#v after=%#v err=%v", noProviderAttempts, replayedNoProviderAttempts, err)
	}
	var operationRoute, operationEndpoint, operationProvider, operationModel, operationClass string
	operationRelation, err := repository.Namespace.Render("operations")
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Pool.QueryRow(ctx,
		"SELECT COALESCE(route_id,''), COALESCE(endpoint_id,''), COALESCE(provider,''), COALESCE(resolved_model,''), COALESCE(attempted_service_class,'') FROM "+operationRelation+" WHERE operation_id=$1",
		operationUUID(noProviderID),
	).Scan(&operationRoute, &operationEndpoint, &operationProvider, &operationModel, &operationClass); err != nil {
		t.Fatal(err)
	}
	if operationRoute != "" || operationEndpoint != "" || operationProvider != "" ||
		operationModel != "" || operationClass != "" {
		t.Fatalf("pre-dispatch operation persisted fallback provider identity: %q/%q/%q/%q/%q", operationRoute, operationEndpoint, operationProvider, operationModel, operationClass)
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
	started, err := repository.Begin(ctx, admission.BeginRequest{ID: completedID, OperationKey: completedID, Actor: "postgres-test", ScopeKey: "attempt-terminal/project", RequestDigest: admission.Digest([]byte(completedID)),
		ReservationUSD: pricing.MustUSD("1.25"), ExpiresAt: time.Now().UTC().Add(time.Hour)})
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
	started, err = repository.Begin(ctx, admission.BeginRequest{ID: ambiguousID, OperationKey: ambiguousID, Actor: "postgres-test", ScopeKey: "attempt-terminal/project", RequestDigest: admission.Digest([]byte(ambiguousID)),
		ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkDispatching(ctx, admission.DispatchRequest{OperationID: ambiguousID, DispatchToken: started.Operation.DispatchToken, Attempt: attempt}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Fail(ctx, admission.FailRequest{OperationID: ambiguousID, DispatchToken: started.Operation.DispatchToken, Certainty: admission.Accepted, Reason: "Provider timeout: request/123", Attempt: attempt}); err != nil {
		t.Fatal(err)
	}
	// The operation outcome is ambiguous, but dispatch itself was accepted.
	assertTerminalAttempt(t, ctx, repository, attempts, ambiguousID, "ambiguous", "accepted", "unknown", "", "")
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
