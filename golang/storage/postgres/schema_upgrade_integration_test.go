package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestReleasedV1UpgradeRemovesContentAndResolvesOriginalOperation(t *testing.T) {
	addr := os.Getenv("LLMTW_POSTGRES_ADDR")
	if addr == "" {
		t.Skip("LLMTW_POSTGRES_ADDR is not configured; set it for PostgreSQL integration tests")
	}
	namespace, err := NewNamespace(
		valueOr("LLMTW_POSTGRES_DATABASE", "llm_worker"),
		fmt.Sprintf("upgrade_worker_%d", time.Now().UnixNano()),
		"worker_",
	)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(context.Background(), PoolOptions{
		Namespace: namespace, Addresses: []string{addr},
		Username:       valueOr("LLMTW_POSTGRES_USER", "llmtw"),
		Password:       valueOr("LLMTW_POSTGRES_PASSWORD", "llmtw"),
		MaxConnections: 4, MinConnections: 1,
		DialTimeout: 5 * time.Second, StatementTimeout: 10 * time.Second,
		LockTimeout: time.Second, IdleTxTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := namespace.SchemaIdentifier().Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")

	v1SQL, err := renderSchemaMigration(namespace, orderedSchemaMigrations[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, v1SQL); err != nil {
		t.Fatalf("install released v1 fixture: %v", err)
	}
	v1Digest := sha256.Sum256([]byte(v1SQL))
	contracts, _ := namespace.Render("schema_contract")
	if _, err := pool.Exec(ctx, "INSERT INTO "+contracts+" (contract_name, contract_version, migration_digest) VALUES ($1,$1,$2)", contractVersionV1, v1Digest[:]); err != nil {
		t.Fatal(err)
	}

	key := []byte("01234567890123456789012345678901")
	scopeKeys := ScopeKeyring{ActiveVersion: "scope-v1", Keys: map[string][]byte{"scope-v1": key}}
	tenantDigest, projectDigest, err := scopeKeys.Derive("upgrade-tenant", "upgrade-project")
	if err != nil {
		t.Fatal(err)
	}
	scopeID := uuid.New()
	scopes, _ := namespace.Render("scopes")
	if _, err := pool.Exec(ctx, "INSERT INTO "+scopes+" (scope_id, tenant_hmac, project_hmac) VALUES ($1,$2,$3)", scopeID, tenantDigest[:], projectDigest[:]); err != nil {
		t.Fatal(err)
	}
	configDigest := sha256.Sum256([]byte("upgrade-config"))
	configs, _ := namespace.Render("configuration_snapshots")
	if _, err := pool.Exec(ctx, "INSERT INTO "+configs+" (config_digest, config_version, source_digest, sanitized_config) VALUES ($1,'upgrade-config',$1,'{}'::jsonb)", configDigest[:]); err != nil {
		t.Fatal(err)
	}

	payload := []byte(`{"api_version":"llm.temporal/v1","operation_key":"released-key","context":{"tenant":"upgrade-tenant","project":"upgrade-project","actor":"forecast-agent"},"secret":"released-v1-plaintext"}`)
	requestDigest := admission.Digest(payload)
	releasedMaterial := append([]byte("generate\x00released-key\x00"), requestDigest[:]...)
	releasedID := uuid.NewSHA1(uuid.NameSpaceOID, releasedMaterial)
	strictMaterial := []byte("llmtw/operation/v2\x00")
	for _, component := range []string{"upgrade-tenant\x00upgrade-project", "forecast-agent", "generate", "llm.temporal/v1", "released-key"} {
		strictMaterial = append(strictMaterial, component...)
		strictMaterial = append(strictMaterial, 0)
	}
	newID := uuid.NewSHA1(uuid.NameSpaceOID, strictMaterial)
	fingerprint := operationHMAC(key, "request-fingerprint", requestDigest[:])
	releasedOperationKey := operationHMAC(key, "operation-key", []byte(releasedID.String()))
	keyring := Keyring{Active: "op-v1", Keys: map[string][]byte{"op-v1": key}}
	sealed, err := keyring.Seal(EnvelopeContext{ScopeID: scopeID, OperationID: releasedID, PayloadKind: "operation-request", Digest: requestDigest}, payload)
	if err != nil {
		t.Fatal(err)
	}
	operations, _ := namespace.Render("operations")
	var operationsOIDBefore uint32
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1)::oid", operations).Scan(&operationsOIDBefore); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO "+operations+" (operation_id, scope_id, operation_kind, api_version, operation_key_hmac, request_fingerprint_hmac, request_digest, request_schema_version, request_manifest_jsonb, request_inline_ciphertext, request_key_id, config_digest, state, operation_expires_at, reserved_cost_usd, incurred_cost_usd, cost_status) VALUES ($1,$2,'generate','llm.temporal/v1',$3,$4,$5,1,$6::jsonb,$7,$8,$9,'dispatching',$10,0,0,'pending')", releasedID, scopeID, releasedOperationKey[:], fingerprint[:], requestDigest[:], payload, sealed.Ciphertext, sealed.KeyID, configDigest[:], time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	originalCiphertext := append([]byte(nil), sealed.Ciphertext...)

	if err := Install(ctx, pool, namespace); err != nil {
		t.Fatalf("upgrade released v1 schema: %v", err)
	}
	if err := Verify(ctx, pool, namespace); err != nil {
		t.Fatalf("verify upgraded schema: %v", err)
	}
	var operationsOIDAfter uint32
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1)::oid", operations).Scan(&operationsOIDAfter); err != nil {
		t.Fatal(err)
	}
	if operationsOIDAfter != operationsOIDBefore {
		t.Fatalf("operations table was recreated during upgrade: oid %d became %d", operationsOIDBefore, operationsOIDAfter)
	}
	var markerCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+contracts).Scan(&markerCount); err != nil {
		t.Fatal(err)
	}
	if markerCount != len(orderedSchemaMigrations) {
		t.Fatalf("schema marker count = %d, want %d", markerCount, len(orderedSchemaMigrations))
	}
	var manifest string
	var payloadSHA []byte
	var payloadBytes int64
	var payloadReference string
	var upgradedCiphertext []byte
	if err := pool.QueryRow(ctx, "SELECT request_manifest_jsonb::text, request_payload_sha256, request_payload_byte_length, request_payload_reference, request_inline_ciphertext FROM "+operations+" WHERE operation_id=$1", releasedID).Scan(&manifest, &payloadSHA, &payloadBytes, &payloadReference, &upgradedCiphertext); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains([]byte(manifest), []byte("released-v1-plaintext")) {
		t.Fatalf("upgraded manifest retained plaintext: %s", manifest)
	}
	if !bytes.Equal(payloadSHA, requestDigest[:]) || payloadBytes != int64(len(payload)) || payloadReference != "inline" {
		t.Fatalf("upgraded request metadata = sha %x bytes %d ref %q", payloadSHA, payloadBytes, payloadReference)
	}
	if !bytes.Equal(upgradedCiphertext, originalCiphertext) {
		t.Fatal("upgrade changed the encrypted canonical request payload")
	}
	envelope := EnvelopeContext{ScopeID: scopeID, OperationID: releasedID, PayloadKind: "operation-request", Digest: requestDigest}
	contextHash, err := contextDigest(envelope)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := keyring.Open(envelope, SealedValue{KeyID: sealed.KeyID, Ciphertext: upgradedCiphertext, ContextHash: contextHash})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(opened, payload) {
		t.Fatal("upgraded canonical request payload did not decrypt exactly")
	}

	repository := DefaultOperationRepository(pool, namespace, keyring, DefaultScopeRepository(pool, namespace, scopeKeys))
	manifestV2, err := canonicalOperationRequestManifest(payload, requestDigest)
	if err != nil {
		t.Fatal(err)
	}
	beginRequest := admission.BeginRequest{
		ID: newID.String(), ReleasedID: releasedID.String(),
		OperationKey: "released-key", Actor: "forecast-agent",
		ScopeKey: "upgrade-tenant\x00upgrade-project", RequestDigest: requestDigest,
		ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour),
		OperationKind: "generate", APIVersion: "llm.temporal/v1", RequestSchemaVersion: 1,
		RequestManifest: manifestV2, RequestPayload: payload, ConfigDigest: configDigest,
	}
	if _, err := pool.Exec(ctx, "UPDATE "+operations+" SET operation_key_hmac=$2 WHERE operation_id=$1", releasedID, bytes.Repeat([]byte{0xff}, sha256.Size)); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Begin(ctx, beginRequest); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("rebind released row with mismatched operation key error = %v, want conflict", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE "+operations+" SET operation_key_hmac=$2 WHERE operation_id=$1", releasedID, releasedOperationKey[:]); err != nil {
		t.Fatal(err)
	}
	replay, err := repository.Begin(ctx, beginRequest)
	if err != nil {
		t.Fatalf("replay upgraded v1 operation: %v", err)
	}
	if !replay.Existing || replay.Operation.ID != releasedID.String() || replay.Operation.State != admission.StateDispatching {
		t.Fatalf("replay resolved operation = %#v, want original %s", replay, releasedID)
	}
	var identityVersion int
	var actorHMAC, operationKeyHMAC []byte
	if err := pool.QueryRow(ctx, "SELECT operation_identity_version, operation_actor_hmac, operation_key_hmac FROM "+operations+" WHERE operation_id=$1", releasedID).Scan(&identityVersion, &actorHMAC, &operationKeyHMAC); err != nil {
		t.Fatal(err)
	}
	expectedActor := operationHMAC(key, "operation-actor", []byte("forecast-agent"))
	expectedOperationKey := operationHMAC(key, "operation-key-v2", []byte("forecast-agent\x00llm.temporal/v1\x00released-key"))
	if identityVersion != 2 || !bytes.Equal(actorHMAC, expectedActor[:]) || !bytes.Equal(operationKeyHMAC, expectedOperationKey[:]) {
		t.Fatalf("released row strict identity = version %d actor %x key %x", identityVersion, actorHMAC, operationKeyHMAC)
	}
	beginRequest.ReleasedID = ""
	restartedRepository := DefaultOperationRepository(pool, namespace, keyring, DefaultScopeRepository(pool, namespace, scopeKeys))
	restartedReplay, err := restartedRepository.Begin(ctx, beginRequest)
	if err != nil {
		t.Fatalf("replay upgraded v1 operation after repository restart: %v", err)
	}
	if !restartedReplay.Existing || restartedReplay.Operation.ID != releasedID.String() {
		t.Fatalf("restart replay resolved operation = %#v, want original %s", restartedReplay, releasedID)
	}
	var operationCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+operations).Scan(&operationCount); err != nil {
		t.Fatal(err)
	}
	if operationCount != 1 {
		t.Fatalf("replay inserted %d operation rows, want exactly one", operationCount)
	}
}
