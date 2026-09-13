package postgres

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestBlobGCRechecksRetainedReferencesAndFinalizesIdempotently(t *testing.T) {
	fixture := newResponseCacheFixture(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	maintenance := MaintenanceRepository{Pool: fixture.operations.Pool, Namespace: fixture.operations.Namespace}
	blobs := BlobRepository{Pool: fixture.operations.Pool, Namespace: fixture.operations.Namespace, Keys: fixture.operations.Keys, NewID: UUIDv7}
	putPayload := func(name string, payload []byte) BlobRecord {
		digest := sha256.Sum256(payload)
		expires := now.Add(-time.Minute)
		record, err := blobs.PutLocator(fixture.ctx, fixture.scope.ID, "blob-gc-test", BlobMetadata{StoreID: "blob-gc-" + name + "-" + uuid.NewString(), Digest: digest, ByteLength: int64(len(payload)), MediaType: "application/octet-stream", ExpiresAt: &expires}, payload)
		if err != nil {
			t.Fatal(err)
		}
		return record
	}
	put := func(name string) BlobRecord {
		return putPayload(name, []byte("blob-gc-"+name+"-"+uuid.NewString()))
	}
	free := put("free")
	operationPayload := []byte(`{"model":"fixture"}`)
	operationBlob := putPayload("operation", operationPayload)
	checkpointBlob := put("checkpoint")
	providerBlob := put("provider")
	staleFinalizeBlob := put("stale-finalize")
	cacheBlob := put("cache")
	cacheStateBlob := put("cache-state")

	// An active operation request keeps its blob retained.
	operationDigest := sha256.Sum256(operationPayload)
	operation, err := fixture.operations.Begin(fixture.ctx, admission.BeginRequest{ID: "blob-gc-operation-" + uuid.NewString(), OperationKey: "blob-gc-operation-" + uuid.NewString(), Actor: "postgres-test", ScopeKey: "cache-integration-tenant/cache-integration-project",
		RequestDigest: operationDigest, ReservationUSD: pricing.MustUSD("0"),
		ExpiresAt: now.Add(time.Hour), RequestManifest: operationPayload})
	if err != nil {
		t.Fatal(err)
	}
	operations, err := fixture.operations.Namespace.Render("operations")
	if err != nil {
		t.Fatal(err)
	}
	if tag, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+operations+" SET request_inline_ciphertext=NULL, request_key_id=NULL, request_blob_id=$1::uuid, request_payload_reference=$1::text, request_manifest_jsonb=jsonb_build_object('schema_version',2,'payload_sha256',encode(request_payload_sha256,'hex'),'payload_bytes',request_payload_byte_length,'payload_reference',$1::text) WHERE operation_id=$2", operationBlob.BlobID, operationUUID(operation.Operation.ID)); err != nil {
		t.Fatal(err)
	} else if tag.RowsAffected() != 1 {
		t.Fatalf("operation blob reference update affected %d rows", tag.RowsAffected())
	}
	var storedRequestBlob uuid.UUID
	if err := fixture.operations.Pool.QueryRow(fixture.ctx, "SELECT request_blob_id FROM "+operations+" WHERE operation_id=$1", operationUUID(operation.Operation.ID)).Scan(&storedRequestBlob); err != nil {
		t.Fatal(err)
	}
	if storedRequestBlob != operationBlob.BlobID {
		t.Fatalf("stored operation blob=%s, want %s", storedRequestBlob, operationBlob.BlobID)
	}
	var retainedOperationRef int
	if err := fixture.operations.Pool.QueryRow(fixture.ctx, "SELECT 1 FROM "+operations+" WHERE request_blob_id=$1 AND (state NOT IN ('completed','definite_failed','canceled') OR retention_expires_at IS NULL OR retention_expires_at > $2)", operationBlob.BlobID, now).Scan(&retainedOperationRef); err != nil {
		t.Fatalf("operation reference predicate did not match: %v", err)
	}

	// The fixture checkpoint keeps a blob through its future expiry. Replacing
	// the metadata expiry with a past value proves reference checks win over the
	// blob's own expiry signal.
	checkpoints, err := fixture.operations.Namespace.Render("conversation_checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	if tag, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+checkpoints+" SET delta_blob_id=$1, response_blob_id=$1, settings_patch_blob_id=$1 WHERE checkpoint_id=$2", checkpointBlob.BlobID, fixture.checkpointID); err != nil {
		t.Fatal(err)
	} else if tag.RowsAffected() != 1 {
		t.Fatalf("checkpoint blob reference update affected %d rows", tag.RowsAffected())
	}

	providerState, err := fixture.operations.Namespace.Render("checkpoint_provider_state")
	if err != nil {
		t.Fatal(err)
	}
	accountDigest := sha256.Sum256([]byte("account"))
	if tag, err := fixture.operations.Pool.Exec(fixture.ctx, "INSERT INTO "+providerState+" (checkpoint_id, ordinal, provider, endpoint_id, endpoint_account_hmac, region, endpoint_family, model_lineage, state_kind, state_blob_id, state_digest, required, immutable_fork_safe, expires_at) VALUES ($1,0,'fixture','endpoint', $2,'region','family','lineage','opaque',$3,$4,true,true,$5)", fixture.checkpointID, accountDigest[:], providerBlob.BlobID, providerBlob.Digest[:], now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	} else if tag.RowsAffected() != 1 {
		t.Fatalf("provider-state blob reference insert affected %d rows", tag.RowsAffected())
	}
	if tag, err := fixture.operations.Pool.Exec(fixture.ctx, "INSERT INTO "+providerState+" (checkpoint_id, ordinal, provider, endpoint_id, endpoint_account_hmac, region, endpoint_family, model_lineage, state_kind, state_blob_id, state_digest, required, immutable_fork_safe, expires_at) VALUES ($1,1,'fixture','endpoint', $2,'region','family','lineage','opaque',$3,$4,true,true,$5)", fixture.checkpointID, accountDigest[:], staleFinalizeBlob.BlobID, staleFinalizeBlob.Digest[:], now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	} else if tag.RowsAffected() != 1 {
		t.Fatalf("stale-finalize provider reference insert affected %d rows", tag.RowsAffected())
	}

	// Publish a normal cache entry, then move its response to a blob. This uses
	// the production cache constraints and exercises the ready-cache fence.
	cache := DefaultResponseCacheRepository(fixture.operations.Pool, fixture.operations.Namespace, fixture.operations.Keys)
	key := testCacheKey()
	key.ScopeID = fixture.scope.ID
	fill := CacheFillRequest{Key: key, OperationID: fixture.originID, Lease: time.Minute}
	if acquired, err := cache.BeginFill(fixture.ctx, fill); err != nil || acquired.Status != CacheFillAcquired {
		t.Fatalf("begin cache fill=%#v err=%v", acquired, err)
	}
	entry, err := cache.Publish(fixture.ctx, CachePublishRequest{Fill: fill, CanonicalRequestJSON: []byte(`{"model":"fixture"}`), SemanticProfileVersion: "blob-gc", CacheEpoch: "blob-gc", OriginOperationID: fixture.originID, OriginCheckpointID: fixture.checkpointID, OriginProvider: "fixture", OriginEndpointID: "endpoint", OriginResolvedModel: "model", Response: []byte(`{"response":"blob-gc"}`)})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := fixture.operations.Namespace.Render("response_cache_entries")
	if err != nil {
		t.Fatal(err)
	}
	if tag, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+entries+" SET response_inline_ciphertext=NULL, response_key_id=NULL, response_blob_id=$1, state='ready' WHERE cache_entry_id=$2", cacheBlob.BlobID, entry.ID); err != nil {
		t.Fatal(err)
	} else if tag.RowsAffected() != 1 {
		t.Fatalf("cache blob reference update affected %d rows", tag.RowsAffected())
	}
	// A tombstoned entry may still carry a response blob. Once that blob is
	// claimed, changing the entry back to ready must be fenced by the database.
	stateOrigin, err := fixture.operations.Begin(fixture.ctx, admission.BeginRequest{ID: "blob-gc-state-origin-" + uuid.NewString(), OperationKey: "blob-gc-state-origin-" + uuid.NewString(), Actor: "postgres-test", ScopeKey: "cache-integration-tenant/cache-integration-project",
		RequestDigest: admission.Digest([]byte("blob-gc-state-origin")), ReservationUSD: pricing.MustUSD("0"),
		ExpiresAt: now.Add(time.Hour), RequestManifest: []byte(`{"model":"fixture-state"}`)})
	if err != nil {
		t.Fatal(err)
	}
	stateKey := testCacheKey()
	stateKey.ScopeID = fixture.scope.ID
	stateFill := CacheFillRequest{Key: stateKey, OperationID: stateOrigin.Operation.ID, Lease: time.Minute}
	if acquired, err := cache.BeginFill(fixture.ctx, stateFill); err != nil || acquired.Status != CacheFillAcquired {
		t.Fatalf("begin cache state fill=%#v err=%v", acquired, err)
	}
	stateEntry, err := cache.Publish(fixture.ctx, CachePublishRequest{Fill: stateFill, CanonicalRequestJSON: []byte(`{"model":"fixture-state"}`), SemanticProfileVersion: "blob-gc", CacheEpoch: "blob-gc", OriginOperationID: stateOrigin.Operation.ID, OriginCheckpointID: fixture.checkpointID, OriginProvider: "fixture", OriginEndpointID: "endpoint", OriginResolvedModel: "model", Response: []byte(`{"response":"blob-gc-state"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if tag, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+entries+" SET response_inline_ciphertext=NULL, response_key_id=NULL, response_blob_id=$1, state='tombstoned' WHERE cache_entry_id=$2", cacheStateBlob.BlobID, stateEntry.ID); err != nil {
		t.Fatal(err)
	} else if tag.RowsAffected() != 1 {
		t.Fatalf("tombstoned cache blob update affected %d rows", tag.RowsAffected())
	}
	uses, err := fixture.operations.Namespace.Render("response_cache_uses")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "DELETE FROM "+uses+" WHERE cache_entry_id=$1", stateEntry.ID); err != nil {
		t.Fatal(err)
	}

	result, err := maintenance.MarkExpiredBlobsEligible(fixture.ctx, now, 100)
	if err != nil {
		t.Fatal(err)
	}
	if result.Eligible < 1 {
		t.Fatalf("eligible result=%#v, expected the unreferenced blob", result)
	}
	blobsTable, err := fixture.operations.Namespace.Render("blobs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+blobsTable+" SET deletion_state='deleting' WHERE blob_id=$1", staleFinalizeBlob.BlobID); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.FinalizeBlobDeletion(fixture.ctx, staleFinalizeBlob.BlobID); !errors.Is(err, ErrBlobDeletionReferences) {
		t.Fatalf("finalize with active reference error=%v, want ErrBlobDeletionReferences", err)
	}
	for name, id := range map[string]uuid.UUID{"operation": operationBlob.BlobID, "checkpoint": checkpointBlob.BlobID, "provider": providerBlob.BlobID, "cache": cacheBlob.BlobID} {
		claims, err := maintenance.ClaimBlobDeletion(fixture.ctx, now, []uuid.UUID{id}, 1)
		if err != nil {
			t.Fatalf("claim %s: %v", name, err)
		}
		if len(claims) != 0 {
			t.Fatalf("claim %s returned retained blob: %#v", name, claims)
		}
	}
	claims, err := maintenance.ClaimBlobDeletion(fixture.ctx, now, []uuid.UUID{free.BlobID}, 1)
	if err != nil || len(claims) != 1 || claims[0].DeletionState != "deleting" {
		t.Fatalf("claim free=%#v err=%v", claims, err)
	}
	stateClaims, err := maintenance.ClaimBlobDeletion(fixture.ctx, now, []uuid.UUID{cacheStateBlob.BlobID}, 1)
	if err != nil || len(stateClaims) != 1 || stateClaims[0].DeletionState != "deleting" {
		t.Fatalf("claim tombstoned cache blob=%#v err=%v", stateClaims, err)
	}
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+entries+" SET state='ready' WHERE cache_entry_id=$1", stateEntry.ID); err == nil {
		t.Fatal("tombstoned cache entry was revived after response blob deletion claim")
	}
	// The claim is committed before the object-store call. Database guards must
	// reject every new direct blob reference during that external-delete window.
	lateOperation, err := fixture.operations.Begin(fixture.ctx, admission.BeginRequest{ID: "blob-gc-late-operation-" + uuid.NewString(), OperationKey: "blob-gc-late-operation-" + uuid.NewString(), Actor: "postgres-test", ScopeKey: "cache-integration-tenant/cache-integration-project",
		RequestDigest: admission.Digest([]byte("blob-gc-late-operation")), ReservationUSD: pricing.MustUSD("0"),
		ExpiresAt: now.Add(time.Hour), RequestManifest: []byte(`{"model":"fixture"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+operations+" SET request_inline_ciphertext=NULL, request_key_id=NULL, request_blob_id=$1 WHERE operation_id=$2", free.BlobID, operationUUID(lateOperation.Operation.ID)); err == nil {
		t.Fatal("late operation blob reference was accepted after deletion claim")
	}
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+checkpoints+" SET delta_blob_id=$1 WHERE checkpoint_id=$2", free.BlobID, fixture.checkpointID); err == nil {
		t.Fatal("late checkpoint blob reference was accepted after deletion claim")
	}
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+providerState+" SET state_blob_id=$1 WHERE checkpoint_id=$2 AND ordinal=0", free.BlobID, fixture.checkpointID); err == nil {
		t.Fatal("late provider-state blob reference was accepted after deletion claim")
	}
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+entries+" SET response_inline_ciphertext=NULL, response_key_id=NULL, response_blob_id=$1 WHERE cache_entry_id=$2", free.BlobID, entry.ID); err == nil {
		t.Fatal("late cache blob reference was accepted after deletion claim")
	}
	if _, err := blobs.PutLocator(fixture.ctx, fixture.scope.ID, "blob-gc-test", BlobMetadata{StoreID: free.StoreID, Digest: free.Digest, ByteLength: free.ByteLength, MediaType: free.MediaType}, []byte("late-reuse")); !errors.Is(err, ErrBlobNotWritable) {
		t.Fatalf("late content-addressed blob reuse error=%v, want ErrBlobNotWritable", err)
	}
	if err := maintenance.FinalizeBlobDeletion(fixture.ctx, free.BlobID); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.FinalizeBlobDeletion(fixture.ctx, cacheStateBlob.BlobID); err != nil {
		t.Fatalf("finalize tombstoned cache blob: %v", err)
	}
	if err := maintenance.FinalizeBlobDeletion(fixture.ctx, free.BlobID); err != nil {
		t.Fatalf("repeat finalize was not idempotent: %v", err)
	}
	if err := maintenance.FinalizeBlobDeletion(fixture.ctx, uuid.New()); err != nil {
		t.Fatalf("missing blob finalize was not a success: %v", err)
	}
}
