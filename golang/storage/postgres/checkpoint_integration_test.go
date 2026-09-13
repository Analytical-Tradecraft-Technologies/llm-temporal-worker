package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/blob"
	"github.com/mfow/llm-temporal-worker/golang/storage/fileblob"
)

func TestCheckpointRepositoryPublishReadAndRetry(t *testing.T) {
	operations, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()
	scope, err := operations.Scopes.Ensure(ctx, "checkpoint-integration-tenant", "checkpoint-integration-project")
	if err != nil {
		t.Fatal(err)
	}
	operationID := "checkpoint-origin-" + uuid.NewString()
	origin, err := operations.Begin(ctx, admission.BeginRequest{ID: operationID, OperationKey: operationID, Actor: "postgres-test", ScopeKey: "checkpoint-integration-tenant/checkpoint-integration-project",
		RequestDigest: admission.Digest([]byte(operationID)), ReservationUSD: pricing.MustUSD("0"),
		ExpiresAt: time.Now().UTC().Add(time.Hour), RequestManifest: []byte(`{"model":"fixture"}`)})
	if err != nil {
		t.Fatal(err)
	}
	blobs := BlobRepository{Pool: operations.Pool, Namespace: operations.Namespace, Keys: operations.Keys, NewID: UUIDv7}
	payload := []byte("checkpoint-fixture")
	digest := sha256.Sum256(payload)
	blob, err := blobs.PutLocator(ctx, scope.ID, "checkpoint-fixture", BlobMetadata{StoreID: "checkpoint-fixture-store", Digest: digest, ByteLength: int64(len(payload)), MediaType: "application/json"}, payload)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	checkpoint := state.DurableCheckpoint{
		ID: state.CheckpointID(uuid.NewString()), ScopeID: scope.ID.String(), PublicIDHMAC: sha256.Sum256([]byte("public")), HandleKeyID: operations.Keys.Active,
		Kind: state.CheckpointGeneration, Depth: 0, OriginOperationID: state.OperationID(operationUUID(origin.Operation.ID).String()),
		DeltaBlob:              state.CheckpointBlobReference{ID: state.BlobID(blob.BlobID.String()), Digest: digest, ByteLength: int64(len(payload)), MediaType: "application/json"},
		ResponseBlob:           state.CheckpointBlobReference{ID: state.BlobID(blob.BlobID.String()), Digest: digest, ByteLength: int64(len(payload)), MediaType: "application/json"},
		SettingsPatchBlob:      state.CheckpointBlobReference{ID: state.BlobID(blob.BlobID.String()), Digest: digest, ByteLength: int64(len(payload)), MediaType: "application/json"},
		CanonicalLineageDigest: sha256.Sum256([]byte("lineage")), MaterializedSettingsDigest: sha256.Sum256([]byte("settings")), ToolFrontierDigest: sha256.Sum256([]byte("frontier")),
		SchemaVersion: 1, CompilerEpoch: "checkpoint-test-v1", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	repository := DurableCheckpointRepository{Pool: operations.Pool, Namespace: operations.Namespace, Now: func() time.Time { return now }}
	if err := state.WithCheckpointUnitOfWork(ctx, repository, func(ctx context.Context, unit state.CheckpointUnitOfWork) error {
		return unit.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: checkpoint})
	}); err != nil {
		t.Fatal(err)
	}
	read, err := repository.Get(ctx, checkpoint.ScopeID, checkpoint.ID)
	if err != nil {
		t.Fatal(err)
	}
	if read.ID != checkpoint.ID || read.ScopeID != checkpoint.ScopeID || read.DeltaBlob != checkpoint.DeltaBlob || read.OriginOperationID != checkpoint.OriginOperationID {
		t.Fatalf("read checkpoint=%#v, want %#v", read, checkpoint)
	}
	if err := state.WithCheckpointUnitOfWork(ctx, repository, func(ctx context.Context, unit state.CheckpointUnitOfWork) error {
		return unit.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: checkpoint})
	}); err != nil {
		t.Fatalf("same immutable retry failed: %v", err)
	}
	childConflict := checkpoint
	childConflict.ProviderState = []state.CheckpointProviderState{{
		Ordinal: 0, Provider: "fixture", EndpointID: "endpoint-fixture", EndpointAccountHMAC: sha256.Sum256([]byte("account")),
		Region: "test", EndpointFamily: "fixture", ModelLineage: "fixture-v1", StateKind: "opaque",
		StateBlob: checkpoint.DeltaBlob, StateDigest: digest, Required: true, ImmutableForkSafe: true, CreatedAt: now,
	}}
	err = state.WithCheckpointUnitOfWork(ctx, repository, func(ctx context.Context, unit state.CheckpointUnitOfWork) error {
		return unit.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: childConflict})
	})
	if err != ErrCheckpointConflict {
		t.Fatalf("different child retry error=%v, want ErrCheckpointConflict", err)
	}
	conflict := checkpoint
	conflict.PublicIDHMAC = sha256.Sum256([]byte("different-public"))
	err = state.WithCheckpointUnitOfWork(ctx, repository, func(ctx context.Context, unit state.CheckpointUnitOfWork) error {
		return unit.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: conflict})
	})
	if err != ErrCheckpointConflict {
		t.Fatalf("different immutable retry error=%v, want ErrCheckpointConflict", err)
	}
}

func TestCheckpointBlobLocatorReuseExtendsAuthenticatedExpiry(t *testing.T) {
	operations, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	scope, err := operations.Scopes.Ensure(ctx, "checkpoint-locator-expiry-"+uuid.NewString(), "checkpoint-project")
	if err != nil {
		t.Fatal(err)
	}
	repository := BlobRepository{Pool: operations.Pool, Namespace: operations.Namespace, Keys: operations.Keys, NewID: UUIDv7}
	payload := []byte("checkpoint-locator-expiry")
	digest := sha256.Sum256(payload)
	originalExpiry := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	ref := blob.Ref{
		Store: "checkpoint-expiry-store", Locator: "checkpoint-expiry-object",
		Digest: blob.Digest(payload), ByteLength: int64(len(payload)),
		MediaType: "application/json", ExpiresAt: originalExpiry,
	}
	encoded, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := llm.CanonicalJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	original, err := repository.PutLocator(ctx, scope.ID, "checkpoint", BlobMetadata{
		StoreID: ref.Store, Digest: digest, ByteLength: ref.ByteLength,
		MediaType: ref.MediaType, ExpiresAt: &originalExpiry,
	}, canonical)
	if err != nil {
		t.Fatal(err)
	}
	extendedExpiry := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	ref.ExpiresAt = extendedExpiry
	writeLocator, err := CheckpointBlobLocatorWriter(repository)
	if err != nil {
		t.Fatal(err)
	}
	reused, err := writeLocator(ctx, scope.ID.String(), ref)
	if err != nil {
		t.Fatalf("reuse checkpoint locator: %v", err)
	}
	if reused.ID != state.BlobID(original.BlobID.String()) {
		t.Fatalf("reuse changed content-addressed blob ID: got %s want %s", reused.ID, original.BlobID)
	}

	refreshed, err := repository.Get(ctx, scope.ID, original.BlobID)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.ExpiresAt == nil || !refreshed.ExpiresAt.Equal(extendedExpiry) {
		t.Fatalf("authoritative expiry = %v, want %v", refreshed.ExpiresAt, extendedExpiry)
	}
	resolve := CheckpointBlobLocator(repository)
	resolved, err := resolve(ctx, scope.ID.String(), reused.ID)
	if err != nil {
		t.Fatalf("resolve reused locator after original expiry: %v", err)
	}
	if !resolved.ExpiresAt.Equal(extendedExpiry) {
		t.Fatalf("decrypted locator expiry = %v, want %v", resolved.ExpiresAt, extendedExpiry)
	}

	relation, err := repository.Namespace.Render("blobs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Pool.Exec(ctx,
		"UPDATE "+relation+" SET locator_ciphertext=$1, locator_key_id=$2, encryption_context_digest=$3 WHERE blob_id=$4",
		original.Locator.Ciphertext, original.Locator.KeyID, original.Locator.ContextHash[:], original.BlobID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := resolve(ctx, scope.ID.String(), reused.ID); err == nil {
		t.Fatal("stale expired locator resolved against extended database expiry")
	}

	tampered := append([]byte(nil), refreshed.Locator.Ciphertext...)
	tampered[len(tampered)-1] ^= 0xff
	if _, err := repository.Pool.Exec(ctx,
		"UPDATE "+relation+" SET locator_ciphertext=$1, locator_key_id=$2, encryption_context_digest=$3 WHERE blob_id=$4",
		tampered, refreshed.Locator.KeyID, refreshed.Locator.ContextHash[:], refreshed.BlobID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := resolve(ctx, scope.ID.String(), reused.ID); err == nil {
		t.Fatal("tampered checkpoint locator resolved")
	}
}

func TestCheckpointRepositoryRejectsCrossScopeCompactionReference(t *testing.T) {
	operations, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	scope, err := operations.Scopes.Ensure(ctx, "checkpoint-scope-a", "checkpoint-project")
	if err != nil {
		t.Fatal(err)
	}
	otherScope, err := operations.Scopes.Ensure(ctx, "checkpoint-scope-b", "checkpoint-project")
	if err != nil {
		t.Fatal(err)
	}
	blobs := BlobRepository{Pool: operations.Pool, Namespace: operations.Namespace, Keys: operations.Keys, NewID: UUIDv7}
	makeOrigin := func(scopeKey string) (admission.BeginResult, error) {
		id := "checkpoint-cross-scope-" + uuid.NewString()
		return operations.Begin(ctx, admission.BeginRequest{ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: scopeKey, RequestDigest: admission.Digest([]byte(id)), ReservationUSD: pricing.MustUSD("0"),
			ExpiresAt: time.Now().UTC().Add(time.Hour), RequestManifest: []byte(`{"model":"fixture"}`)})
	}
	originA, err := makeOrigin("checkpoint-scope-a/checkpoint-project")
	if err != nil {
		t.Fatal(err)
	}
	originB, err := makeOrigin("checkpoint-scope-b/checkpoint-project")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("checkpoint-cross-scope-fixture")
	digest := sha256.Sum256(payload)
	blobA, err := blobs.PutLocator(ctx, scope.ID, "checkpoint-cross-scope-a", BlobMetadata{StoreID: "checkpoint-cross-scope-store", Digest: digest, ByteLength: int64(len(payload)), MediaType: "application/json"}, payload)
	if err != nil {
		t.Fatal(err)
	}
	blobB, err := blobs.PutLocator(ctx, otherScope.ID, "checkpoint-cross-scope-b", BlobMetadata{StoreID: "checkpoint-cross-scope-store", Digest: digest, ByteLength: int64(len(payload)), MediaType: "application/json"}, payload)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	newCheckpoint := func(scopeID uuid.UUID, operationID string, blob BlobRecord, name string) state.DurableCheckpoint {
		return state.DurableCheckpoint{
			ID: state.CheckpointID(uuid.NewString()), ScopeID: scopeID.String(), PublicIDHMAC: sha256.Sum256([]byte(name)), HandleKeyID: operations.Keys.Active,
			Kind: state.CheckpointGeneration, Depth: 0, OriginOperationID: state.OperationID(operationUUID(operationID).String()),
			DeltaBlob:              state.CheckpointBlobReference{ID: state.BlobID(blob.BlobID.String()), Digest: digest, ByteLength: int64(len(payload)), MediaType: "application/json"},
			ResponseBlob:           state.CheckpointBlobReference{ID: state.BlobID(blob.BlobID.String()), Digest: digest, ByteLength: int64(len(payload)), MediaType: "application/json"},
			SettingsPatchBlob:      state.CheckpointBlobReference{ID: state.BlobID(blob.BlobID.String()), Digest: digest, ByteLength: int64(len(payload)), MediaType: "application/json"},
			CanonicalLineageDigest: sha256.Sum256([]byte(name + "-lineage")), MaterializedSettingsDigest: sha256.Sum256([]byte(name + "-settings")), ToolFrontierDigest: sha256.Sum256([]byte(name + "-frontier")),
			SchemaVersion: 1, CompilerEpoch: "checkpoint-cross-scope-v1", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		}
	}
	checkpointA := newCheckpoint(scope.ID, originA.Operation.ID, blobA, "checkpoint-a")
	checkpointB := newCheckpoint(otherScope.ID, originB.Operation.ID, blobB, "checkpoint-b")
	repository := DurableCheckpointRepository{Pool: operations.Pool, Namespace: operations.Namespace, Now: func() time.Time { return now }}
	for _, checkpoint := range []state.DurableCheckpoint{checkpointA, checkpointB} {
		if err := state.WithCheckpointUnitOfWork(ctx, repository, func(ctx context.Context, unit state.CheckpointUnitOfWork) error {
			return unit.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: checkpoint})
		}); err != nil {
			t.Fatal(err)
		}
	}
	crossScope := checkpointA
	crossScope.ID = state.CheckpointID(uuid.NewString())
	crossScope.PublicIDHMAC = sha256.Sum256([]byte("cross-scope-compaction"))
	crossScope.CompactedThroughID = &checkpointB.ID
	err = state.WithCheckpointUnitOfWork(ctx, repository, func(ctx context.Context, unit state.CheckpointUnitOfWork) error {
		return unit.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: crossScope})
	})
	if err == nil || !strings.Contains(err.Error(), "compacted-through checkpoint belongs to a different scope") {
		t.Fatalf("cross-scope compaction reference error=%v", err)
	}
}

// TestCheckpointRepositoryRestoresForksThroughPostgresAndBlobs proves the
// durable read path across freshly constructed PostgreSQL and blob-store
// clients. The fixture publishes one root and three immutable children, then
// reconstructs each branch through DurableCheckpointMaterializer. It is a
// bounded repository/blob recovery proof; Temporal crash injection and
// operator backup/restore remain separate integration gates.
func TestCheckpointRepositoryRestoresForksThroughPostgresAndBlobs(t *testing.T) {
	operations, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	tenant := "checkpoint-restore-tenant-" + uuid.NewString()
	project := "checkpoint-restore-project-" + uuid.NewString()
	scopeKey := tenant + "/" + project
	scope, err := operations.Scopes.Ensure(ctx, tenant, project)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	expiresAt := now.Add(time.Hour)
	codec := state.CheckpointBlobCodec{}
	root := t.TempDir()
	payloadStore, err := fileblob.New(fileblob.Options{Root: root, MaxBytes: 1 << 20, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	locators := BlobRepository{Pool: operations.Pool, Namespace: operations.Namespace, Keys: operations.Keys, NewID: UUIDv7}
	writeLocator, err := CheckpointBlobLocatorWriter(locators)
	if err != nil {
		t.Fatal(err)
	}

	putBlobs := func(delta, response []llm.Item, patch state.SettingsPatch) ([3]state.CheckpointBlobReference, error) {
		var references [3]state.CheckpointBlobReference
		encodedDelta, err := codec.EncodeDelta(delta)
		if err != nil {
			return references, err
		}
		encodedResponse, err := codec.EncodeResponse(response)
		if err != nil {
			return references, err
		}
		encodedPatch, err := codec.EncodeSettingsPatch(patch)
		if err != nil {
			return references, err
		}
		for index, data := range [][]byte{encodedDelta, encodedResponse, encodedPatch} {
			ref, putErr := payloadStore.Put(ctx, blob.PutRequest{Tenant: scope.ID.String(), MediaType: "application/json", Data: data, ExpiresAt: expiresAt})
			if putErr != nil {
				return references, putErr
			}
			reference, putLocatorErr := writeLocator(ctx, scope.ID.String(), ref)
			if putLocatorErr != nil {
				return references, putLocatorErr
			}
			references[index] = reference
			blobID, parseErr := uuid.Parse(string(reference.ID))
			if parseErr != nil {
				return references, parseErr
			}
			record, getErr := locators.Get(ctx, scope.ID, blobID)
			if getErr != nil {
				return references, getErr
			}
			opened, openErr := locators.OpenLocator(ctx, scope.ID, "checkpoint", record)
			if openErr != nil {
				return references, openErr
			}
			encoded, marshalErr := json.Marshal(ref)
			if marshalErr != nil {
				return references, marshalErr
			}
			canonical, canonicalErr := llm.CanonicalJSON(encoded)
			if canonicalErr != nil {
				return references, canonicalErr
			}
			if !bytes.Equal(opened, canonical) {
				return references, fmt.Errorf("checkpoint locator is not the canonical full blob reference")
			}
		}
		return references, nil
	}

	beginOperation := func(name string) (state.OperationID, error) {
		id := uuid.NewString()
		result, beginErr := operations.Begin(ctx, admission.BeginRequest{ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: scopeKey,
			RequestDigest: admission.Digest([]byte(name)), ReservationUSD: pricing.MustUSD("0"),
			ExpiresAt: expiresAt, RequestManifest: []byte(`{"model":"fixture"}`)})
		if beginErr != nil {
			return "", beginErr
		}
		if result.Existing {
			return "", fmt.Errorf("restore fixture operation %q unexpectedly replayed", name)
		}
		return state.OperationID(id), nil
	}

	rootOperation, err := beginOperation("root")
	if err != nil {
		t.Fatal(err)
	}
	rootBlobs, err := putBlobs(
		[]llm.Item{checkpointRecoveryMessage(llm.ActorHuman, "root prompt")}, nil,
		state.SettingsPatch{Model: state.SetPatch("gpt-test"), ServiceClass: state.SetPatch(llm.ServiceClassStandard), Portability: state.SetPatch(llm.PortabilityStrict)},
	)
	if err != nil {
		t.Fatal(err)
	}
	checkpointRepository := DurableCheckpointRepository{Pool: operations.Pool, Namespace: operations.Namespace, Now: func() time.Time { return now }}
	rootID := state.CheckpointID(uuid.NewString())
	publishCheckpoint := func(checkpoint state.DurableCheckpoint) error {
		return state.WithCheckpointUnitOfWork(ctx, checkpointRepository, func(ctx context.Context, unit state.CheckpointUnitOfWork) error {
			return unit.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: checkpoint})
		})
	}
	if err := publishCheckpoint(checkpointRecoveryRow(rootID, nil, 0, scope.ID, rootOperation, rootBlobs, now, expiresAt, "root")); err != nil {
		t.Fatalf("publish root checkpoint: %v", err)
	}

	branchIDs := make([]state.CheckpointID, 3)
	for index := range branchIDs {
		branchOperation, beginErr := beginOperation(fmt.Sprintf("branch-%d", index))
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		branchBlobs, putErr := putBlobs(
			[]llm.Item{checkpointRecoveryMessage(llm.ActorHuman, fmt.Sprintf("branch-%d prompt", index))},
			[]llm.Item{checkpointRecoveryMessage(llm.ActorModel, fmt.Sprintf("branch-%d answer", index))}, state.SettingsPatch{},
		)
		if putErr != nil {
			t.Fatal(putErr)
		}
		branchID := state.CheckpointID(uuid.NewString())
		if publishErr := publishCheckpoint(checkpointRecoveryRow(branchID, &rootID, 1, scope.ID, branchOperation, branchBlobs, now, expiresAt, fmt.Sprintf("branch-%d", index))); publishErr != nil {
			t.Fatalf("publish branch %d: %v", index, publishErr)
		}
		branchIDs[index] = branchID
	}

	// A fresh pool and store client model replacement worker processes: no
	// repository or object-store client from publication is reused for replay.
	restoredPool, err := NewPool(ctx, PoolOptions{
		Namespace: operations.Namespace, Addresses: []string{os.Getenv("LLMTW_POSTGRES_ADDR")},
		Username: valueOr("LLMTW_POSTGRES_USER", "llmtw"), Password: valueOr("LLMTW_POSTGRES_PASSWORD", "llmtw"),
		MaxConnections: 4, MinConnections: 1, DialTimeout: 5 * time.Second,
		StatementTimeout: 5 * time.Second, LockTimeout: time.Second, IdleTxTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("open restored PostgreSQL pool: %v", err)
	}
	defer restoredPool.Close()
	restoredStore, err := fileblob.New(fileblob.Options{Root: root, MaxBytes: 1 << 20, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("open restored blob store: %v", err)
	}
	restoredLocators := BlobRepository{Pool: restoredPool, Namespace: operations.Namespace, Keys: operations.Keys, NewID: UUIDv7}
	reader := state.ScopedBlobReader{
		Store: restoredStore, MaxBytes: 1 << 20, Now: func() time.Time { return now },
		Resolve: CheckpointBlobLocator(restoredLocators),
	}
	materializer := &state.DurableCheckpointMaterializer{
		Repository: DurableCheckpointRepository{Pool: restoredPool, Namespace: operations.Namespace, Now: func() time.Time { return now }},
		Blobs:      reader, Codec: codec, Now: func() time.Time { return now },
	}
	for index, branchID := range branchIDs {
		got, materializeErr := materializer.Materialize(ctx, scope.ID.String(), branchID, state.MaterializeLimits{})
		if materializeErr != nil {
			t.Fatalf("materialize restored branch %d: %v", index, materializeErr)
		}
		if got.Handle != state.Handle(branchID) || got.Depth != 1 || len(got.Lineage) != 2 || got.Lineage[0] != state.Handle(rootID) || len(got.Items) != 3 {
			t.Fatalf("restored branch %d state = %#v", index, got)
		}
		if gotText := checkpointRecoveryText(got.Items[1]); gotText != fmt.Sprintf("branch-%d prompt", index) {
			t.Fatalf("restored branch %d prompt = %q", index, gotText)
		}
		if gotText := checkpointRecoveryText(got.Items[2]); gotText != fmt.Sprintf("branch-%d answer", index) {
			t.Fatalf("restored branch %d response = %q", index, gotText)
		}
	}
	if _, err := reader.Read(ctx, uuid.NewString(), rootBlobs[0]); err == nil {
		t.Fatal("checkpoint blob read succeeded under the wrong opaque scope")
	}
}

func checkpointRecoveryRow(id state.CheckpointID, parent *state.CheckpointID, depth int32, scopeID uuid.UUID, operationID state.OperationID, blobs [3]state.CheckpointBlobReference, createdAt, expiresAt time.Time, label string) state.DurableCheckpoint {
	return state.DurableCheckpoint{
		ID: id, ScopeID: scopeID.String(), PublicIDHMAC: sha256.Sum256([]byte(label + "-public")), HandleKeyID: "op-v1",
		ParentID: parent, Kind: state.CheckpointGeneration, Depth: depth, OriginOperationID: operationID,
		DeltaBlob: blobs[0], ResponseBlob: blobs[1], SettingsPatchBlob: blobs[2],
		CanonicalLineageDigest: sha256.Sum256([]byte(label + "-lineage")), MaterializedSettingsDigest: sha256.Sum256([]byte(label + "-settings")), ToolFrontierDigest: sha256.Sum256([]byte(label + "-frontier")),
		SchemaVersion: 1, CompilerEpoch: "checkpoint-restore-v1", CreatedAt: createdAt, ExpiresAt: expiresAt,
	}
}

func checkpointRecoveryMessage(actor llm.Actor, text string) llm.Message {
	return llm.Message{Actor: actor, Content: []llm.Part{llm.TextPart{Text: text}}}
}

func checkpointRecoveryText(item llm.Item) string {
	message, ok := item.(llm.Message)
	if !ok || len(message.Content) != 1 {
		return ""
	}
	text, _ := message.Content[0].(llm.TextPart)
	return text.Text
}
