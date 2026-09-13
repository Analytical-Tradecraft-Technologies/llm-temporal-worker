package postgres

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	blobstore "github.com/mfow/llm-temporal-worker/golang/storage/blob"
)

func TestAtomicFinalizerRollsBackCheckpointLocatorsAndCompletionTogether(t *testing.T) {
	operations, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()
	now := time.Now().UTC().Truncate(time.Microsecond)
	operationID := "atomic-finalization-" + uuid.NewString()
	started, err := operations.Begin(ctx, admission.BeginRequest{ID: operationID, OperationKey: operationID, Actor: "postgres-test", ScopeKey: "atomic-finalization/project",
		RequestDigest: admission.Digest([]byte(operationID)), ReservationUSD: pricing.MustUSD("0"),
		ExpiresAt: now.Add(time.Hour), RequestManifest: []byte(`{"model":"fixture"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := operations.MarkDispatching(ctx, admission.DispatchRequest{
		OperationID: operationID, DispatchToken: started.Operation.DispatchToken,
		Attempt: admission.AttemptFacts{RouteID: "route", EndpointID: "endpoint", Provider: "fixture", ResolvedModel: "model", AttemptNumber: 1},
	}); err != nil {
		t.Fatal(err)
	}
	scope, err := operations.Scopes.Ensure(ctx, "atomic-finalization", "project")
	if err != nil {
		t.Fatal(err)
	}
	objects := make([]blobstore.Ref, 2)
	for index := range objects {
		payload := []byte(fmt.Sprintf("checkpoint-object-%d", index))
		objects[index] = blobstore.Ref{
			Store: "s3", Locator: fmt.Sprintf("opaque/%d", index), Digest: fmt.Sprintf("%x", sha256.Sum256(payload)),
			ByteLength: int64(len(payload)), MediaType: "application/json", ExpiresAt: now.Add(time.Hour),
		}
	}
	checkpointID := state.CheckpointID(uuid.NewString())
	checkpoint := state.DurableCheckpoint{
		ID: checkpointID, ScopeID: scope.ID.String(), PublicIDHMAC: sha256.Sum256([]byte("atomic-" + string(checkpointID))), HandleKeyID: operations.Keys.Active,
		Kind: state.CheckpointGeneration, OriginOperationID: state.OperationID(operationUUID(operationID).String()),
		CanonicalLineageDigest: sha256.Sum256([]byte("lineage")), MaterializedSettingsDigest: sha256.Sum256([]byte("settings")), ToolFrontierDigest: sha256.Sum256([]byte("frontier")),
		SchemaVersion: 1, CompilerEpoch: "atomic-finalization-test-v1", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	resultDigest := sha256.Sum256([]byte("result"))
	resultRef := state.BlobRef{Digest: resultDigest, Size: 6, Media: "application/json"}
	complete := admission.CompleteRequest{
		OperationID: operationID, DispatchToken: "invalid-token", ActualCostUSD: pricing.MustUSD("0"), ResultRef: &resultRef,
		Attempt:    admission.AttemptFacts{RouteID: "route", EndpointID: "endpoint", Provider: "fixture", ResolvedModel: "model", Dispatch: admission.Accepted, AttemptNumber: 1},
		CostStatus: "exact", CostMethod: "provider_reported",
	}
	finalizer := AtomicFinalizationRepository{
		Blobs:       BlobRepository{Pool: operations.Pool, Namespace: operations.Namespace, Keys: operations.Keys, NewID: UUIDv7},
		Checkpoints: DurableCheckpointRepository{Pool: operations.Pool, Namespace: operations.Namespace, Now: func() time.Time { return now }},
		Operations:  operations, Now: func() time.Time { return now },
	}
	request := admission.AtomicFinalization{ScopeID: scope.ID.String(), Checkpoint: checkpoint, CheckpointObjects: objects, Complete: complete}
	if _, err := finalizer.Finalize(ctx, request); !errors.Is(err, admission.ErrInvalidToken) {
		t.Fatalf("invalid terminal transition error = %v, want ErrInvalidToken", err)
	}
	if _, err := finalizer.Checkpoints.Get(ctx, scope.ID.String(), checkpointID); !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("checkpoint survived rolled-back completion: %v", err)
	}
	operation, err := operations.Get(ctx, operationID)
	if err != nil || operation.State != admission.StateDispatching || operation.ResultRef != nil {
		t.Fatalf("operation crossed rollback boundary: %#v, %v", operation, err)
	}
	request.Complete.DispatchToken = started.Operation.DispatchToken
	published, err := finalizer.Finalize(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if published.DeltaBlob.ID == "" || published.ResponseBlob.ID == "" || published.SettingsPatchBlob.ID == "" {
		t.Fatalf("published checkpoint lost locator references: %#v", published)
	}
	if published.DeltaBlob != published.ResponseBlob || published.DeltaBlob != published.SettingsPatchBlob || published.MaterializedSnapshotBlob == nil {
		t.Fatalf("published checkpoint did not bind one bundle plus periodic snapshot: %#v", published)
	}
	operation, err = operations.Get(ctx, operationID)
	if err != nil || operation.State != admission.StateCompleted || operation.ResultRef == nil || *operation.ResultRef != resultRef {
		t.Fatalf("terminal operation did not commit with checkpoint: %#v, %v", operation, err)
	}
}
