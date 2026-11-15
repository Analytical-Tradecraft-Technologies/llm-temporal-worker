package cloudstate

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func TestCheckpointFinalizationPlanIsSavedBeforePublication(t *testing.T) {
	r, table, blobs, record, checkpoint, handoff := handoffFixture(t)
	ctx := context.Background()
	plan, err := normalizeCheckpointFinalization(CheckpointFinalization{Checkpoint: checkpoint, Handoff: handoff})
	if err != nil {
		t.Fatal(err)
	}
	mustSaveCheckpointFinalization(t, r, record.Request.Scope, record.Request.ID, plan, checkpoint.CreatedAt)
	requireErrorIs(t, errOf(r.Checkpoints().Get(ctx, checkpoint.ScopeID, checkpoint.ID)), contracts.ErrNotFound, "checkpoint published before resume")
	requireErrorIs(t, errOf(r.LoadFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID)), ErrFinalizationHandoffMissing, "handoff published before checkpoint")
	restarted := reopen(t, table, blobs)
	got, err := restarted.LoadCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || !equalCheckpointFinalization(got, plan) {
		t.Fatal("plan not durable", err)
	}
	mustNoError(t, errOf(restarted.ResumeCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, checkpoint.CreatedAt.Add(time.Hour))))
	cp, err := restarted.Checkpoints().Get(ctx, checkpoint.ScopeID, checkpoint.ID)
	if err != nil {
		t.Fatal(err)
	}
	expected, _ := checkpoint.CanonicalDigest()
	actual, _ := cp.CanonicalDigest()
	if expected != actual || !cp.CreatedAt.Equal(checkpoint.CreatedAt) {
		t.Fatal("checkpoint inputs changed")
	}
	after, err := restarted.Read(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || after.Status != StatusRunning {
		t.Fatal("publication terminalized before settlement", err)
	}
	requireErrorIs(t, errOf(restarted.LoadCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID)), ErrCheckpointFinalizationMissing, "plan duplicated the readable handoff")
	mustSaveCheckpointFinalization(t, restarted, record.Request.Scope, record.Request.ID, plan, checkpoint.CreatedAt.Add(2*time.Hour))
	mustNoError(t, errOf(restarted.ResumeCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, checkpoint.CreatedAt.Add(2*time.Hour))))
	again, _ := restarted.Read(ctx, record.Request.Scope, record.Request.ID)
	if again.Revision != after.Revision {
		t.Fatal("identical completed retry appended writes")
	}
	changed := plan
	changed.Checkpoint.CompilerEpoch = "other-compiler"
	requireErrorIs(t, restarted.SaveCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, changed, checkpoint.CreatedAt), contracts.ErrConflict, "changed committed checkpoint accepted")
}

func TestCheckpointFinalizationRecoveryAtEveryPublicationBoundary(t *testing.T) {
	for _, stage := range []string{"plan-event", "plan-index", "checkpoint-id", "checkpoint-handle", "checkpoint-operation", "handoff-event", "handoff-index"} {
		for _, ack := range []bool{false, true} {
			name := stage + "/before"
			if ack {
				name = stage + "/lost-ack"
			}
			t.Run(name, func(t *testing.T) {
				r, table, blobs, record, checkpoint, handoff := handoffFixture(t)
				ctx := context.Background()
				plan := CheckpointFinalization{Checkpoint: checkpoint, Handoff: handoff}
				if !strings.HasPrefix(stage, "plan-") {
					mustSaveCheckpointFinalization(t, r, record.Request.Scope, record.Request.ID, plan, checkpoint.CreatedAt)
				}
				fired := false
				table.hook = func(action string, item kv.KeyValueItem) (error, error) {
					if fired {
						return nil, nil
					}
					match := false
					switch stage {
					case "plan-event", "handoff-event":
						match = action == "create" && strings.Contains(item.PartitionKey, "/request/")
					case "plan-index", "handoff-index":
						match = action == "replace" && strings.Contains(item.PartitionKey, "/pending/")
					default:
						match = action == "create" && strings.Contains(item.PartitionKey, "/checkpoint/"+strings.TrimPrefix(stage, "checkpoint-")+"/")
					}
					if match {
						fired = true
						if ack {
							return nil, contracts.ErrOutcomeUnknown
						}
						return contracts.ErrUnavailable, nil
					}
					return nil, nil
				}
				var err error
				if strings.HasPrefix(stage, "plan-") {
					err = r.SaveCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, plan, checkpoint.CreatedAt)
				} else {
					_, err = r.ResumeCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, checkpoint.CreatedAt)
				}
				if err == nil || !fired {
					t.Fatal("fault not exercised", err)
				}
				table.hook = nil
				restarted := reopen(t, table, blobs)
				if err := restarted.SaveCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, plan, checkpoint.CreatedAt); err != nil {
					t.Fatal("save retry", err)
				}
				got, err := restarted.ResumeCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, checkpoint.CreatedAt)
				normalized, _ := normalizeHandoff(handoff)
				if err != nil || !equalHandoff(got, normalized) {
					t.Fatal("resume retry", err)
				}
				mustNoError(t, errOf(restarted.Checkpoints().Get(ctx, checkpoint.ScopeID, checkpoint.ID)))
				current, _ := restarted.Read(ctx, record.Request.Scope, record.Request.ID)
				if current.Status != StatusRunning {
					t.Fatal("paid settlement prematurely completed")
				}
			})
		}
	}
}

func TestCheckpointFinalizationImmutableConcurrentPlans(t *testing.T) {
	r, table, blobs, record, checkpoint, handoff := handoffFixture(t)
	ctx := context.Background()
	plan := CheckpointFinalization{Checkpoint: checkpoint, Handoff: handoff}
	for _, resume := range []bool{false, true} {
		var wg sync.WaitGroup
		out := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				other := reopen(t, table, blobs)
				if resume {
					_, err := other.ResumeCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, checkpoint.CreatedAt)
					out <- err
				} else {
					out <- other.SaveCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, plan, checkpoint.CreatedAt)
				}
			}()
		}
		wg.Wait()
		close(out)
		for err := range out {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	changed := handoff
	changed.Payload = json.RawMessage(`{"version":1,"different":true}`)
	requireErrorIs(t, r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, changed, checkpoint.CreatedAt), contracts.ErrConflict, "changed handoff accepted")
}

func TestCheckpointFinalizationMissingReferencesRemainRecoverable(t *testing.T) {
	r, _, blobs, record, checkpoint, handoff := handoffFixture(t)
	plan := CheckpointFinalization{Checkpoint: checkpoint, Handoff: handoff}
	ctx := context.Background()
	mustSaveCheckpointFinalization(t, r, record.Request.Scope, record.Request.ID, plan, checkpoint.CreatedAt)
	// Content blobs are prerequisites, not permission to fall back to provider work.
	tag, _, _ := strings.Cut(strings.TrimPrefix(string(checkpoint.ResponseBlob.ID), checkpointBlobPrefix), ".")
	delete(blobs.values, blob.BlobKey(r.namespace+"/payload/"+tag))
	if _, err := r.ResumeCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, checkpoint.CreatedAt); err == nil {
		t.Fatal("missing blobs committed")
	}
	if _, err := r.LoadCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID); err != nil {
		t.Fatal("lost recovery plan", err)
	}
	requireErrorIs(t, errOf(r.LoadFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID)), ErrFinalizationHandoffMissing, "premature handoff")
}

func TestCheckpointFinalizationRejectsChangedPlanBeforePublication(t *testing.T) {
	r, _, _, record, checkpoint, handoff := handoffFixture(t)
	ctx := context.Background()
	plan := CheckpointFinalization{Checkpoint: checkpoint, Handoff: handoff}
	mustSaveCheckpointFinalization(t, r, record.Request.Scope, record.Request.ID, plan, checkpoint.CreatedAt)
	changed := plan
	changed.Checkpoint.CompilerEpoch = "new-version"
	requireErrorIs(t, r.SaveCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, changed, checkpoint.CreatedAt), contracts.ErrConflict, "changed plan accepted")
	handoff.Payload = json.RawMessage(`{"version":1,"response":"different"}`)
	mustPublishCheckpoint(t, r.Checkpoints(), checkpoint)
	requireErrorIs(t, r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt), contracts.ErrConflict, "saved plan bypassed")
	wrongScope := record.Request.Scope
	wrongScope.Tenant = "other-tenant"
	requireErrorIs(t, errOf(r.LoadCheckpointFinalization(ctx, wrongScope, record.Request.ID)), contracts.ErrNotFound, "cross-tenant plan disclosed")
	wrong := plan
	wrong.Handoff.CheckpointID = state.CheckpointID("other-checkpoint")
	requireErrorIs(t, r.SaveCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, wrong, checkpoint.CreatedAt), ErrInvalid, "cross-checkpoint plan accepted")
}

func TestCheckpointFinalizationDoesNotDuplicateLargeResponse(t *testing.T) {
	r, _, _, record, checkpoint, handoff := handoffFixture(t)
	handoff.Payload, _ = json.Marshal(map[string]any{"version": 1, "response": strings.Repeat("x", maxPayloadBytes/2+1024)})
	plan := CheckpointFinalization{Checkpoint: checkpoint, Handoff: handoff}
	ctx := context.Background()
	mustSaveCheckpointFinalization(t, r, record.Request.Scope, record.Request.ID, plan, checkpoint.CreatedAt)
	if _, err := r.ResumeCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, checkpoint.CreatedAt); err != nil {
		t.Fatal("large payload duplicated during promotion", err)
	}
}

func TestCheckpointFinalizationHandoffCannotPromoteDifferentCheckpoint(t *testing.T) {
	r, _, _, record, checkpoint, handoff := handoffFixture(t)
	ctx := context.Background()
	plan := CheckpointFinalization{Checkpoint: checkpoint, Handoff: handoff}
	mustSaveCheckpointFinalization(t, r, record.Request.Scope, record.Request.ID, plan, checkpoint.CreatedAt)
	changed := checkpoint
	changed.CompilerEpoch = "competing-compiler"
	mustPublishCheckpoint(t, r.Checkpoints(), changed)
	requireErrorIs(t, r.SaveFinalizationHandoff(ctx, record.Request.Scope, record.Request.ID, handoff, checkpoint.CreatedAt), contracts.ErrConflict, "different committed checkpoint promoted")
	requireErrorIs(t, errOf(r.ResumeCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID, checkpoint.CreatedAt)), contracts.ErrConflict, "competing checkpoint overwritten")
	if _, err := r.LoadCheckpointFinalization(ctx, record.Request.Scope, record.Request.ID); err != nil {
		t.Fatal("pending plan lost", err)
	}
}
