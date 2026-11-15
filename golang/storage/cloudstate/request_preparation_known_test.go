package cloudstate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

// TestRequestPreparationKnownParent: loading with a parent the caller already
// validated in this step (#1112) returns exactly what a plain load returns,
// without decoding an identical parent again and, when verified, without
// reading it again. A different parent loads exactly as before.
func TestRequestPreparationKnownParent(t *testing.T) {
	for _, referenced := range []bool{false, true} {
		t.Run(fmt.Sprintf("blob=%t", referenced), func(t *testing.T) {
			r, table, blobs, record, preparation := preparationFixture(t)
			r.parentSnapshotBlob = referenced
			ctx := context.Background()
			mustSavePreparation(t, r, record.Request.Scope, record.Request.ID, preparation)
			reopened := reopen(t, table, blobs)
			want, wantParent, err := reopened.LoadRequestPreparationParent(ctx, record.Request.Scope, record.Request.ID)
			if err != nil {
				t.Fatal(err)
			}
			parentKey := blob.BlobKey(r.newParentSnapshotRef(record.Request.Scope, preparation.ParentSnapshot).Blob)
			reads := 0
			blobs.open = func(key blob.BlobKey) error {
				if key == parentKey {
					reads++
				}
				return nil
			}
			other, err := (state.CheckpointBlobCodec{}).EncodeSnapshot(*state.NewCheckpointSnapshot(state.MaterializedState{
				Items:    []llm.Item{llm.HumanText("another transcript")},
				Settings: state.RootModelState("private-model"), Lineage: []state.Handle{"00000000-0000-4000-8000-000000000002"},
			}))
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name      string
				known     *KnownParent
				reads     int
				reuseKnow bool
			}{
				{"none", nil, 1, false},
				{"same parent", &KnownParent{Snapshot: bytes.Clone(preparation.ParentSnapshot), Decoded: &state.CheckpointSnapshot{}}, 1, true},
				{"same parent verified", &KnownParent{Snapshot: bytes.Clone(preparation.ParentSnapshot), Decoded: &state.CheckpointSnapshot{}, Verified: true}, 0, true},
				{"other parent", &KnownParent{Snapshot: other, Decoded: &state.CheckpointSnapshot{}}, 1, false},
				{"other parent verified", &KnownParent{Snapshot: other, Decoded: &state.CheckpointSnapshot{}, Verified: true}, 1, false},
				{"empty", &KnownParent{Verified: true}, 1, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					reads = 0
					got, parent, err := reopened.LoadRequestPreparationKnown(ctx, record.Request.Scope, record.Request.ID, tc.known)
					if err != nil || !bytes.Equal(got.ParentSnapshot, want.ParentSnapshot) || !equalExecutionJSON(got, want) {
						t.Fatalf("load differs: %v", err)
					}
					if !referenced {
						tc.reads = 0
					}
					if reads != tc.reads {
						t.Fatalf("read the parent %d times, want %d", reads, tc.reads)
					}
					if tc.reuseKnow {
						// The caller's decoded snapshot stands in for decoding.
						if parent != tc.known.Decoded {
							t.Fatal("decoded an identical known parent again")
						}
						if &got.ParentSnapshot[0] == &tc.known.Snapshot[0] {
							t.Fatal("the result shares the known parent's bytes")
						}
					} else if parent == nil || !reflect.DeepEqual(*parent, *wantParent) {
						t.Fatal("parent differs from a plain load")
					}
				})
			}
			blobs.open = nil
		})
	}
}

// TestRequestPreparationKnownParentStillRejectsCorruption: a known parent
// never hides corruption it was not verified against. Unverified, every
// corrupt case fails closed as before; verified, only the parent blob read
// earlier in the step is not read again, and every record check still runs.
func TestRequestPreparationKnownParentStillRejectsCorruption(t *testing.T) {
	cases := map[string]struct {
		blobOnly bool
		corrupt  parentCorruption
	}{
		"missing blob":         {true, deleteParentBlob},
		"tampered blob":        {true, tamperParentBlob},
		"wrong digest":         {false, zeroParentDigest},
		"wrong length":         {false, extendParentLength},
		"other scope":          {false, moveParentToForeignScope},
		"inline and reference": {false, inlineAndReferenceParent},
		"invalid fields": {false, func(t *testing.T, r *Repository, _ *memoryBlobs, record Record, _ RequestPreparation, stored storedRequestPreparation) {
			stored.Version = 2
			rewritePreparation(t, r, record, stored)
		}},
	}
	for name, tc := range cases {
		for _, verified := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/verified=%t", name, verified), func(t *testing.T) {
				r, record, preparation := corruptedPreparation(t, tc.corrupt)
				known := &KnownParent{Snapshot: preparation.ParentSnapshot, Verified: verified}
				_, _, err := r.LoadRequestPreparationKnown(context.Background(), record.Request.Scope, record.Request.ID, known)
				if verified && tc.blobOnly {
					// Verified means this step already read and checked
					// that blob; it is not read again.
					if err != nil {
						t.Fatalf("verified known parent: %v", err)
					}
					return
				}
				if !errors.Is(err, ErrCorrupt) || errors.Is(err, contracts.ErrNotFound) {
					t.Fatalf("accepted a corrupt preparation: %v", err)
				}
			})
		}
	}
}

// TestRequestPreparationKnownParentSave: saving with a known parent stores
// exactly what a plain save stores, and still validates a parent that is not
// the known one.
func TestRequestPreparationKnownParentSave(t *testing.T) {
	for _, referenced := range []bool{false, true} {
		t.Run(fmt.Sprintf("blob=%t", referenced), func(t *testing.T) {
			ctx := context.Background()
			plain, _, _, record, preparation := preparationFixture(t)
			plain.parentSnapshotBlob = referenced
			mustNoError(t, plain.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, preparation))
			want, err := plain.Read(ctx, record.Request.Scope, record.Request.ID)
			if err != nil {
				t.Fatal(err)
			}
			known, _, _, record, preparation := preparationFixture(t)
			known.parentSnapshotBlob = referenced
			// A different, corrupt parent is still decoded and rejected.
			corrupt := preparation
			corrupt.ParentSnapshot = append([]byte(nil), preparation.ParentSnapshot[:len(preparation.ParentSnapshot)/2]...)
			requireErrorIs(t, known.SaveRequestPreparationKnown(ctx, record.Request.Scope, record.Request.ID, corrupt, &KnownParent{Snapshot: preparation.ParentSnapshot}), ErrInvalid, "saved a corrupt parent")
			invalid := preparation
			invalid.Version = 2
			requireErrorIs(t, known.SaveRequestPreparationKnown(ctx, record.Request.Scope, record.Request.ID, invalid, &KnownParent{Snapshot: preparation.ParentSnapshot}), ErrInvalid, "saved invalid fields")
			mustNoError(t, known.SaveRequestPreparationKnown(ctx, record.Request.Scope, record.Request.ID, preparation, &KnownParent{Snapshot: preparation.ParentSnapshot}))
			got, err := known.Read(ctx, record.Request.Scope, record.Request.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(storedPreparationOf(t, got), storedPreparationOf(t, want)) {
				t.Fatal("a known parent changed what was stored")
			}
		})
	}
}
