package state

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"strings"
	"testing"
	"time"
)

func validDurableCheckpoint() DurableCheckpoint {
	created := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	return DurableCheckpoint{
		ID:                         CheckpointID("checkpoint-1"),
		ScopeID:                    "scope-1",
		PublicIDHMAC:               [32]byte{1},
		HandleKeyID:                "key-1",
		Kind:                       CheckpointGeneration,
		Depth:                      0,
		OriginOperationID:          OperationID("operation-1"),
		DeltaBlob:                  validCheckpointBlob("delta"),
		ResponseBlob:               validCheckpointBlob("response"),
		SettingsPatchBlob:          validCheckpointBlob("settings"),
		CanonicalLineageDigest:     [32]byte{2},
		MaterializedSettingsDigest: [32]byte{3},
		ToolFrontierDigest:         [32]byte{4},
		SchemaVersion:              1,
		CompilerEpoch:              "compiler-1",
		CreatedAt:                  created,
		ExpiresAt:                  created.Add(time.Hour),
	}
}

func validCheckpointBlob(id string) CheckpointBlobReference {
	return CheckpointBlobReference{ID: BlobID(id), Digest: [32]byte{9}, ByteLength: 10, MediaType: "application/json"}
}

func TestDurableCheckpointValidateAndCanonicalDigest(t *testing.T) {
	checkpoint := validDurableCheckpoint()
	testassert.NoError(t, checkpoint.Validate(time.Now()), "Validate() error")
	first, err := checkpoint.CanonicalDigest()
	testassert.NoError(t, err, "CanonicalDigest() error")
	second, err := checkpoint.CanonicalDigest()
	if err != nil || first != second {
		t.Fatalf("CanonicalDigest() is not stable: first=%x second=%x err=%v", first, second, err)
	}
	checkpoint.ResponseBlob.ID = "different-response"
	third, err := checkpoint.CanonicalDigest()
	testassert.NoError(t, err, "changed CanonicalDigest() error")
	if first == third {
		t.Fatal("CanonicalDigest() ignored an immutable blob reference")
	}
}

func TestDurableCheckpointValidationRejectsUnsafeRows(t *testing.T) {
	base := validDurableCheckpoint()
	tests := []struct {
		name string
		edit func(*DurableCheckpoint)
	}{
		{name: "missing scope", edit: func(value *DurableCheckpoint) { value.ScopeID = "" }},
		{name: "invalid kind", edit: func(value *DurableCheckpoint) { value.Kind = "future" }},
		{name: "parent depth mismatch", edit: func(value *DurableCheckpoint) { parent := CheckpointID("parent"); value.ParentID = &parent }},
		{name: "self parent", edit: func(value *DurableCheckpoint) { parent := value.ID; value.ParentID = &parent; value.Depth = 1 }},
		{name: "cache replay missing provenance", edit: func(value *DurableCheckpoint) { value.Kind = CheckpointCacheReplay }},
		{name: "missing digest", edit: func(value *DurableCheckpoint) { value.ToolFrontierDigest = [32]byte{} }},
		{name: "expired before creation", edit: func(value *DurableCheckpoint) { value.ExpiresAt = value.CreatedAt }},
		{name: "negative blob length", edit: func(value *DurableCheckpoint) { value.DeltaBlob.ByteLength = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			test.edit(&value)
			if err := value.Validate(time.Now()); err == nil {
				t.Fatal("Validate() accepted an invalid durable row")
			}
		})
	}
}

func TestDurableCheckpointProviderStateProvenanceIsAdditive(t *testing.T) {
	old := validDurableCheckpoint()
	encoded, err := json.Marshal(old)
	if err != nil || strings.Contains(string(encoded), "ProviderStateProvenance") {
		t.Fatalf("a checkpoint without provenance changed its encoding: %s err=%v", encoded, err)
	}
	var decoded DurableCheckpoint
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.ProviderStateProvenance != nil {
		t.Fatalf("decoded old row = %+v err=%v", decoded.ProviderStateProvenance, err)
	}
	empty := old
	empty.ProviderStateProvenance = []ProviderStateProvenance{}
	oldDigest, _ := old.CanonicalDigest()
	emptyDigest, _ := empty.CanonicalDigest()
	if oldDigest != emptyDigest || oldDigest == ([32]byte{}) {
		t.Fatal("empty provenance changed the canonical digest")
	}
	pinned := old
	pinned.ProviderStateProvenance = []ProviderStateProvenance{{Ordinal: 1, Provider: "openai", EndpointID: "endpoint", EndpointFamily: "openai_responses", ModelLineage: "model"}}
	pinnedDigest, err := pinned.CanonicalDigest()
	if err != nil || pinnedDigest == oldDigest {
		t.Fatalf("provenance is not part of the digest: err=%v", err)
	}
	for _, bad := range [][]ProviderStateProvenance{
		{{Ordinal: -1, Provider: "openai", EndpointID: "endpoint", EndpointFamily: "openai_responses", ModelLineage: "model"}},
		{{Ordinal: 1, Provider: "openai", EndpointFamily: "openai_responses", ModelLineage: "model"}},
		append(append([]ProviderStateProvenance(nil), pinned.ProviderStateProvenance...), pinned.ProviderStateProvenance...),
	} {
		invalid := old
		invalid.ProviderStateProvenance = bad
		if invalid.Validate(invalid.CreatedAt) == nil {
			t.Fatalf("accepted provenance %+v", bad)
		}
	}
}

func TestDurableCheckpointCanonicalDigestNormalizesEmptyChildren(t *testing.T) {
	nilChildren := validDurableCheckpoint()
	emptyChildren := nilChildren
	emptyChildren.ProviderState = []CheckpointProviderState{}
	emptyChildren.Affinities = ProviderCacheAffinitySet{}

	nilDigest, err := nilChildren.CanonicalDigest()
	testassert.NoError(t, err, "nil child digest error")
	emptyDigest, err := emptyChildren.CanonicalDigest()
	testassert.NoError(t, err, "empty child digest error")
	if nilDigest != emptyDigest {
		t.Fatalf("nil and empty child slices produced different digests: nil=%x empty=%x", nilDigest, emptyDigest)
	}
}

type checkpointPortRepository struct{ unit *checkpointPortUnit }

func (repository *checkpointPortRepository) Get(context.Context, string, CheckpointID) (DurableCheckpoint, error) {
	return DurableCheckpoint{}, ErrNotFound
}

func (repository *checkpointPortRepository) BeginCheckpoint(context.Context) (CheckpointUnitOfWork, error) {
	if repository.unit == nil {
		repository.unit = &checkpointPortUnit{}
	}
	return repository.unit, nil
}

type checkpointPortUnit struct {
	putCount      int
	commitCount   int
	rollbackCount int
}

func (unit *checkpointPortUnit) PutCheckpoint(context.Context, CheckpointWrite) error {
	unit.putCount++
	return nil
}

func (unit *checkpointPortUnit) Commit(context.Context) error {
	unit.commitCount++
	return nil
}

func (unit *checkpointPortUnit) Rollback(context.Context) error {
	unit.rollbackCount++
	return nil
}

func TestWithCheckpointUnitOfWorkCommitsAndRollsBack(t *testing.T) {
	repository := &checkpointPortRepository{}
	testassert.NoError(t, WithCheckpointUnitOfWork(context.Background(), repository, func(ctx context.Context, unit CheckpointUnitOfWork) error {
		return unit.PutCheckpoint(ctx, CheckpointWrite{Checkpoint: validDurableCheckpoint()})
	}), "committing unit of work error")
	if repository.unit.putCount != 1 || repository.unit.commitCount != 1 || repository.unit.rollbackCount != 0 {
		t.Fatalf("committing lifecycle = %#v", repository.unit)
	}
	callbackErr := errors.New("write failed")
	testassert.ErrorIs(t, WithCheckpointUnitOfWork(context.Background(), repository, func(context.Context, CheckpointUnitOfWork) error { return callbackErr }), callbackErr, "callback")
	if repository.unit.rollbackCount != 1 {
		t.Fatalf("rollback count = %d, want 1", repository.unit.rollbackCount)
	}
}

func TestWithCheckpointUnitOfWorkRejectsInvalidInputs(t *testing.T) {
	repository := &checkpointPortRepository{}
	for name, callback := range map[string]func(context.Context, CheckpointUnitOfWork) error{
		"nil context":  nil,
		"nil callback": nil,
	} {
		if name == "nil context" {
			if err := WithCheckpointUnitOfWork(nil, repository, func(context.Context, CheckpointUnitOfWork) error { return nil }); err == nil {
				t.Fatal("nil context accepted")
			}
			continue
		}
		if err := WithCheckpointUnitOfWork(context.Background(), repository, callback); err == nil {
			t.Fatal("nil callback accepted")
		}
	}
}
