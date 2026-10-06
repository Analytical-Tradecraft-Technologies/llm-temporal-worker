package state

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// TestDurableCheckpointMaterializerReplaysThreeWayForkAfterRestart exercises
// the durable replay boundary rather than the in-memory graph directly. The
// repository and blob reader stand in for rows/blobs that survived a worker
// restart; each replacement materializer must reconstruct one immutable child
// from the same parent without sharing branch state.
func TestDurableCheckpointMaterializerReplaysThreeWayForkAfterRestart(t *testing.T) {
	fixture := newDurableMaterializeFixture(t)
	branches := []struct {
		id   CheckpointID
		text string
	}{
		{id: "fork-a", text: "branch-a"},
		{id: "fork-b", text: "branch-b"},
		{id: "fork-c", text: "branch-c"},
	}

	for _, branch := range branches {
		branch := branch
		delta, err := fixture.codec.EncodeDelta([]llm.Item{llm.Message{
			Actor:   llm.ActorHuman,
			Content: []llm.Part{llm.TextPart{Text: branch.text}},
		}})
		if err != nil {
			t.Fatalf("encode %s delta: %v", branch.id, err)
		}
		response, err := fixture.codec.EncodeResponse([]llm.Item{llm.Message{
			Actor:   llm.ActorModel,
			Content: []llm.Part{llm.TextPart{Text: "answer-" + branch.text}},
		}})
		if err != nil {
			t.Fatalf("encode %s response: %v", branch.id, err)
		}
		patch, err := fixture.codec.EncodeSettingsPatch(SettingsPatch{})
		if err != nil {
			t.Fatalf("encode %s settings patch: %v", branch.id, err)
		}
		refs := durableMaterializePutBlobs(fixture.reader, delta, response, patch)
		parent := fixture.rootID
		row := fixture.repository.rows[fixture.childID]
		row.ID = branch.id
		row.PublicIDHMAC = sha256.Sum256([]byte(string(branch.id) + "-public"))
		row.ParentID = &parent
		row.OriginOperationID = OperationID("operation-" + string(branch.id))
		row.DeltaBlob, row.ResponseBlob, row.SettingsPatchBlob = refs[0], refs[1], refs[2]
		row.CanonicalLineageDigest = sha256.Sum256([]byte(string(branch.id) + "-lineage"))
		row.MaterializedSettingsDigest = sha256.Sum256([]byte(string(branch.id) + "-settings"))
		row.ToolFrontierDigest = sha256.Sum256([]byte(string(branch.id) + "-frontier"))
		fixture.repository.rows[branch.id] = row
	}

	newMaterializer := func() *DurableCheckpointMaterializer {
		return &DurableCheckpointMaterializer{
			Repository: fixture.repository,
			Blobs:      fixture.reader,
			Codec:      fixture.codec,
			Now:        func() time.Time { return fixture.now },
		}
	}

	// Materialize one branch before the simulated restart. A later materializer
	// must not depend on the first instance's in-memory graph.
	first, err := newMaterializer().Materialize(context.Background(), "scope-a", branches[0].id, MaterializeLimits{})
	if err != nil {
		t.Fatalf("materialize first branch: %v", err)
	}
	assertDurableFork(t, first, fixture.rootID, branches[0])

	for _, branch := range branches[1:] {
		got, err := newMaterializer().Materialize(context.Background(), "scope-a", branch.id, MaterializeLimits{})
		if err != nil {
			t.Fatalf("materialize %s after restart: %v", branch.id, err)
		}
		assertDurableFork(t, got, fixture.rootID, branch)
	}
}

func assertDurableFork(t *testing.T, got MaterializedState, rootID CheckpointID, branch struct {
	id   CheckpointID
	text string
}) {
	t.Helper()
	if got.Handle != Handle(branch.id) || got.Depth != 1 || len(got.Items) != 3 {
		t.Fatalf("materialized %s state = %#v", branch.id, got)
	}
	if len(got.Lineage) != 2 || got.Lineage[0] != Handle(rootID) || got.Lineage[1] != Handle(branch.id) {
		t.Fatalf("materialized %s lineage = %#v", branch.id, got.Lineage)
	}
	input := got.Items[1].(llm.Message).Content[0].(llm.TextPart).Text
	output := got.Items[2].(llm.Message).Content[0].(llm.TextPart).Text
	if input != branch.text || output != "answer-"+branch.text {
		t.Fatalf("materialized %s items = %#v", branch.id, got.Items)
	}
}
