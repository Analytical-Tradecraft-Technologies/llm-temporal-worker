package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/google/uuid"
)

// publishStoredRoot writes a root checkpoint through the real blob codec and
// cloud checkpoint store without passing request ingress, as a checkpoint
// written by an earlier release would have been.
func publishStoredRoot(t *testing.T, f *boundedCloudFixture, delta []llm.Item) llm.CheckpointHandle {
	t.Helper()
	ctx := context.Background()
	publication, err := f.cap.NewCheckpointPublication(f.options.Keyring, f.options.Limits)
	if err != nil {
		t.Fatal(err)
	}
	identity := CheckpointPublicationIdentity{Scope: "trusted-scope", OperationID: state.OperationID(uuid.NewString()), CheckpointID: state.CheckpointID(uuid.NewString()), CreatedAt: f.now, ExpiresAt: f.now.Add(time.Hour)}
	checkpoint, metadata, err := publication.metadata(ctx, identity, nil, state.MaterializedState{}, state.CheckpointGeneration, nil)
	if err != nil {
		t.Fatalf("stored root metadata: %v", err)
	}
	settings, err := state.ApplySettingsPatchV1(state.RootModelState(""), f.request.SettingsPatch)
	if err != nil {
		t.Fatal(err)
	}
	patch, err := publication.codec.EncodeSettingsPatchV1(f.request.SettingsPatch)
	if err != nil {
		t.Fatal(err)
	}
	output := []llm.Item{llm.ModelText("answer")}
	items := append(append([]llm.Item(nil), delta...), output...)
	checkpoint, err = publication.blobs(ctx, checkpoint, state.MaterializedState{}, settings, delta, output, patch, items, false, nil, nil)
	if err != nil {
		t.Fatalf("stored root blobs: %v", err)
	}
	unit, err := f.repository.Checkpoints().BeginCheckpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := unit.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: checkpoint}); err != nil {
		t.Fatalf("stored root put: %v", err)
	}
	if err := unit.Commit(ctx); err != nil {
		t.Fatalf("stored root commit: %v", err)
	}
	return metadata.Handle
}

func pendingCloudRequests(t *testing.T, f *boundedCloudFixture) int {
	t.Helper()
	pending := 0
	for shard := range cloudstate.PendingShards {
		page, err := f.repository.ListPending(context.Background(), shard, 100, "")
		if err != nil {
			t.Fatal(err)
		}
		pending += len(page.Requests)
	}
	return pending
}

// A checkpoint that holds a media URL accepted when it was written still
// decodes and materializes, so the failure is not an untyped decode error that
// is retried as transient. Continuing from it would replay the URL to a
// provider, so Generate and Compact fail it as non-retryable invalid_argument
// before any operation is recorded or any provider work is done.
func TestCloudExecutionRuntimeFailsStoredBlockedMediaURLAsInvalidArgument(t *testing.T) {
	for _, raw := range []string{"http://localhost:8080/chart.png", "http://10.0.0.7/chart.png", "http://127.1/chart.png"} {
		t.Run(raw, func(t *testing.T) {
			f := boundedCloud(t, false)
			ctx := context.Background()
			stored := llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "describe"}, llm.ImagePart{URL: raw, MediaType: "image/png"}}}
			parent := publishStoredRoot(t, f, []llm.Item{stored})

			materialized, err := f.cap.Checkpoints.Materializer.MaterializeHandle(ctx, "trusted-scope", string(parent), f.options.Limits)
			if err != nil {
				t.Fatalf("stored checkpoint no longer materializes: %v", err)
			}
			if got, _ := json.Marshal(materialized.Items); !strings.Contains(string(got), raw) {
				t.Fatalf("materialized parent lost the stored media URL: %s", got)
			}

			// The continuation adds no media of its own.
			f.continueTurn(parent, "after-policy")
			compact := llm.CompactRequestV1{OperationKey: "compact-after-policy", Context: f.request.Context, Parent: parent, Cache: &llm.CachePolicyV1{}}
			for name, input := range map[string]llm.PrepareExecutionV1{"generate": {Generate: &f.request}, "compact": {Compact: &compact}} {
				_, err := f.runtime.PrepareExecutionV1(ctx, input)
				assertCheckpointReplayError(t, err, provider.CodeInvalidArgument)
				var mapped *provider.Error
				if !errors.As(err, &mapped) || mapped.Retry != provider.RetryNever {
					t.Fatalf("%s: error = %#v, want non-retryable", name, err)
				}
			}
			if _, err := f.runtime.PlanGenerationV1(ctx, f.request); err == nil {
				t.Fatal("generation planning accepted a replayed blocked media URL")
			}
			if pending := pendingCloudRequests(t, f); pending != 0 || f.submits.Load() != 0 {
				t.Fatalf("pending=%d submits=%d after rejected continuations", pending, f.submits.Load())
			}
		})
	}
}

// A stored public media URL keeps replaying, and a blocked URL in new input is
// still rejected at request ingress.
func TestCloudExecutionRuntimeReplaysStoredPublicMediaURL(t *testing.T) {
	f := boundedCloud(t, false)
	ctx := context.Background()
	stored := llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "describe"}, llm.ImagePart{URL: "https://cdn1.example.com/chart.png", MediaType: "image/png"}}}
	parent := publishStoredRoot(t, f, []llm.Item{stored})
	f.continueTurn(parent, "public-replay")
	f.prepareGenerate(t, ctx)

	before := pendingCloudRequests(t, f)
	rejected := f.request
	rejected.OperationKey = "new-private-url"
	rejected.Append = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.ImagePart{URL: "http://127.1/chart.png", MediaType: "image/png"}}}}
	_, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &rejected})
	assertCheckpointReplayError(t, err, provider.CodeInvalidArgument)
	if after := pendingCloudRequests(t, f); after != before {
		t.Fatalf("rejected request changed pending records from %d to %d", before, after)
	}
}

// Each checkpoint fits MaxBytes but the lineage does not. The violation is
// deterministic, so the cloud replay path must fail it as non-retryable
// invalid_argument instead of retryable state_unavailable.
func TestCloudExecutionRuntimeAggregateLineageByteLimitIsNotRetried(t *testing.T) {
	f := boundedCloud(t, false)
	ctx := context.Background()
	delta := []llm.Item{llm.HumanText(strings.Repeat("x", 600))}
	parent := publishStoredRoot(t, f, delta)
	encoded := encodeJSON(t, delta)
	// Larger than the delta or the output alone, smaller than both together.
	f.options.Limits.MaxBytes = int64(len(encoded)) + 16
	f.restart(t)

	_, err := f.cap.Checkpoints.Materializer.MaterializeHandle(ctx, "trusted-scope", string(parent), f.options.Limits)
	if !errors.Is(err, state.ErrLimitExceeded) {
		t.Fatalf("materialize error = %v, want ErrLimitExceeded", err)
	}

	f.continueTurn(parent, "over-lineage-limit")
	_, err = f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	assertCheckpointReplayError(t, err, provider.CodeInvalidArgument)
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Retry != provider.RetryNever {
		t.Fatalf("error = %#v, want non-retryable", err)
	}
}
