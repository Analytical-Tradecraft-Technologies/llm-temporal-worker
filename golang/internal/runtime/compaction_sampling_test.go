package runtime

import (
	"context"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// compactCacheDigest prepares the summarizer request for the fixture parent
// after mutate, and returns it with the semantic digest the compaction cache
// key is built from.
func compactCacheDigest(t *testing.T, mutate func(*state.ModelState)) (llm.Request, string) {
	t.Helper()
	_, replay, request := preparationFixture()
	mutate(&replay.State.Settings)
	prepared, err := PrepareCompactInput(context.Background(), request, durable.CompactReplay{State: replay.State})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Request == nil {
		t.Fatal("fixture parent has nothing to compact")
	}
	_, digest, err := cacheSemanticDigest(*prepared.Request, nil, nil, prepared.Policy.Version, prepared.Policy.PromptVersion)
	if err != nil {
		t.Fatal(err)
	}
	return *prepared.Request, hex.EncodeToString(digest[:])
}

// TestCompactionSummarizerDropsStopSequencesAndSeed checks that application
// stop sequences and the seed never reach the summarizer's provider call,
// while temperature and top_p are inherited as before.
func TestCompactionSummarizerDropsStopSequencesAndSeed(t *testing.T) {
	withLeaves, withLeavesDigest := compactCacheDigest(t, func(settings *state.ModelState) {
		settings.TopP = preparationPointer(llm.DecimalV1("0.25"))
		settings.StopSequences = []string{"\n\n", "END"}
		settings.Seed = preparationPointer(int64(7))
		settings.ReasoningMode = llm.ReasoningModeEnabled
		settings.ReasoningTokenBudget = preparationPointer(4096)
	})
	sampling := withLeaves.Sampling
	if sampling == nil || sampling.StopSequences != nil || sampling.Seed != nil {
		t.Fatalf("summarizer sampling = %+v; want no stop sequences or seed", sampling)
	}
	if sampling.Temperature == nil || *sampling.Temperature != 0.2 || sampling.TopP == nil || *sampling.TopP != 0.25 {
		t.Fatalf("summarizer sampling = %+v; want inherited temperature and top_p", sampling)
	}
	if withLeaves.Reasoning != nil {
		t.Fatalf("summarizer reasoning = %+v; want none", withLeaves.Reasoning)
	}

	// Stop sequences and the seed do not participate in the compaction cache
	// key: the summarizer request is the one a parent without them produces.
	withoutStop, withoutStopDigest := compactCacheDigest(t, func(settings *state.ModelState) {
		settings.TopP = preparationPointer(llm.DecimalV1("0.25"))
	})
	if !reflect.DeepEqual(withLeaves, withoutStop) || withLeavesDigest != withoutStopDigest {
		t.Fatal("stop sequences or seed changed the summarizer request or its cache digest")
	}

	// A parent whose only sampling leaves are dropped compiles to no sampling
	// spec at all, exactly like a parent that never set any.
	only, _ := compactCacheDigest(t, func(settings *state.ModelState) {
		settings.Temperature, settings.TemperatureDecimal = nil, nil
		settings.StopSequences = []string{"END"}
		settings.Seed = preparationPointer(int64(0))
	})
	if only.Sampling != nil {
		t.Fatalf("summarizer sampling = %+v; want nil", only.Sampling)
	}
}

// TestCompactionCacheDigestUnchangedWithoutNewLeaves pins the compaction
// cache digest for a parent that sets none of the top_p, stop_sequences,
// seed, reasoning_mode or reasoning_token_budget leaves. The value was
// recorded before the summarizer stopped inheriting stop sequences and the
// seed, so existing compaction cache entries for such parents still hit.
func TestCompactionCacheDigestUnchangedWithoutNewLeaves(t *testing.T) {
	_, digest := compactCacheDigest(t, func(*state.ModelState) {})
	const want = "b10eeae5833601560855f76ee0972c3e96d9970b3494a054391f246dcf7ce01d"
	if digest != want {
		t.Fatalf("compaction cache digest = %s; want %s", digest, want)
	}
}
