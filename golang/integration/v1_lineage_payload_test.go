package integration_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// TestV1SyntheticLineageKeepsActivityPayloadBounded exercises the part of the
// Task 21 lineage contract that is deterministic and safe to run in every CI
// job. A checkpoint handle is the only ancestor reference on the wire: the
// 10,000-turn lineage, periodic compactions, and three-way forks must not make
// a Generate or Compact Activity payload grow with transcript length.
//
// Durable checkpoint persistence, provider dispatch recovery, and backup/
// restore are intentionally covered by the separately guarded integration
// suites; this test proves the payload-size invariant without requiring those
// services or credentials.
func TestV1SyntheticLineageKeepsActivityPayloadBounded(t *testing.T) {
	const (
		turns            = 10_000
		compactionEvery  = 257
		forkEvery        = 509
		maxPayloadBytes  = 16 << 10
		maxAllowedGrowth = 512
	)
	limits := activity.PayloadLimits{MaxInlineBytes: maxPayloadBytes}
	context := llm.RequestContext{Tenant: "tenant-lineage", Project: "project-lineage", Actor: "test"}
	var parent *llm.CheckpointHandle
	maximum := 0
	var previousPayload []byte

	for turn := 1; turn <= turns; turn++ {
		previous := parent
		request := llm.GenerateRequestV1{
			OperationKey: fmt.Sprintf("lineage-%05d", turn),
			Context:      context,
			Parent:       parent,
			Append:       []llm.Item{lineageMessage(fmt.Sprintf("turn-%05d", turn))},
		}
		payload, err := activity.MarshalGenerateV1(request, limits)
		if err != nil {
			t.Fatalf("turn %d generate payload: %v", turn, err)
		}
		if strings.Contains(string(payload), "turn-00001") && turn > 1 {
			t.Fatalf("turn %d payload unexpectedly contains an ancestor transcript item", turn)
		}
		if growth := len(payload) - len(previousPayload); growth > maxAllowedGrowth {
			t.Fatalf("turn %d payload grew by %d bytes, want <= %d", turn, growth, maxAllowedGrowth)
		}
		previousPayload = payload
		if len(payload) > maximum {
			maximum = len(payload)
		}
		checkpoint := llm.CheckpointHandle(fmt.Sprintf("checkpoint-%05d", turn))
		parent = &checkpoint
		actualCost := "0"
		responsePayload, err := activity.MarshalGenerateResponseV1(llm.GenerateResponseV1{
			OperationKey: fmt.Sprintf("lineage-%05d", turn),
			OperationID:  fmt.Sprintf("operation-%05d", turn),
			Status:       llm.ResponseStatusCompleted,
			Output:       []llm.Item{lineageMessage(fmt.Sprintf("turn-%05d", turn))},
			Checkpoint:   llm.CheckpointMetadata{Handle: checkpoint, Parent: previous, Kind: "generation", Depth: int32(turn)},
			Cache:        llm.CacheDispositionV1{Disposition: "disabled", Variant: 0},
			Cost:         llm.CostV1{Status: "exact", ActualCostUSD: &actualCost, Method: "provider_reported"},
		}, limits)
		if err != nil {
			t.Fatalf("turn %d generate response payload: %v", turn, err)
		}
		if len(responsePayload) > maxPayloadBytes || (strings.Contains(string(responsePayload), "turn-00001") && turn > 1) {
			t.Fatalf("turn %d response payload is unbounded or contains transcript: %d bytes", turn, len(responsePayload))
		}
		if len(responsePayload) > maximum {
			maximum = len(responsePayload)
		}

		// A compaction still carries only the current checkpoint and bounded
		// policy metadata. Its resulting checkpoint becomes the next parent.
		if turn%compactionEvery == 0 {
			compactParent := parent
			compact := llm.CompactRequestV1{
				OperationKey: fmt.Sprintf("compact-%05d", turn),
				Context:      context,
				Parent:       *parent,
				Policy:       []byte(`{"target_tokens":256,"summary_style":"balanced"}`),
			}
			compactPayload, err := activity.MarshalCompactV1(compact, limits)
			if err != nil {
				t.Fatalf("turn %d compact payload: %v", turn, err)
			}
			if len(compactPayload) > maximum {
				maximum = len(compactPayload)
			}
			if len(compactPayload) > maxPayloadBytes || strings.Contains(string(compactPayload), "turn-00001") {
				t.Fatalf("turn %d compact payload is unbounded or contains transcript: %d bytes", turn, len(compactPayload))
			}
			compactCheckpoint := llm.CheckpointHandle(fmt.Sprintf("checkpoint-compact-%05d", turn))
			parent = &compactCheckpoint
			compactActualCost := "0"
			compactResponsePayload, err := activity.MarshalCompactResponseV1(llm.CompactResponseV1{
				OperationKey: fmt.Sprintf("compact-%05d", turn),
				OperationID:  fmt.Sprintf("compact-operation-%05d", turn),
				Checkpoint:   llm.CheckpointMetadata{Handle: compactCheckpoint, Parent: compactParent, Kind: "compaction", Depth: int32(turn) + 1},
				Cache:        llm.CacheDispositionV1{Disposition: "disabled", Variant: 0},
				Cost:         llm.CostV1{Status: "exact", ActualCostUSD: &compactActualCost, Method: "provider_reported"},
			}, limits)
			if err != nil {
				t.Fatalf("turn %d compact response payload: %v", turn, err)
			}
			if len(compactResponsePayload) > maxPayloadBytes || strings.Contains(string(compactResponsePayload), "turn-00001") {
				t.Fatalf("turn %d compact response is unbounded or contains transcript: %d bytes", turn, len(compactResponsePayload))
			}
			if len(compactResponsePayload) > maximum {
				maximum = len(compactResponsePayload)
			}
		}

		// Forks share one immutable parent. Encode all three siblings and then
		// continue from the first child, proving that no hidden mutable head is
		// needed to keep sibling payloads bounded.
		if turn%forkEvery == 0 {
			forkParent := *parent
			for branch := 0; branch < 3; branch++ {
				forkRequest := llm.GenerateRequestV1{
					OperationKey: fmt.Sprintf("fork-%05d-%d", turn, branch),
					Context:      context,
					Parent:       &forkParent,
					Append:       []llm.Item{lineageMessage(fmt.Sprintf("fork-%05d-%d", turn, branch))},
				}
				forkPayload, err := activity.MarshalGenerateV1(forkRequest, limits)
				if err != nil {
					t.Fatalf("turn %d branch %d payload: %v", turn, branch, err)
				}
				if len(forkPayload) > maxPayloadBytes || strings.Contains(string(forkPayload), "turn-00001") {
					t.Fatalf("turn %d branch %d payload is unbounded or contains transcript: %d bytes", turn, branch, len(forkPayload))
				}
				if len(forkPayload) > maximum {
					maximum = len(forkPayload)
				}
				if branch == 0 {
					forkCheckpoint := llm.CheckpointHandle(fmt.Sprintf("checkpoint-fork-%05d-0", turn))
					parent = &forkCheckpoint
				}
			}
		}
	}

	if maximum >= maxPayloadBytes {
		t.Fatalf("maximum lineage payload = %d, want comfortably below %d", maximum, maxPayloadBytes)
	}
}

func lineageMessage(text string) llm.Item {
	return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
}
