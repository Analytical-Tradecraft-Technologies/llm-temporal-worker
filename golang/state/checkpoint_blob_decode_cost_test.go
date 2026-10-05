package state

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// snapshotDecodeAllocsPerItem bounds what the production snapshot codec may
// allocate per small transcript item. About 310 are the envelope's
// canonical-form check, one duplicate-key scan and typed decode of the items,
// and the encoding check of each decoded item. Rescanning every nested value
// of every item brought the same snapshot to about 900.
const snapshotDecodeAllocsPerItem = 400

// Every Activity step restores the parent transcript through DecodeSnapshot,
// so its per-item cost is paid on each turn for the whole conversation.
func TestCheckpointSnapshotDecodeAllocationsAreBoundedPerItem(t *testing.T) {
	const items = 4000
	transcript := make([]llm.Item, 0, items)
	for index := 0; len(transcript) < items; index++ {
		callID := fmt.Sprintf("call-%d", index)
		transcript = append(transcript,
			llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "next"}}},
			llm.ToolCall{ID: callID, Name: "lookup", Arguments: json.RawMessage(`{"q":"a"}`)},
			llm.ToolResult{CallID: callID, Name: "lookup", Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"hits":1}`)}}},
			llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "ok"}}},
		)
	}
	snapshot := CheckpointSnapshot{Items: transcript, Settings: ModelState{Model: "gpt-test", ServiceClass: llm.ServiceClassStandard, Portability: llm.PortabilityStrict}, Depth: 1, Lineage: []Handle{"root", "child"}}
	snapshot.Digest = snapshot.digest()
	codec := CheckpointBlobCodec{}
	encoded, err := codec.EncodeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}

	var failure error
	allocations := testing.AllocsPerRun(2, func() {
		decoded, err := codec.DecodeSnapshot(encoded)
		if err == nil && len(decoded.Items) != items {
			err = fmt.Errorf("decoded %d items", len(decoded.Items))
		}
		if err != nil {
			failure = err
		}
	})
	if failure != nil {
		t.Fatal(failure)
	}
	if limit := float64(items * snapshotDecodeAllocsPerItem); allocations > limit {
		t.Fatalf("decoding a %d-item snapshot made %.0f allocations (%.0f per item), limit %d per item", items, allocations, allocations/items, snapshotDecodeAllocsPerItem)
	}
}
