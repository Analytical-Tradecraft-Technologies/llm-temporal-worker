package state

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// snapshotDecodeAllocsPerItem bounds what the production snapshot codec may
// allocate per small transcript item. One envelope canonicalization, typed
// decoding and snapshot validation cost about 192. Re-canonicalizing the
// payload and checking item encoding before snapshot validation cost 311.
const snapshotDecodeAllocsPerItem = 230

// Every Activity step restores the parent transcript through DecodeSnapshot,
// so its per-item cost is paid on each turn for the whole conversation.
func TestCheckpointSnapshotDecodeAllocationsAreBoundedPerItem(t *testing.T) {
	const items = 4000
	transcript := make([]llm.Item, 0, items)
	for index := 0; len(transcript) < items; index++ {
		callID := fmt.Sprintf("call-%d", index)
		transcript = append(transcript,
			llm.HumanText("next"),
			lookupCall(callID, `{"q":"a"}`),
			llm.ToolResult{CallID: callID, Name: "lookup", Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"hits":1}`)}}},
			llm.ModelText("ok"),
		)
	}
	snapshot := CheckpointSnapshot{Items: transcript, Settings: ModelState{Model: "gpt-test", ServiceClass: llm.ServiceClassStandard, Portability: llm.PortabilityStrict}, Depth: 1, Lineage: []Handle{"root", "child"}}
	snapshot.Digest = snapshot.digest()
	codec := CheckpointBlobCodec{}
	encoded, err := codec.EncodeSnapshot(snapshot)
	testassert.NoError(t, err)

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
	testassert.NoError(t, failure)
	t.Logf("snapshot allocations: %.0f total, %.1f per item", allocations, allocations/items)
	if limit := float64(items * snapshotDecodeAllocsPerItem); allocations > limit {
		t.Fatalf("decoding a %d-item snapshot made %.0f allocations (%.0f per item), limit %d per item", items, allocations, allocations/items, snapshotDecodeAllocsPerItem)
	}
}
