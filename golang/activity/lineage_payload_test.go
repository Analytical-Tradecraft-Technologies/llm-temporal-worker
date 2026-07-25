package activity

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// opaqueLineageHandle deliberately hashes the synthetic ancestor transcript.
// The Activity boundary carries only this fixed-size checkpoint handle; it
// must never carry the transcript that was materialized behind it.
func opaqueLineageHandle(turns int) llm.CheckpointHandle {
	digest := sha256.Sum256([]byte(strings.Repeat("ancestor-turn\n", turns)))
	return llm.CheckpointHandle("ckp_" + hex.EncodeToString(digest[:]))
}

func TestV1PayloadSizeDoesNotFollowAncestorLineage(t *testing.T) {
	limits := PayloadLimits{MaxInlineBytes: 1 << 20}
	turns := []int{1, 100, 10000}
	var generateRequestSize, compactRequestSize, generateResponseSize, compactResponseSize int

	for _, count := range turns {
		handle := opaqueLineageHandle(count)
		parent := handle
		generateRequest := llm.GenerateRequestV1{
			APIVersion:   llm.APIVersion,
			OperationKey: "lineage-generate",
			Context:      llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "workflow"},
			Parent:       &parent,
			Append:       []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "one delta"}}}},
		}
		compactRequest := llm.CompactRequestV1{
			APIVersion:   llm.CompactAPIVersion,
			OperationKey: "lineage-compact",
			Context:      generateRequest.Context,
			Parent:       handle,
			Policy:       []byte(`{"target_tokens":128}`),
		}
		generateResponse := llm.GenerateResponseV1{
			APIVersion: llm.APIVersion, OperationKey: "lineage-generate", OperationID: "operation-generate",
			Status:     llm.ResponseStatusCompleted,
			Output:     []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "one response"}}}},
			Checkpoint: llm.CheckpointMetadata{Handle: handle, Kind: "generation", Depth: int32(count)},
			Cache:      llm.CacheDispositionV1{Disposition: "disabled"},
			Cost:       llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "provider_reported"},
		}
		compactParent := handle
		compactResponse := llm.CompactResponseV1{
			APIVersion: llm.CompactAPIVersion, OperationKey: "lineage-compact", OperationID: "operation-compact",
			Checkpoint: llm.CheckpointMetadata{Handle: handle, Parent: &compactParent, Kind: "compaction", Depth: int32(count)},
			Cache:      llm.CacheDispositionV1{Disposition: "disabled"},
			Cost:       llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "provider_reported"},
		}

		generateRequestWire, err := MarshalGenerateV1(generateRequest, limits)
		if err != nil {
			t.Fatalf("%d-turn Generate request: %v", count, err)
		}
		compactRequestWire, err := MarshalCompactV1(compactRequest, limits)
		if err != nil {
			t.Fatalf("%d-turn Compact request: %v", count, err)
		}
		generateResponseWire, err := MarshalGenerateResponseV1(generateResponse, limits)
		if err != nil {
			t.Fatalf("%d-turn Generate response: %v", count, err)
		}
		compactResponseWire, err := MarshalCompactResponseV1(compactResponse, limits)
		if err != nil {
			t.Fatalf("%d-turn Compact response: %v", count, err)
		}
		if count == turns[0] {
			generateRequestSize, compactRequestSize = len(generateRequestWire), len(compactRequestWire)
			generateResponseSize, compactResponseSize = len(generateResponseWire), len(compactResponseWire)
			continue
		}
		if got := len(generateRequestWire); got != generateRequestSize {
			t.Fatalf("%d-turn Generate request is %d bytes, want fixed %d-byte boundary", count, got, generateRequestSize)
		}
		if got := len(compactRequestWire); got != compactRequestSize {
			t.Fatalf("%d-turn Compact request is %d bytes, want fixed %d-byte boundary", count, got, compactRequestSize)
		}
		wantResponseSize := generateResponseSize + len(strconv.Itoa(count)) - len(strconv.Itoa(turns[0]))
		if got := len(generateResponseWire); got != wantResponseSize {
			t.Fatalf("%d-turn Generate response is %d bytes, want %d bytes (opaque handle plus serialized depth only)", count, got, wantResponseSize)
		}
		wantCompactResponseSize := compactResponseSize + len(strconv.Itoa(count)) - len(strconv.Itoa(turns[0]))
		if got := len(compactResponseWire); got != wantCompactResponseSize {
			t.Fatalf("%d-turn Compact response is %d bytes, want %d bytes (opaque handle plus serialized depth only)", count, got, wantCompactResponseSize)
		}
	}

	if generateRequestSize == 0 || compactRequestSize == 0 || generateResponseSize == 0 || compactResponseSize == 0 {
		t.Fatal("lineage payload test did not produce all four payloads")
	}
}
