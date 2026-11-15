package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func TestCloudResponseCacheCompactionOriginAndReplay(t *testing.T) {
	r, table, blobs, entry, origin := responseCacheFixture(t)
	ctx := context.Background()
	cp := childCheckpoint(origin, "compact-origin")
	cp.Kind = state.CheckpointCompaction
	policy, prompt := "policy-v1", "prompt-v1"
	cp.CompactionPolicyVersion, cp.CompactionPromptVersion = &policy, &prompt
	cp.CompactedThroughID = &origin.ID
	mustPublishCheckpoint(t, r.Checkpoints(), cp)
	parent := llm.CheckpointHandle("ckp_v1.parent")
	cost := "0.25"
	response := llm.CompactResponseV1{OperationKey: "compact-key", OperationID: string(cp.OriginOperationID),
		Checkpoint: llm.CheckpointMetadata{Handle: "ckp_v1.compact", Parent: &parent, Kind: "compaction", Depth: cp.Depth},
		Cache:      llm.CacheDispositionV1{Disposition: "miss_populated"}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: &cost, Method: "provider_reported"}}
	var err error
	entry.ID, entry.Key.Operation = "compact-entry", cache.OperationCompact
	entry.OriginOperationID, entry.OriginCheckpointID = cp.OriginOperationID, cp.ID
	entry.Response, err = json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	mustPublishResponse(t, r, entry)
	r = reopen(t, table, blobs)
	requireResponseHit(t, r, entry, entry.CompletedAt.Add(365*24*time.Hour), "lost compaction artifact")
	key := entry.Key
	key.Fingerprint[0]++ // A source-content/policy change gets a different fingerprint.
	if got := lookupResponse(t, r, key, entry.CompletedAt); got != nil {
		t.Fatal("compaction source/policy collision")
	}
	child := childCheckpoint(cp, "compact-consumer")
	child.OriginCacheEntryID = &entry.ID
	mustPublishCheckpoint(t, r.Checkpoints(), child)
	// The finalization handoff and cache receipt must accept the same compact
	// consumer row; checking those paths separately missed a kind mismatch.
	manifest, _ := json.Marshal(map[string]string{"operation_key": string(child.OriginOperationID)})
	record := mustBeginOperation(t, r, Operation{Scope: Scope{Tenant: "tenant", Project: "project"}, Kind: "compact", Key: string(child.OriginOperationID), Manifest: manifest, Now: child.CreatedAt})
	handoff := FinalizationHandoff{OperationID: child.OriginOperationID, Mode: "cache", CheckpointScope: child.ScopeID, CheckpointID: child.ID, Payload: json.RawMessage(`{"version":1}`)}
	mustSaveHandoff(t, r, record.Request.Scope, record.Request.ID, handoff, child.CreatedAt)
	use := cache.ResponseUse{ScopeID: cp.ScopeID, OperationID: child.OriginOperationID, EntryID: entry.ID, CheckpointID: child.ID, CompletedAt: entry.CompletedAt}
	mustRecordUse(t, r, use)
	if got, err := r.Responses().ReadUse(ctx, use.ScopeID, use.OperationID); err != nil || got != use {
		t.Fatalf("compact receipt: %#v %v", got, err)
	}
}

func TestCloudResponseCacheLargeAndToolResponses(t *testing.T) {
	for _, tools := range []bool{false, true} {
		t.Run(fmt.Sprint(tools), func(t *testing.T) {
			r, table, blobs, entry, parent := responseCacheFixture(t)
			ctx := context.Background()
			var response llm.GenerateResponseV1
			mustNoError(t, json.Unmarshal(entry.Response, &response))
			response.Output = []llm.Item{llm.ModelText(strings.Repeat("large answer ", 30000))}
			if tools {
				response.Status = llm.ResponseStatusToolCalls
				response.Output = []llm.Item{llm.ToolCall{ID: "call-1", Name: "weather", Arguments: json.RawMessage(`{"city":"Sydney"}`)}}
			}
			child := childCheckpoint(parent, "new-result")
			data, err := (state.CheckpointBlobCodec{MaxBytes: maxPayloadBytes}).EncodeResponse(response.Output)
			if err != nil {
				t.Fatal(err)
			}
			child.ResponseBlob, err = r.Checkpoints().Write(ctx, child.ScopeID, data, "application/json")
			if err != nil {
				t.Fatal(err)
			}
			mustPublishCheckpoint(t, r.Checkpoints(), child)
			parentHandle := llm.CheckpointHandle("ckp_v1.parent")
			response.OperationID, response.Checkpoint.Depth, response.Checkpoint.Parent = string(child.OriginOperationID), child.Depth, &parentHandle
			entry.Response, err = json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			entry.OriginOperationID, entry.OriginCheckpointID = child.OriginOperationID, child.ID
			mustPublishResponse(t, r, entry)
			got := lookupResponse(t, reopen(t, table, blobs), entry.Key, entry.CompletedAt)
			if got == nil || len(got.Response) < len(entry.Response)-10 {
				t.Fatal("result truncated")
			}
		})
	}
}

func TestCloudResponseCacheRequiresMatchingCommittedOrigin(t *testing.T) {
	for _, edit := range []func(*cache.ResponseEntry){
		func(e *cache.ResponseEntry) { e.OriginCheckpointID = "missing" },
		func(e *cache.ResponseEntry) { e.Key.ScopeID = "another-scope" },
		func(e *cache.ResponseEntry) { e.CompletedAt = e.CompletedAt.Add(-time.Hour) },
		func(e *cache.ResponseEntry) {
			e.Response = json.RawMessage(strings.Replace(string(e.Response), "private answer", "different answer", 1))
		},
		func(e *cache.ResponseEntry) {
			e.Response = json.RawMessage(strings.Replace(string(e.Response), `"depth":0`, `"depth":10`, 1))
		},
		func(e *cache.ResponseEntry) {
			e.Response = json.RawMessage(strings.Replace(string(e.Response), "miss_populated", "hit", 1))
		},
	} {
		r, table, blobs, entry, _ := responseCacheFixture(t)
		writes := 0
		table.trace = func(string) { writes++ }
		blobs.trace = table.trace
		edit(&entry)
		if err := r.Responses().Publish(context.Background(), entry); err == nil || writes != 0 {
			t.Fatalf("unbound origin accepted: writes=%d err=%v", writes, err)
		}
	}
}

func TestCloudResponseCacheReceiptFailureBoundaries(t *testing.T) {
	for _, phase := range []string{"blob", "row"} {
		for _, committed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", phase, committed), func(t *testing.T) {
				r, table, blobs, entry, origin := responseCacheFixture(t)
				ctx := context.Background()
				child := publishEntryWithReplayChild(t, r, entry, origin)
				mustPublishCheckpoint(t, r.Checkpoints(), child)
				use := replayUse(origin, child, entry, entry.CompletedAt)
				fault := func() (error, error) {
					if committed {
						return nil, contracts.ErrOutcomeUnknown
					}
					return contracts.ErrOutcomeUnknown, nil
				}
				if phase == "blob" {
					blobs.hook = func(blob.BlobKey) (error, error) { return fault() }
				} else {
					table.hook = func(_ string, item kv.KeyValueItem) (error, error) {
						if strings.Contains(item.PartitionKey, "/cache/use/") {
							return fault()
						}
						return nil, nil
					}
				}
				requireErrorIs(t, r.Responses().RecordUse(ctx, use), contracts.ErrOutcomeUnknown, "")
				_, err := r.Responses().ReadUse(ctx, use.ScopeID, use.OperationID)
				if phase == "row" && committed {
					if err != nil {
						t.Fatal(err)
					}
				} else if !errors.Is(err, contracts.ErrNotFound) {
					t.Fatalf("partial receipt: %v", err)
				}
				table.hook, blobs.hook = nil, nil
				mustRecordUse(t, reopen(t, table, blobs), use)
			})
		}
	}
}
