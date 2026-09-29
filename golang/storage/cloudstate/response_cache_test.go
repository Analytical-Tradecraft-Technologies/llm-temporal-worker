package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func responseCacheFixture(t *testing.T) (*Repository, *memoryTable, *memoryBlobs, cache.ResponseEntry, state.DurableCheckpoint) {
	t.Helper()
	r, table, blobs, cp := checkpointFixture(t)
	if err := publishCheckpoint(context.Background(), r.Checkpoints(), cp); err != nil {
		t.Fatal(err)
	}
	cost := "0.12"
	value := llm.GenerateResponseV1{OperationKey: "private-key", OperationID: string(cp.OriginOperationID), Status: llm.ResponseStatusCompleted,
		Output:     []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "private answer"}}}},
		Checkpoint: llm.CheckpointMetadata{Handle: "ckp_v1.origin", Kind: "generation"},
		Cache:      llm.CacheDispositionV1{Disposition: "miss_populated"}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: &cost, Method: "provider_reported"}}
	response, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	entry := cache.ResponseEntry{ID: "entry-original", Key: cache.ResponseKey{ScopeID: cp.ScopeID, Operation: cache.OperationGenerate,
		Route: cache.RouteIdentity{Provider: "private-provider", Endpoint: "private-endpoint", Account: "private-account", Region: "private-region", Model: "private-model", Revision: "revision-1", Compiler: "compiler-1"}, Fingerprint: cache.Fingerprint{1}},
		OriginOperationID: cp.OriginOperationID, OriginCheckpointID: cp.ID, CompletedAt: cp.CreatedAt.Add(time.Minute), Response: response}
	return r, table, blobs, entry, cp
}

func lookupResponse(t *testing.T, r *Repository, key cache.ResponseKey, now time.Time) *cache.ResponseEntry {
	t.Helper()
	entry, err := r.Responses().Lookup(context.Background(), cache.ResponseLookup{Key: key, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestCloudResponseCacheFreshnessIsolationAndRestart(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	ctx := context.Background()
	if got := lookupResponse(t, r, entry.Key, entry.CompletedAt); got != nil {
		t.Fatal("initial hit")
	}
	if err := r.Responses().Publish(ctx, entry); err != nil {
		t.Fatal(err)
	}
	r = reopen(t, table, blobs)
	if got := lookupResponse(t, r, entry.Key, entry.CompletedAt.Add(10*365*24*time.Hour)); got == nil || got.ID != entry.ID {
		t.Fatal("omitted age did not preserve old success")
	}
	age := time.Hour
	for _, test := range []struct {
		now time.Time
		hit bool
	}{
		{entry.CompletedAt.Add(-time.Nanosecond), false}, {entry.CompletedAt, true},
		{entry.CompletedAt.Add(age), true}, {entry.CompletedAt.Add(age + time.Nanosecond), false},
	} {
		got, err := r.Responses().Lookup(ctx, cache.ResponseLookup{Key: entry.Key, Now: test.now, MaxAge: &age})
		if err != nil || (got != nil) != test.hit {
			t.Fatalf("freshness %v: %v %v", test.now, got, err)
		}
	}
	for _, edit := range []func(*cache.ResponseKey){
		func(k *cache.ResponseKey) { k.ScopeID = "other-scope" }, func(k *cache.ResponseKey) { k.Operation = cache.OperationCompact },
		func(k *cache.ResponseKey) { k.RequestIndex++ }, func(k *cache.ResponseKey) { k.Fingerprint[1]++ },
		func(k *cache.ResponseKey) { k.Route.Provider += "-other" }, func(k *cache.ResponseKey) { k.Route.Endpoint += "-other" },
		func(k *cache.ResponseKey) { k.Route.Account += "-other" }, func(k *cache.ResponseKey) { k.Route.Region += "-other" },
		func(k *cache.ResponseKey) { k.Route.Model += "-other" }, func(k *cache.ResponseKey) { k.Route.Revision += "-other" },
		func(k *cache.ResponseKey) { k.Route.Compiler += "-other" },
	} {
		key := entry.Key
		edit(&key)
		if got := lookupResponse(t, r, key, entry.CompletedAt); got != nil {
			t.Fatal("cross-key cache hit")
		}
	}
	// Returned JSON is caller-owned; mutation cannot alter a later lookup.
	got := lookupResponse(t, r, entry.Key, entry.CompletedAt)
	got.Response[0] = '!'
	if got := lookupResponse(t, r, entry.Key, entry.CompletedAt); got.Response[0] != '{' {
		t.Fatal("shared response bytes")
	}
	for _, row := range table.rows {
		data, _ := row.Item.Fields.MarshalBinary()
		for _, secret := range []string{"private", string(entry.OriginCheckpointID), string(entry.ID)} {
			if bytes.Contains(data, []byte(secret)) || strings.Contains(row.Item.PartitionKey, secret) {
				t.Fatal("plaintext cache metadata")
			}
		}
	}
	for _, data := range blobs.values {
		if bytes.Contains(data, []byte("private answer")) {
			t.Fatal("plaintext blob")
		}
	}
}

func TestCloudResponseCacheConcurrentPublicationAndMonotonicHead(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	ctx := context.Background()
	// One hundred independent adapter instances reconcile the same success.
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for range 100 {
		store := reopen(t, table, blobs).Responses()
		wg.Go(func() { errs <- store.Publish(ctx, entry) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(table.rows) != 5 {
		t.Fatalf("duplicate entries: %d rows", len(table.rows))
	}
	// Independent successful samples race to advance the same key. An older
	// completion arriving last (including an old retry) must never win.
	errs = make(chan error, 30)
	for i := range 30 {
		candidate := entry
		candidate.ID = state.CacheEntryID(fmt.Sprintf("entry-%02d", i))
		candidate.CompletedAt = entry.CompletedAt.Add(time.Duration(i+1) * time.Second)
		wg.Go(func() { errs <- r.Responses().Publish(ctx, candidate) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Responses().Publish(ctx, entry); err != nil {
		t.Fatal(err)
	}
	if got := lookupResponse(t, r, entry.Key, entry.CompletedAt.Add(time.Hour)); got.ID != "entry-29" {
		t.Fatalf("head regressed: %s", got.ID)
	}
	writes := 0
	table.trace = func(string) { writes++ }
	blobs.trace = table.trace
	if err := r.Responses().Publish(ctx, entry); err != nil || writes != 0 {
		t.Fatalf("retry wrote: %d %v", writes, err)
	}
	conflict := entry
	conflict.CompletedAt = conflict.CompletedAt.Add(time.Second)
	if err := r.Responses().Publish(ctx, conflict); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("ID reused: %v", err)
	}
}

func TestCloudResponseCacheIncompleteResultsPreserveSuccess(t *testing.T) {
	r, _, _, entry, _ := responseCacheFixture(t)
	ctx := context.Background()
	if err := r.Responses().Publish(ctx, entry); err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"length", "refused", "content_filtered", "failed", "pending"} {
		candidate := entry
		candidate.ID = state.CacheEntryID(status)
		candidate.CompletedAt = candidate.CompletedAt.Add(time.Hour)
		candidate.Response = bytes.Replace(entry.Response, []byte(`"status":"completed"`), []byte(`"status":"`+status+`"`), 1)
		if err := r.Responses().Publish(ctx, candidate); !errors.Is(err, ErrInvalid) {
			t.Fatalf("cached %s: %v", status, err)
		}
		if got := lookupResponse(t, r, entry.Key, candidate.CompletedAt); got.ID != entry.ID {
			t.Fatal("failure invalidated success")
		}
	}
	tools := entry
	tools.ID = "tool-calls"
	tools.CompletedAt = tools.CompletedAt.Add(time.Second)
	tools.Response = bytes.Replace(entry.Response, []byte(`"status":"completed"`), []byte(`"status":"tool_calls"`), 1)
	if err := r.Responses().Publish(ctx, tools); err != nil {
		t.Fatalf("tool-call success rejected: %v", err)
	}
}

func TestCloudResponseCachePublicationUnknownOutcomes(t *testing.T) {
	for _, phase := range []string{"blob", "entry", "success", "replace"} {
		for _, committed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/committed=%t", phase, committed), func(t *testing.T) {
				r, table, blobs, entry, _ := responseCacheFixture(t)
				ctx := context.Background()
				var previous *cache.ResponseEntry
				if phase == "replace" {
					if err := r.Responses().Publish(ctx, entry); err != nil {
						t.Fatal(err)
					}
					copy := entry
					previous = &copy
					entry.ID = "newer"
					entry.CompletedAt = entry.CompletedAt.Add(time.Second)
				}
				fault := func() (error, error) {
					if committed {
						return nil, contracts.ErrOutcomeUnknown
					}
					return contracts.ErrOutcomeUnknown, nil
				}
				if phase == "blob" {
					blobs.hook = func(blob.BlobKey) (error, error) { return fault() }
				} else {
					table.hook = func(op string, item kv.KeyValueItem) (error, error) {
						if strings.Contains(item.PartitionKey, "/cache/"+phase+"/") || (phase == "replace" && op == "replace") {
							return fault()
						}
						return nil, nil
					}
				}
				if err := r.Responses().Publish(ctx, entry); !errors.Is(err, contracts.ErrOutcomeUnknown) {
					t.Fatalf("uncertain publication: %v", err)
				}
				got := lookupResponse(t, r, entry.Key, entry.CompletedAt)
				visible := committed && (phase == "success" || phase == "replace")
				if visible {
					if got == nil || got.ID != entry.ID {
						t.Fatal("lost committed head")
					}
				} else if previous != nil {
					if got == nil || got.ID != previous.ID {
						t.Fatal("old success hidden")
					}
				} else if got != nil {
					t.Fatal("partial publication visible")
				}
				table.hook, blobs.hook = nil, nil
				r = reopen(t, table, blobs)
				if err := r.Responses().Publish(ctx, entry); err != nil {
					t.Fatalf("restart retry: %v", err)
				}
				if got := lookupResponse(t, r, entry.Key, entry.CompletedAt); got == nil || got.ID != entry.ID {
					t.Fatal("retry lost result")
				}
			})
		}
	}
}

func TestCloudResponseCacheUseReceipts(t *testing.T) {
	r, table, blobs, entry, origin := responseCacheFixture(t)
	ctx := context.Background()
	if err := r.Responses().Publish(ctx, entry); err != nil {
		t.Fatal(err)
	}
	child := childCheckpoint(origin, "consumer")
	child.Kind, child.OriginCacheEntryID = state.CheckpointCacheReplay, &entry.ID
	use := cache.ResponseUse{ScopeID: origin.ScopeID, OperationID: child.OriginOperationID, EntryID: entry.ID, CheckpointID: child.ID, CompletedAt: entry.CompletedAt.Add(time.Hour)}
	if err := r.Responses().RecordUse(ctx, use); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("uncommitted checkpoint consumed: %v", err)
	}
	if err := publishCheckpoint(ctx, r.Checkpoints(), child); err != nil {
		t.Fatal(err)
	}
	table.hook = func(_ string, item kv.KeyValueItem) (error, error) {
		if strings.Contains(item.PartitionKey, "/cache/use/") {
			return nil, contracts.ErrOutcomeUnknown
		}
		return nil, nil
	}
	if err := r.Responses().RecordUse(ctx, use); !errors.Is(err, contracts.ErrOutcomeUnknown) {
		t.Fatal(err)
	}
	table.hook = nil
	r = reopen(t, table, blobs)
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for range 100 {
		wg.Go(func() { errs <- r.Responses().RecordUse(ctx, use) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := r.Responses().ReadUse(ctx, use.ScopeID, use.OperationID)
	if err != nil || got != use {
		t.Fatalf("receipt: %#v %v", got, err)
	}
	count := 0
	for key := range table.rows {
		if strings.Contains(key.PartitionKey, "/cache/use/") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("counted replay %d times", count)
	}
	conflict := use
	conflict.CompletedAt = conflict.CompletedAt.Add(time.Second)
	if err := r.Responses().RecordUse(ctx, conflict); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("receipt changed: %v", err)
	}
	if _, err := r.Responses().ReadUse(ctx, "other-scope", use.OperationID); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("cross-scope receipt: %v", err)
	}
	age := time.Minute
	if got, err := r.Responses().Lookup(ctx, cache.ResponseLookup{Key: entry.Key, Now: use.CompletedAt, MaxAge: &age}); err != nil || got != nil {
		t.Fatal("use refreshed age")
	}
}

func TestCloudResponseCacheInvalidInputsAndCorruption(t *testing.T) {
	r, table, blobs, entry, _ := responseCacheFixture(t)
	ctx := context.Background()
	for _, edit := range []func(*cache.ResponseEntry){
		func(e *cache.ResponseEntry) { e.Key.RequestIndex = -1 }, func(e *cache.ResponseEntry) { e.Key.Route.Revision = "" },
		func(e *cache.ResponseEntry) { e.Key.Fingerprint = cache.Fingerprint{} }, func(e *cache.ResponseEntry) { e.ID = "" },
		func(e *cache.ResponseEntry) { e.CompletedAt = time.Time{} }, func(e *cache.ResponseEntry) { e.OriginOperationID = "wrong" },
		func(e *cache.ResponseEntry) { e.Response = []byte(`{"status":"completed"}`) },
		func(e *cache.ResponseEntry) { e.Response = []byte(strings.Repeat(" ", maxPayloadBytes+1)) },
	} {
		candidate := entry
		edit(&candidate)
		if err := r.Responses().Publish(ctx, candidate); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid accepted: %v", err)
		}
	}
	if err := r.Responses().Publish(nil, entry); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := r.Responses().Publish(canceled, entry); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := r.Responses().Publish(ctx, entry); err != nil {
		t.Fatal(err)
	}
	for _, age := range []time.Duration{0, -time.Second} {
		if _, err := r.Responses().Lookup(ctx, cache.ResponseLookup{Key: entry.Key, Now: entry.CompletedAt, MaxAge: &age}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	s := r.Responses().(*responseCache)
	head := s.key("success", entry.Key)
	row := table.rows[head]
	bad := row
	bad.Item = cloneItem(row.Item)
	bad.Item.Fields["cache"] = kv.String("bad")
	table.rows[head] = bad
	if _, err := s.Lookup(ctx, cache.ResponseLookup{Key: entry.Key, Now: entry.CompletedAt}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corruption hidden: %v", err)
	}
	table.rows[head] = row
	pointer, _, err := s.readPointer(ctx, head)
	if err != nil {
		t.Fatal(err)
	}
	data := blobs.values[blob.BlobKey(pointer.Blob)]
	data[len(data)-1] ^= 1
	if _, err := s.Lookup(ctx, cache.ResponseLookup{Key: entry.Key, Now: entry.CompletedAt}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("tampering hidden: %v", err)
	}
}
