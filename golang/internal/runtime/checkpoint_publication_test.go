package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// Storage doubles retain the real codecs, signed handles, graph materializer,
// request preparation and publication boundary exercised by these tests.
type publicationStore struct {
	state.CheckpointRepository
	rows    map[state.CheckpointID]state.DurableCheckpoint
	blobs   map[string][]byte
	writes  int
	failAt  int
	corrupt bool
}

func (s *publicationStore) Get(_ context.Context, scope string, id state.CheckpointID) (state.DurableCheckpoint, error) {
	row, ok := s.rows[id]
	if !ok || row.ScopeID != scope {
		return state.DurableCheckpoint{}, state.ErrNotFound
	}
	return row, nil
}
func (s *publicationStore) Write(ctx context.Context, scope string, data []byte, media string) (state.CheckpointBlobReference, error) {
	if err := ctx.Err(); err != nil {
		return state.CheckpointBlobReference{}, err
	}
	digest := sha256.Sum256(data)
	id := hex.EncodeToString(digest[:])
	s.blobs[scope+"/"+id] = append([]byte(nil), data...)
	s.writes++
	if s.writes == s.failAt {
		return state.CheckpointBlobReference{}, errors.New("lost write reply")
	}
	ref := state.CheckpointBlobReference{ID: state.BlobID(id), Digest: digest, ByteLength: int64(len(data)), MediaType: media}
	if s.corrupt {
		ref.Digest = [32]byte{9}
	}
	return ref, nil
}
func (s *publicationStore) Read(_ context.Context, scope string, ref state.CheckpointBlobReference) ([]byte, error) {
	data, ok := s.blobs[scope+"/"+string(ref.ID)]
	if !ok {
		return nil, state.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}
func publicationFixture(t *testing.T) (*CheckpointPublication, *CheckpointReplay, *publicationStore, CheckpointPublicationIdentity, llm.GenerateRequestV1, llm.Response) {
	t.Helper()
	keys, err := state.NewKeyring([]state.Key{{ID: "test", Primary: true, Secret: bytes.Repeat([]byte{7}, 32)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 25, 1, 0, 0, 0, time.UTC)
	store := &publicationStore{rows: map[state.CheckpointID]state.DurableCheckpoint{}, blobs: map[string][]byte{}}
	materializer := &state.DurableCheckpointMaterializer{Repository: store, Blobs: store, HandleVerifier: keys, Now: func() time.Time { return now }}
	cap := V1RuntimeCapabilities{Checkpoints: CheckpointCapabilities{Repository: store, Blobs: store, BlobWriter: store, Materializer: materializer}}
	p, err := cap.NewCheckpointPublication(keys, state.MaterializeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := cap.NewCheckpointReplay(func(_ context.Context, caller llm.RequestContext) (string, error) {
		if caller.Tenant != "tenant" || caller.Project != "project" {
			return "", errors.New("denied")
		}
		return "scope", nil
	}, state.MaterializeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	identity := CheckpointPublicationIdentity{Scope: "scope", OperationID: state.OperationID(uuid.NewString()), CheckpointID: state.CheckpointID(uuid.NewString()), CreatedAt: now, ExpiresAt: now.Add(24 * time.Hour)}
	model := "reasoning"
	decimal, _ := llm.NewDecimalV1("0.123456789012345678")
	req := llm.GenerateRequestV1{OperationKey: "root", Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}, Append: []llm.Item{preparationMessage("hello")}, SettingsPatch: llm.SettingsPatchV1{Model: llm.Patch[string]{Set: &model}, Temperature: llm.Patch[llm.DecimalV1]{Set: &decimal}}}
	amount, _ := pricing.ParseUSD("0.00001")
	result := llm.Response{OperationKey: req.OperationKey, OperationID: string(identity.OperationID), Status: llm.ResponseStatusCompleted, Output: []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "answer"}}}}, Cost: llm.Cost{Status: llm.CostStatusKnown, ActualCostUSD: &amount, Method: "catalog_usage"}}
	return p, replay, store, identity, req, result
}
func publishRoot(t *testing.T, p *CheckpointPublication, store *publicationStore, identity CheckpointPublicationIdentity, req llm.GenerateRequestV1, result llm.Response) (state.DurableCheckpoint, llm.GenerateResponseV1) {
	t.Helper()
	cp, response, err := p.Generate(context.Background(), identity, req, durable.GenerateReplay{}, result, llm.CacheDispositionV1{Disposition: "disabled"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.rows[cp.ID] = cp
	return cp, response
}
func nextPublication(identity CheckpointPublicationIdentity) CheckpointPublicationIdentity {
	identity.OperationID = state.OperationID(uuid.NewString())
	identity.CheckpointID = state.CheckpointID(uuid.NewString())
	identity.CreatedAt = identity.CreatedAt.Add(time.Minute)
	return identity
}
func TestCheckpointPublicationGenerationRoundTripAndFork(t *testing.T) {
	p, replay, store, identity, request, result := publicationFixture(t)
	cp, response := publishRoot(t, p, store, identity, request, result)
	request.Parent = &response.Checkpoint.Handle
	request.OperationKey = "child"
	request.SettingsPatch = llm.SettingsPatchV1{}
	request.Append = []llm.Item{preparationMessage("follow up")}
	base, err := replay.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if base.State.Tenant != "tenant" || base.State.Project != "project" || base.State.Settings.TemperatureDecimal.String() != "0.123456789012345678" || len(base.State.Items) != 2 {
		t.Fatal("lost authorized scope, exact settings or transcript", base)
	}
	identity = nextPublication(identity)
	result.OperationKey = request.OperationKey
	result.OperationID = string(identity.OperationID)
	child, out, err := p.Generate(context.Background(), identity, request, base, result, llm.CacheDispositionV1{Disposition: "disabled"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if child.ParentID == nil || *child.ParentID != cp.ID || child.Depth != 1 {
		t.Fatal("lost parent")
	}
	store.rows[child.ID] = child
	request.Parent = &out.Checkpoint.Handle
	next, err := replay.Generate(context.Background(), request)
	if err != nil || len(next.State.Items) != 4 || next.State.Depth != 1 {
		t.Fatal(next, err)
	}
	// Reusing the original parent creates a sibling, not an append to the latest child.
	request.Parent = &response.Checkpoint.Handle
	identity = nextPublication(identity)
	result.OperationID = string(identity.OperationID)
	fork, _, err := p.Generate(context.Background(), identity, request, base, result, llm.CacheDispositionV1{Disposition: "disabled"}, nil)
	if err != nil || *fork.ParentID != cp.ID || fork.Depth != 1 {
		t.Fatal(fork, err)
	}
}
func TestCheckpointPublicationRetriesIdenticalBlobs(t *testing.T) {
	for fail := 1; fail <= 3; fail++ {
		t.Run(string(rune('0'+fail)), func(t *testing.T) {
			p, _, store, id, req, result := publicationFixture(t)
			store.failAt = fail
			if _, _, err := p.Generate(context.Background(), id, req, durable.GenerateReplay{}, result, llm.CacheDispositionV1{Disposition: "disabled"}, nil); err == nil {
				t.Fatal("lost reply hidden")
			}
			store.failAt = 0
			cp, a, err := p.Generate(context.Background(), id, req, durable.GenerateReplay{}, result, llm.CacheDispositionV1{Disposition: "disabled"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			again, b, err := p.Generate(context.Background(), id, req, durable.GenerateReplay{}, result, llm.CacheDispositionV1{Disposition: "disabled"}, nil)
			if err != nil || !reflect.DeepEqual(cp, again) || !reflect.DeepEqual(a, b) || len(store.blobs) != 3 {
				t.Fatal("retry changed immutable publication", err)
			}
		})
	}
}
func TestCheckpointPublicationCompactionRoundTrip(t *testing.T) {
	for _, noWork := range []bool{false, true} {
		t.Run(map[bool]string{false: "summary", true: "no_work"}[noWork], func(t *testing.T) {
			p, replay, store, id, req, result := publicationFixture(t)
			if !noWork {
				for i := 0; i < 20; i++ {
					req.Append = append(req.Append, preparationMessage("old message"))
				}
			}
			_, root := publishRoot(t, p, store, id, req, result)
			compact := llm.CompactRequestV1{OperationKey: "compact", Context: req.Context, Parent: root.Checkpoint.Handle}
			base, err := replay.Compact(context.Background(), compact)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := PrepareCompactInput(context.Background(), compact, base)
			if err != nil {
				t.Fatal(err)
			}
			id = nextPublication(id)
			result.OperationKey = compact.OperationKey
			result.OperationID = string(id.OperationID)
			var summary *llm.Response
			if !noWork {
				summary = &result
			}
			cp, out, err := p.Compact(context.Background(), id, compact, base, summary, llm.CacheDispositionV1{Disposition: "disabled"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			store.rows[cp.ID] = cp
			generate := llm.GenerateRequestV1{OperationKey: "after", Context: req.Context, Parent: &out.Checkpoint.Handle}
			next, err := replay.Generate(context.Background(), generate)
			if err != nil {
				t.Fatal(err)
			}
			want := base.State.Items
			if !noWork {
				want = append(append([]llm.Item(nil), result.Output...), prepared.Selection.Retained...)
			}
			if !reflect.DeepEqual(next.State.Items, want) || !reflect.DeepEqual(next.State.Settings, base.State.Settings) {
				t.Fatal("compaction changed suffix or application settings")
			}
			if noWork && (out.Cost.ActualCostUSD == nil || *out.Cost.ActualCostUSD != "0") {
				t.Fatal("no-work compaction charged")
			}
		})
	}
}
func TestCheckpointPublicationCacheHitMakesDistinctZeroCostChild(t *testing.T) {
	p, replay, store, id, req, result := publicationFixture(t)
	cp, originResponse := publishRoot(t, p, store, id, req, result)
	data, _ := json.Marshal(originResponse)
	origin := &cache.ResponseEntry{ID: "cache-origin", Key: cache.ResponseKey{ScopeID: id.Scope, Operation: cache.OperationGenerate}, OriginOperationID: cp.OriginOperationID, OriginCheckpointID: cp.ID, CompletedAt: id.CreatedAt, Response: data}
	id = nextPublication(id)
	req.OperationKey = "cache-consumer"
	result.OperationKey = req.OperationKey
	child, out, err := p.Generate(context.Background(), id, req, durable.GenerateReplay{}, result, llm.CacheDispositionV1{Disposition: "hit"}, origin)
	if err != nil {
		t.Fatal(err)
	}
	if child.ID == cp.ID || child.ParentID != nil || child.Kind != state.CheckpointCacheReplay || child.OriginCacheEntryID == nil || *child.OriginCacheEntryID != origin.ID || out.Cost.ActualCostUSD == nil || *out.Cost.ActualCostUSD != "0" || out.Usage != nil {
		t.Fatal("cache hit reused origin identity or charge")
	}
	store.rows[child.ID] = child
	req.Parent = &out.Checkpoint.Handle
	if _, err := replay.Generate(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointPublicationCachedCompactionKeepsConsumerSuffix(t *testing.T) {
	p, replay, store, id, request, result := publicationFixture(t)
	for i := 0; i < 20; i++ {
		request.Append = append(request.Append, preparationMessage("shared source content"))
	}
	_, root := publishRoot(t, p, store, id, request, result)
	compact := llm.CompactRequestV1{OperationKey: "compact-origin", Context: request.Context, Parent: root.Checkpoint.Handle}
	base, err := replay.Compact(context.Background(), compact)
	if err != nil {
		t.Fatal(err)
	}
	id = nextPublication(id)
	result.OperationKey, result.OperationID = compact.OperationKey, string(id.OperationID)
	originCP, originResponse, err := p.Compact(context.Background(), id, compact, base, &result, llm.CacheDispositionV1{Disposition: "disabled"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.rows[originCP.ID] = originCP
	originData, _ := json.Marshal(originResponse)
	origin := &cache.ResponseEntry{ID: "summary-origin", Key: cache.ResponseKey{ScopeID: id.Scope, Operation: cache.OperationCompact}, OriginOperationID: id.OperationID, OriginCheckpointID: originCP.ID, CompletedAt: id.CreatedAt, Response: originData}
	// A different root can reuse the summary, but must preserve its own suffix,
	// settings, parent and lineage rather than copying the origin's snapshot.
	id = nextPublication(id)
	request.OperationKey = "consumer-root"
	request.Append[len(request.Append)-1] = preparationMessage("consumer private suffix")
	result.OperationKey, result.OperationID = request.OperationKey, string(id.OperationID)
	consumerCP, consumerRoot := publishRoot(t, p, store, id, request, result)
	compact.Parent, compact.OperationKey = consumerRoot.Checkpoint.Handle, "compact-consumer"
	base, err = replay.Compact(context.Background(), compact)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareCompactInput(context.Background(), compact, base)
	if err != nil {
		t.Fatal(err)
	}
	id = nextPublication(id)
	result.OperationKey = compact.OperationKey
	cp, response, err := p.Compact(context.Background(), id, compact, base, &result, llm.CacheDispositionV1{Disposition: "hit"}, origin)
	if err != nil {
		t.Fatal(err)
	}
	if cp.ParentID == nil || *cp.ParentID != consumerCP.ID || cp.Kind != state.CheckpointCompaction || *cp.OriginCacheEntryID != origin.ID || response.Usage != nil || *response.Cost.ActualCostUSD != "0" {
		t.Fatal("cached compaction reused origin lineage or charge")
	}
	store.rows[cp.ID] = cp
	request.Parent = &response.Checkpoint.Handle
	next, err := replay.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]llm.Item(nil), result.Output...), prepared.Selection.Retained...)
	if !reflect.DeepEqual(next.State.Items, want) || !reflect.DeepEqual(next.State.Settings, base.State.Settings) {
		t.Fatal("cached compaction lost consumer state")
	}
}
func TestCheckpointPublicationRejectsInvalidStateBeforeWriting(t *testing.T) {
	for _, fault := range []string{"id", "operation", "scope", "expiry", "sample", "hit_without_origin", "tool_frontier", "bytes", "items"} {
		t.Run(fault, func(t *testing.T) {
			p, _, store, id, req, result := publicationFixture(t)
			disposition := llm.CacheDispositionV1{Disposition: "disabled"}
			switch fault {
			case "id":
				id.CheckpointID = "bad"
			case "operation":
				result.OperationID = "another"
			case "scope":
				id.Scope = ""
			case "expiry":
				id.ExpiresAt = id.CreatedAt
			case "sample":
				disposition.Variant = 1
			case "hit_without_origin":
				disposition.Disposition = "hit"
			case "tool_frontier":
				result.Output = []llm.Item{llm.ToolResult{CallID: "missing", Content: []llm.Part{llm.TextPart{Text: "bad"}}}}
			case "bytes":
				p.codec.MaxBytes = 10
			case "items":
				p.limits.MaxItems = 1
			}
			if _, _, err := p.Generate(context.Background(), id, req, durable.GenerateReplay{}, result, disposition, nil); err == nil {
				t.Fatal("invalid publication accepted")
			}
			if store.writes != 0 {
				t.Fatal("wrote before validation")
			}
		})
	}
}
func TestCheckpointPublicationRejectsWrongBlobReference(t *testing.T) {
	p, _, store, id, req, result := publicationFixture(t)
	store.corrupt = true
	if _, _, err := p.Generate(context.Background(), id, req, durable.GenerateReplay{}, result, llm.CacheDispositionV1{Disposition: "disabled"}, nil); err == nil {
		t.Fatal("corrupt blob accepted")
	}
}

func TestCheckpointPublicationSnapshotCadence(t *testing.T) {
	p, replay, store, identity, request, result := publicationFixture(t)
	capabilities := V1RuntimeCapabilities{Checkpoints: p.checkpoints}
	if _, err := capabilities.NewCheckpointPublication(p.keyring, state.MaterializeLimits{SnapshotInterval: -1}); err == nil {
		t.Fatal("negative snapshot interval accepted")
	}
	root, response := publishRoot(t, p, store, identity, request, result)
	if root.MaterializedSnapshotBlob != nil {
		t.Fatal("root wrote a snapshot")
	}
	request.Parent = &response.Checkpoint.Handle
	request.OperationKey = "child"
	request.SettingsPatch = llm.SettingsPatchV1{}
	request.Append = []llm.Item{preparationMessage("follow up")}
	base, err := replay.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	transcript, err := state.CheckpointBlobCodec{}.EncodeDelta(append(append(append([]llm.Item(nil), base.State.Items...), request.Append...), result.Output...))
	if err != nil {
		t.Fatal(err)
	}
	result.OperationKey = request.OperationKey
	for name, test := range map[string]struct {
		limits   state.MaterializeLimits
		snapshot bool
	}{
		"default cadence skips depth one": {state.MaterializeLimits{}, false},
		"boundary":                        {state.MaterializeLimits{SnapshotInterval: 1}, true},
		// The transcript fits the blob bound but the snapshot, which also holds
		// settings and lineage, does not: the paid result still publishes.
		"oversized snapshot": {state.MaterializeLimits{SnapshotInterval: 1, MaxBytes: int64(len(transcript)) + 16}, false},
	} {
		t.Run(name, func(t *testing.T) {
			publication, err := capabilities.NewCheckpointPublication(p.keyring, test.limits)
			if err != nil {
				t.Fatal(err)
			}
			identity := nextPublication(identity)
			result := result
			result.OperationID = string(identity.OperationID)
			child, out, err := publication.Generate(context.Background(), identity, request, base, result, llm.CacheDispositionV1{Disposition: "disabled"}, nil)
			if err != nil || child.Depth != 1 || (child.MaterializedSnapshotBlob != nil) != test.snapshot {
				t.Fatalf("child depth=%d snapshot=%t err=%v", child.Depth, child.MaterializedSnapshotBlob != nil, err)
			}
			again, _, err := publication.Generate(context.Background(), identity, request, base, result, llm.CacheDispositionV1{Disposition: "disabled"}, nil)
			if err != nil || !reflect.DeepEqual(child, again) {
				t.Fatal("retry changed the snapshot decision", err)
			}
			store.rows[child.ID] = child
			next := request
			next.Parent = &out.Checkpoint.Handle
			if got, err := replay.Generate(context.Background(), next); err != nil || len(got.State.Items) != 4 || got.State.Depth != 1 || len(got.State.Lineage) != 2 {
				t.Fatal(got, err)
			}
		})
	}
}
