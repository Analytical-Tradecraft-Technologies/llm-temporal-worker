package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/compaction"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// CheckpointPublicationIdentity must come from the persisted operation, not
// fresh clock/UUID reads on each retry. Scope is already authorized. Publication
// writes immutable blobs only; CloudFinalizer commits the checkpoint and effects.
type CheckpointPublicationIdentity struct {
	Scope        string
	OperationID  state.OperationID
	CheckpointID state.CheckpointID
	CreatedAt    time.Time
	ExpiresAt    time.Time
}

type CheckpointPublication struct {
	checkpoints CheckpointCapabilities
	keyring     *state.Keyring
	limits      state.MaterializeLimits
	codec       state.CheckpointBlobCodec
}

func (capabilities V1RuntimeCapabilities) NewCheckpointPublication(keyring *state.Keyring, limits state.MaterializeLimits) (*CheckpointPublication, error) {
	if keyring == nil || capabilities.Checkpoints.RequireMaterializer() != nil || isNilCapability(capabilities.Checkpoints.BlobWriter) || limits.MaxDepth < 0 || limits.MaxRows < 0 || limits.MaxItems < 0 || limits.MaxBytes < 0 || limits.SnapshotInterval < 0 {
		return nil, checkpointPublicationError(provider.CodeConfiguration)
	}
	if limits.SnapshotInterval == 0 {
		limits.SnapshotInterval = state.DefaultSnapshotInterval
	}
	if limits.MaxDepth == 0 {
		limits.MaxDepth = 256
	}
	if limits.MaxRows == 0 {
		limits.MaxRows = 512
	}
	if limits.MaxItems == 0 {
		limits.MaxItems = 4096
	}
	if limits.MaxBytes == 0 {
		limits.MaxBytes = 16 << 20
	}
	// A configured blob bound must also fit the codec's integer bound.
	if limits.MaxBytes > 16<<20 {
		return nil, checkpointPublicationError(provider.CodeConfiguration)
	}
	return &CheckpointPublication{checkpoints: capabilities.Checkpoints, keyring: keyring, limits: limits, codec: state.CheckpointBlobCodec{MaxBytes: int(limits.MaxBytes)}}, nil
}

// Generate builds a distinct child even for cache hits. The origin checkpoint
// is provenance only and is never used as the new child's parent.
func (p *CheckpointPublication) Generate(ctx context.Context, identity CheckpointPublicationIdentity, request llm.GenerateRequestV1, replay durable.GenerateReplay, result llm.Response, disposition llm.CacheDispositionV1, origin *cache.ResponseEntry) (state.DurableCheckpoint, llm.GenerateResponseV1, error) {
	var zero state.DurableCheckpoint
	var response llm.GenerateResponseV1
	prepared, err := PrepareGenerateInput(ctx, request, replay)
	if err != nil {
		return zero, response, err
	}
	if result.OperationKey != request.OperationKey || (origin == nil && result.OperationID != string(identity.OperationID)) || disposition.Variant != int32(prepared.SampleIndex) {
		return zero, response, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	if _, err := result.MarshalJSON(); err != nil {
		return zero, response, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	kind := state.CheckpointGeneration
	if origin != nil {
		kind = state.CheckpointCacheReplay
	}
	cp, metadata, err := p.metadata(ctx, identity, request.Parent, replay.State, kind, origin)
	if err != nil {
		return zero, response, err
	}
	response = llm.GenerateResponseV1{APIVersion: llm.APIVersion, OperationKey: request.OperationKey, OperationID: string(identity.OperationID), Status: result.Status,
		Output: result.Output, Checkpoint: metadata, Cache: disposition, Route: &result.Route, Usage: &result.Usage, Cost: publicationCost(result.Cost), Diagnostics: result.Diagnostics}
	if origin != nil {
		response.Cost = zeroPublicationCost()
		response.Usage = nil
	}
	if err := validatePublicationCache(origin, disposition, cache.OperationGenerate, identity, prepared.SampleIndex); err != nil {
		return zero, llm.GenerateResponseV1{}, err
	}
	if origin != nil && result.Status != llm.ResponseStatusCompleted && result.Status != llm.ResponseStatusToolCalls {
		return zero, llm.GenerateResponseV1{}, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	if _, err := response.MarshalJSON(); err != nil {
		return zero, llm.GenerateResponseV1{}, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	patch, err := p.codec.EncodeSettingsPatchV1(request.SettingsPatch)
	if err != nil {
		return zero, llm.GenerateResponseV1{}, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	items := append(append([]llm.Item(nil), prepared.Request.Input...), result.Output...)
	// A snapshot on every SnapshotInterval-th depth keeps the next turns'
	// lineage walks short without a full-transcript blob write on each turn.
	// Depth is immutable, so a retry and every fork make the same choice.
	snapshot := cp.Depth > 0 && cp.Depth%p.limits.SnapshotInterval == 0
	cp, err = p.blobs(ctx, cp, replay.State, prepared.Settings, request.Append, result.Output, patch, items, snapshot)
	return cp, response, err
}

// Compact replaces only a safe prefix with plain text, preserving the suffix
// and the application's settings. A nil result is permitted only when there is
// no prefix to compact; that path produces a zero-cost child without a model call.
// A cache hit supplies the summary result extracted from its origin artifact,
// never the origin's suffix or application settings.
func (p *CheckpointPublication) Compact(ctx context.Context, identity CheckpointPublicationIdentity, request llm.CompactRequestV1, replay durable.CompactReplay, result *llm.Response, disposition llm.CacheDispositionV1, origin *cache.ResponseEntry) (state.DurableCheckpoint, llm.CompactResponseV1, error) {
	var zero state.DurableCheckpoint
	var response llm.CompactResponseV1
	prepared, err := PrepareCompactInput(ctx, request, replay)
	if err != nil {
		return zero, response, err
	}
	if (prepared.Request == nil) != (result == nil) || (result == nil && (origin != nil || disposition.Disposition != "disabled")) {
		return zero, response, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	cp, metadata, err := p.metadata(ctx, identity, &request.Parent, replay.State, state.CheckpointCompaction, origin)
	if err != nil {
		return zero, response, err
	}
	if err := validatePublicationCache(origin, disposition, cache.OperationCompact, identity, request.Cache.SampleIndex()); err != nil {
		return zero, response, err
	}
	response = llm.CompactResponseV1{APIVersion: llm.CompactAPIVersion, OperationKey: request.OperationKey, OperationID: string(identity.OperationID), Checkpoint: metadata, Cache: disposition, Cost: zeroPublicationCost()}
	items := append([]llm.Item(nil), replay.State.Items...)
	var summaryItems []llm.Item
	source := "no_work"
	if result != nil {
		if result.OperationKey != request.OperationKey || (origin == nil && result.OperationID != string(identity.OperationID)) {
			return zero, response, checkpointPublicationError(provider.CodeStateCorrupt)
		}
		if _, err := result.MarshalJSON(); err != nil {
			return zero, response, checkpointPublicationError(provider.CodeStateCorrupt)
		}
		summary, err := compaction.PlainTextSummary(*result, int(p.limits.MaxBytes))
		if err != nil {
			return zero, response, checkpointPublicationError(provider.CodeStateCorrupt)
		}
		summaryItems = []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: summary}}}}
		items = append(append([]llm.Item(nil), summaryItems...), prepared.Selection.Retained...)
		response.Cost, response.Usage, response.Diagnostics = publicationCost(result.Cost), &result.Usage, result.Diagnostics
		source = "provider"
		if origin != nil {
			source = "worker_cache"
			response.Cost = zeroPublicationCost()
			response.Usage = nil
		}
	}
	response.Provenance, _ = json.Marshal(struct {
		Source string `json:"source"`
		Policy string `json:"policy_version"`
		Prompt string `json:"prompt_version"`
	}{source, prepared.Policy.Version, prepared.Policy.PromptVersion})
	if _, err := response.MarshalJSON(); err != nil {
		return zero, llm.CompactResponseV1{}, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	cp.CompactionPolicyVersion, cp.CompactionPromptVersion = &prepared.Policy.Version, &prepared.Policy.PromptVersion
	cp.CompactedThroughID = cp.ParentID
	patch, err := p.codec.EncodeSettingsPatch(state.SettingsPatch{})
	if err != nil {
		return zero, llm.CompactResponseV1{}, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	// ResponseBlob retains the summary alone, so cache consumers can attach it
	// to their own suffix without recovering a provider ID or copying lineage.
	cp, err = p.blobs(ctx, cp, replay.State, prepared.Settings, nil, summaryItems, patch, items, true)
	return cp, response, err
}

func (p *CheckpointPublication) metadata(ctx context.Context, identity CheckpointPublicationIdentity, parent *llm.CheckpointHandle, replay state.MaterializedState, kind state.CheckpointKind, origin *cache.ResponseEntry) (state.DurableCheckpoint, llm.CheckpointMetadata, error) {
	var cp state.DurableCheckpoint
	var metadata llm.CheckpointMetadata
	id, err := uuid.Parse(string(identity.CheckpointID))
	if p == nil || p.keyring == nil || ctx == nil || err != nil || id == uuid.Nil || id.String() != string(identity.CheckpointID) || strings.TrimSpace(identity.Scope) == "" || durable.OperationID(identity.OperationID).Validate() != nil || identity.CreatedAt.IsZero() || !identity.ExpiresAt.After(identity.CreatedAt) {
		return cp, metadata, checkpointPublicationError(provider.CodeConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return cp, metadata, err
	}
	cp = state.DurableCheckpoint{ID: identity.CheckpointID, ScopeID: identity.Scope, Kind: kind, OriginOperationID: identity.OperationID, CreatedAt: identity.CreatedAt, ExpiresAt: identity.ExpiresAt, SchemaVersion: 1, CompilerEpoch: cloudCompilerVersion}
	if parent != nil {
		parentID, err := p.keyring.VerifyCheckpointHandle(ctx, identity.Scope, string(*parent))
		if err != nil {
			return state.DurableCheckpoint{}, metadata, checkpointPublicationError(provider.CodeInvalidArgument)
		}
		row, err := p.checkpoints.Repository.Get(ctx, identity.Scope, parentID)
		if err != nil {
			return state.DurableCheckpoint{}, metadata, checkpointPublicationError(provider.CodeStateUnavailable)
		}
		if row.Validate(identity.CreatedAt) != nil || row.ScopeID != identity.Scope || row.ID != parentID || row.Depth != replay.Depth || !identity.CreatedAt.Before(row.ExpiresAt) || len(replay.Lineage) == 0 || replay.Lineage[len(replay.Lineage)-1] != state.Handle(parentID) {
			return state.DurableCheckpoint{}, metadata, checkpointPublicationError(provider.CodeStateCorrupt)
		}
		if replay.Depth >= p.limits.MaxDepth {
			return state.DurableCheckpoint{}, metadata, checkpointPublicationError(provider.CodeInvalidArgument)
		}
		cp.ParentID, cp.Depth = &parentID, replay.Depth+1
		if row.ExpiresAt.Before(cp.ExpiresAt) {
			cp.ExpiresAt = row.ExpiresAt
		}
	}
	if origin != nil {
		originID := origin.ID
		cp.OriginCacheEntryID = &originID
	}
	handle, err := p.keyring.IssueCheckpointHandle(identity.Scope, cp.ID)
	if err != nil {
		return state.DurableCheckpoint{}, metadata, checkpointPublicationError(provider.CodeConfiguration)
	}
	cp.PublicIDHMAC = sha256.Sum256([]byte(handle))
	cp.HandleKeyID = strings.Split(handle, ".")[1] // format issued by this keyring
	metadata = llm.CheckpointMetadata{Handle: llm.CheckpointHandle(handle), Parent: parent, Kind: string(kind), Depth: cp.Depth}
	return cp, metadata, nil
}

func (p *CheckpointPublication) blobs(ctx context.Context, cp state.DurableCheckpoint, replay state.MaterializedState, settings state.ModelState, delta, output []llm.Item, patch []byte, items []llm.Item, snapshot bool) (state.DurableCheckpoint, error) {
	pending, err := state.ValidateTranscript(items)
	if err != nil || settings.Validate() != nil || len(items) > p.limits.MaxItems || len(replay.Lineage)+1 > p.limits.MaxRows {
		return state.DurableCheckpoint{}, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	lineage := append(append([]state.Handle(nil), replay.Lineage...), state.Handle(cp.ID))
	var encoded [][]byte
	for _, value := range []any{lineage, settings, pending} {
		data, err := json.Marshal(value)
		if err != nil {
			return state.DurableCheckpoint{}, checkpointPublicationError(provider.CodeStateCorrupt)
		}
		encoded = append(encoded, data)
	}
	cp.CanonicalLineageDigest, cp.MaterializedSettingsDigest, cp.ToolFrontierDigest = sha256.Sum256(encoded[0]), sha256.Sum256(encoded[1]), sha256.Sum256(encoded[2])
	deltaBytes, err := p.codec.EncodeDelta(delta)
	if err != nil {
		return state.DurableCheckpoint{}, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	outputBytes, err := p.codec.EncodeResponse(output)
	if err != nil {
		return state.DurableCheckpoint{}, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	// Bound the full materialized transcript, not just each incremental blob.
	if _, err := p.codec.EncodeDelta(items); err != nil {
		return state.DurableCheckpoint{}, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	payloads := [][]byte{deltaBytes, outputBytes, patch}
	if snapshot {
		data, err := p.codec.EncodeSnapshot(*state.NewCheckpointSnapshot(state.MaterializedState{Items: items, Settings: settings, Depth: cp.Depth, Lineage: lineage}))
		switch {
		case err == nil:
			payloads = append(payloads, data)
		case cp.Kind == state.CheckpointCompaction:
			return state.DurableCheckpoint{}, checkpointPublicationError(provider.CodeStateCorrupt)
		default:
			// A cadence snapshot is only a read optimization. Settings and
			// lineage can push it past the blob bound when the transcript alone
			// still fits; the paid result must then publish without one.
			snapshot = false
		}
	}
	refs := make([]state.CheckpointBlobReference, len(payloads))
	for i, data := range payloads {
		refs[i], err = p.checkpoints.BlobWriter.Write(ctx, cp.ScopeID, data, "application/json")
		if err != nil {
			return state.DurableCheckpoint{}, checkpointPublicationError(provider.CodeStateUnavailable)
		}
		if refs[i].Digest != sha256.Sum256(data) || refs[i].ByteLength != int64(len(data)) || refs[i].MediaType != "application/json" {
			return state.DurableCheckpoint{}, checkpointPublicationError(provider.CodeStateCorrupt)
		}
	}
	cp.DeltaBlob, cp.ResponseBlob, cp.SettingsPatchBlob = refs[0], refs[1], refs[2]
	if snapshot {
		cp.MaterializedSnapshotBlob = &refs[3]
	}
	if cp.Validate(cp.CreatedAt) != nil {
		return state.DurableCheckpoint{}, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	return cp, nil
}

func validatePublicationCache(origin *cache.ResponseEntry, disposition llm.CacheDispositionV1, kind cache.OperationKind, identity CheckpointPublicationIdentity, index int64) error {
	if int64(disposition.Variant) != index || (disposition.Disposition == "hit") != (origin != nil) {
		return checkpointPublicationError(provider.CodeStateCorrupt)
	}
	if origin != nil && (origin.ID == "" || origin.Key.ScopeID != identity.Scope || origin.Key.Operation != kind || origin.Key.RequestIndex != index || origin.OriginOperationID == "" || origin.OriginOperationID == identity.OperationID || origin.OriginCheckpointID == "" || origin.OriginCheckpointID == identity.CheckpointID || origin.CompletedAt.IsZero() || origin.CompletedAt.After(identity.CreatedAt)) {
		return checkpointPublicationError(provider.CodeStateCorrupt)
	}
	return nil
}

func publicationCost(cost llm.Cost) llm.CostV1 {
	if cost.Status == llm.CostStatusKnown && cost.ActualCostUSD != nil {
		amount := cost.ActualCostUSD.String()
		return llm.CostV1{Status: "exact", ActualCostUSD: &amount, Method: cost.Method, CatalogVersion: cost.CatalogVersion}
	}
	return llm.CostV1{Status: "unknown", UnknownReason: "provider_did_not_report_cost"}
}
func zeroPublicationCost() llm.CostV1 {
	zero := "0"
	return llm.CostV1{Status: "exact", ActualCostUSD: &zero, Method: "provider_reported"}
}
func checkpointPublicationError(code provider.Code) error {
	retry := provider.RetryNever
	if code == provider.CodeStateUnavailable {
		retry = provider.RetrySameOperation
	}
	return provider.NewError(code, provider.PhaseFinalize, provider.DispatchAccepted, retry, "checkpoint publication failed")
}
