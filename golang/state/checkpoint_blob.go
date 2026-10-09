package state

// Versioned checkpoint blob codecs and the scoped blob-read boundary live in
// this file.  Checkpoint rows contain references only; the codec makes the
// bytes behind those references an explicit, bounded protocol rather than an
// implementation-detail JSON dump.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

const CheckpointBlobCodecVersion = "checkpoint-blob/v1"

type CheckpointBlobKind string

const (
	CheckpointDeltaBlob    CheckpointBlobKind = "delta"
	CheckpointResponseBlob CheckpointBlobKind = "response"
	CheckpointSettingsBlob CheckpointBlobKind = "settings_patch"
	CheckpointSnapshotBlob CheckpointBlobKind = "materialized_snapshot"
)

// CheckpointBlobCodec bounds both the encoded object and canonical JSON depth.
// Limits are applied before decoding any polymorphic transcript values.
type CheckpointBlobCodec struct {
	MaxBytes int
	MaxDepth int
}

func (codec CheckpointBlobCodec) withDefaults() CheckpointBlobCodec {
	if codec.MaxBytes <= 0 {
		codec.MaxBytes = llm.DefaultCanonicalMaxBytes
	}
	if codec.MaxDepth <= 0 {
		codec.MaxDepth = llm.DefaultCanonicalMaxDepth
	}
	return codec
}

type checkpointBlobEnvelope struct {
	Version string             `json:"version"`
	Kind    CheckpointBlobKind `json:"kind"`
	Payload json.RawMessage    `json:"payload"`
}

// encode wraps one canonical payload in a closed, versioned envelope.
func (codec CheckpointBlobCodec) encode(kind CheckpointBlobKind, payload any) ([]byte, error) {
	codec = codec.withDefaults()
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal %s checkpoint blob: %w", kind, err)
	}
	canonicalPayload, err := llm.CanonicalJSONWithLimits(encodedPayload, codec.MaxBytes, codec.MaxDepth)
	if err != nil {
		return nil, fmt.Errorf("canonicalize %s checkpoint blob: %w", kind, typedCanonicalLimit(err))
	}
	encoded, err := json.Marshal(checkpointBlobEnvelope{Version: CheckpointBlobCodecVersion, Kind: kind, Payload: canonicalPayload})
	if err != nil {
		return nil, fmt.Errorf("marshal %s checkpoint blob envelope: %w", kind, err)
	}
	canonical, err := llm.CanonicalJSONWithLimits(encoded, codec.MaxBytes, codec.MaxDepth)
	if err != nil {
		return nil, fmt.Errorf("canonicalize %s checkpoint blob envelope: %w", kind, typedCanonicalLimit(err))
	}
	return canonical, nil
}

func (codec CheckpointBlobCodec) decode(kind CheckpointBlobKind, data []byte, target any) error {
	codec = codec.withDefaults()
	canonical, err := llm.CanonicalJSONWithLimits(data, codec.MaxBytes, codec.MaxDepth)
	if err != nil {
		return fmt.Errorf("canonicalize %s checkpoint blob: %w", kind, typedCanonicalLimit(err))
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(canonical, &fields); err != nil {
		return fmt.Errorf("decode %s checkpoint blob envelope: %w", kind, err)
	}
	for name := range fields {
		if name != "version" && name != "kind" && name != "payload" {
			return fmt.Errorf("decode %s checkpoint blob: unknown envelope field %q", kind, name)
		}
	}
	var version string
	if raw, ok := fields["version"]; !ok || json.Unmarshal(raw, &version) != nil || version != CheckpointBlobCodecVersion {
		return fmt.Errorf("decode %s checkpoint blob: unsupported version", kind)
	}
	var gotKind CheckpointBlobKind
	if raw, ok := fields["kind"]; !ok || json.Unmarshal(raw, &gotKind) != nil || gotKind != kind {
		return fmt.Errorf("decode %s checkpoint blob: kind mismatch", kind)
	}
	payload, ok := fields["payload"]
	if !ok || string(payload) == "null" {
		return fmt.Errorf("decode %s checkpoint blob: payload is required", kind)
	}
	// The enclosing canonicalization already checked every payload value for
	// duplicate keys, syntax and depth, and bounded its canonical bytes. The
	// payload is a subtree of that checked document; parsing and emitting it
	// again adds a full transcript pass without enforcing a stricter limit.
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("decode %s checkpoint blob payload: %w", kind, err)
	}
	return nil
}

func (codec CheckpointBlobCodec) EncodeDelta(items []llm.Item) ([]byte, error) {
	if items == nil {
		items = []llm.Item{}
	}
	if err := validateItemEncoding(items); err != nil {
		return nil, err
	}
	return codec.encode(CheckpointDeltaBlob, items)
}

func (codec CheckpointBlobCodec) DecodeDelta(data []byte) ([]llm.Item, error) {
	var payload json.RawMessage
	if err := codec.decode(CheckpointDeltaBlob, data, &payload); err != nil {
		return nil, err
	}
	items, err := llm.DecodeItems(payload)
	if err != nil {
		return nil, fmt.Errorf("decode delta items: %w", err)
	}
	if err := validateItemEncoding(items); err != nil {
		return nil, err
	}
	return items, nil
}

func (codec CheckpointBlobCodec) EncodeResponse(items []llm.Item) ([]byte, error) {
	if items == nil {
		items = []llm.Item{}
	}
	if err := validateItemEncoding(items); err != nil {
		return nil, err
	}
	return codec.encode(CheckpointResponseBlob, items)
}

func (codec CheckpointBlobCodec) DecodeResponse(data []byte) ([]llm.Item, error) {
	var payload json.RawMessage
	if err := codec.decode(CheckpointResponseBlob, data, &payload); err != nil {
		return nil, err
	}
	items, err := llm.DecodeItems(payload)
	if err != nil {
		return nil, fmt.Errorf("decode response items: %w", err)
	}
	if err := validateItemEncoding(items); err != nil {
		return nil, err
	}
	return items, nil
}

func (codec CheckpointBlobCodec) EncodeSettingsPatch(patch SettingsPatch) ([]byte, error) {
	if err := patch.Validate(); err != nil {
		return nil, err
	}
	wire := settingsPatchToWire(patch)
	return codec.encode(CheckpointSettingsBlob, wire)
}

// EncodeSettingsPatchV1 preserves the public patch's exact decimal and its
// omitted/set/clear states when publishing a durable checkpoint.
func (codec CheckpointBlobCodec) EncodeSettingsPatchV1(patch llm.SettingsPatchV1) ([]byte, error) {
	if _, err := patch.MarshalJSON(); err != nil {
		return nil, err
	}
	return codec.encode(CheckpointSettingsBlob, patch)
}

func (codec CheckpointBlobCodec) DecodeSettingsPatch(data []byte) (SettingsPatch, error) {
	var payload json.RawMessage
	if err := codec.decode(CheckpointSettingsBlob, data, &payload); err != nil {
		return SettingsPatch{}, err
	}
	var wire llm.SettingsPatchV1
	if err := json.Unmarshal(payload, &wire); err != nil {
		return SettingsPatch{}, fmt.Errorf("decode settings patch: %w", err)
	}
	patch, err := settingsPatchFromWire(wire)
	if err != nil {
		return SettingsPatch{}, err
	}
	return patch, nil
}

type checkpointSnapshotPayload struct {
	Items    json.RawMessage     `json:"items"`
	Settings llm.SettingsPatchV1 `json:"settings"`
	Depth    int32               `json:"depth"`
	Lineage  []string            `json:"lineage"`
}

func (codec CheckpointBlobCodec) EncodeSnapshot(snapshot CheckpointSnapshot) ([]byte, error) {
	if err := snapshot.validate(); err != nil {
		return nil, err
	}
	lineage := make([]string, len(snapshot.Lineage))
	for i, handle := range snapshot.Lineage {
		lineage[i] = string(handle)
	}
	itemsValue := snapshot.Items
	if itemsValue == nil {
		itemsValue = []llm.Item{}
	}
	items, err := json.Marshal(itemsValue)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot items: %w", err)
	}
	payload := checkpointSnapshotPayload{Items: items, Settings: settingsPatchToWire(settingsPatchFromModel(snapshot.Settings)), Depth: snapshot.Depth, Lineage: lineage}
	return codec.encode(CheckpointSnapshotBlob, payload)
}

func (codec CheckpointBlobCodec) DecodeSnapshot(data []byte) (CheckpointSnapshot, error) {
	var payload checkpointSnapshotPayload
	if err := codec.decode(CheckpointSnapshotBlob, data, &payload); err != nil {
		return CheckpointSnapshot{}, err
	}
	items, err := llm.DecodeItems(payload.Items)
	if err != nil {
		return CheckpointSnapshot{}, fmt.Errorf("decode snapshot items: %w", err)
	}
	// snapshot.validate below checks item encoding together with the tool
	// frontier. Do not marshal every decoded item a second time here.
	patch, err := settingsPatchFromWire(payload.Settings)
	if err != nil {
		return CheckpointSnapshot{}, err
	}
	settings, err := ApplySettingsPatch(RootModelState(""), patch)
	if err != nil {
		return CheckpointSnapshot{}, fmt.Errorf("decode snapshot settings: %w", err)
	}
	lineage := make([]Handle, len(payload.Lineage))
	for i, handle := range payload.Lineage {
		if strings.TrimSpace(handle) == "" {
			return CheckpointSnapshot{}, fmt.Errorf("decode snapshot lineage contains an empty handle")
		}
		lineage[i] = Handle(handle)
	}
	snapshot := CheckpointSnapshot{Items: cloneItems(items), Settings: settings, Depth: payload.Depth, Lineage: lineage}
	snapshot.Digest = snapshot.digest()
	if err := snapshot.validate(); err != nil {
		return CheckpointSnapshot{}, err
	}
	return snapshot, nil
}

func settingsPatchFromModel(model ModelState) SettingsPatch {
	patch := SettingsPatch{WebFetch: SetPatch(model.WebFetch), CodeExecution: SetPatch(model.CodeExecution), WebSearch: SetPatch(model.WebSearch), Model: SetPatch(model.Model), ServiceClass: SetPatch(model.ServiceClass), Portability: SetPatch(model.Portability), ReasoningEffort: SetPatch(model.ReasoningEffort), ReasoningSummary: SetPatch(model.ReasoningSummary)}
	// Empty reasoning values are the materialized zero (provider-inherited)
	// state, not explicit wire patch values. Leave them omitted so the closed
	// v1 patch enum cannot emit schema-invalid Set("") values.
	if model.ReasoningEffort == "" {
		patch.ReasoningEffort = Patch[llm.ReasoningEffort]{}
	}
	if model.ReasoningSummary == "" {
		patch.ReasoningSummary = Patch[llm.ReasoningSummary]{}
	}
	if model.ToolPolicy != (llm.ToolPolicy{}) {
		patch.ToolPolicy = SetPatch(model.ToolPolicy)
	}
	if model.ServiceClassFallbacks != nil {
		patch.ServiceClassFallbacks = SetPatch(append([]llm.ServiceClass(nil), model.ServiceClassFallbacks...))
	}
	if model.Instructions != nil {
		patch.Instructions = SetPatch(cloneInstructions(model.Instructions))
	}
	if model.Tools != nil {
		patch.Tools = SetPatch(cloneTools(model.Tools))
	}
	if model.Output != nil {
		patch.Output = SetPatch(*cloneOutput(model.Output))
	}
	if model.Temperature != nil {
		patch.Temperature = SetPatch(*model.Temperature)
	}
	if model.TemperatureDecimal != nil {
		patch.TemperatureDecimal = SetPatch(*model.TemperatureDecimal)
	}
	// The later sampling and reasoning leaves are emitted only when set, so a
	// snapshot that does not use them encodes exactly as it did before.
	if model.TopP != nil {
		patch.TopP = SetPatch(*model.TopP)
	}
	if model.StopSequences != nil {
		patch.StopSequences = SetPatch(append([]string(nil), model.StopSequences...))
	}
	if model.Seed != nil {
		patch.Seed = SetPatch(*model.Seed)
	}
	if model.ReasoningMode != "" {
		patch.ReasoningMode = SetPatch(model.ReasoningMode)
	}
	if model.ReasoningTokenBudget != nil {
		patch.ReasoningTokenBudget = SetPatch(*model.ReasoningTokenBudget)
	}
	if model.CompactionPolicy != nil {
		patch.CompactionPolicy = SetPatch(append(json.RawMessage(nil), model.CompactionPolicy...))
	}
	if model.Extensions != nil {
		patch.Extensions = SetPatch(cloneRawMap(model.Extensions))
	}
	return patch
}

func settingsPatchToWire(patch SettingsPatch) llm.SettingsPatchV1 {
	temperature := llm.Patch[llm.DecimalV1]{Clear: patch.Temperature.Clear || patch.TemperatureDecimal.Clear}
	if patch.TemperatureDecimal.Set != nil {
		value := *patch.TemperatureDecimal.Set
		temperature.Set = &value
	} else if patch.Temperature.Set != nil {
		// Materialized state uses float64 for provider adapters. Convert only at
		// this persistence boundary; the v1 wire record itself retains the
		// canonical decimal string and never routes it through float64.
		value := llm.DecimalV1(strconv.FormatFloat(*patch.Temperature.Set, 'f', -1, 64))
		temperature.Set = &value
	}
	return llm.SettingsPatchV1{
		WebFetch: patchToWire(patch.WebFetch), CodeExecution: patchToWire(patch.CodeExecution), WebSearch: patchToWire(patch.WebSearch), Model: patchToWire(patch.Model), ServiceClass: patchToWire(patch.ServiceClass), ServiceClassFallbacks: patchToWire(patch.ServiceClassFallbacks), Portability: patchToWire(patch.Portability),
		Instructions: patchToWire(patch.Instructions), Tools: patchToWire(patch.Tools), ToolPolicy: patchToWire(patch.ToolPolicy), Output: patchToWire(patch.Output), Temperature: temperature,
		TopP: patchToWire(patch.TopP), StopSequences: patchToWire(patch.StopSequences), Seed: patchToWire(patch.Seed),
		ReasoningMode: patchToWire(patch.ReasoningMode), ReasoningTokenBudget: patchToWire(patch.ReasoningTokenBudget),
		ReasoningEffort: patchToWire(patch.ReasoningEffort), ReasoningSummary: patchToWire(patch.ReasoningSummary), CompactionPolicy: patchToWire(patch.CompactionPolicy), Extensions: patchToWire(patch.Extensions),
	}
}

func patchToWire[T any](patch Patch[T]) llm.Patch[T] {
	return llm.Patch[T]{Set: patch.Set, Clear: patch.Clear}
}

func settingsPatchFromWire(wire llm.SettingsPatchV1) (SettingsPatch, error) {
	patch := SettingsPatch{
		WebFetch: patchFromWire(wire.WebFetch), CodeExecution: patchFromWire(wire.CodeExecution), WebSearch: patchFromWire(wire.WebSearch), Model: patchFromWire(wire.Model), ServiceClass: patchFromWire(wire.ServiceClass), ServiceClassFallbacks: patchFromWire(wire.ServiceClassFallbacks), Portability: patchFromWire(wire.Portability),
		Instructions: patchFromWire(wire.Instructions), Tools: patchFromWire(wire.Tools), ToolPolicy: patchFromWire(wire.ToolPolicy), Output: patchFromWire(wire.Output),
		TopP: patchFromWire(wire.TopP), StopSequences: patchFromWire(wire.StopSequences), Seed: patchFromWire(wire.Seed),
		ReasoningMode: patchFromWire(wire.ReasoningMode), ReasoningTokenBudget: patchFromWire(wire.ReasoningTokenBudget),
		ReasoningEffort: patchFromWire(wire.ReasoningEffort), ReasoningSummary: patchFromWire(wire.ReasoningSummary), CompactionPolicy: patchFromWire(wire.CompactionPolicy), Extensions: patchFromWire(wire.Extensions),
	}
	patch.TemperatureDecimal.Clear = wire.Temperature.Clear
	if wire.Temperature.Set != nil {
		value, err := wire.Temperature.Set.Float64()
		if err != nil {
			return SettingsPatch{}, fmt.Errorf("temperature cannot be represented by provider state: %w", err)
		}
		patch.Temperature.Set = &value
		decimal := *wire.Temperature.Set
		patch.TemperatureDecimal.Set = &decimal
	}
	if err := patch.Validate(); err != nil {
		return SettingsPatch{}, err
	}
	return patch, nil
}

func patchFromWire[T any](patch llm.Patch[T]) Patch[T] {
	return Patch[T]{Set: patch.Set, Clear: patch.Clear}
}

// CheckpointBlobReader is the only byte-read capability exposed to durable
// materialization. Implementations must scope the resolver and store lookup
// by scopeID; callers never supply a raw locator.
type CheckpointBlobReader interface {
	Read(context.Context, string, CheckpointBlobReference) ([]byte, error)
}

// CheckpointBlobWriter persists immutable, scope-bound bytes before checkpoint
// publication. Retrying the same bytes and media type must return the same
// reference. Callers retain that reference across uncertain publication results.
type CheckpointBlobWriter interface {
	Write(context.Context, string, []byte, string) (CheckpointBlobReference, error)
}

// CheckpointStore is a complete portable checkpoint persistence capability.
type CheckpointStore interface {
	CheckpointRepository
	CheckpointBlobReader
	CheckpointBlobWriter
}

// BlobLocator resolves a durable ID to an object-store reference after applying
// an authorization/scope filter. Returning a locator for another scope is a
// fault and is rejected by ScopedBlobReader's metadata checks and the store's
// own tenant binding.
type BlobLocator func(context.Context, string, BlobID) (blob.Ref, error)

type ScopedBlobReader struct {
	Store    blob.Store
	Resolve  BlobLocator
	MaxBytes int64
	Now      func() time.Time
}

func (reader ScopedBlobReader) Read(ctx context.Context, scopeID string, reference CheckpointBlobReference) ([]byte, error) {
	if reader.Store == nil || reader.Resolve == nil {
		return nil, errors.New("checkpoint blob reader is not configured")
	}
	if strings.TrimSpace(scopeID) == "" {
		return nil, errors.New("checkpoint blob scope is required")
	}
	if err := reference.validate("read"); err != nil {
		return nil, err
	}
	if reader.MaxBytes > 0 && reference.ByteLength > reader.MaxBytes {
		return nil, fmt.Errorf("checkpoint blob exceeds reader byte limit: %w", ErrLimitExceeded)
	}
	ref, err := reader.Resolve(ctx, scopeID, reference.ID)
	if err != nil {
		return nil, fmt.Errorf("resolve checkpoint blob %s: %w", reference.ID, err)
	}
	if err := ref.Validate(reader.clock()); err != nil {
		return nil, fmt.Errorf("validate checkpoint blob %s locator: %w", reference.ID, err)
	}
	wantDigest := hex.EncodeToString(reference.Digest[:])
	if ref.Digest != wantDigest || ref.ByteLength != reference.ByteLength || ref.MediaType != reference.MediaType {
		return nil, fmt.Errorf("checkpoint blob %s metadata mismatch", reference.ID)
	}
	data, err := reader.Store.Get(ctx, scopeID, ref)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint blob %s: %w", reference.ID, err)
	}
	if int64(len(data)) != reference.ByteLength || sha256.Sum256(data) != reference.Digest {
		return nil, fmt.Errorf("checkpoint blob %s digest or length mismatch", reference.ID)
	}
	return append([]byte(nil), data...), nil
}

func (reader ScopedBlobReader) clock() time.Time {
	if reader.Now != nil {
		return reader.Now().UTC()
	}
	return time.Now().UTC()
}
