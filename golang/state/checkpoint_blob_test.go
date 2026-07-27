package state

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/storage/blob"
)

func TestCheckpointBlobCodecRoundTripsEveryKind(t *testing.T) {
	codec := CheckpointBlobCodec{}
	items := []llm.Item{
		llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}},
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{"q":"x"}`)},
	}
	delta, err := codec.EncodeDelta(items)
	if err != nil {
		t.Fatal(err)
	}
	gotDelta, err := codec.DecodeDelta(delta)
	if err != nil || !reflect.DeepEqual(gotDelta, items) {
		t.Fatalf("delta round trip = %#v, %v; want %#v", gotDelta, err, items)
	}
	response, err := codec.EncodeResponse([]llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "world"}}}})
	if err != nil {
		t.Fatal(err)
	}
	gotResponse, err := codec.DecodeResponse(response)
	if err != nil || len(gotResponse) != 1 {
		t.Fatalf("response round trip = %#v, %v", gotResponse, err)
	}
	patch := SettingsPatch{Model: SetPatch("gpt-test"), ServiceClass: SetPatch(llm.ServiceClassPriority), Instructions: SetPatch([]llm.Instruction{{Kind: llm.InstructionKindParts, Level: llm.InstructionLevelApplication, Content: []llm.Part{llm.TextPart{Text: "be concise"}}}}), Tools: ClearPatch[[]llm.Tool](), Temperature: SetPatch(0.25)}
	encodedPatch, err := codec.EncodeSettingsPatch(patch)
	if err != nil {
		t.Fatal(err)
	}
	gotPatch, err := codec.DecodeSettingsPatch(encodedPatch)
	if err != nil {
		t.Fatalf("settings patch decode = %v", err)
	}
	// The wire contract now records the exact decimal alongside the legacy
	// provider-facing float projection. A patch authored with a float gains
	// that exact projection when decoded.
	patch.TemperatureDecimal = gotPatch.TemperatureDecimal
	if !reflect.DeepEqual(gotPatch, patch) {
		t.Fatalf("settings patch round trip = %#v, %v; want %#v", gotPatch, err, patch)
	}
	snapshot := CheckpointSnapshot{Items: items, Settings: ModelState{Model: "gpt-test", ServiceClass: llm.ServiceClassStandard, Portability: llm.PortabilityStrict}, Depth: 1, Lineage: []Handle{"root", "child"}}
	snapshot.Digest = snapshot.digest()
	encodedSnapshot, err := codec.EncodeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	gotSnapshot, err := codec.DecodeSnapshot(encodedSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotSnapshot.Items, snapshot.Items) || !reflect.DeepEqual(gotSnapshot.Settings, snapshot.Settings) || gotSnapshot.Depth != snapshot.Depth || !reflect.DeepEqual(gotSnapshot.Lineage, snapshot.Lineage) {
		t.Fatalf("snapshot round trip = %#v; want %#v", gotSnapshot, snapshot)
	}
	if string(encodedSnapshot) != string(mustCanonical(t, encodedSnapshot)) {
		t.Fatal("snapshot encoding is not canonical")
	}
}

func TestCheckpointBlobCodecPreservesExactTemperatureDecimal(t *testing.T) {
	codec := CheckpointBlobCodec{}
	decimal, err := llm.NewDecimalV1("0.123456789012345678")
	if err != nil {
		t.Fatal(err)
	}
	patch := SettingsPatch{TemperatureDecimal: SetPatch(decimal)}
	encoded, err := codec.EncodeSettingsPatch(patch)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"temperature":{"set":"0.123456789012345678"}`) {
		t.Fatalf("encoded patch rounded exact decimal: %s", encoded)
	}
	decoded, err := codec.DecodeSettingsPatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.TemperatureDecimal.Set == nil || decoded.TemperatureDecimal.Set.String() != decimal.String() {
		t.Fatalf("decoded exact decimal = %#v", decoded.TemperatureDecimal.Set)
	}
	base := RootModelState("gpt-test")
	materialized, err := ApplySettingsPatch(base, decoded)
	if err != nil {
		t.Fatal(err)
	}
	if materialized.TemperatureDecimal == nil || materialized.TemperatureDecimal.String() != decimal.String() {
		t.Fatalf("materialized exact decimal = %#v", materialized.TemperatureDecimal)
	}
	if materialized.Temperature == nil {
		t.Fatal("materialized provider temperature projection is missing")
	}
}

func TestCheckpointBlobCodecRejectsEmptyReasoningPatchValues(t *testing.T) {
	codec := CheckpointBlobCodec{}
	emptyEffort := llm.ReasoningEffort("")
	emptySummary := llm.ReasoningSummary("")
	for name, patch := range map[string]SettingsPatch{
		"reasoning effort":  {ReasoningEffort: SetPatch(emptyEffort)},
		"reasoning summary": {ReasoningSummary: SetPatch(emptySummary)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := codec.EncodeSettingsPatch(patch); err == nil {
				t.Fatal("EncodeSettingsPatch accepted an explicit empty reasoning value")
			}
		})
	}
}

func TestCheckpointBlobCodecRejectsVersionKindAndEnvelopeTampering(t *testing.T) {
	codec := CheckpointBlobCodec{}
	encoded, err := codec.EncodeDelta(nil)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data string
		want string
	}{
		{"wrong version", `{"kind":"delta","payload":[],"version":"checkpoint-blob/v0"}`, "unsupported version"},
		{"wrong kind", `{"kind":"response","payload":[],"version":"checkpoint-blob/v1"}`, "kind mismatch"},
		{"unknown field", `{"kind":"delta","payload":[],"version":"checkpoint-blob/v1","extra":true}`, "unknown envelope field"},
		{"duplicate field", `{"kind":"delta","payload":[],"kind":"delta","version":"checkpoint-blob/v1"}`, "duplicate"},
		{"trailing value", `{"kind":"delta","payload":[],"version":"checkpoint-blob/v1"} null`, "trailing"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := codec.DecodeDelta([]byte(test.data)); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("DecodeDelta() = %v, want error containing %q", err, test.want)
			}
		})
	}
	if _, err := codec.DecodeDelta(append(encoded, byte('x'))); err == nil {
		t.Fatal("DecodeDelta accepted a non-JSON suffix")
	}
}

func FuzzCheckpointBlobCodecDecodeDeltaNeverPanics(f *testing.F) {
	codec := CheckpointBlobCodec{}
	seed, err := codec.EncodeDelta([]llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "seed"}}}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte(`{"version":"checkpoint-blob/v1","kind":"delta","payload":[]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = codec.DecodeDelta(data)
	})
}

func TestScopedBlobReaderBindsScopeAndReferenceMetadata(t *testing.T) {
	now := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	data := []byte("checkpoint")
	store := &testCheckpointBlobStore{tenant: "scope-a", data: append([]byte(nil), data...), ref: blob.Ref{Store: "test", Locator: "scope-a/blob-1", Digest: blob.Digest(data), ByteLength: int64(len(data)), MediaType: "application/json", ExpiresAt: now.Add(time.Hour)}}
	ref := store.ref
	digest := sha256.Sum256(data)
	reader := ScopedBlobReader{Store: store, Now: func() time.Time { return now }, Resolve: func(_ context.Context, scope string, id BlobID) (blob.Ref, error) {
		if scope != "scope-a" || id != "blob-1" {
			return blob.Ref{}, blob.ErrTenantMismatch
		}
		return ref, nil
	}}
	checkpointRef := CheckpointBlobReference{ID: "blob-1", Digest: digest, ByteLength: int64(len(data)), MediaType: "application/json"}
	got, err := reader.Read(context.Background(), "scope-a", checkpointRef)
	if err != nil || string(got) != string(data) {
		t.Fatalf("Read() = %q, %v; want %q", got, err, data)
	}
	wrong := checkpointRef
	wrong.ByteLength++
	if _, err := reader.Read(context.Background(), "scope-a", wrong); err == nil {
		t.Fatal("Read accepted mismatched reference metadata")
	}
	if _, err := reader.Read(context.Background(), "scope-b", checkpointRef); !errors.Is(err, blob.ErrTenantMismatch) {
		t.Fatalf("cross-scope Read() = %v, want tenant mismatch", err)
	}
}

func TestScopedBlobReaderRejectsCorruptedPayload(t *testing.T) {
	now := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	written := []byte("checkpoint")
	corrupted := []byte("checkpoinT")
	if len(written) != len(corrupted) {
		t.Fatal("corruption fixture must preserve the published byte length")
	}
	digest := sha256.Sum256(written)
	store := &testCheckpointBlobStore{
		tenant: "scope-a",
		data:   corrupted,
		ref: blob.Ref{
			Store: "test", Locator: "scope-a/blob-corrupt", Digest: blob.Digest(written),
			ByteLength: int64(len(written)), MediaType: "application/json", ExpiresAt: now.Add(time.Hour),
		},
	}
	reader := ScopedBlobReader{
		Store: store,
		Now:   func() time.Time { return now },
		Resolve: func(_ context.Context, scope string, id BlobID) (blob.Ref, error) {
			if scope != "scope-a" || id != "blob-corrupt" {
				return blob.Ref{}, blob.ErrTenantMismatch
			}
			return store.ref, nil
		},
	}
	reference := CheckpointBlobReference{
		ID: "blob-corrupt", Digest: digest, ByteLength: int64(len(written)), MediaType: "application/json",
	}
	if _, err := reader.Read(context.Background(), "scope-a", reference); err == nil || !strings.Contains(err.Error(), "digest or length mismatch") {
		t.Fatalf("Read() = %v, want corrupted-payload digest error", err)
	}
}

type testCheckpointBlobStore struct {
	tenant string
	data   []byte
	ref    blob.Ref
}

func (store *testCheckpointBlobStore) Put(context.Context, blob.PutRequest) (blob.Ref, error) {
	return blob.Ref{}, errors.New("test blob store is read-only")
}

func (store *testCheckpointBlobStore) Get(ctx context.Context, tenant string, ref blob.Ref) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tenant != store.tenant || ref != store.ref {
		return nil, blob.ErrTenantMismatch
	}
	return append([]byte(nil), store.data...), nil
}

func mustCanonical(t *testing.T, data []byte) []byte {
	t.Helper()
	canonical, err := llm.CanonicalJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}
