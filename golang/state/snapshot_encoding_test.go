package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// encodingTestItems covers every value type in the closed Item union, with
// strings json.Marshal escapes and raw JSON it compacts, plus pointer, nil
// and failing items that keep the json.Marshal path.
func encodingTestItems() []llm.Item {
	text := "<b>&amp;</b> \x01    café \xff \"quoted\" \\ / \t"
	parts := []llm.Part{
		llm.TextPart{Text: text},
		llm.ImagePart{URL: "https://example.test/a.png?x=<1>&y=2", MediaType: "image/png", Detail: "high"},
		llm.ImagePart{Bytes: []byte{0, 1, 2, 0xff}, MediaType: "image/png"},
		llm.DocumentPart{URL: "https://example.test/d.pdf", MediaType: "application/pdf", Title: text},
		llm.JSONPart{Value: json.RawMessage(`{ "z" : [ 1 , 2.50 ], "a" : "<&>" }`)},
		llm.RefusalPart{Text: text, ProviderCode: "x<y"},
		llm.ProviderStatePart{Provider: "p", EndpointFamily: "f", MediaType: "m", Opaque: []byte("opaque<>")},
		&llm.TextPart{Text: "pointer part"},
	}
	message := llm.Message{Actor: llm.ActorModel, Content: parts}
	return []llm.Item{
		llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}},
		message,
		&message,
		llm.Message{Actor: llm.ActorHuman},
		llm.ToolCall{ID: "call<1>", Name: "lookup", Arguments: json.RawMessage(`{ "b" : "<x>", "a" : [ 1 , { "d" : null , "c" : true } ] }`)},
		&llm.ToolCall{ID: "call-2", Name: "lookup", Arguments: json.RawMessage(`[]`)},
		llm.ToolResult{CallID: "call<1>", Name: "lookup", Content: parts, IsError: true},
		llm.ToolResult{CallID: "call-2", Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(` [ 1 ] `)}}},
		llm.ProviderState{Provider: "p", EndpointFamily: "f", MediaType: "m", Opaque: []byte{0, 0xfe, '<'}},
		llm.ProviderState{Provider: "p", EndpointFamily: "f", MediaType: "m"},
		llm.Reference{URI: "https://example.test/source?a=<b>&c=d"},
		llm.Reference{URI: "https://example.test/meta", Metadata: map[string]json.RawMessage{"z": json.RawMessage(` { "y" : 1 , "x" : "<" } `), "a": json.RawMessage(`2`)}},
		&llm.Reference{URI: "https://example.test/pointer"},
	}
}

func legacyItemsJSON(items []llm.Item) ([]byte, error) { return json.Marshal(items) }

func TestAppendItemsJSONMatchesJSONMarshal(t *testing.T) {
	items := encodingTestItems()
	cases := map[string][]llm.Item{
		"nil":            nil,
		"empty":          {},
		"all":            items,
		"nil item":       {items[0], nil},
		"typed nil item": {(*llm.Message)(nil)},
		"invalid actor":  {items[0], llm.Message{Actor: "system"}},
		"invalid call":   {llm.ToolCall{ID: "a", Name: "lookup", Arguments: []byte("{")}},
		"invalid result": {llm.ToolResult{}},
		"invalid state":  {llm.ProviderState{}},
		"invalid ref":    {llm.Reference{}},
		"invalid part":   {llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.ImagePart{}}}},
		"malformed":      {items[0], malformedItem{}},
	}
	for _, item := range items {
		cases[fmt.Sprintf("%T %d", item, len(cases))] = []llm.Item{item}
	}
	for name, items := range cases {
		t.Run(name, func(t *testing.T) {
			want, wantErr := legacyItemsJSON(items)
			got, gotErr := appendItemsJSON([]byte("prefix"), items)
			if (gotErr == nil) != (wantErr == nil) || (gotErr != nil && gotErr.Error() != wantErr.Error()) {
				t.Fatalf("error = %v, want %v", gotErr, wantErr)
			}
			if wantErr == nil && !bytes.Equal(got, append([]byte("prefix"), want...)) {
				t.Fatalf("encoding\n got %s\nwant prefix%s", got, want)
			}
			if wantErr != nil && got != nil {
				t.Fatalf("failed encoding returned %q", got)
			}
		})
	}
}

func legacySnapshotDigest(snapshot CheckpointSnapshot) [32]byte {
	data, _ := json.Marshal(struct {
		Schema   string
		Items    []llm.Item
		Settings ModelState
		Depth    int32
		Lineage  []Handle
	}{checkpointSchemaVersion, snapshot.Items, snapshot.Settings, snapshot.Depth, snapshot.Lineage})
	return sha256.Sum256(data)
}

// The snapshot digest is a persisted identity: writing it without
// json.Marshal must hash exactly the same bytes, and the empty input when
// any field fails to encode.
func TestSnapshotDigestMatchesLegacyEncoding(t *testing.T) {
	temperature, seed := 0.5, int64(7)
	decimal := llm.DecimalV1("0.50")
	settings := RootModelState("model<1>")
	settings.Instructions = []llm.Instruction{{Text: "be <brief> & kind  "}, {Level: llm.InstructionLevelPolicy, Content: []llm.Part{llm.TextPart{Text: "policy"}}}}
	settings.Temperature, settings.TemperatureDecimal, settings.TopP, settings.Seed = &temperature, &decimal, &decimal, &seed
	settings.StopSequences = []string{"<end>"}
	settings.CompactionPolicy = json.RawMessage(` { "b" : 1 , "a" : [ ] } `)
	settings.Extensions = map[string]json.RawMessage{"x": json.RawMessage(` "<y>" `)}
	badSettings := settings
	badSettings.Instructions = []llm.Instruction{{Kind: "bogus"}}
	items := encodingTestItems()
	for name, snapshot := range map[string]CheckpointSnapshot{
		"zero":           {},
		"empty":          {Items: []llm.Item{}, Lineage: []Handle{}, Settings: RootModelState("m")},
		"full":           {Items: items, Settings: settings, Depth: 42, Lineage: []Handle{"root", "child<1>"}},
		"negative depth": {Items: items[:2], Settings: settings, Depth: -1},
		"nil item":       {Items: []llm.Item{items[0], nil}, Settings: settings},
		"failing item":   {Items: []llm.Item{items[0], llm.Message{Actor: "system"}}, Settings: settings, Lineage: []Handle{"a"}},
		"malformed item": {Items: []llm.Item{malformedItem{}}, Settings: settings},
		"bad settings":   {Items: items, Settings: badSettings},
	} {
		t.Run(name, func(t *testing.T) {
			got, want := snapshot.digest(), legacySnapshotDigest(snapshot)
			if got != want {
				t.Fatalf("digest %x, want %x", got, want)
			}
			if encodes := name == "zero" || name == "empty" || name == "full" || name == "negative depth" || name == "nil item"; encodes == (got == sha256.Sum256(nil)) {
				t.Fatalf("digest of the empty input is %t", got == sha256.Sum256(nil))
			}
		})
	}
}

// legacyEncode is CheckpointBlobCodec.encode before encodeEnvelope: the
// envelope is marshalled and canonicalized again around the payload.
func legacyEncode(codec CheckpointBlobCodec, kind CheckpointBlobKind, payload any) ([]byte, error) {
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

func nestedArray(depth int) json.RawMessage {
	return json.RawMessage(strings.Repeat("[", depth) + strings.Repeat("]", depth))
}

func TestCheckpointBlobEnvelopeMatchesLegacyEncode(t *testing.T) {
	items := encodingTestItems()[:2]
	snapshotItems, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := checkpointSnapshotPayload{Items: snapshotItems, Settings: settingsPatchToWire(settingsPatchFromModel(RootModelState("m"))), Depth: 3, Lineage: []string{"a", "b<c>"}}
	payloads := map[string]any{
		"items":    items,
		"no items": []llm.Item{},
		"snapshot": snapshot,
		"settings": settingsPatchToWire(SettingsPatch{Model: SetPatch("m<1>")}),
		"string":   "<&>   \xff",
		"number":   json.Number("1.500"),
		"raw":      json.RawMessage(` { "b" : [ 1 , 2 ] , "a" : { "d" : "<" , "c" : null } } `),
		"invalid":  json.RawMessage(`{"a":1,"a":2}`),
		"failing":  []llm.Item{llm.Message{Actor: "system"}},
	}
	for depth := 1; depth <= 6; depth++ {
		payloads[fmt.Sprintf("depth %d", depth)] = nestedArray(depth)
	}
	type limits struct{ bytes, depth int }
	for name, payload := range payloads {
		for _, kind := range []CheckpointBlobKind{CheckpointDeltaBlob, CheckpointSnapshotBlob, `odd<kind>"`} {
			whole, _ := legacyEncode(CheckpointBlobCodec{}, kind, payload)
			if encodedPayload, err := json.Marshal(payload); err == nil {
				if _, fast := (CheckpointBlobCodec{}).withDefaults().encodeEnvelope(kind, encodedPayload); fast != (whole != nil) {
					t.Fatalf("%s %s: direct envelope %t for legacy result %s", name, kind, fast, whole)
				}
			}
			cases := []limits{{0, 0}, {0, 1}, {0, 2}, {0, 4}, {0, 5}, {0, 6}, {0, 7}}
			if whole != nil {
				cases = append(cases, limits{len(whole), 0}, limits{len(whole) - 1, 0}, limits{len(whole) - 40, 0})
			}
			for _, limit := range cases {
				codec := CheckpointBlobCodec{MaxBytes: limit.bytes, MaxDepth: limit.depth}
				want, wantErr := legacyEncode(codec, kind, payload)
				got, gotErr := codec.encode(kind, payload)
				if (gotErr == nil) != (wantErr == nil) || (gotErr != nil && (gotErr.Error() != wantErr.Error() || errors.Is(gotErr, ErrLimitExceeded) != errors.Is(wantErr, ErrLimitExceeded))) {
					t.Fatalf("%s %s %+v: error = %v, want %v", name, kind, limit, gotErr, wantErr)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("%s %s %+v: encoding\n got %s\nwant %s", name, kind, limit, got, want)
				}
			}
		}
	}
}

// BenchmarkCheckpointSnapshotCodec encodes and decodes a snapshot of about
// 1 MB, the parent size in issue #1112.
func BenchmarkCheckpointSnapshotCodec(b *testing.B) {
	snapshot := CheckpointSnapshot{Settings: RootModelState("m"), Depth: 1, Lineage: []Handle{"root"}}
	for i := 0; i < 10; i++ {
		snapshot.Items = append(snapshot.Items, llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: strings.Repeat(fmt.Sprintf("parent %d ", i), 100<<10/9)}}})
	}
	snapshot.Digest = snapshot.digest()
	codec := CheckpointBlobCodec{MaxBytes: 4 << 20}
	encoded, err := codec.EncodeSnapshot(snapshot)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := codec.EncodeSnapshot(snapshot); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := codec.DecodeSnapshot(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}
