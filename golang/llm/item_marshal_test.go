package llm

import (
	"bytes"
	"encoding/json"
	"testing"
)

// legacyMarshalMessage and legacyMarshalToolResult are the map-based
// encodings Message and ToolResult used before they wrote their parts
// directly. Stored transcripts, snapshot digests and request identities
// depend on the encoding staying byte-identical.
func legacyMarshalMessage(message Message) ([]byte, error) {
	content := message.Content
	if content == nil {
		content = []Part{}
	}
	return json.Marshal(map[string]any{
		"kind":    ItemKindMessage,
		"actor":   message.Actor,
		"content": content,
	})
}

func legacyMarshalToolResult(result ToolResult) ([]byte, error) {
	content := result.Content
	if content == nil {
		content = []Part{}
	}
	fields := map[string]any{
		"kind":     ItemKindToolResult,
		"call_id":  result.CallID,
		"content":  content,
		"is_error": result.IsError,
	}
	if result.Name != "" {
		fields["name"] = result.Name
	}
	return json.Marshal(fields)
}

func TestItemEncodingMatchesLegacyMapEncoding(t *testing.T) {
	blob := &BlobRef{Digest: "sha256:a", ByteLength: 3, MediaType: "image/png", Locator: "s3://bucket/<a>&b"}
	texts := []string{
		"",
		"plain text",
		`quote " backslash \ slash /`,
		"html <script>&amp;</script>",
		"control \x00\x01\t\n\r\x1f del \x7f",
		"line separators    ",
		"non-ASCII é 日本 🙂",
		"invalid UTF-8 \xff\xfe end",
	}
	var parts []Part
	for _, text := range texts {
		parts = append(parts, TextPart{Text: text}, RefusalPart{Text: text, ProviderCode: text})
	}
	textPart := TextPart{Text: "pointer <part>"}
	parts = append(parts,
		ImagePart{URL: "https://example.test/i?a=1&b=<2>", MediaType: "image/png", Detail: "high"},
		ImagePart{Bytes: []byte{0, 1, 2}, MediaType: "image/png"},
		ImagePart{Blob: blob, MediaType: "image/png"},
		DocumentPart{Bytes: []byte{3, 4}, MediaType: "application/pdf", Title: "t <1>"},
		JSONPart{Value: json.RawMessage(" {\"b\" : [1, 2], \"a\":\"<x>\"} ")},
		ProviderStatePart{Provider: "p", EndpointFamily: "f", MediaType: "m", Opaque: []byte{9}},
		ProviderStatePart{Provider: "p", EndpointFamily: "f", MediaType: "m"},
		&textPart,
		(*TextPart)(nil),
		nil,
	)

	contents := [][]Part{nil, {}, parts, {TextPart{Text: texts[3]}}}
	for index, content := range contents {
		for _, actor := range []Actor{ActorHuman, ActorModel} {
			message := Message{Actor: actor, Content: content}
			assertSameEncoding(t, "message", index, message, message.MarshalJSON, func() ([]byte, error) {
				return legacyMarshalMessage(message)
			})
		}
		for _, name := range []string{"", "lookup", texts[3], texts[7]} {
			for _, isError := range []bool{false, true} {
				result := ToolResult{CallID: texts[3], Name: name, Content: content, IsError: isError}
				assertSameEncoding(t, "tool result", index, result, result.MarshalJSON, func() ([]byte, error) {
					return legacyMarshalToolResult(result)
				})
			}
		}
	}
	for _, text := range texts {
		part := TextPart{Text: text}
		got, err := part.MarshalJSON()
		if err != nil {
			t.Fatalf("text part %q: %v", text, err)
		}
		want, err := json.Marshal(map[string]any{"kind": PartKindText, "text": text})
		if err != nil {
			t.Fatalf("legacy text part %q: %v", text, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("text part %q encoding = %s, want %s", text, got, want)
		}
	}
}

// malformedPart embeds a built-in part but returns invalid JSON, which only
// json.Marshal's scan of the encoding detects.
type malformedPart struct{ TextPart }

func (malformedPart) MarshalJSON() ([]byte, error) { return []byte("{"), nil }

func TestItemEncodingErrorsMatchLegacyMapEncoding(t *testing.T) {
	invalid := []Part{TextPart{Text: "ok"}, ImagePart{MediaType: "image/png"}}
	pointerInvalid := []Part{&ImagePart{MediaType: "image/png"}}
	malformed := []Part{malformedPart{}}
	for index, content := range [][]Part{invalid, pointerInvalid, malformed, {JSONPart{Value: json.RawMessage("{")}}} {
		message := Message{Actor: ActorHuman, Content: content}
		_, gotErr := message.MarshalJSON()
		_, wantErr := legacyMarshalMessage(message)
		if gotErr == nil || wantErr == nil || gotErr.Error() != wantErr.Error() {
			t.Fatalf("message content %d error = %v, want %v", index, gotErr, wantErr)
		}
		result := ToolResult{CallID: "call", Content: content}
		_, gotErr = result.MarshalJSON()
		_, wantErr = legacyMarshalToolResult(result)
		if gotErr == nil || wantErr == nil || gotErr.Error() != wantErr.Error() {
			t.Fatalf("tool result content %d error = %v, want %v", index, gotErr, wantErr)
		}
	}
}

func FuzzItemEncodingMatchesLegacyMapEncoding(f *testing.F) {
	f.Add("plain", "call", "lookup")
	f.Add("<html> &  ", "\x00", "\xff")
	f.Fuzz(func(t *testing.T, text, callID, name string) {
		content := []Part{TextPart{Text: text}, RefusalPart{Text: name, ProviderCode: text}}
		message := Message{Actor: ActorModel, Content: content}
		assertSameEncoding(t, "message", 0, message, message.MarshalJSON, func() ([]byte, error) {
			return legacyMarshalMessage(message)
		})
		if callID == "" {
			return
		}
		result := ToolResult{CallID: callID, Name: name, Content: content, IsError: len(text)%2 == 0}
		assertSameEncoding(t, "tool result", 0, result, result.MarshalJSON, func() ([]byte, error) {
			return legacyMarshalToolResult(result)
		})
	})
}

func assertSameEncoding(t *testing.T, label string, index int, item Item, marshal, legacy func() ([]byte, error)) {
	t.Helper()
	got, err := marshal()
	if err != nil {
		t.Fatalf("%s content %d: %v", label, index, err)
	}
	want, err := legacy()
	if err != nil {
		t.Fatalf("legacy %s content %d: %v", label, index, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s content %d encoding:\n got %s\nwant %s", label, index, got, want)
	}
	enclosed, err := json.Marshal([]Item{item})
	if err != nil {
		t.Fatalf("enclosed %s content %d: %v", label, index, err)
	}
	if want := append(append([]byte{'['}, want...), ']'); !bytes.Equal(enclosed, want) {
		t.Fatalf("enclosed %s content %d encoding:\n got %s\nwant %s", label, index, enclosed, want)
	}
}
