package llm_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestRequestRoundTripsEveryItemAndPartVariant(t *testing.T) {
	imageBlob := &llm.BlobRef{
		Digest:     "sha256:image",
		ByteLength: 3,
		MediaType:  "image/png",
		Locator:    "s3://bucket/image",
	}
	documentBlob := &llm.BlobRef{
		Digest:     "sha256:document",
		ByteLength: 4,
		MediaType:  "application/pdf",
		Locator:    "s3://bucket/document",
	}
	content := []llm.Part{
		llm.TextPart{Text: "plain text"},
		llm.ImagePart{URL: "https://example.test/image", MediaType: "image/png", Detail: "high"},
		llm.ImagePart{Bytes: []byte{1, 2, 3}, MediaType: "image/png"},
		llm.ImagePart{Blob: imageBlob, MediaType: "image/png"},
		llm.DocumentPart{URL: "https://example.test/document", MediaType: "application/pdf", Title: "invoice"},
		llm.DocumentPart{Bytes: []byte{4, 5, 6}, MediaType: "application/pdf", Title: "bytes"},
		llm.DocumentPart{Blob: documentBlob, MediaType: "application/pdf", Title: "stored"},
		llm.JSONPart{Value: json.RawMessage(`{"amount":1200,"currency":"USD"}`)},
		llm.RefusalPart{Text: "cannot comply", ProviderCode: "policy"},
		llm.ProviderStatePart{
			Provider:       "openai",
			EndpointFamily: "responses",
			MediaType:      "application/octet-stream",
			Opaque:         []byte{7, 8, 9},
		},
	}
	request := llm.Request{
		OperationKey: "all-wire-variants",
		Model:        "test-model",
		Input: []llm.Item{
			llm.Message{Actor: llm.ActorHuman, Content: content},
			llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"id":1}`)},
			llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "found"}}, IsError: true},
			llm.ProviderState{
				Provider:       "anthropic",
				EndpointFamily: "messages",
				MediaType:      "application/octet-stream",
				Opaque:         []byte{10, 11},
			},
			llm.Reference{
				URI:      "https://example.test/source",
				Metadata: map[string]json.RawMessage{"source": json.RawMessage(`"erp"`)},
			},
		},
	}

	data, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal all wire variants: %v", err)
	}
	var decoded llm.Request
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal all wire variants: %v\n%s", err, data)
	}
	if len(decoded.Input) != len(request.Input) {
		t.Fatalf("decoded item count = %d, want %d", len(decoded.Input), len(request.Input))
	}
	for index, want := range []llm.ItemKind{
		llm.ItemKindMessage,
		llm.ItemKindToolCall,
		llm.ItemKindToolResult,
		llm.ItemKindProviderState,
		llm.ItemKindReference,
	} {
		if got := decoded.Input[index].ItemKind(); got != want {
			t.Fatalf("decoded item[%d] kind = %q, want %q", index, got, want)
		}
	}
	message, ok := decoded.Input[0].(llm.Message)
	if !ok {
		t.Fatalf("decoded input[0] = %T, want llm.Message", decoded.Input[0])
	}
	if len(message.Content) != len(content) {
		t.Fatalf("decoded content count = %d, want %d", len(message.Content), len(content))
	}
	for index, want := range []llm.PartKind{
		llm.PartKindText,
		llm.PartKindImage,
		llm.PartKindImage,
		llm.PartKindImage,
		llm.PartKindDocument,
		llm.PartKindDocument,
		llm.PartKindDocument,
		llm.PartKindJSON,
		llm.PartKindRefusal,
		llm.PartKindProviderState,
	} {
		if got := message.Content[index].PartKind(); got != want {
			t.Fatalf("decoded content[%d] kind = %q, want %q", index, got, want)
		}
	}
	if got := message.Content[1].(llm.ImagePart); got.URL != "https://example.test/image" || got.Detail != "high" {
		t.Fatalf("decoded URL image = %#v", got)
	}
	if got := message.Content[2].(llm.ImagePart); !reflect.DeepEqual(got.Bytes, []byte{1, 2, 3}) {
		t.Fatalf("decoded byte image = %#v", got)
	}
	if got := message.Content[3].(llm.ImagePart); got.Blob == nil || got.Blob.Locator != imageBlob.Locator {
		t.Fatalf("decoded blob image = %#v", got)
	}
	if got := message.Content[6].(llm.DocumentPart); got.Blob == nil || got.Title != "stored" {
		t.Fatalf("decoded blob document = %#v", got)
	}
	if got := message.Content[7].(llm.JSONPart); string(got.Value) != string(content[7].(llm.JSONPart).Value) {
		t.Fatalf("decoded JSON part = %s", got.Value)
	}
	if got := message.Content[8].(llm.RefusalPart); got.ProviderCode != "policy" {
		t.Fatalf("decoded refusal part = %#v", got)
	}
	if got := message.Content[9].(llm.ProviderStatePart); !reflect.DeepEqual(got.Opaque, []byte{7, 8, 9}) {
		t.Fatalf("decoded provider state part = %#v", got)
	}
}

func TestItemAndPartJSONRejectUnsafeAndAmbiguousMedia(t *testing.T) {
	imageBlob := &llm.BlobRef{Digest: "sha256:image", ByteLength: 1, MediaType: "image/png", Locator: "s3://bucket/image"}
	documentBlob := &llm.BlobRef{Digest: "sha256:document", ByteLength: 1, MediaType: "application/pdf", Locator: "s3://bucket/document"}
	cases := []struct {
		name  string
		value any
	}{
		{name: "image has no source", value: llm.ImagePart{MediaType: "image/png"}},
		{name: "image has multiple sources", value: llm.ImagePart{URL: "https://example.test/image", Bytes: []byte{1}, MediaType: "image/png"}},
		{name: "image rejects javascript URL", value: llm.ImagePart{URL: "javascript:alert(1)", MediaType: "image/png"}},
		{name: "image blob media mismatch", value: llm.ImagePart{Blob: documentBlob, MediaType: "image/png"}},
		{name: "document blob media mismatch", value: llm.DocumentPart{Blob: imageBlob, MediaType: "application/pdf"}},
		{name: "reference rejects data URL", value: llm.Reference{URI: "data:text/plain,secret"}},
		{name: "reference rejects empty metadata key", value: llm.Reference{URI: "https://example.test/source", Metadata: map[string]json.RawMessage{"": json.RawMessage(`true`)}}},
		{name: "tool call rejects invalid arguments", value: llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"unterminated"`)}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := json.Marshal(test.value); err == nil {
				t.Fatalf("json.Marshal(%#v) unexpectedly succeeded", test.value)
			}
		})
	}
}
