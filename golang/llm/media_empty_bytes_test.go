package llm

import (
	"encoding/json"
	"testing"
)

func TestInlineMediaRejectsEmptyBytesOnMarshalAndDecode(t *testing.T) {
	for name, part := range map[string]Part{
		"image":    ImagePart{Bytes: []byte{}, MediaType: "image/png"},
		"document": DocumentPart{Bytes: []byte{}, MediaType: "application/pdf"},
	} {
		if data, err := json.Marshal(Message{Actor: ActorHuman, Content: []Part{part}}); err == nil {
			t.Fatalf("%s with empty bytes marshalled to %s, want a validation error", name, data)
		}
	}
	for name, raw := range map[string]string{
		"image":    `{"kind":"message","actor":"human","content":[{"kind":"image","media_type":"image/png","bytes":""}]}`,
		"document": `{"kind":"message","actor":"human","content":[{"kind":"document","media_type":"application/pdf","bytes":""}]}`,
	} {
		if _, err := DecodeItems([]byte("[" + raw + "]")); err == nil {
			t.Fatalf("%s with empty bytes decoded, want a validation error", name)
		}
	}
	if _, err := json.Marshal(Message{Actor: ActorHuman, Content: []Part{ImagePart{Bytes: []byte{1}, MediaType: "image/png"}}}); err != nil {
		t.Fatalf("one-byte image rejected: %v", err)
	}
}
