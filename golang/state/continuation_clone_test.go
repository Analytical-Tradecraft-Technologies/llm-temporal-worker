package state

import (
	"encoding/json"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestContinuationClonePreservesEmptyNonNilBytes(t *testing.T) {
	original := Continuation{Transcript: []llm.Item{
		llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.ImagePart{Bytes: []byte{}, MediaType: "image/png"},
			llm.DocumentPart{Bytes: []byte{}, MediaType: "application/pdf"},
		}},
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage{}},
	}}
	clone := original.Clone()
	parts := clone.Transcript[0].(llm.Message).Content
	if parts[0].(llm.ImagePart).Bytes == nil || parts[1].(llm.DocumentPart).Bytes == nil {
		t.Fatalf("clone dropped non-nil empty media bytes: %#v", parts)
	}
	if clone.Transcript[1].(llm.ToolCall).Arguments == nil {
		t.Fatal("clone dropped non-nil empty tool arguments")
	}
	nilClone := Continuation{Transcript: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.ImagePart{URL: "https://example.test/a.png", MediaType: "image/png"}}}}}.Clone()
	if nilClone.Transcript[0].(llm.Message).Content[0].(llm.ImagePart).Bytes != nil {
		t.Fatal("clone invented media bytes")
	}
}
