package state

import (
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestCheckpointGraphGetDetachesNestedItemData(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	original := Checkpoint{
		Handle:       "root",
		Tenant:       "tenant-a",
		OperationKey: "operation-root",
		Delta: []llm.Item{
			llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
				llm.ImagePart{Bytes: []byte{1, 2, 3}, MediaType: "image/png"},
			}},
			llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"key":"value"}`)},
			llm.ProviderState{Provider: "test", EndpointFamily: "chat", MediaType: "application/json", Opaque: []byte{4, 5, 6}},
			llm.Reference{URI: "https://example.com", Metadata: map[string]json.RawMessage{"key": json.RawMessage(`{"value":1}`)}},
		},
	}
	if err := graph.PutRoot(original); err != nil {
		t.Fatalf("put root: %v", err)
	}

	got, err := graph.Get(original.Handle)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	message := got.Delta[0].(llm.Message)
	image := message.Content[0].(llm.ImagePart)
	image.Bytes[0] = 99
	message.Content[0] = image
	got.Delta[0] = message
	call := got.Delta[1].(llm.ToolCall)
	call.Arguments[0] = '['
	got.Delta[1] = call
	providerState := got.Delta[2].(llm.ProviderState)
	providerState.Opaque[0] = 98
	got.Delta[2] = providerState
	got.Delta[3].(llm.Reference).Metadata["key"][0] = '['

	again, err := graph.Get(original.Handle)
	if err != nil {
		t.Fatalf("get root after mutation: %v", err)
	}
	if got := got.Delta[0].(llm.Message).Content[0].(llm.ImagePart).Bytes[0]; got != 99 {
		t.Fatalf("mutated clone image bytes were not retained locally: %d", got)
	}
	if got := again.Delta[0].(llm.Message).Content[0].(llm.ImagePart).Bytes[0]; got != 1 {
		t.Fatalf("graph image bytes were aliased: %d", got)
	}
	if got := again.Delta[1].(llm.ToolCall).Arguments[0]; got != '{' {
		t.Fatalf("graph tool arguments were aliased: %q", got)
	}
	if got := again.Delta[2].(llm.ProviderState).Opaque[0]; got != 4 {
		t.Fatalf("graph provider state was aliased: %d", got)
	}
	if got := again.Delta[3].(llm.Reference).Metadata["key"][0]; got != '{' {
		t.Fatalf("graph reference metadata was aliased: %q", got)
	}
}
