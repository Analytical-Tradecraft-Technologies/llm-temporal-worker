package state

import (
	"encoding/json"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"reflect"
	"testing"
)

func TestPointerToolFrontiers(t *testing.T) {
	call := &llm.ToolCall{ID: "a", Name: "lookup", Arguments: []byte("{}")}
	result := &llm.ToolResult{CallID: "a"}
	runTranscriptPendingCases(t, []transcriptPendingCase{
		{"open", []llm.Item{call}, []string{"a"}, false},
		{"resolved", []llm.Item{call, result}, []string{}, false},
		{"orphan", []llm.Item{result}, nil, true},
		{"duplicate", []llm.Item{call, call}, nil, true},
		{"human interrupts", []llm.Item{call, &llm.Message{Actor: llm.ActorHuman}}, nil, true},
		{"typed nil call", []llm.Item{(*llm.ToolCall)(nil)}, nil, true},
		{"typed nil result", []llm.Item{(*llm.ToolResult)(nil)}, nil, true},
		{"typed nil message", []llm.Item{(*llm.Message)(nil)}, nil, true},
		{"typed nil state", []llm.Item{(*llm.ProviderState)(nil)}, nil, true},
		{"typed nil reference", []llm.Item{(*llm.Reference)(nil)}, nil, true},
	})
}

func TestCheckpointCopiesPointerToolItems(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	root := rootCheckpoint("pointer-root", "tenant-a", "pointer-op")
	call := &llm.ToolCall{ID: "a", Name: "lookup", Arguments: []byte("{}")}
	root.Output = []llm.Item{call}
	mustPutRoot(t, graph, root)
	call.ID = "mutated"
	call.Arguments[0] = '['
	got := mustMaterialize(t, graph, "tenant-a", root.Handle)
	if !reflect.DeepEqual(got.PendingToolCalls, []string{"a"}) {
		t.Fatalf("pending=%v", got.PendingToolCalls)
	}
	stored, ok := got.Items[len(got.Items)-1].(llm.ToolCall)
	if !ok || string(stored.Arguments) != "{}" {
		t.Fatalf("stored call=%#v", got.Items)
	}
}

// malformedItem embeds a built-in item but returns invalid JSON, which only
// json.Marshal's scan of the encoding detects.
type malformedItem struct{ llm.Message }

func (malformedItem) MarshalJSON() ([]byte, error) { return []byte("{"), nil }

// checkItemEncoding skips json.Marshal's re-scan of the encoding, so it must
// still accept and reject exactly what json.Marshal does, with the same error.
func TestCheckItemEncodingMatchesJSONMarshal(t *testing.T) {
	for _, item := range []llm.Item{
		llm.HumanText("<ok>"),
		llm.Message{Actor: "system"},
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.ImagePart{MediaType: "image/png"}}},
		lookupCall("a", "{}"),
		lookupCall("a", "{"),
		llm.ToolResult{CallID: "a", Content: []llm.Part{llm.JSONPart{Value: []byte("[1]")}}},
		llm.ToolResult{},
		llm.ProviderState{Provider: "p", EndpointFamily: "f", MediaType: "m"},
		llm.ProviderState{},
		llm.Reference{URI: "https://example.test/source"},
		llm.Reference{},
		malformedItem{},
	} {
		_, want := json.Marshal(item)
		got := checkItemEncoding(item)
		if (got == nil) != (want == nil) || (got != nil && got.Error() != want.Error()) {
			t.Fatalf("checkItemEncoding(%#v) = %v, want %v", item, got, want)
		}
	}
}
