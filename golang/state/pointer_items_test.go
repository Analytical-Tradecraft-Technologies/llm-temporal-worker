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
	for _, test := range []struct {
		name    string
		items   []llm.Item
		pending []string
		invalid bool
	}{
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
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ValidateTranscript(test.items)
			if test.invalid {
				if err == nil {
					t.Fatal("accepted invalid transcript")
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, test.pending) {
				t.Fatalf("pending=%v error=%v", got, err)
			}
		})
	}
}

func TestCheckpointCopiesPointerToolItems(t *testing.T) {
	graph := NewCheckpointGraph(MaterializeLimits{})
	root := rootCheckpoint("pointer-root", "tenant-a", "pointer-op")
	call := &llm.ToolCall{ID: "a", Name: "lookup", Arguments: []byte("{}")}
	root.Output = []llm.Item{call}
	if err := graph.PutRoot(root); err != nil {
		t.Fatal(err)
	}
	call.ID = "mutated"
	call.Arguments[0] = '['
	got, err := graph.Materialize("tenant-a", root.Handle)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.PendingToolCalls, []string{"a"}) {
		t.Fatalf("pending=%v", got.PendingToolCalls)
	}
	stored, ok := got.Items[len(got.Items)-1].(llm.ToolCall)
	if !ok || string(stored.Arguments) != "{}" {
		t.Fatalf("stored call=%#v", got.Items)
	}
}

// checkItemEncoding skips json.Marshal's re-scan of the encoding, so it must
// still accept and reject exactly what json.Marshal does, with the same error.
func TestCheckItemEncodingMatchesJSONMarshal(t *testing.T) {
	for _, item := range []llm.Item{
		llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "<ok>"}}},
		llm.Message{Actor: "system"},
		llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.ImagePart{MediaType: "image/png"}}},
		llm.ToolCall{ID: "a", Name: "lookup", Arguments: []byte("{}")},
		llm.ToolCall{ID: "a", Name: "lookup", Arguments: []byte("{")},
		llm.ToolResult{CallID: "a", Content: []llm.Part{llm.JSONPart{Value: []byte("[1]")}}},
		llm.ToolResult{},
		llm.ProviderState{Provider: "p", EndpointFamily: "f", MediaType: "m"},
		llm.ProviderState{},
		llm.Reference{URI: "https://example.test/source"},
		llm.Reference{},
	} {
		_, want := json.Marshal(item)
		got := checkItemEncoding(item)
		if (got == nil) != (want == nil) || (got != nil && got.Error() != want.Error()) {
			t.Fatalf("checkItemEncoding(%#v) = %v, want %v", item, got, want)
		}
	}
}
