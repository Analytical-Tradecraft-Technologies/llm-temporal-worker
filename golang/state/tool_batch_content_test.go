package state

import (
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// Provider state and references are model-turn content: like model text they
// may sit between or after the calls of an open batch, but never after its
// results have started.
func TestToolBatchAcceptsModelTurnContent(t *testing.T) {
	call := func(id string) llm.ToolCall { return llm.ToolCall{ID: id, Name: "lookup", Arguments: []byte("{}")} }
	result := func(id string) llm.ToolResult { return llm.ToolResult{CallID: id} }
	thinking := llm.ProviderState{Provider: "provider", EndpointFamily: "family", MediaType: "application/json", Opaque: []byte("{}")}
	citation := llm.Reference{URI: "https://example.com/source"}
	text := llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "checking"}}}
	for _, test := range []struct {
		name    string
		items   []llm.Item
		pending []string
		invalid bool
	}{
		{"call then state", []llm.Item{call("a"), thinking}, []string{"a"}, false},
		{"state call state call", []llm.Item{thinking, call("a"), thinking, call("b")}, []string{"a", "b"}, false},
		{"call then reference", []llm.Item{call("a"), citation}, []string{"a"}, false},
		{"pointer items", []llm.Item{call("a"), &thinking, &citation}, []string{"a"}, false},
		{"mixed content then results", []llm.Item{call("a"), text, thinking, citation, call("b"), result("b"), result("a")}, []string{}, false},
		{"state after resolved batch", []llm.Item{call("a"), thinking, result("a"), thinking, call("b")}, []string{"b"}, false},
		{"state after partial results", []llm.Item{call("a"), call("b"), result("a"), thinking}, nil, true},
		{"reference after partial results", []llm.Item{call("a"), call("b"), result("a"), citation}, nil, true},
		{"human message inside batch", []llm.Item{call("a"), thinking, llm.Message{Actor: llm.ActorHuman}}, nil, true},
		{"reused ID after state", []llm.Item{call("a"), thinking, call("a")}, nil, true},
		{"reused ID in a later turn", []llm.Item{call("a"), result("a"), call("a")}, nil, true},
		{"unmatched result after state", []llm.Item{call("a"), thinking, result("b")}, nil, true},
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
