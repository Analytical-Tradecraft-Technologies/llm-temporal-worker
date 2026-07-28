package compaction

import (
	"reflect"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func textMessage(actor llm.Actor, text string) llm.Message {
	return llm.Message{Actor: actor, Content: []llm.Part{llm.TextPart{Text: text}}}
}

func TestSelectPrefixRetainsRecentTurns(t *testing.T) {
	items := []llm.Item{
		textMessage(llm.ActorHuman, "one"),
		textMessage(llm.ActorModel, "two"),
		textMessage(llm.ActorHuman, "three"),
		textMessage(llm.ActorModel, "four"),
	}
	selection, err := SelectPrefix(items, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := selection.Prefix, items[:2]; !reflect.DeepEqual(got, want) {
		t.Fatalf("prefix = %#v, want %#v", got, want)
	}
	if got, want := selection.Retained, items[2:]; !reflect.DeepEqual(got, want) {
		t.Fatalf("retained = %#v, want %#v", got, want)
	}
	if selection.RetainedTurns != 2 {
		t.Fatalf("retained turns = %d, want 2", selection.RetainedTurns)
	}
}

func TestSelectPrefixNeverSplitsToolExchange(t *testing.T) {
	items := []llm.Item{
		textMessage(llm.ActorHuman, "question"),
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)},
		llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "answer"}}},
		textMessage(llm.ActorModel, "result"),
	}
	selection, err := SelectPrefix(items, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := selection.Prefix, items[:1]; !reflect.DeepEqual(got, want) {
		t.Fatalf("prefix = %#v, want %#v", got, want)
	}
	if got, want := selection.Retained, items[1:]; !reflect.DeepEqual(got, want) {
		t.Fatalf("retained = %#v, want %#v", got, want)
	}
}

func TestSelectPrefixRetainsOpenToolFrontierEvenWithZeroRecentTurns(t *testing.T) {
	items := []llm.Item{
		textMessage(llm.ActorHuman, "question"),
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)},
	}
	selection, err := SelectPrefix(items, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := selection.Prefix, items[:1]; !reflect.DeepEqual(got, want) {
		t.Fatalf("prefix = %#v, want %#v", got, want)
	}
	if got, want := selection.Retained, items[1:]; !reflect.DeepEqual(got, want) {
		t.Fatalf("retained = %#v, want %#v", got, want)
	}
	if selection.RetainedTurns != 1 {
		t.Fatalf("retained turns = %d, want 1", selection.RetainedTurns)
	}
}

func TestSelectPrefixRejectsInvalidTranscript(t *testing.T) {
	_, err := SelectPrefix([]llm.Item{
		llm.ToolResult{CallID: "missing", Name: "lookup"},
	}, 1)
	if err == nil {
		t.Fatal("unmatched tool result unexpectedly accepted")
	}
	if _, err := SelectPrefix(nil, 0); err == nil {
		t.Fatal("empty transcript unexpectedly accepted")
	}
	if _, err := SelectPrefix([]llm.Item{textMessage(llm.ActorHuman, "x")}, -1); err == nil {
		t.Fatal("negative recent turns unexpectedly accepted")
	}
}

func TestSelectPrefixPreservesProviderStateAsAnAtomicItem(t *testing.T) {
	items := []llm.Item{
		textMessage(llm.ActorHuman, "question"),
		llm.ProviderState{Provider: "provider", EndpointFamily: "family", MediaType: "opaque", Opaque: []byte{1, 2, 3}},
		textMessage(llm.ActorModel, "answer"),
	}
	selection, err := SelectPrefix(items, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Prefix) != 2 || len(selection.Retained) != 1 {
		t.Fatalf("selection split provider state: %#v", selection)
	}
}

func TestSelectPrefixBoundaryProperty(t *testing.T) {
	items := []llm.Item{
		textMessage(llm.ActorHuman, "one"),
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)},
		llm.ToolResult{CallID: "call-1", Name: "lookup"},
		textMessage(llm.ActorModel, "two"),
		llm.ToolCall{ID: "call-2", Name: "lookup", Arguments: []byte(`{}`)},
		llm.ToolResult{CallID: "call-2", Name: "lookup"},
		textMessage(llm.ActorHuman, "three"),
	}
	for recent := 0; recent <= len(items)+1; recent++ {
		selection, err := SelectPrefix(items, recent)
		if err != nil {
			t.Fatalf("recent=%d: %v", recent, err)
		}
		combined := append(append([]llm.Item{}, selection.Prefix...), selection.Retained...)
		if !reflect.DeepEqual(combined, items) {
			t.Fatalf("recent=%d changed transcript: %#v", recent, combined)
		}
		for _, id := range []string{"call-1", "call-2"} {
			prefixHas, retainedHas := containsToolCall(selection.Prefix, id), containsToolCall(selection.Retained, id)
			resultPrefixHas, resultRetainedHas := containsToolResult(selection.Prefix, id), containsToolResult(selection.Retained, id)
			if prefixHas != resultPrefixHas || retainedHas != resultRetainedHas {
				t.Fatalf("recent=%d split tool exchange %s", recent, id)
			}
		}
	}
}

func containsToolCall(items []llm.Item, id string) bool {
	for _, item := range items {
		if value, ok := item.(llm.ToolCall); ok && value.ID == id {
			return true
		}
	}
	return false
}

func containsToolResult(items []llm.Item, id string) bool {
	for _, item := range items {
		if value, ok := item.(llm.ToolResult); ok && value.CallID == id {
			return true
		}
	}
	return false
}
