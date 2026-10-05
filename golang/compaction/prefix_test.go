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

func TestSelectPrefixKeepsProviderStateWithFollowingOutput(t *testing.T) {
	items := []llm.Item{
		textMessage(llm.ActorHuman, "question"),
		llm.ProviderState{Provider: "provider", EndpointFamily: "family", MediaType: "opaque", Opaque: []byte{1, 2, 3}},
		textMessage(llm.ActorModel, "answer"),
	}
	selection, err := SelectPrefix(items, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Prefix) != 1 || len(selection.Retained) != 2 {
		t.Fatalf("selection separated provider state from its output: %#v", selection)
	}
	if _, ok := selection.Retained[0].(llm.ProviderState); !ok {
		t.Fatalf("retained suffix does not start with the provider state: %#v", selection.Retained)
	}
}

func TestSelectPrefixKeepsThinkingWithPendingToolCall(t *testing.T) {
	thinking := llm.ProviderState{Provider: "anthropic", EndpointFamily: "messages", MediaType: "application/vnd.anthropic.content-block+json", Opaque: []byte(`{"type":"thinking"}`)}
	items := []llm.Item{
		textMessage(llm.ActorHuman, "first"),
		textMessage(llm.ActorModel, "reply"),
		textMessage(llm.ActorHuman, "second"),
		thinking,
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)},
	}
	for _, recent := range []int{0, 1} {
		selection, err := SelectPrefix(items, recent)
		if err != nil {
			t.Fatal(err)
		}
		if last := selection.Prefix[len(selection.Prefix)-1]; isProviderState(last) {
			t.Fatalf("recent=%d prefix ends with provider state: %#v", recent, selection.Prefix)
		}
		if _, ok := selection.Retained[0].(llm.ProviderState); !ok {
			t.Fatalf("recent=%d retained suffix starts without its thinking block: %#v", recent, selection.Retained)
		}
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

func TestSelectPrefixPreservesPointerToolFrontiers(t *testing.T) {
	items := []llm.Item{
		&llm.ToolCall{ID: "a", Name: "lookup", Arguments: []byte("{}")},
		&llm.ToolResult{CallID: "a"},
	}
	for _, test := range []struct {
		name           string
		length, retain int
	}{
		{"open", 1, 0}, {"resolved retained", 2, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := SelectPrefix(items[:test.length], test.retain)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Prefix) != 0 || !reflect.DeepEqual(got.Retained, items[:test.length]) {
				t.Fatalf("split pointer tool exchange: %#v", got)
			}
		})
	}
	if _, err := SelectPrefix(items[1:], 0); err == nil {
		t.Fatal("accepted orphan pointer result")
	}
}

func TestSelectPrefixKeepsMixedToolResponseAtomic(t *testing.T) {
	items := []llm.Item{
		textMessage(llm.ActorHuman, "question"),
		llm.ToolCall{ID: "a", Name: "lookup", Arguments: []byte("{}")},
		textMessage(llm.ActorModel, "Checking another source"),
		llm.ToolCall{ID: "b", Name: "lookup", Arguments: []byte("{}")},
		llm.ToolResult{CallID: "a"},
		llm.ToolResult{CallID: "b"},
		textMessage(llm.ActorModel, "done"),
	}
	for _, test := range []struct {
		name                string
		length, retain, cut int
	}{
		{"open", 4, 0, 1}, {"partially resolved", 5, 0, 1},
		{"retain exchange", 7, 2, 1}, {"compact exchange", 7, 1, 6},
	} {
		t.Run(test.name, func(t *testing.T) {
			selection, err := SelectPrefix(items[:test.length], test.retain)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(selection.Prefix, items[:test.cut]) || !reflect.DeepEqual(selection.Retained, items[test.cut:test.length]) {
				t.Fatalf("selection split the mixed tool exchange: %#v", selection)
			}
		})
	}
}

func TestSelectPrefixKeepsThinkingWithToolCallPastInterveningText(t *testing.T) {
	thinking := llm.ProviderState{Provider: "anthropic", EndpointFamily: "messages", MediaType: "application/vnd.anthropic.content-block+json", Opaque: []byte(`{"type":"thinking"}`)}
	items := []llm.Item{
		textMessage(llm.ActorHuman, "first"),
		textMessage(llm.ActorModel, "reply"),
		textMessage(llm.ActorHuman, "second"),
		thinking,
		thinking,
		textMessage(llm.ActorModel, "I will look that up."),
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)},
	}
	for _, recent := range []int{0, 1} {
		selection, err := SelectPrefix(items, recent)
		if err != nil {
			t.Fatal(err)
		}
		if len(selection.Retained) != 4 {
			t.Fatalf("recent=%d retained = %#v, want the whole thinking response with its tool call", recent, selection.Retained)
		}
		for _, item := range selection.Prefix {
			if isProviderState(item) {
				t.Fatalf("recent=%d summarised a thinking block of the retained tool call: %#v", recent, selection.Prefix)
			}
		}
	}
	// A following human turn still starts a new turn.
	closed := append(append([]llm.Item(nil), items[:6]...), textMessage(llm.ActorHuman, "third"))
	selection, err := SelectPrefix(closed, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Retained) != 1 {
		t.Fatalf("human turn after a thinking response = %#v", selection.Retained)
	}
}
