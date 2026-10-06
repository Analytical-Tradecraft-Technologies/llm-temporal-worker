package compaction

import (
	"reflect"
	"strings"
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

func TestSelectPrefixKeepsReferenceWithPrecedingResponse(t *testing.T) {
	// A citation annotates the response before it. It is not a turn, so the
	// recent window keeps the cited answer instead of only its citations.
	items := []llm.Item{
		textMessage(llm.ActorHuman, "first question"),
		textMessage(llm.ActorModel, "first answer"),
		llm.Reference{URI: "https://example.com/first"},
		textMessage(llm.ActorHuman, "second question"),
		textMessage(llm.ActorModel, "second answer"),
		llm.Reference{URI: "https://example.com/second-a"},
		&llm.Reference{URI: "https://example.com/second-b"},
	}
	selection, err := SelectPrefix(items, 2)
	if err != nil {
		t.Fatal(err)
	}
	// The summarized turn's citation cannot survive a plain-text summary, so
	// it leaves the lossy prefix and stays verbatim ahead of the recent turns.
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

func TestSelectPrefixCarriesEveryReferencePastTheSummary(t *testing.T) {
	first, second := llm.Reference{URI: "https://example.com/first"}, &llm.Reference{URI: "https://example.com/second"}
	items := []llm.Item{
		textMessage(llm.ActorHuman, "first question"),
		textMessage(llm.ActorModel, "first answer"),
		first,
		textMessage(llm.ActorHuman, "second question"),
		textMessage(llm.ActorModel, "second answer"),
		second,
		textMessage(llm.ActorHuman, "third question"),
	}
	selection, err := SelectPrefix(items, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := selection.Prefix, []llm.Item{items[0], items[1], items[3], items[4]}; !reflect.DeepEqual(got, want) {
		t.Fatalf("prefix = %#v, want %#v", got, want)
	}
	if got, want := selection.Retained, []llm.Item{first, second, items[6]}; !reflect.DeepEqual(got, want) {
		t.Fatalf("retained = %#v, want %#v", got, want)
	}
	if selection.RetainedTurns != 1 {
		t.Fatalf("retained turns = %d, want 1", selection.RetainedTurns)
	}
}

// SelectRequestPrefix keeps the policy's window when the retained request
// fits target_tokens and otherwise shortens it, oldest turn first, down to one
// turn. The window is measured with the instructions and tools that stay in
// every request, not the items alone.
func TestSelectRequestPrefixShortensWindowToTargetTokens(t *testing.T) {
	items := []llm.Item{
		textMessage(llm.ActorHuman, "one"),
		textMessage(llm.ActorModel, strings.Repeat("x", 4000)),
		textMessage(llm.ActorHuman, "three"),
		textMessage(llm.ActorModel, "four"),
	}
	source := llm.Request{OperationKey: "compact-1", Model: "alias", Input: items, Instructions: []llm.Instruction{{Kind: llm.InstructionKindText, Text: strings.Repeat("p", 400)}}}
	policy := DefaultPolicy()
	policy.RecentTurns, policy.TriggerTokens, policy.TargetTokens = 3, 2000, 600
	selection, err := SelectRequestPrefix(source, policy)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := selection.Retained, items[2:]; !reflect.DeepEqual(got, want) || selection.RetainedTurns != 2 {
		t.Fatalf("retained = %#v (%d turns), want the two turns that fit", got, selection.RetainedTurns)
	}
	if got, want := selection.Prefix, items[:2]; !reflect.DeepEqual(got, want) {
		t.Fatalf("prefix = %#v, want %#v", got, want)
	}

	// A window that fits is kept in full.
	policy.TargetTokens = 1900
	selection, err = SelectRequestPrefix(source, policy)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := selection.Retained, items[1:]; !reflect.DeepEqual(got, want) || selection.RetainedTurns != 3 {
		t.Fatalf("retained = %#v (%d turns), want the whole window", got, selection.RetainedTurns)
	}

	// Instructions count: the same items no longer fit once the prompt grows.
	source.Instructions[0].Text = strings.Repeat("p", 4000)
	selection, err = SelectRequestPrefix(source, policy)
	if err != nil {
		t.Fatal(err)
	}
	if selection.RetainedTurns != 2 {
		t.Fatalf("retained turns = %d, want 2 after the prompt grew", selection.RetainedTurns)
	}

	// The last turn stays even when it alone exceeds the target, and a zero
	// window stays empty.
	policy.TargetTokens = 1
	selection, err = SelectRequestPrefix(source, policy)
	if err != nil || selection.RetainedTurns != 1 || !reflect.DeepEqual(selection.Retained, items[3:]) {
		t.Fatalf("selection = %#v, err = %v, want the last turn alone", selection, err)
	}
	policy.RecentTurns = 0
	selection, err = SelectRequestPrefix(source, policy)
	if err != nil || selection.RetainedTurns != 0 || len(selection.Retained) != 0 {
		t.Fatalf("selection = %#v, err = %v, want an empty window", selection, err)
	}
}

func TestSelectRequestPrefixRejectsInvalidPolicy(t *testing.T) {
	policy := DefaultPolicy()
	policy.TargetTokens = policy.TriggerTokens
	if _, err := SelectRequestPrefix(llm.Request{OperationKey: "compact-1", Model: "alias", Input: []llm.Item{textMessage(llm.ActorHuman, "x")}}, policy); err == nil {
		t.Fatal("invalid policy accepted")
	}
}

// Inline media bytes are not text: a recent image that a provider bills on its
// pixels stays in the window instead of being moved into the lossy summary
// because its base64 is large.
func TestSelectRequestPrefixDoesNotCountInlineMediaBytesAsText(t *testing.T) {
	image := llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "look"}, llm.ImagePart{Bytes: make([]byte, 64<<10), MediaType: "image/png"}}}
	items := []llm.Item{textMessage(llm.ActorHuman, "one"), textMessage(llm.ActorModel, "two"), image, textMessage(llm.ActorModel, "four")}
	source := llm.Request{OperationKey: "compact-1", Model: "alias", Input: items}
	policy := DefaultPolicy()
	policy.RecentTurns, policy.TriggerTokens, policy.TargetTokens = 2, 2000, 600
	selection, err := SelectRequestPrefix(source, policy)
	if err != nil {
		t.Fatal(err)
	}
	if selection.RetainedTurns != 2 || !reflect.DeepEqual(selection.Retained, items[2:]) {
		t.Fatalf("retained = %#v (%d turns), want the image turn kept", selection.Retained, selection.RetainedTurns)
	}
}
