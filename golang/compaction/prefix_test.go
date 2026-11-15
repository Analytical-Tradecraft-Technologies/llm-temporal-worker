package compaction

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func textMessage(actor llm.Actor, text string) llm.Message {
	return llm.Message{Actor: actor, Content: []llm.Part{llm.TextPart{Text: text}}}
}

// mustSelectPrefix runs SelectPrefix and fails the test on an error.
func mustSelectPrefix(t *testing.T, items []llm.Item, recentTurns int) PrefixSelection {
	t.Helper()
	selection, err := SelectPrefix(items, recentTurns)
	if err != nil {
		t.Fatal(err)
	}
	return selection
}

// mustSelectRequestPrefix runs SelectRequestPrefix and fails the test on an
// error.
func mustSelectRequestPrefix(t *testing.T, source llm.Request, policy Policy) PrefixSelection {
	t.Helper()
	selection, err := SelectRequestPrefix(source, policy)
	if err != nil {
		t.Fatal(err)
	}
	return selection
}

// assertPrefixSplit checks a selection's prefix and retained items and, when
// turns is not negative, its retained turn count.
func assertPrefixSplit(t *testing.T, selection PrefixSelection, prefix, retained []llm.Item, turns int) {
	t.Helper()
	if !reflect.DeepEqual(selection.Prefix, prefix) {
		t.Fatalf("prefix = %#v, want %#v", selection.Prefix, prefix)
	}
	if !reflect.DeepEqual(selection.Retained, retained) {
		t.Fatalf("retained = %#v, want %#v", selection.Retained, retained)
	}
	if turns >= 0 && selection.RetainedTurns != turns {
		t.Fatalf("retained turns = %d, want %d", selection.RetainedTurns, turns)
	}
}

func TestSelectPrefixRetainsRecentTurns(t *testing.T) {
	items := []llm.Item{
		textMessage(llm.ActorHuman, "one"),
		textMessage(llm.ActorModel, "two"),
		textMessage(llm.ActorHuman, "three"),
		textMessage(llm.ActorModel, "four"),
	}
	assertPrefixSplit(t, mustSelectPrefix(t, items, 2), items[:2], items[2:], 2)
}

func TestSelectPrefixNeverSplitsToolExchange(t *testing.T) {
	items := []llm.Item{
		textMessage(llm.ActorHuman, "question"),
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)},
		llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "answer"}}},
		textMessage(llm.ActorModel, "result"),
	}
	assertPrefixSplit(t, mustSelectPrefix(t, items, 2), items[:1], items[1:], -1)
}

func TestSelectPrefixRetainsOpenToolFrontierEvenWithZeroRecentTurns(t *testing.T) {
	items := []llm.Item{
		textMessage(llm.ActorHuman, "question"),
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)},
	}
	assertPrefixSplit(t, mustSelectPrefix(t, items, 0), items[:1], items[1:], 1)
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
	selection := mustSelectPrefix(t, items, 1)
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
		selection := mustSelectPrefix(t, items, recent)
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
			got := mustSelectPrefix(t, items[:test.length], test.retain)
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
			selection := mustSelectPrefix(t, items[:test.length], test.retain)
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
		selection := mustSelectPrefix(t, items, recent)
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
	selection := mustSelectPrefix(t, closed, 1)
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
	// The summarized turn's citation cannot survive a plain-text summary, so
	// it leaves the lossy prefix and stays verbatim ahead of the recent turns.
	assertPrefixSplit(t, mustSelectPrefix(t, items, 2), items[:2], items[2:], 2)
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
	assertPrefixSplit(t, mustSelectPrefix(t, items, 1), []llm.Item{items[0], items[1], items[3], items[4]}, []llm.Item{first, second, items[6]}, 1)
}

// SelectRequestPrefix keeps the policy's window when the retained request
// fits target_tokens and otherwise shortens it, oldest turn first, down to one
// turn. The window is measured with the instructions and tools that stay in
// every request and the summary compaction adds, not the items alone.
func TestSelectRequestPrefixShortensWindowToTargetTokens(t *testing.T) {
	items := []llm.Item{
		textMessage(llm.ActorHuman, "one"),
		textMessage(llm.ActorModel, strings.Repeat("x", 4000)),
		textMessage(llm.ActorHuman, "three"),
		textMessage(llm.ActorModel, "four"),
	}
	source := llm.Request{OperationKey: "compact-1", Model: "alias", Input: items, Instructions: []llm.Instruction{{Kind: llm.InstructionKindText, Text: strings.Repeat("p", 400)}}}
	policy := DefaultPolicy()
	policy.RecentTurns, policy.TriggerTokens, policy.TargetTokens, policy.OutputReserveTokens = 3, 2000, 600, 1
	assertPrefixSplit(t, mustSelectRequestPrefix(t, source, policy), items[:2], items[2:], 2)

	// A window that fits is kept in full.
	policy.TargetTokens = 1900
	if selection := mustSelectRequestPrefix(t, source, policy); !reflect.DeepEqual(selection.Retained, items[1:]) || selection.RetainedTurns != 3 {
		t.Fatalf("retained = %#v (%d turns), want the whole window", selection.Retained, selection.RetainedTurns)
	}

	// The summary counts: the same window no longer fits once its output
	// reserve would push the compacted request past the target (#1347).
	policy.OutputReserveTokens = 1000
	if selection := mustSelectRequestPrefix(t, source, policy); !reflect.DeepEqual(selection.Retained, items[2:]) || selection.RetainedTurns != 2 {
		t.Fatalf("retained = %#v (%d turns), want the two turns that leave room for the summary", selection.Retained, selection.RetainedTurns)
	}
	policy.OutputReserveTokens = 1

	// Instructions count: the same items no longer fit once the prompt grows.
	source.Instructions[0].Text = strings.Repeat("p", 4000)
	if selection := mustSelectRequestPrefix(t, source, policy); selection.RetainedTurns != 2 {
		t.Fatalf("retained turns = %d, want 2 after the prompt grew", selection.RetainedTurns)
	}

	// The last turn stays even when it alone exceeds the target, and a zero
	// window stays empty.
	policy.TargetTokens = 1
	selection, err := SelectRequestPrefix(source, policy)
	if err != nil || selection.RetainedTurns != 1 || !reflect.DeepEqual(selection.Retained, items[3:]) {
		t.Fatalf("selection = %#v, err = %v, want the last turn alone", selection, err)
	}
	policy.RecentTurns = 0
	selection, err = SelectRequestPrefix(source, policy)
	if err != nil || selection.RetainedTurns != 0 || len(selection.Retained) != 0 {
		t.Fatalf("selection = %#v, err = %v, want an empty window", selection, err)
	}
}

// SelectBoundedRequestPrefix shortens a window that fits target_tokens but
// not the caller's bound, such as recent inline images the token estimate
// leaves out, down to one turn; a bound error is returned (#1346).
func TestSelectBoundedRequestPrefixShortensWindowToBound(t *testing.T) {
	image := func(text string) llm.Item {
		return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}, llm.ImagePart{Bytes: make([]byte, 1000), MediaType: "image/png"}}}
	}
	items := []llm.Item{image("one"), image("two"), image("three"), image("four")}
	source := llm.Request{OperationKey: "compact-1", Model: "alias", Input: items}
	policy := DefaultPolicy()
	policy.RecentTurns = 3
	images := func(limit int) func([]llm.Item) (bool, error) {
		return func(retained []llm.Item) (bool, error) { return len(retained) <= limit, nil }
	}
	selection, err := SelectBoundedRequestPrefix(source, policy, images(2))
	if err != nil || selection.RetainedTurns != 2 || !reflect.DeepEqual(selection.Retained, items[2:]) || !reflect.DeepEqual(selection.Prefix, items[:2]) {
		t.Fatalf("selection = %#v, err = %v, want the two turns within the bound", selection, err)
	}
	selection, err = SelectBoundedRequestPrefix(source, policy, images(0))
	if err != nil || selection.RetainedTurns != 1 || !reflect.DeepEqual(selection.Retained, items[3:]) {
		t.Fatalf("selection = %#v, err = %v, want the last turn alone", selection, err)
	}
	// A long window is searched, not probed once per turn.
	long := make([]llm.Item, 256)
	for i := range long {
		long[i] = image(fmt.Sprint(i))
	}
	policy.RecentTurns = len(long)
	calls := 0
	selection, err = SelectBoundedRequestPrefix(llm.Request{OperationKey: "compact-1", Model: "alias", Input: long}, policy, func(retained []llm.Item) (bool, error) {
		calls++
		return len(retained) <= 37, nil
	})
	if err != nil || selection.RetainedTurns != 37 || calls > 10 {
		t.Fatalf("selection kept %d turns after %d probes, err = %v; want 37 turns within 10 probes", selection.RetainedTurns, calls, err)
	}
	policy.RecentTurns = 3
	failure := errors.New("bound failed")
	if _, err := SelectBoundedRequestPrefix(source, policy, func([]llm.Item) (bool, error) { return false, failure }); !errors.Is(err, failure) {
		t.Fatalf("err = %v, want the bound's error", err)
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
	policy.RecentTurns, policy.TriggerTokens, policy.TargetTokens, policy.OutputReserveTokens = 2, 2000, 600, 1
	if selection := mustSelectRequestPrefix(t, source, policy); selection.RetainedTurns != 2 || !reflect.DeepEqual(selection.Retained, items[2:]) {
		t.Fatalf("retained = %#v (%d turns), want the image turn kept", selection.Retained, selection.RetainedTurns)
	}
}
