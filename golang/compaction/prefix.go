package compaction

import (
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// PrefixSelection is the deterministic boundary used by generic compaction.
// Prefix is the lossy input supplied to the summarizer and Retained is the
// recent suffix that remains verbatim. The two slices are copies of the input
// slice; the items themselves are immutable semantic values by contract.
//
// A selection may have an empty Prefix when the transcript does not contain a
// safe compaction boundary. Callers should treat that as "nothing to compact"
// rather than sending an empty request to PrepareRequest.
type PrefixSelection struct {
	Prefix        []llm.Item
	Retained      []llm.Item
	RetainedTurns int
}

// SelectPrefix chooses a complete prefix while retaining at least recentTurns
// logical turns. A turn begins whenever the tool frontier is empty. Tool calls
// and all of their results form one atomic turn, including an unresolved final
// frontier. This makes every returned boundary frontier-empty and prevents a
// compaction request from splitting a tool exchange. Provider-state items are
// ordinary atomic items and are never split because selection only cuts between
// items.
//
// The transcript is validated using the checkpoint materializer's canonical
// tool-frontier rules. recentTurns must be non-negative. If an open tool
// frontier is present at the end, that entire turn is retained even when
// recentTurns is zero.
func SelectPrefix(items []llm.Item, recentTurns int) (PrefixSelection, error) {
	if recentTurns < 0 {
		return PrefixSelection{}, fmt.Errorf("compaction recent turns must not be negative")
	}
	if len(items) == 0 {
		return PrefixSelection{}, fmt.Errorf("compaction transcript must not be empty")
	}
	pending, err := state.ValidateTranscript(items)
	if err != nil {
		return PrefixSelection{}, fmt.Errorf("compaction transcript: %w", err)
	}

	turns := splitTurns(items)
	if len(turns) == 0 {
		return PrefixSelection{}, fmt.Errorf("compaction transcript has no turns")
	}
	// An unresolved final frontier is not safe to summarize. It is already an
	// atomic final turn, so keep it in addition to the requested recent window.
	retain := recentTurns
	if len(pending) > 0 && retain < 1 {
		retain = 1
	}
	if retain > len(turns) {
		retain = len(turns)
	}
	cut := len(turns) - retain
	boundary := 0
	if cut > 0 {
		boundary = turns[cut-1].end
	}
	selection := PrefixSelection{
		Prefix:        append([]llm.Item(nil), items[:boundary]...),
		Retained:      append([]llm.Item(nil), items[boundary:]...),
		RetainedTurns: len(turns) - cut,
	}
	return selection, nil
}

type turnRange struct {
	start int
	end   int
}

// splitTurns uses the same frontier transitions as state.ValidateTranscript.
// A ToolCall starts a turn when the frontier is empty; subsequent calls and
// matching results remain in that turn until the frontier is resolved. Every
// other item with an empty frontier is a single atomic turn.
func splitTurns(items []llm.Item) []turnRange {
	turns := make([]turnRange, 0, len(items))
	start := 0
	pending := make(map[string]struct{})
	for index, item := range items {
		if index > start && len(pending) == 0 {
			turns = append(turns, turnRange{start: start, end: index})
			start = index
		}
		switch value := item.(type) {
		case llm.ToolCall:
			pending[value.ID] = struct{}{}
		case llm.ToolResult:
			delete(pending, value.CallID)
		}
	}
	turns = append(turns, turnRange{start: start, end: len(items)})
	return turns
}
