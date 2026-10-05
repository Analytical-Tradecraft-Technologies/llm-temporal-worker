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
// Reference items (output annotations such as citations) are never lossy
// input: those that fall before the boundary are left out of Prefix and lead
// Retained in their original order, so a compacted transcript keeps them.
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
// compaction request from splitting a tool exchange. Provider-state items
// (such as thinking blocks) are grouped with the item that follows them, so a
// cut never separates them from their model output or tool call. Reference
// items (output annotations such as citations) are grouped with the item that
// precedes them, so they are never counted as turns of their own.
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
	selection := PrefixSelection{RetainedTurns: len(turns) - cut}
	// A summary is plain text and cannot carry a citation's URI or metadata,
	// so references before the boundary stay verbatim instead of being
	// summarized away.
	for _, item := range items[:boundary] {
		if isReference(item) {
			selection.Retained = append(selection.Retained, item)
		} else {
			selection.Prefix = append(selection.Prefix, item)
		}
	}
	selection.Retained = append(selection.Retained, items[boundary:]...)
	return selection, nil
}

type turnRange struct {
	start int
	end   int
}

// splitTurns uses the same frontier transitions as state.ValidateTranscript.
// A ToolCall starts a turn when the frontier is empty; subsequent calls and
// matching results remain in that turn until the frontier is resolved. Every
// other item with an empty frontier is a single atomic turn, except that an
// assistant response containing provider state stays one turn through its
// following model text and tool calls, and a reference annotation stays with
// the item it follows.
func splitTurns(items []llm.Item) []turnRange {
	turns := make([]turnRange, 0, len(items))
	start := 0
	pending := make(map[string]struct{})
	// reasoning is true while the current turn holds provider state (for
	// example a thinking block) from an assistant response that may still
	// continue with text and tool calls. That whole response stays one turn,
	// so a cut can never separate thinking from the tool_use it belongs to.
	reasoning := false
	for index, item := range items {
		if index > start && len(pending) == 0 && !(reasoning && isModelOutput(item)) && !isReference(item) {
			turns = append(turns, turnRange{start: start, end: index})
			start = index
			reasoning = false
		}
		if isProviderState(item) {
			reasoning = true
		} else if !isModelOutput(item) && !isReference(item) {
			reasoning = false
		}
		switch value := item.(type) {
		case llm.ToolCall:
			pending[value.ID] = struct{}{}
		case llm.ToolResult:
			delete(pending, value.CallID)
		case *llm.ToolCall:
			pending[value.ID] = struct{}{}
		case *llm.ToolResult:
			delete(pending, value.CallID)
		}
	}
	turns = append(turns, turnRange{start: start, end: len(items)})
	return turns
}

// isModelOutput reports whether an item can be part of one assistant
// response: provider state, model text, or a tool call.
func isModelOutput(item llm.Item) bool {
	switch value := item.(type) {
	case llm.ProviderState, *llm.ProviderState, llm.ToolCall, *llm.ToolCall:
		return true
	case llm.Message:
		return value.Actor == llm.ActorModel
	case *llm.Message:
		return value != nil && value.Actor == llm.ActorModel
	default:
		return false
	}
}

func isProviderState(item llm.Item) bool {
	switch item.(type) {
	case llm.ProviderState, *llm.ProviderState:
		return true
	default:
		return false
	}
}

func isReference(item llm.Item) bool {
	switch item.(type) {
	case llm.Reference, *llm.Reference:
		return true
	default:
		return false
	}
}
