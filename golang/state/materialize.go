package state

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// ValidateTranscript enforces the tool-call frontier across an entire
// materialized lineage. A child may resolve an outstanding frontier with
// matching results. Model messages, provider state, references and parallel
// calls remain in the same turn until results begin; user messages and new
// model output cannot interrupt partially resolved results. Tool-call IDs are
// unique for a lineage.
func ValidateTranscript(items []llm.Item) ([]string, error) {
	return validateItems(items)
}

func validateItems(items []llm.Item) ([]string, error) {
	pending := make(map[string]struct{})
	seenCalls := make(map[string]struct{})
	resultsStarted := false
	for index, item := range items {
		item = itemValue(item)
		if item == nil {
			return nil, fmt.Errorf("transcript item %d is nil", index)
		}
		if err := checkItemEncoding(item); err != nil {
			return nil, fmt.Errorf("transcript item %d: %w", index, err)
		}
		if len(pending) == 0 {
			// A fully resolved tool exchange ends the model turn. A subsequent
			// call starts a new turn and may again contain parallel calls.
			resultsStarted = false
		}
		if len(pending) > 0 {
			switch value := item.(type) {
			case llm.ToolResult:
			case llm.Message:
				if value.Actor != llm.ActorModel || resultsStarted {
					return nil, fmt.Errorf("transcript item %d starts a new turn before pending tool results", index)
				}
				// Providers may interleave text and tool calls in one response.
			case llm.ToolCall:
				if resultsStarted {
					return nil, fmt.Errorf("transcript item %d starts a new tool-call turn before pending tool results", index)
				}
				// Parallel tool calls share one model turn, potentially
				// interleaved with model text before any result.
			case llm.ProviderState, llm.Reference:
				if resultsStarted {
					return nil, fmt.Errorf("transcript item %d starts a new turn before pending tool results", index)
				}
				// Reasoning state and citations are model-turn content too: a
				// provider may emit them between or after its tool calls.
			default:
				return nil, fmt.Errorf("transcript item %d starts a new turn before pending tool results", index)
			}
		}
		switch value := item.(type) {
		case llm.ToolCall:
			if value.ID == "" {
				return nil, fmt.Errorf("transcript item %d has an empty tool call ID", index)
			}
			if _, exists := seenCalls[value.ID]; exists {
				return nil, fmt.Errorf("transcript item %d reuses tool call ID %q", index, value.ID)
			}
			seenCalls[value.ID] = struct{}{}
			pending[value.ID] = struct{}{}
		case llm.ToolResult:
			if _, exists := pending[value.CallID]; !exists {
				return nil, fmt.Errorf("transcript item %d has an unmatched tool result %q", index, value.CallID)
			}
			resultsStarted = true
			delete(pending, value.CallID)
		}
	}
	result := make([]string, 0, len(pending))
	for callID := range pending {
		result = append(result, callID)
	}
	// Sort so a materialized view is deterministic despite map iteration.
	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && result[j] < result[j-1]; j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}
	return result, nil
}

func validateItemEncoding(items []llm.Item) error {
	for index, item := range items {
		item = itemValue(item)
		if item == nil {
			return fmt.Errorf("transcript item %d is nil", index)
		}
		if err := checkItemEncoding(item); err != nil {
			return fmt.Errorf("transcript item %d: %w", index, err)
		}
	}
	return nil
}

// checkItemEncoding reports the error json.Marshal(item) would return for a
// value from itemValue. It calls the item's MarshalJSON directly because
// json.Marshal would also re-scan and compact the whole encoding, which for a
// large transcript dominates validation; every type in the closed Item union
// builds its encoding from json.Marshal output, so that scan cannot fail.
// Other types, such as one embedding a built-in item with its own
// MarshalJSON, keep the full json.Marshal check.
func checkItemEncoding(item llm.Item) error {
	switch value := item.(type) {
	case llm.Message, llm.ToolCall, llm.ToolResult, llm.ProviderState, llm.Reference:
		if _, err := value.(json.Marshaler).MarshalJSON(); err != nil {
			return &json.MarshalerError{Type: reflect.TypeOf(value), Err: err}
		}
		return nil
	default:
		_, err := json.Marshal(item)
		return err
	}
}

// appendItemsJSON appends items exactly as json.Marshal encodes a
// []llm.Item, with a nil slice written as null, and returns the same error.
// json.Marshal re-scans and compacts every MarshalJSON result it embeds; for
// the value types checkItemEncoding trusts that output is already compact
// and HTML-escaped, so it is used as is (#1112). Other items, including nil
// and pointer items, keep the json.Marshal path.
func appendItemsJSON(dst []byte, items []llm.Item) ([]byte, error) {
	err := writeItemsJSON(items, func(encoded []byte) { dst = append(dst, encoded...) })
	if err != nil {
		return nil, err
	}
	return dst, nil
}

// writeItemsJSON passes the encoding appendItemsJSON appends to write, in
// pieces, so that a caller can hash it without buffering it.
func writeItemsJSON(items []llm.Item, write func([]byte)) error {
	if items == nil {
		write([]byte("null"))
		return nil
	}
	write([]byte{'['})
	for index, item := range items {
		if index > 0 {
			write([]byte{','})
		}
		var encoded []byte
		var err error
		switch value := item.(type) {
		case llm.Message, llm.ToolCall, llm.ToolResult, llm.ProviderState, llm.Reference:
			encoded, err = value.(json.Marshaler).MarshalJSON()
			if err != nil {
				return &json.MarshalerError{Type: reflect.TypeOf(value), Err: err}
			}
		default:
			encoded, err = json.Marshal(item)
			if err != nil {
				return err
			}
		}
		write(encoded)
	}
	write([]byte{']'})
	return nil
}

// itemValue normalizes the closed Item union without a JSON round trip.
// It is a shallow view; cloneItem detaches mutable fields before storage.
func itemValue(item llm.Item) llm.Item {
	switch value := item.(type) {
	case *llm.Message:
		if value != nil {
			return *value
		}
	case *llm.ToolCall:
		if value != nil {
			return *value
		}
	case *llm.ToolResult:
		if value != nil {
			return *value
		}
	case *llm.ProviderState:
		if value != nil {
			return *value
		}
	case *llm.Reference:
		if value != nil {
			return *value
		}
	default:
		return item
	}
	return nil
}
