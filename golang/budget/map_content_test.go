package budget

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestMapContentCopiesContentItemsOnly(t *testing.T) {
	replaced := []llm.Part{llm.TextPart{Text: "replaced"}}
	calls := 0
	replace := func([]llm.Part) []llm.Part {
		calls++
		return replaced
	}

	message := &llm.Message{Content: []llm.Part{llm.TextPart{Text: "original"}}}
	mapped, ok := mapContent(message, replace).(*llm.Message)
	if !ok || mapped == message || len(mapped.Content) != 1 || mapped.Content[0] != replaced[0] {
		t.Fatalf("pointer message = %#v", mapped)
	}
	if message.Content[0] != (llm.TextPart{Text: "original"}) {
		t.Fatalf("pointer message was written through: %#v", message)
	}
	if result, ok := mapContent(llm.ToolResult{CallID: "call"}, replace).(llm.ToolResult); !ok || result.CallID != "call" || len(result.Content) != 1 {
		t.Fatalf("tool result = %#v", result)
	}
	if calls != 2 {
		t.Fatalf("content calls = %d, want 2", calls)
	}

	var nilMessage *llm.Message
	if mapContent(nilMessage, replace) != llm.Item(nilMessage) {
		t.Fatal("nil pointer message changed")
	}
	if call, ok := mapContent(llm.ToolCall{ID: "call"}, replace).(llm.ToolCall); !ok || call.ID != "call" {
		t.Fatalf("non-content item = %#v", call)
	}
	if calls != 2 {
		t.Fatalf("content calls after non-content items = %d, want 2", calls)
	}
}
