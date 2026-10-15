package bedrockmessages

import (
	"regexp"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// IDs from other families, such as "functions.lookup:0", violate the
// tool_use.id pattern. Lowering remaps them identically on the call and the
// result so the pair still matches.
func TestLowerItemRemapsNonPortableToolCallIDs(t *testing.T) {
	const id = "functions.lookup:0"
	call, err := lowerItem(llm.ToolCall{ID: id, Name: "lookup", Arguments: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := lowerItem(llm.ToolResult{CallID: id, Content: []llm.Part{llm.TextPart{Text: "ok"}}})
	if err != nil {
		t.Fatal(err)
	}
	callID := call["content"].([]any)[0].(map[string]any)["id"].(string)
	resultID := result["content"].([]any)[0].(map[string]any)["tool_use_id"].(string)
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(callID) || callID != resultID {
		t.Fatalf("tool_use.id = %q, tool_use_id = %q", callID, resultID)
	}
}
