package bedrockconverse

import (
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// IDs from other families, such as "functions.lookup:0", violate Converse's
// toolUseId pattern. Lowering remaps them identically on the call and the
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
	callID := *call.Content[0].(*types.ContentBlockMemberToolUse).Value.ToolUseId
	resultID := *result.Content[0].(*types.ContentBlockMemberToolResult).Value.ToolUseId
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(callID) || callID != resultID {
		t.Fatalf("toolUse.toolUseId = %q, toolResult.toolUseId = %q", callID, resultID)
	}
}
