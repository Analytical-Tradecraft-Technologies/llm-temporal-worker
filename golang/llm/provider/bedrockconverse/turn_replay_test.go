package bedrockconverse

import (
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

func TestReplayGroupsParallelToolTurnWithoutReordering(t *testing.T) {
	original := []types.ContentBlock{
		&types.ContentBlockMemberText{Value: "Before tools"},
		&types.ContentBlockMemberToolUse{Value: types.ToolUseBlock{ToolUseId: aws.String("a"), Name: aws.String("lookup"), Input: document.NewLazyDocument(map[string]any{"city": "Sydney"})}},
		&types.ContentBlockMemberText{Value: "Between tools"},
		&types.ContentBlockMemberToolUse{Value: types.ToolUseBlock{ToolUseId: aws.String("b"), Name: aws.String("lookup"), Input: document.NewLazyDocument(map[string]any{"city": "Melbourne"})}},
	}
	lifted, hasTools, err := liftOutput(&types.ConverseOutputMemberMessage{Value: types.Message{Role: types.ConversationRoleAssistant, Content: original}})
	if err != nil || !hasTools {
		t.Fatalf("lift tool turn: %v", err)
	}
	text := func(actor llm.Actor, content string) llm.Item {
		return llm.Message{Actor: actor, Content: []llm.Part{llm.TextPart{Text: content}}}
	}
	items := []llm.Item{text(llm.ActorHuman, "First question"), text(llm.ActorHuman, "More context")}
	items = append(items, lifted...)
	items = append(items, llm.ToolResult{CallID: "a", Content: []llm.Part{llm.TextPart{Text: "Sydney result"}}}, llm.ToolResult{CallID: "b", Content: []llm.Part{llm.TextPart{Text: "Melbourne result"}}}, text(llm.ActorModel, "Final answer"), text(llm.ActorHuman, "Follow up"))
	before, err := llm.NormalizeRequest(llm.Request{OperationKey: "converse-turn-replay", Model: "amazon.nova-pro-v1:0", Input: items})
	if err != nil {
		t.Fatal(err)
	}
	input, err := lowerRequest(before, DefaultProfile("nova"), string(types.ServiceTierTypeDefault), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Messages) != 5 {
		t.Fatalf("message count=%d, want five alternating turns", len(input.Messages))
	}
	wantRoles := []types.ConversationRole{types.ConversationRoleUser, types.ConversationRoleAssistant, types.ConversationRoleUser, types.ConversationRoleAssistant, types.ConversationRoleUser}
	for i, role := range wantRoles {
		if input.Messages[i].Role != role {
			t.Fatalf("turn %d has role %s", i, input.Messages[i].Role)
		}
	}
	if len(input.Messages[0].Content) != 2 || len(input.Messages[1].Content) != 4 || len(input.Messages[2].Content) != 2 {
		t.Fatal("turn grouping lost content")
	}
	replayed, _, err := liftOutput(&types.ConverseOutputMemberMessage{Value: input.Messages[1]})
	if err != nil || !reflect.DeepEqual(replayed, lifted) {
		t.Fatalf("assistant replay changed content order or tool arguments: %v", err)
	}
	for i, id := range []string{"a", "b"} {
		block, ok := input.Messages[2].Content[i].(*types.ContentBlockMemberToolResult)
		if !ok || *block.Value.ToolUseId != id {
			t.Fatal("parallel tool results changed order")
		}
	}
	if input.Messages[0].Content[0].(*types.ContentBlockMemberText).Value != "First question" || input.Messages[0].Content[1].(*types.ContentBlockMemberText).Value != "More context" {
		t.Fatal("adjacent human content changed")
	}
	after, err := llm.NormalizeRequest(llm.Request{OperationKey: "converse-turn-replay", Model: "amazon.nova-pro-v1:0", Input: items})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("lowering mutated the caller transcript")
	}
}
