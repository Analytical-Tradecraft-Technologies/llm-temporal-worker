package bedrockconverse

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestLiftOmitsReasoningContentWithoutFailingTheResponse(t *testing.T) {
	text := "reasoning text"
	items, hasTools, err := liftOutput(&types.ConverseOutputMemberMessage{Value: types.Message{Role: types.ConversationRoleAssistant, Content: []types.ContentBlock{
		&types.ContentBlockMemberReasoningContent{Value: &types.ReasoningContentBlockMemberReasoningText{Value: types.ReasoningTextBlock{Text: &text}}},
		&types.ContentBlockMemberText{Value: "answer"},
	}}})
	if err != nil {
		t.Fatalf("reasoning content failed the response: %v", err)
	}
	if hasTools || len(items) != 1 || items[0].(llm.Message).Content[0].(llm.TextPart).Text != "answer" {
		t.Fatalf("lifted output = %#v", items)
	}
}

func TestLoweringRejectsReasoningAndExtensionsItCannotSend(t *testing.T) {
	base := llm.Request{Model: "nova", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hi"}}}}}
	budget := 4000
	for _, test := range []struct {
		name    string
		mutate  func(*llm.Request)
		strict  bool
		wantErr string
	}{
		{name: "extension strict", strict: true, mutate: func(r *llm.Request) { r.Extensions = map[string]json.RawMessage{"bedrock": json.RawMessage(`{}`)} }, wantErr: "extensions are not supported"},
		{name: "extension best effort", mutate: func(r *llm.Request) { r.Extensions = map[string]json.RawMessage{"bedrock": json.RawMessage(`{}`)} }, wantErr: "extensions are not supported"},
		{name: "enabled reasoning strict", strict: true, mutate: func(r *llm.Request) {
			r.Reasoning = &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: &budget}
		}, wantErr: "reasoning controls are not implemented"},
		{name: "effort only strict", strict: true, mutate: func(r *llm.Request) {
			r.Reasoning = &llm.ReasoningSpec{Effort: llm.ReasoningEffortHigh}
		}, wantErr: "reasoning controls are not implemented"},
		{name: "enabled reasoning best effort", mutate: func(r *llm.Request) {
			r.Reasoning = &llm.ReasoningSpec{Mode: llm.ReasoningModeEnabled, TokenBudget: &budget}
		}},
		{name: "provider default strict", strict: true, mutate: func(r *llm.Request) {
			r.Reasoning = &llm.ReasoningSpec{Mode: llm.ReasoningModeProviderDefault}
		}},
		{name: "explicit provider defaults strict", strict: true, mutate: func(r *llm.Request) {
			r.Reasoning = &llm.ReasoningSpec{Mode: llm.ReasoningModeProviderDefault, Effort: llm.ReasoningEffortProviderDefault, Summary: llm.ReasoningSummaryProviderDefault}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := base
			test.mutate(&request)
			_, err := lowerRequest(request, DefaultProfile("nova"), string(types.ServiceTierTypeDefault), test.strict)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("lowerRequest() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("lowerRequest() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}
