package bedrockconverse

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Reasoning the adapter cannot replay is omitted from the output, but never
// silently: the response carries a reasoning_dropped warning (#1504).
func TestLiftOmitsReasoningContentWithADiagnostic(t *testing.T) {
	text := "reasoning text"
	adapter := &Adapter{endpointID: bedrockFixtureEndpoint, profile: mustBedrockConverseProfile(t)}
	call := provider.Call{OperationKey: "op-reasoning", EndpointID: bedrockFixtureEndpoint, Model: bedrockFixtureModel, ServiceClass: llm.ServiceClassStandard}
	for name, test := range map[string]struct {
		content []types.ContentBlock
		dropped string
	}{
		"reasoning then text": {content: []types.ContentBlock{
			&types.ContentBlockMemberReasoningContent{Value: &types.ReasoningContentBlockMemberReasoningText{Value: types.ReasoningTextBlock{Text: &text}}},
			&types.ContentBlockMemberText{Value: "answer"},
		}, dropped: "1"},
		"redacted and text reasoning": {content: []types.ContentBlock{
			&types.ContentBlockMemberReasoningContent{Value: &types.ReasoningContentBlockMemberRedactedContent{Value: []byte("opaque")}},
			&types.ContentBlockMemberReasoningContent{Value: &types.ReasoningContentBlockMemberReasoningText{Value: types.ReasoningTextBlock{Text: &text}}},
			&types.ContentBlockMemberText{Value: "answer"},
		}, dropped: "2"},
		"no reasoning": {content: []types.ContentBlock{&types.ContentBlockMemberText{Value: "answer"}}},
	} {
		t.Run(name, func(t *testing.T) {
			response := bedrockConverseResponse("unused", 3, 4)
			response.Output = &types.ConverseOutputMemberMessage{Value: types.Message{Role: types.ConversationRoleAssistant, Content: test.content}}
			lifted, err := adapter.liftResponse(call, &response, bedrockFixtureRequestID, nil)
			if err != nil {
				t.Fatalf("reasoning content failed the response: %v", err)
			}
			if len(lifted.Output) != 1 || lifted.Output[0].(llm.Message).Content[0].(llm.TextPart).Text != "answer" || lifted.Usage.OutputTokens != 4 {
				t.Fatalf("lifted output = %#v usage = %#v", lifted.Output, lifted.Usage)
			}
			var found []llm.Diagnostic
			for _, diagnostic := range lifted.Diagnostics {
				if diagnostic.Code == "reasoning_dropped" {
					found = append(found, diagnostic)
				}
			}
			if test.dropped == "" {
				if len(found) != 0 {
					t.Fatalf("diagnostics = %#v, want none", lifted.Diagnostics)
				}
				return
			}
			if len(found) != 1 || found[0].Severity != llm.DiagnosticWarning || found[0].Details["blocks"] != test.dropped {
				t.Fatalf("diagnostics = %#v, want one reasoning_dropped warning for %s blocks", lifted.Diagnostics, test.dropped)
			}
			if _, err := json.Marshal(lifted); err != nil {
				t.Fatalf("lifted response does not encode: %v", err)
			}
		})
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
			var mapped *provider.Error
			if strings.Contains(test.wantErr, "reasoning controls") && (!errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability) {
				t.Fatalf("strict reasoning rejection = %v, want unsupported_capability", err)
			}
		})
	}
}
