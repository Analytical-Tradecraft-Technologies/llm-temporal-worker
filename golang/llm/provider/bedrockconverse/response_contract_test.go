package bedrockconverse

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

func TestMalformedToolOutputReturnsAcceptedErrorWithoutPanic(t *testing.T) {
	valid := types.ToolUseBlock{ToolUseId: aws.String("call"), Name: aws.String("lookup"), Input: document.NewLazyDocument(map[string]any{})}
	tests := map[string]types.ConverseOutput{
		"nil message": (*types.ConverseOutputMemberMessage)(nil),
	}
	blocks := map[string]types.ContentBlock{
		"nil tool":      (*types.ContentBlockMemberToolUse)(nil),
		"nil text":      (*types.ContentBlockMemberText)(nil),
		"missing input": &types.ContentBlockMemberToolUse{Value: types.ToolUseBlock{ToolUseId: valid.ToolUseId, Name: valid.Name}},
		"empty name":    &types.ContentBlockMemberToolUse{Value: types.ToolUseBlock{ToolUseId: valid.ToolUseId, Name: aws.String(""), Input: valid.Input}},
		"empty id":      &types.ContentBlockMemberToolUse{Value: types.ToolUseBlock{ToolUseId: aws.String(""), Name: valid.Name, Input: valid.Input}},
	}
	for name, block := range blocks {
		tests[name] = &types.ConverseOutputMemberMessage{Value: types.Message{Content: []types.ContentBlock{block}}}
	}
	adapter, err := New(&Client{converse: &fakeConverse{}}, "endpoint", DefaultProfile("nova"))
	if err != nil {
		t.Fatal(err)
	}
	for name, output := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := adapter.liftResponse(provider.Call{}, &bedrockruntime.ConverseOutput{Output: output, StopReason: types.StopReasonMalformedToolUse, ServiceTier: &types.ServiceTier{Type: types.ServiceTierTypeDefault}}, "request-id")
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Dispatch != provider.DispatchAccepted {
				t.Fatalf("malformed output lost paid dispatch: %v", err)
			}
		})
	}
}

func TestToolDescriptionIsOmittedOnlyWhenEmpty(t *testing.T) {
	for _, description := range []string{"", "Find a city"} {
		tools, err := lowerTools([]llm.Tool{{Name: "lookup", Description: description, InputSchema: json.RawMessage(`{"type":"object"}`)}})
		if err != nil {
			t.Fatal(err)
		}
		spec := tools[0].(*types.ToolMemberToolSpec).Value
		if description == "" {
			if spec.Description != nil {
				t.Fatal("empty description was sent")
			}
		} else if spec.Description == nil || *spec.Description != description {
			t.Fatal("description was lost")
		}
	}
}

func TestConversePreservesCacheUsage(t *testing.T) {
	adapter, err := New(&Client{converse: &fakeConverse{}}, "endpoint", DefaultProfile("nova"))
	if err != nil {
		t.Fatal(err)
	}
	for _, cached := range []bool{false, true} {
		usage := &types.TokenUsage{InputTokens: aws.Int32(4), OutputTokens: aws.Int32(3)}
		if cached {
			usage.CacheReadInputTokens = aws.Int32(7)
			usage.CacheWriteInputTokens = aws.Int32(5)
		}
		result, err := adapter.liftResponse(provider.Call{}, &bedrockruntime.ConverseOutput{Output: &types.ConverseOutputMemberMessage{Value: types.Message{Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: "answer"}}}}, StopReason: types.StopReasonEndTurn, Usage: usage, ServiceTier: &types.ServiceTier{Type: types.ServiceTierTypeDefault}}, "request-id")
		if err != nil {
			t.Fatal(err)
		}
		want := llm.Usage{InputTokens: 4, OutputTokens: 3}
		if cached {
			want.CacheReadTokens = 7
			want.CacheWriteTokens = 5
		}
		if result.Usage.InputTokens != want.InputTokens || result.Usage.OutputTokens != want.OutputTokens || result.Usage.CacheReadTokens != want.CacheReadTokens || result.Usage.CacheWriteTokens != want.CacheWriteTokens {
			t.Fatalf("usage = %+v, want %+v", result.Usage, want)
		}
	}
}
