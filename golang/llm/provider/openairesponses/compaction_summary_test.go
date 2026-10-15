package openairesponses

import (
	"context"
	"fmt"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/compaction"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Reasoning models always emit a reasoning item before the message. The
// summarizer request leaves reasoning unset, so the lifted provider state
// arrives beside the summary text and must not make the summary unusable.
// A truncated summary must still be rejected.
func TestCompactionSummaryIgnoresLiftedReasoning(t *testing.T) {
	const reasoningThenMessage = `{"id":"resp-sum","created_at":1710000000,"model":"gpt-contract","object":"response","status":"%s",%s"output":[` +
		`{"id":"rs-1","type":"reasoning","status":"completed","summary":[],"encrypted_content":null},` +
		`{"id":"msg-1","type":"message","role":"assistant","status":"%s","content":[{"type":"output_text","text":"the summary","annotations":[]}]}` +
		`],"parallel_tool_calls":false,"tool_choice":"none","tools":[],"text":{"format":{"type":"text"}},` +
		`"usage":{"input_tokens":10,"input_tokens_details":{"cached_tokens":0},"output_tokens":7,"output_tokens_details":{"reasoning_tokens":4},"total_tokens":17}}`
	for name, test := range map[string]struct {
		body    string
		summary string
	}{
		"completed": {body: fmt.Sprintf(reasoningThenMessage, "completed", "", "completed"), summary: "the summary"},
		"truncated": {body: fmt.Sprintf(reasoningThenMessage, "incomplete", `"incomplete_details":{"reason":"max_output_tokens"},`, "incomplete")},
	} {
		t.Run(name, func(t *testing.T) {
			adapter := newFixtureAdapter(t, []byte(test.body))
			input := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "a long conversation"}}}}
			request, err := compaction.PrepareRequest(llm.Request{OperationKey: "op", Model: "gpt-contract", Input: input}, "op/compact", input, compaction.DefaultPolicy())
			if err != nil {
				t.Fatal(err)
			}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: request.Model}, Strict: true})
			if err != nil {
				t.Fatal(err)
			}
			result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Response.Output) != 2 {
				t.Fatalf("output = %#v, want reasoning and message", result.Response.Output)
			}
			if _, ok := result.Response.Output[0].(llm.ProviderState); !ok {
				t.Fatalf("output[0] = %#v, want lifted reasoning", result.Response.Output[0])
			}
			summary, err := compaction.PlainTextSummary(result.Response, 1024)
			if test.summary == "" {
				if err == nil {
					t.Fatalf("truncated summary %q accepted", summary)
				}
				return
			}
			if err != nil || summary != test.summary {
				t.Fatalf("summary = %q, err = %v; want %q", summary, err, test.summary)
			}
		})
	}
}
