package openaichat

import (
	"encoding/json"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Some providers send an empty string as the arguments of a zero-argument
// tool. The paid response is kept and the call carries the empty object.
func TestEmptyToolArgumentsLiftAsEmptyObject(t *testing.T) {
	for name, arguments := range map[string]string{"empty": ``, "whitespace": ` \n\t`} {
		t.Run(name, func(t *testing.T) {
			var response openai.ChatCompletion
			body := `{"id":"empty-args","object":"chat.completion","created":1,"model":"chat-model","service_tier":"default","usage":{"prompt_tokens":10,"completion_tokens":8,"total_tokens":18},"choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":null,"tool_calls":[{"id":"call-1","type":"function","function":{"name":"ping","arguments":"` + arguments + `"}}]}}]}`
			if err := json.Unmarshal([]byte(body), &response); err != nil {
				t.Fatal(err)
			}
			lifted, err := testProfile().liftResponse(provider.Call{ServiceClass: llm.ServiceClassStandard}, &response, "req")
			if err != nil {
				t.Fatalf("empty tool arguments failed the response: %v", err)
			}
			if lifted.Status != llm.ResponseStatusToolCalls || len(lifted.Output) != 1 {
				t.Fatalf("lifted = status %q output %#v", lifted.Status, lifted.Output)
			}
			call, ok := lifted.Output[0].(llm.ToolCall)
			if !ok || call.ID != "call-1" || call.Name != "ping" || string(call.Arguments) != `{}` {
				t.Fatalf("tool call = %#v", lifted.Output[0])
			}

			// A length finish can end a call before any argument is written,
			// so the cut-off call is still dropped there.
			response.Choices[0].FinishReason = "length"
			truncated, err := testProfile().liftResponse(provider.Call{ServiceClass: llm.ServiceClassStandard}, &response, "req")
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range truncated.Output {
				if _, ok := item.(llm.ToolCall); ok {
					t.Fatalf("cut-off tool call was kept: %#v", truncated.Output)
				}
			}
		})
	}
}
