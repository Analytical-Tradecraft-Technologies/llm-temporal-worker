package openaichat

import (
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// Some providers send an empty string as the arguments of a zero-argument
// tool. The paid response is kept and the call carries the empty object.
func TestEmptyToolArgumentsLiftAsEmptyObject(t *testing.T) {
	for name, arguments := range map[string]string{"empty": ``, "whitespace": ` \n\t`} {
		t.Run(name, func(t *testing.T) {
			body := `{"id":"empty-args","object":"chat.completion","created":1,"model":"chat-model","service_tier":"default","usage":{"prompt_tokens":10,"completion_tokens":8,"total_tokens":18},"choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":null,"tool_calls":[{"id":"call-1","type":"function","function":{"name":"ping","arguments":"` + arguments + `"}}]}}]}`
			response := testassert.UnmarshalJSON[openai.ChatCompletion](t, []byte(body))
			lifted, err := liftStandard(&response)
			testassert.NoError(t, err, "empty tool arguments failed the response")
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
			truncated, err := liftStandard(&response)
			testassert.NoError(t, err)
			for _, item := range truncated.Output {
				if _, ok := item.(llm.ToolCall); ok {
					t.Fatalf("cut-off tool call was kept: %#v", truncated.Output)
				}
			}
		})
	}
}
