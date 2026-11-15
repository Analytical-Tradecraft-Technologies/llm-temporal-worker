package openaichat

import (
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func TestLengthTruncatedToolCallKeepsTheResponseAsLength(t *testing.T) {
	body := `{"id":"truncated","object":"chat.completion","created":1,"model":"chat-model","service_tier":"default","usage":{"prompt_tokens":10,"completion_tokens":64,"total_tokens":74},"choices":[{"index":0,"finish_reason":"length","message":{"role":"assistant","content":"Looking that up","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"q\": \"abc"}}]}}]}`
	truncated := contract.DecodeJSON[openai.ChatCompletion](t, []byte(body))
	lifted, err := liftStandard(&truncated)
	if err != nil {
		t.Fatalf("truncated tool call failed the response: %v", err)
	}
	if lifted.Status != llm.ResponseStatusLength || lifted.Usage.OutputTokens != 64 {
		t.Fatalf("lifted = status %q usage %#v", lifted.Status, lifted.Usage)
	}
	for _, item := range lifted.Output {
		if _, ok := item.(llm.ToolCall); ok {
			t.Fatalf("incomplete tool call was kept: %#v", lifted.Output)
		}
	}

	// Invalid arguments on a non-truncated response are still rejected.
	truncated.Choices[0].FinishReason = "tool_calls"
	if _, err := liftStandard(&truncated); err == nil {
		t.Fatal("invalid tool arguments accepted outside a length truncation")
	}
}
