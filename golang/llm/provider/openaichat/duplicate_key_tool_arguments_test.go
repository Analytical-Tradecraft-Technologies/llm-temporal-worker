package openaichat

import (
	"encoding/json"
	"strings"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func TestDuplicateKeyToolArgumentsAreAnAcceptedInvalidResponse(t *testing.T) {
	body := `{"id":"duplicate","object":"chat.completion","created":1,"model":"chat-model","service_tier":"default","usage":{"prompt_tokens":10,"completion_tokens":8,"total_tokens":18},"choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"a\",\"q\":\"b\"}"}}]}}]}`
	response := contract.DecodeJSON[openai.ChatCompletion](t, []byte(body))
	_, err := liftStandard(&response)
	adaptertest.RequireLiftInvalidResponse(t, err, "duplicate-key tool arguments error")

	// On a length truncation the paid response is kept and only the unusable
	// call is dropped, exactly as for arguments cut off mid-JSON.
	response.Choices[0].FinishReason = "length"
	lifted, err := liftStandard(&response)
	if err != nil || lifted.Status != llm.ResponseStatusLength || lifted.Usage.OutputTokens != 8 {
		t.Fatalf("truncated response = status %q usage %#v err %v", lifted.Status, lifted.Usage, err)
	}
	for _, item := range lifted.Output {
		if _, ok := item.(llm.ToolCall); ok {
			t.Fatalf("duplicate-key tool call was kept: %#v", lifted.Output)
		}
	}
}

func TestToolArgumentsDeeperThanTheToolArgumentDepthAreAnAcceptedInvalidResponse(t *testing.T) {
	response := func(depth int) *openai.ChatCompletion {
		arguments := strings.Repeat(`{"a":`, depth-1) + `{}` + strings.Repeat(`}`, depth-1)
		encoded, _ := json.Marshal(arguments)
		body := `{"id":"deep","object":"chat.completion","created":1,"model":"chat-model","service_tier":"default","usage":{"prompt_tokens":10,"completion_tokens":8,"total_tokens":18},"choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":` + string(encoded) + `}}]}}]}`
		response := contract.DecodeJSON[openai.ChatCompletion](t, []byte(body))
		return &response
	}
	lifted, err := liftStandard(response(llm.MaxToolArgumentsDepth))
	if err != nil || len(lifted.Output) != 1 {
		t.Fatalf("tool arguments at the depth limit = %#v, %v", lifted.Output, err)
	}
	_, err = liftStandard(response(llm.MaxToolArgumentsDepth + 1))
	adaptertest.RequireLiftInvalidResponse(t, err, "too-deep tool arguments error")
}
