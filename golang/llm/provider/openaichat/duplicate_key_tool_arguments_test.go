package openaichat

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestDuplicateKeyToolArgumentsAreAnAcceptedInvalidResponse(t *testing.T) {
	var response openai.ChatCompletion
	body := `{"id":"duplicate","object":"chat.completion","created":1,"model":"chat-model","service_tier":"default","usage":{"prompt_tokens":10,"completion_tokens":8,"total_tokens":18},"choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"a\",\"q\":\"b\"}"}}]}}]}`
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		t.Fatal(err)
	}
	_, err := testProfile().liftResponse(provider.Call{ServiceClass: llm.ServiceClassStandard}, &response, "req")
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Phase != provider.PhaseLift || mapped.Dispatch != provider.DispatchAccepted {
		t.Fatalf("duplicate-key tool arguments error = %#v", err)
	}

	// On a length truncation the paid response is kept and only the unusable
	// call is dropped, exactly as for arguments cut off mid-JSON.
	response.Choices[0].FinishReason = "length"
	lifted, err := testProfile().liftResponse(provider.Call{ServiceClass: llm.ServiceClassStandard}, &response, "req")
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
		var response openai.ChatCompletion
		body := `{"id":"deep","object":"chat.completion","created":1,"model":"chat-model","service_tier":"default","usage":{"prompt_tokens":10,"completion_tokens":8,"total_tokens":18},"choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":` + string(encoded) + `}}]}}]}`
		if err := json.Unmarshal([]byte(body), &response); err != nil {
			t.Fatal(err)
		}
		return &response
	}
	lifted, err := testProfile().liftResponse(provider.Call{ServiceClass: llm.ServiceClassStandard}, response(llm.MaxToolArgumentsDepth), "req")
	if err != nil || len(lifted.Output) != 1 {
		t.Fatalf("tool arguments at the depth limit = %#v, %v", lifted.Output, err)
	}
	_, err = testProfile().liftResponse(provider.Call{ServiceClass: llm.ServiceClassStandard}, response(llm.MaxToolArgumentsDepth+1), "req")
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Phase != provider.PhaseLift || mapped.Dispatch != provider.DispatchAccepted {
		t.Fatalf("too-deep tool arguments error = %#v", err)
	}
}
