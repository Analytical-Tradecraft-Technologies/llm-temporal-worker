package openaichat

import (
	"encoding/json"
	"errors"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
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
}
