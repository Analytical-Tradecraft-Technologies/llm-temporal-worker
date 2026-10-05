package openairesponses

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestDuplicateKeyFunctionArgumentsAreAnAcceptedInvalidResponse(t *testing.T) {
	body := `{"id":"resp-duplicate","object":"response","created_at":1,"model":"gpt-contract","status":"completed","service_tier":"default","output":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup","arguments":"{\"q\":\"a\",\"q\":\"b\"}","status":"completed"}],"usage":{"input_tokens":10,"output_tokens":8,"total_tokens":18,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}`
	var response responses.Response
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		t.Fatal(err)
	}
	call := provider.Call{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract", OperationKey: "op", ServiceClass: llm.ServiceClassStandard}
	_, err := liftResponse(call, &response, "req")
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Phase != provider.PhaseLift || mapped.Dispatch != provider.DispatchAccepted {
		t.Fatalf("duplicate-key function arguments error = %#v", err)
	}
}
