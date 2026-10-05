package openairesponses

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestIncompleteResponseDropsCutOffFunctionCall(t *testing.T) {
	body := `{"id":"resp-truncated","object":"response","created_at":1,"model":"gpt-contract","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"service_tier":"default","output":[{"type":"message","id":"msg-1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"Looking that up","annotations":[]}]},{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup","arguments":"{\"q\": \"abc","status":"incomplete"}],"usage":{"input_tokens":10,"output_tokens":64,"total_tokens":74,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}`
	var response responses.Response
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		t.Fatal(err)
	}
	call := provider.Call{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract", OperationKey: "op", ServiceClass: llm.ServiceClassStandard}
	lifted, err := liftResponse(call, &response, "req")
	if err != nil {
		t.Fatalf("cut-off function call failed the response: %v", err)
	}
	if lifted.Status != llm.ResponseStatusLength || lifted.Usage.OutputTokens != 64 || len(lifted.Output) != 1 {
		t.Fatalf("lifted = status %q usage %#v output %#v", lifted.Status, lifted.Usage, lifted.Output)
	}

	var control responses.Response
	if err := json.Unmarshal([]byte(replaceStatus(body)), &control); err != nil {
		t.Fatal(err)
	}
	if _, err := liftResponse(call, &control, "req"); err == nil {
		t.Fatal("invalid function arguments accepted on a completed response")
	}

	// An incomplete response must not hide a completed call's invalid arguments.
	var completedCall responses.Response
	if err := json.Unmarshal([]byte(strings.Replace(body, `"status":"incomplete"}]`, `"status":"completed"}]`, 1)), &completedCall); err != nil {
		t.Fatal(err)
	}
	if _, err := liftResponse(call, &completedCall, "req"); err == nil {
		t.Fatal("completed function call with invalid arguments was dropped")
	}
}

func replaceStatus(body string) string {
	var fields map[string]any
	_ = json.Unmarshal([]byte(body), &fields)
	fields["status"] = "completed"
	delete(fields, "incomplete_details")
	encoded, _ := json.Marshal(fields)
	return string(encoded)
}
