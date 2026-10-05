package openairesponses

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// Some providers send an empty string as the arguments of a zero-argument
// tool. The paid response is kept and the call carries the empty object.
func TestEmptyFunctionCallArgumentsLiftAsEmptyObject(t *testing.T) {
	call := provider.Call{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract", OperationKey: "op", ServiceClass: llm.ServiceClassStandard}
	for name, arguments := range map[string]string{"empty": ``, "whitespace": ` \n\t`} {
		t.Run(name, func(t *testing.T) {
			body := `{"id":"resp-empty-args","object":"response","created_at":1,"model":"gpt-contract","status":"completed","service_tier":"default","output":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"ping","arguments":"` + arguments + `","status":"completed"}],"usage":{"input_tokens":10,"output_tokens":8,"total_tokens":18,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}`
			var response responses.Response
			if err := json.Unmarshal([]byte(body), &response); err != nil {
				t.Fatal(err)
			}
			lifted, err := liftResponse(call, &response, "req")
			if err != nil {
				t.Fatalf("empty function call arguments failed the response: %v", err)
			}
			if lifted.Status != llm.ResponseStatusToolCalls || len(lifted.Output) != 1 {
				t.Fatalf("lifted = status %q output %#v", lifted.Status, lifted.Output)
			}
			toolCall, ok := lifted.Output[0].(llm.ToolCall)
			if !ok || toolCall.ID != "call-1" || toolCall.Name != "ping" || string(toolCall.Arguments) != `{}` {
				t.Fatalf("tool call = %#v", lifted.Output[0])
			}

			// A completed response whose call is explicitly unfinished is
			// contradictory; blank arguments are not completed for it. A
			// call without a status keeps the normalization.
			for status, wantErr := range map[string]bool{"incomplete": true, "in_progress": true, "": false} {
				item := `"status":"` + status + `"}]`
				if status == "" {
					item = `"type":"function_call"}]`
				}
				var contradictory responses.Response
				if err := json.Unmarshal([]byte(strings.Replace(body, `"status":"completed"}]`, item, 1)), &contradictory); err != nil {
					t.Fatal(err)
				}
				lifted, err := liftResponse(call, &contradictory, "req")
				if wantErr != (err != nil) {
					t.Fatalf("call status %q: lifted = %#v, error = %v", status, lifted.Output, err)
				}
			}

			// A call cut off by the output limit before any argument was
			// written is still dropped.
			cutOff := strings.Replace(body, `"status":"completed","service_tier"`, `"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"service_tier"`, 1)
			cutOff = strings.Replace(cutOff, `"status":"completed"}]`, `"status":"incomplete"}]`, 1)
			var truncated responses.Response
			if err := json.Unmarshal([]byte(cutOff), &truncated); err != nil {
				t.Fatal(err)
			}
			lifted, err = liftResponse(call, &truncated, "req")
			if err != nil {
				t.Fatal(err)
			}
			if lifted.Status != llm.ResponseStatusLength || len(lifted.Output) != 0 {
				t.Fatalf("cut-off lifted = status %q output %#v", lifted.Status, lifted.Output)
			}
		})
	}
}
