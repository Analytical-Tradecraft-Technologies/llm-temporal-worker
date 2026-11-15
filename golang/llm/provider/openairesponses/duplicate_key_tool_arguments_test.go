package openairesponses

import (
	"strings"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func TestDuplicateKeyFunctionArgumentsAreAnAcceptedInvalidResponse(t *testing.T) {
	body := `{"id":"resp-duplicate","object":"response","created_at":1,"model":"gpt-contract","status":"completed","service_tier":"default","output":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup","arguments":"{\"q\":\"a\",\"q\":\"b\"}","status":"completed"}],"usage":{"input_tokens":10,"output_tokens":8,"total_tokens":18,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}`
	response := contract.DecodeJSON[responses.Response](t, []byte(body))
	call := liftCall("op", llm.ServiceClassStandard)
	_, err := liftResponse(call, &response, "req")
	adaptertest.RequireLiftInvalidResponse(t, err, "duplicate-key function arguments error")

	// An incomplete response keeps its paid output and drops only a call that
	// was itself cut off; a completed call is still a malformed response.
	incomplete := strings.Replace(body, `"status":"completed","service_tier"`, `"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"service_tier"`, 1)
	completedCall := contract.DecodeJSON[responses.Response](t, []byte(incomplete))
	if _, err := liftResponse(call, &completedCall, "req"); err == nil {
		t.Fatal("completed function call with duplicate-key arguments was dropped")
	}
	cutOff := contract.DecodeJSON[responses.Response](t, []byte(strings.Replace(incomplete, `"status":"completed"}]`, `"status":"incomplete"}]`, 1)))
	lifted, err := liftResponse(call, &cutOff, "req")
	if err != nil || lifted.Status != llm.ResponseStatusLength || lifted.Usage.OutputTokens != 8 || len(lifted.Output) != 0 {
		t.Fatalf("cut-off response = status %q usage %#v output %#v err %v", lifted.Status, lifted.Usage, lifted.Output, err)
	}
}
