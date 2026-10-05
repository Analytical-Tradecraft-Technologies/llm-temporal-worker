package anthropicmessages

import (
	"encoding/json"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestContextWindowExceededLiftsAsLengthWithOutputAndUsage(t *testing.T) {
	var response anthropic.Message
	if err := json.Unmarshal(mustReadFixture(t, "response.completed.json"), &response); err != nil {
		t.Fatal(err)
	}
	response.StopReason = anthropic.StopReasonModelContextWindowExceeded
	call := provider.Call{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract", OperationKey: "fixture-op", ServiceClass: llm.ServiceClassStandard}
	lifted, err := mustProfile(t, testProfile()).liftResponse(call, &response, "req-fixture")
	if err != nil {
		t.Fatal(err)
	}
	if lifted.Status != llm.ResponseStatusLength || len(lifted.Output) == 0 || lifted.Usage.InputTokens == 0 || lifted.Usage.OutputTokens == 0 {
		t.Fatalf("lifted = status %q output %d usage %#v", lifted.Status, len(lifted.Output), lifted.Usage)
	}
}
