package bedrockmessages

import (
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestContextWindowExceededLiftsAsLengthWithOutputAndUsage(t *testing.T) {
	adapter := fixtureBedrockAdapter(t)
	call := compileBedrockFixture(t, adapter, loadBedrockContractRequest(t, "request.semantic.json"))
	response := loadBedrockContractResponse(t, "response.completed.json")
	response.StopReason = anthropic.StopReasonModelContextWindowExceeded
	lifted, err := adapter.profile.liftResponse(call, &response, bedrockFixtureRequestID)
	if err != nil {
		t.Fatal(err)
	}
	if lifted.Status != llm.ResponseStatusLength || len(lifted.Output) == 0 || lifted.Usage.InputTokens == 0 || lifted.Usage.OutputTokens == 0 {
		t.Fatalf("lifted = status %q output %d usage %#v", lifted.Status, len(lifted.Output), lifted.Usage)
	}
}
