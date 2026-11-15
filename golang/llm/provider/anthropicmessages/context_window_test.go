package anthropicmessages

import (
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func TestContextWindowExceededLiftsAsLengthWithOutputAndUsage(t *testing.T) {
	response := contract.DecodeJSON[anthropic.Message](t, mustReadFixture(t, "response.completed.json"))
	response.StopReason = anthropic.StopReasonModelContextWindowExceeded
	lifted := mustLift(t, mustProfile(t, testProfile()), liftCall("fixture-op", llm.ServiceClassStandard), &response, "req-fixture")
	if lifted.Status != llm.ResponseStatusLength || len(lifted.Output) == 0 || lifted.Usage.InputTokens == 0 || lifted.Usage.OutputTokens == 0 {
		t.Fatalf("lifted = status %q output %d usage %#v", lifted.Status, len(lifted.Output), lifted.Usage)
	}
}
