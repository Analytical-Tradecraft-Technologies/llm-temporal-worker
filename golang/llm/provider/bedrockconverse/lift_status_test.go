package bedrockconverse

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestLiftStatusMapsEveryKnownStopReason(t *testing.T) {
	for reason, want := range map[types.StopReason]llm.ResponseStatus{
		types.StopReasonEndTurn:                    llm.ResponseStatusCompleted,
		types.StopReasonStopSequence:               llm.ResponseStatusCompleted,
		types.StopReasonMaxTokens:                  llm.ResponseStatusLength,
		types.StopReasonModelContextWindowExceeded: llm.ResponseStatusLength,
		types.StopReasonMalformedModelOutput:       llm.ResponseStatusLength,
		types.StopReasonMalformedToolUse:           llm.ResponseStatusLength,
		// Filtering is content_filtered on every adapter, not a refusal (#1505).
		types.StopReasonContentFiltered:     llm.ResponseStatusContentFiltered,
		types.StopReasonGuardrailIntervened: llm.ResponseStatusContentFiltered,
	} {
		if got, err := liftStatus(reason, false); err != nil || got != want {
			t.Errorf("liftStatus(%q) = %q, %v; want %q", reason, got, err, want)
		}
	}
	if got, err := liftStatus(types.StopReasonToolUse, true); err != nil || got != llm.ResponseStatusToolCalls {
		t.Errorf("liftStatus(tool_use) = %q, %v", got, err)
	}
}
