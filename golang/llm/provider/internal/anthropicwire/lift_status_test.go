package anthropicwire

import (
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestLiftStatusPauseTurnAndStopReasons(t *testing.T) {
	cases := []struct {
		name               string
		reason             anthropic.StopReason
		toolCalls, refusal bool
		pauseTurnPauses    bool
		want               llm.ResponseStatus
		wantErr            bool
	}{
		{name: "pause turn pauses", reason: anthropic.StopReasonPauseTurn, pauseTurnPauses: true, want: llm.ResponseStatusPaused},
		{name: "pause turn ends turn", reason: anthropic.StopReasonPauseTurn, want: llm.ResponseStatusCompleted},
		{name: "pause turn refusal", reason: anthropic.StopReasonPauseTurn, refusal: true, want: llm.ResponseStatusRefused},
		{name: "end turn tool calls", reason: anthropic.StopReasonEndTurn, toolCalls: true, pauseTurnPauses: true, want: llm.ResponseStatusToolCalls},
		{name: "stop sequence", reason: anthropic.StopReasonStopSequence, want: llm.ResponseStatusCompleted},
		{name: "tool use", reason: anthropic.StopReasonToolUse, toolCalls: true, want: llm.ResponseStatusToolCalls},
		{name: "tool use without call", reason: anthropic.StopReasonToolUse, wantErr: true},
		{name: "max tokens", reason: anthropic.StopReasonMaxTokens, want: llm.ResponseStatusLength},
		{name: "context window", reason: anthropic.StopReasonModelContextWindowExceeded, want: llm.ResponseStatusLength},
		{name: "refusal", reason: anthropic.StopReasonRefusal, want: llm.ResponseStatusRefused},
		{name: "unknown", reason: "mystery", wantErr: true},
	}
	for _, tc := range cases {
		got, err := LiftStatus(tc.reason, tc.toolCalls, tc.refusal, tc.pauseTurnPauses)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("%s: LiftStatus = %q, %v; want %q, error %v", tc.name, got, err, tc.want, tc.wantErr)
		}
	}
}
