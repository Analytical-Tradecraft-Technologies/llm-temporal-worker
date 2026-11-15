package anthropicmessages

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestDuplicateKeyToolInputIsAnAcceptedInvalidResponse(t *testing.T) {
	profile := mustProfile(t, testProfile())
	call := liftCall("anthropic-duplicate", llm.ServiceClassStandard)
	response := anthropic.Message{ID: "duplicate", Model: "claude-contract", StopReason: anthropic.StopReasonToolUse, Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTierStandard}, Content: []anthropic.ContentBlockUnion{{Type: "tool_use", ID: "toolu", Name: "lookup", Input: json.RawMessage(`{"q":"a","q":"b"}`)}}}
	_, err := profile.liftResponse(call, &response, "req")
	adaptertest.RequireLiftInvalidResponse(t, err, "duplicate-key tool input error")
}

func TestToolInputDeeperThanTheToolArgumentDepthIsAnAcceptedInvalidResponse(t *testing.T) {
	profile := mustProfile(t, testProfile())
	call := liftCall("anthropic-deep", llm.ServiceClassStandard)
	nested := func(depth int) json.RawMessage {
		return json.RawMessage(strings.Repeat(`{"a":`, depth-1) + `{}` + strings.Repeat(`}`, depth-1))
	}
	response := func(depth int) *anthropic.Message {
		return &anthropic.Message{ID: "deep", Model: "claude-contract", StopReason: anthropic.StopReasonToolUse, Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTierStandard}, Content: []anthropic.ContentBlockUnion{{Type: "tool_use", ID: "toolu", Name: "lookup", Input: nested(depth)}}}
	}
	lifted, err := profile.liftResponse(call, response(llm.MaxToolArgumentsDepth), "req")
	if err != nil || len(lifted.Output) != 1 {
		t.Fatalf("tool input at the depth limit = %#v, %v", lifted.Output, err)
	}
	_, err = profile.liftResponse(call, response(llm.MaxToolArgumentsDepth+1), "req")
	adaptertest.RequireLiftInvalidResponse(t, err, "too-deep tool input error")
}
