package bedrockmessages

import (
	"encoding/json"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestDuplicateKeyToolInputIsAnAcceptedInvalidResponse(t *testing.T) {
	profile := mustBedrockProfile(t, "")
	call := liftCall("bedrock-duplicate", llm.ServiceClassStandard)
	response := anthropic.Message{ID: "duplicate", Model: "claude-contract", StopReason: anthropic.StopReasonToolUse, Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTier("default")}, Content: []anthropic.ContentBlockUnion{{Type: "tool_use", ID: "toolu", Name: "lookup", Input: json.RawMessage(`{"q":"a","q":"b"}`)}}}
	_, err := profile.liftResponse(call, &response, "req")
	adaptertest.RequireLiftInvalidResponse(t, err, "duplicate-key tool input error")
}
