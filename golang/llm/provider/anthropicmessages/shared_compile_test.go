package anthropicmessages

import (
	"context"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

// compileShared compiles on an Anthropic Messages adapter whose contract
// profile expects request.Model, for the tests shared with Bedrock Messages.
func compileShared(t *testing.T, request llm.Request, strict bool) (provider.Call, error) {
	t.Helper()
	profile := testProfile()
	profile.ExpectedModel = request.Model
	adapter := &Adapter{endpointID: "anthropic-test", profile: mustProfile(t, profile)}
	return adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-test", Family: provider.FamilyAnthropicMessages, Model: request.Model},
		Strict:  strict,
	})
}

func TestCompileRejectsIncompatibleManualThinkingCombinations(t *testing.T) {
	messagestest.ManualThinkingCombinations(t, compileShared)
}

func TestCompileRejectsSamplingTheModelRefuses(t *testing.T) {
	messagestest.SamplingTheModelRefuses(t, compileShared)
}

func TestCompileKeepsSamplingTheModelAccepts(t *testing.T) {
	messagestest.SamplingTheModelAccepts(t, compileShared)
}

func TestCompileSendsExtraHighEffortToModelsThatAcceptIt(t *testing.T) {
	messagestest.ExtraHighEffortByModel(t, compileShared)
}

func TestCompileSkipsReplayedEmptyModelMessage(t *testing.T) {
	messagestest.SkipsReplayedEmptyModelMessage(t, compileShared)
}

func TestCompileSkipsReplayedReferenceAnnotations(t *testing.T) {
	messagestest.SkipsReplayedReferenceAnnotations(t, compileShared)
}

func TestCompileModelessReasoningNeverEnablesThinking(t *testing.T) {
	messagestest.ModelessReasoningNeverEnablesThinking(t, compileShared)
}

func TestCompileReasoningSummaryMapsToThinkingDisplay(t *testing.T) {
	messagestest.ReasoningSummaryMapsToThinkingDisplay(t, compileShared)
}

func TestCompileExtraHighReturnsUnsupportedCapability(t *testing.T) {
	messagestest.ExtraHighReturnsUnsupportedCapability(t, compileShared)
}

func TestCompileDisabledReasoningIgnoresEffort(t *testing.T) {
	messagestest.DisabledReasoningIgnoresEffort(t, compileShared)
}
