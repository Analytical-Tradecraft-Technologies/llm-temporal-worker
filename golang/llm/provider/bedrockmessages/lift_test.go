package bedrockmessages

import (
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

func TestLiftPreservesThinkingToolOrderUsageAndActualTier(t *testing.T) {
	profile := mustBedrockProfile(t, "")
	messagestest.LiftPreservesThinkingToolOrderUsageAndActualTier(t, provider.FamilyBedrockMessages, "bedrock-messages:msg_1", func(response *anthropic.Message, requestID string) (llm.Response, error) {
		return profile.liftResponse(liftCall("bedrock-lift", llm.ServiceClassPriority), response, requestID)
	})
}

func TestLiftEmitsContinuationOnlyForReplayableProviderState(t *testing.T) {
	profile := mustBedrockProfile(t, "")
	messagestest.LiftEmitsContinuationOnlyForReplayableProviderState(t, "default", func(response *anthropic.Message, requestID string) (llm.Response, error) {
		return profile.liftResponse(liftCall("bedrock-continuation", llm.ServiceClassStandard), response, requestID)
	})
}

func TestLiftMapsTerminalReasonsAndRejectsInvalidProviderFacts(t *testing.T) {
	profile := mustBedrockProfile(t, "")
	call := liftCall("bedrock-status", llm.ServiceClassStandard)
	for _, test := range []struct {
		reason string
		want   llm.ResponseStatus
	}{
		{reason: "end_turn", want: llm.ResponseStatusCompleted},
		{reason: "stop_sequence", want: llm.ResponseStatusCompleted},
		{reason: "max_tokens", want: llm.ResponseStatusLength},
		{reason: "pause_turn", want: llm.ResponseStatusCompleted},
		{reason: "refusal", want: llm.ResponseStatusRefused},
	} {
		response := anthropic.Message{ID: "status-" + test.reason, Model: "claude-contract", StopReason: anthropic.StopReason(test.reason), Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTier("default")}}
		if test.reason == "refusal" {
			response.StopDetails.Category = anthropic.RefusalStopDetailsCategoryCyber
		}
		got, err := profile.liftResponse(call, &response, "req")
		if err != nil || got.Status != test.want {
			t.Fatalf("reason %q = %#v, %v", test.reason, got, err)
		}
	}
	for _, test := range []struct {
		name     string
		response anthropic.Message
	}{
		{name: "tool stop without tool", response: anthropic.Message{ID: "missing-tool", Model: "claude-contract", StopReason: anthropic.StopReasonToolUse, Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTier("default")}}},
		{name: "unknown stop reason", response: anthropic.Message{ID: "unknown-stop", Model: "claude-contract", StopReason: anthropic.StopReason("future_reason"), Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTier("default")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := profile.liftResponse(call, &test.response, "req")
			adaptertest.RequireAcceptedInvalidResponse(t, err, "invalid response error")
		})
	}
}

// A paid response is never discarded over its tier label. InvokeModel reports
// the tier in a response header, so the body tier is optional; a missing or
// unrecognized tier reports no actual class and keeps the raw value.
func TestLiftKeepsUnreportedTierUnclassifiedAndReadsHeaderTier(t *testing.T) {
	profile := mustBedrockProfile(t, "")
	call := liftCall("bedrock-tier", llm.ServiceClassPriority)
	standard, priority := llm.ServiceClassStandard, llm.ServiceClassPriority
	for _, test := range []struct {
		name, body, header, wantValue string
		want                          *llm.ServiceClass
	}{
		{name: "no tier anywhere"},
		{name: "header only", header: "priority", wantValue: "priority", want: &priority},
		{name: "body wins over header", body: "default", header: "priority", wantValue: "default", want: &standard},
		{name: "reserved capacity", header: "reserved", wantValue: "reserved"},
		{name: "unknown body tier", body: "scale", wantValue: "scale"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := anthropic.Message{ID: "tier", Model: "claude-contract", StopReason: anthropic.StopReasonEndTurn, Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTier(test.body)}}
			got, err := profile.liftResponseWithTier(call, &response, "req", test.header)
			testassert.NoError(t, err)
			if got.Service.Attempted != llm.ServiceClassPriority || got.Service.ProviderValue != test.wantValue {
				t.Fatalf("service facts = %+v", got.Service)
			}
			if (got.Service.Actual == nil) != (test.want == nil) || (test.want != nil && *got.Service.Actual != *test.want) {
				t.Fatalf("actual class = %v, want %v", got.Service.Actual, test.want)
			}
		})
	}
}

// liftCall is a call for the contract model on the bedrock-prod endpoint.
func liftCall(operationKey string, class llm.ServiceClass) provider.Call {
	return provider.Call{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract", OperationKey: operationKey, ServiceClass: class}
}
