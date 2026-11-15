package anthropicmessages

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/compaction"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func TestLiftPreservesThinkingToolOrderUsageAndActualTier(t *testing.T) {
	profile := mustProfile(t, testProfile())
	messagestest.LiftPreservesThinkingToolOrderUsageAndActualTier(t, provider.FamilyAnthropicMessages, "anthropic-messages:msg_1", func(response *anthropic.Message, requestID string) (llm.Response, error) {
		return profile.liftResponse(liftCall("anthropic-lift", llm.ServiceClassPriority), response, requestID)
	})
}

func TestLiftEmitsContinuationOnlyForReplayableProviderState(t *testing.T) {
	profile := mustProfile(t, testProfile())
	messagestest.LiftEmitsContinuationOnlyForReplayableProviderState(t, "standard", func(response *anthropic.Message, requestID string) (llm.Response, error) {
		return profile.liftResponse(liftCall("anthropic-continuation", llm.ServiceClassStandard), response, requestID)
	})
}

func TestLiftMapsTerminalReasons(t *testing.T) {
	profile := mustProfile(t, testProfile())
	call := liftCall("anthropic-status", llm.ServiceClassStandard)
	for _, test := range []struct {
		reason string
		want   llm.ResponseStatus
	}{
		{reason: "end_turn", want: llm.ResponseStatusCompleted},
		{reason: "stop_sequence", want: llm.ResponseStatusCompleted},
		{reason: "max_tokens", want: llm.ResponseStatusLength},
		{reason: "pause_turn", want: llm.ResponseStatusPaused},
		{reason: "refusal", want: llm.ResponseStatusRefused},
	} {
		response := anthropic.Message{ID: "status-" + test.reason, Model: "claude-contract", StopReason: anthropic.StopReason(test.reason), Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTierStandard}}
		if test.reason == "refusal" {
			response.StopDetails.Category = anthropic.RefusalStopDetailsCategoryCyber
		}
		got, err := profile.liftResponse(call, &response, "req")
		if err != nil || got.Status != test.want {
			t.Fatalf("reason %q = %#v, %v", test.reason, got, err)
		}
	}
}

// Anthropic documents usage.service_tier as nullable. A null, missing or
// unrecognized tier is not evidence of any class, so the paid response lifts
// with service.actual omitted and the raw label kept for audit.
func TestLiftAcceptsUnmappableServiceTier(t *testing.T) {
	profile := mustProfile(t, testProfile())
	call := liftCall("anthropic-tier", llm.ServiceClassStandard)
	for _, test := range []struct {
		name  string
		usage string
		want  string
	}{
		{name: "null", usage: `"service_tier":null`, want: ""},
		{name: "missing", usage: `"input_tokens":1`, want: ""},
		{name: "unrecognized", usage: `"service_tier":"scale"`, want: "scale"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := contract.DecodeJSON[anthropic.Message](t, []byte(`{"id":"msg-tier","type":"message","role":"assistant","model":"claude-contract","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{`+test.usage+`}}`))
			got, err := profile.liftResponse(call, &response, "req")
			if err != nil {
				t.Fatalf("lift: %v", err)
			}
			if got.Status != llm.ResponseStatusCompleted || got.Service.Actual != nil || got.Service.ProviderValue != test.want {
				t.Fatalf("service = %#v status = %q, want no actual class and provider value %q", got.Service, got.Status, test.want)
			}
		})
	}

	missing := testProfile()
	missing.MissingActualServiceClass = llm.ServiceClassStandard
	profile = mustProfile(t, missing)
	response := contract.DecodeJSON[anthropic.Message](t, []byte(`{"id":"msg-tier","type":"message","role":"assistant","model":"claude-contract","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"service_tier":null}}`))
	got, err := profile.liftResponse(call, &response, "req")
	if err != nil || got.Service.Actual == nil || *got.Service.Actual != llm.ServiceClassStandard {
		t.Fatalf("missing actual class = %#v, %v", got.Service, err)
	}
}

func TestLiftRejectsMalformedToolArgumentsAndUnknownStopReason(t *testing.T) {
	profile := mustProfile(t, testProfile())
	call := liftCall("anthropic-invalid", llm.ServiceClassStandard)
	response := anthropic.Message{ID: "bad-tool", Model: "claude-contract", StopReason: anthropic.StopReasonToolUse, Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTierStandard}, Content: []anthropic.ContentBlockUnion{{Type: "tool_use", ID: "toolu", Name: "lookup", Input: json.RawMessage(`{`)}}}
	_, err := profile.liftResponse(call, &response, "req")
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse {
		t.Fatalf("malformed tool args error = %#v", err)
	}
	unknown := anthropic.Message{ID: "bad-stop", Model: "claude-contract", StopReason: anthropic.StopReason("future_reason"), Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTierStandard}}
	_, err = profile.liftResponse(call, &unknown, "req")
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse {
		t.Fatalf("unknown stop reason error = %#v", err)
	}
}

func TestMixedToolResponseSupportsCheckpointAndCompaction(t *testing.T) {
	var response anthropic.Message
	err := json.Unmarshal([]byte(`{
 "id":"mixed", "type":"message", "role":"assistant", "model":"claude-contract",
 "content":[
 {"type":"tool_use","id":"a","name":"lookup","input":{}},
 {"type":"text","text":"Checking another source"},
 {"type":"tool_use","id":"b","name":"lookup","input":{}}
 ], "stop_reason":"tool_use", "usage":{"input_tokens":5,"output_tokens":5,"service_tier":"standard"}
 }`), &response)
	if err != nil {
		t.Fatal(err)
	}
	profile := mustProfile(t, testProfile())
	lifted := mustLift(t, profile, liftCall("mixed", llm.ServiceClassStandard), &response, "request-mixed")
	if lifted.Status != llm.ResponseStatusToolCalls {
		t.Fatalf("status = %v", lifted.Status)
	}
	pending, err := state.ValidateTranscript(lifted.Output)
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending = %v, error = %v", pending, err)
	}
	items := append([]llm.Item{}, lifted.Output...)
	items = append(items, llm.ToolResult{CallID: "a"}, llm.ToolResult{CallID: "b"})
	selection, err := compaction.SelectPrefix(items, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Prefix) != len(items) || len(selection.Retained) != 0 {
		t.Fatalf("selection = %#v", selection)
	}
}

// liftCall is a call for the contract model on the anthropic-prod endpoint.
func liftCall(operationKey string, class llm.ServiceClass) provider.Call {
	return provider.Call{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract", OperationKey: operationKey, ServiceClass: class}
}

// mustLift lifts response for call with profile and fails t on error.
func mustLift(t *testing.T, profile Profile, call provider.Call, response *anthropic.Message, requestID string) llm.Response {
	t.Helper()
	lifted, err := profile.liftResponse(call, response, requestID)
	if err != nil {
		t.Fatal(err)
	}
	return lifted
}
