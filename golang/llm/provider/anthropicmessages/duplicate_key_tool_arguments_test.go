package anthropicmessages

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestDuplicateKeyToolInputIsAnAcceptedInvalidResponse(t *testing.T) {
	profile := mustProfile(t, testProfile())
	call := provider.Call{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract", OperationKey: "anthropic-duplicate", ServiceClass: llm.ServiceClassStandard}
	response := anthropic.Message{ID: "duplicate", Model: "claude-contract", StopReason: anthropic.StopReasonToolUse, Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTierStandard}, Content: []anthropic.ContentBlockUnion{{Type: "tool_use", ID: "toolu", Name: "lookup", Input: json.RawMessage(`{"q":"a","q":"b"}`)}}}
	_, err := profile.liftResponse(call, &response, "req")
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Phase != provider.PhaseLift || mapped.Dispatch != provider.DispatchAccepted {
		t.Fatalf("duplicate-key tool input error = %#v", err)
	}
}

func TestToolInputDeeperThanTheToolArgumentDepthIsAnAcceptedInvalidResponse(t *testing.T) {
	profile := mustProfile(t, testProfile())
	call := provider.Call{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract", OperationKey: "anthropic-deep", ServiceClass: llm.ServiceClassStandard}
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
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Phase != provider.PhaseLift || mapped.Dispatch != provider.DispatchAccepted {
		t.Fatalf("too-deep tool input error = %#v", err)
	}
}
