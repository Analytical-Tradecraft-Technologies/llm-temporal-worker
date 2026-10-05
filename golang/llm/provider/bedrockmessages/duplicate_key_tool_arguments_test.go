package bedrockmessages

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestDuplicateKeyToolInputIsAnAcceptedInvalidResponse(t *testing.T) {
	profile := mustBedrockProfile(t, "")
	call := provider.Call{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract", OperationKey: "bedrock-duplicate", ServiceClass: llm.ServiceClassStandard}
	response := anthropic.Message{ID: "duplicate", Model: "claude-contract", StopReason: anthropic.StopReasonToolUse, Usage: anthropic.Usage{ServiceTier: anthropic.UsageServiceTier("default")}, Content: []anthropic.ContentBlockUnion{{Type: "tool_use", ID: "toolu", Name: "lookup", Input: json.RawMessage(`{"q":"a","q":"b"}`)}}}
	_, err := profile.liftResponse(call, &response, "req")
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Phase != provider.PhaseLift || mapped.Dispatch != provider.DispatchAccepted {
		t.Fatalf("duplicate-key tool input error = %#v", err)
	}
}
