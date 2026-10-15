package bedrockmessages

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Bedrock Messages has no extension allowlist, so an extension must never be
// dropped silently: it is rejected before dispatch in both portability modes.
func TestCompileRejectsExtensions(t *testing.T) {
	for _, strict := range []bool{false, true} {
		adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
		call, err := adapter.Compile(context.Background(), provider.CompileInput{
			Request: llm.Request{
				OperationKey: "bedrock-extension",
				Model:        "claude-contract",
				Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
				Extensions:   map[string]json.RawMessage{"anthropic": json.RawMessage(`{"temperature":1.7}`)},
			},
			Query:  provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"},
			Strict: strict,
		})
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.Phase != provider.PhaseCompile || mapped.Dispatch != provider.DispatchNotDispatched || !strings.Contains(mapped.SafeMessage, "extensions are not supported") {
			t.Fatalf("strict=%v Compile() = %v, want extension rejection before dispatch", strict, err)
		}
		if call.SDKParams != nil {
			t.Fatalf("strict=%v rejected extension produced dispatch parameters", strict)
		}
	}
}
