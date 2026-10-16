package bedrockmessages

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func compileExtensionRequest(t *testing.T, adapter *Adapter, strict bool, extensions map[string]json.RawMessage) (provider.Call, error) {
	t.Helper()
	return adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{
			OperationKey: "bedrock-extension",
			Model:        "claude-contract",
			Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
			Extensions:   extensions,
		},
		Query:  provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"},
		Strict: strict,
	})
}

// Bedrock Messages has no extension allowlist, so an extension is never
// dropped silently: strict mode rejects it before dispatch.
func TestCompileRejectsExtensionsInStrictMode(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
	call, err := compileExtensionRequest(t, adapter, true, map[string]json.RawMessage{"anthropic": json.RawMessage(`{"temperature":1.7}`)})
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability || mapped.Phase != provider.PhaseCompile ||
		mapped.Dispatch != provider.DispatchNotDispatched || !strings.Contains(mapped.SafeMessage, "extensions are not supported") {
		t.Fatalf("Compile() = %v, want strict portability rejection before dispatch", err)
	}
	if call.SDKParams != nil {
		t.Fatal("rejected extension produced dispatch parameters")
	}
}

// Best-effort mode leaves the extensions out of the wire body, and the lift
// reports the drop as a warning diagnostic.
func TestCompileDropsExtensionsWithDiagnosticInBestEffortMode(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
	call, err := compileExtensionRequest(t, adapter, false, map[string]json.RawMessage{"anthropic": json.RawMessage(`{"temperature":1.7}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !call.Metadata.ExtensionsDropped {
		t.Fatal("best-effort compile did not record the dropped extensions")
	}
	encoded, err := json.Marshal(call.SDKParams)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "temperature") || strings.Contains(string(encoded), "anthropic") {
		t.Fatalf("dropped extension reached the wire: %s", encoded)
	}
	response := anthropic.Message{ID: "msg_1", Model: "claude-contract", StopReason: anthropic.StopReasonEndTurn,
		Content: []anthropic.ContentBlockUnion{{Type: "text", Text: "hi"}}}
	lifted, err := adapter.profile.liftResponse(call, &response, "req_1")
	if err != nil {
		t.Fatal(err)
	}
	if len(lifted.Diagnostics) != 1 || lifted.Diagnostics[0].Code != "extensions_dropped" ||
		lifted.Diagnostics[0].Severity != llm.DiagnosticWarning || lifted.Diagnostics[0].Path != "/extensions" {
		t.Fatalf("diagnostics = %#v, want one extensions_dropped warning", lifted.Diagnostics)
	}
}

// Without extensions neither mode records a drop or adds a diagnostic.
func TestCompileWithoutExtensionsRecordsNoDrop(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
	for _, strict := range []bool{false, true} {
		call, err := compileExtensionRequest(t, adapter, strict, nil)
		if err != nil || call.Metadata.ExtensionsDropped {
			t.Fatalf("strict=%v: dropped=%v err=%v", strict, call.Metadata.ExtensionsDropped, err)
		}
		if diagnostics := liftDiagnostics(call); diagnostics != nil {
			t.Fatalf("strict=%v: diagnostics = %#v", strict, diagnostics)
		}
	}
}
