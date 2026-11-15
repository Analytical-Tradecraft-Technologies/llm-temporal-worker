package anthropicwire

import (
	"encoding/json"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/anthropicschema"
)

func TestLiftContentKeepsBlockOrder(t *testing.T) {
	blocks := []anthropic.ContentBlockUnion{
		{Type: "text", Text: "a"},
		{Type: "text", Text: "b"},
		{Type: "thinking", Thinking: "hidden", Signature: "sig"},
		{Type: "tool_use", ID: "toolu", Name: "lookup"},
	}
	output, states, hasToolCalls, err := LiftContent(blocks, "bedrock", []string{"thinking"})
	if err != nil || !hasToolCalls || len(output) != 3 || len(states) != 1 {
		t.Fatalf("LiftContent() = %#v, %#v, %t, %v", output, states, hasToolCalls, err)
	}
	if message := output[0].(llm.Message); len(message.Content) != 2 {
		t.Fatalf("LiftContent() did not merge adjacent text: %#v", message)
	}
	if state := states[0]; state.Provider != "bedrock" || state.EndpointFamily != "messages" || !json.Valid(state.Opaque) {
		t.Fatalf("LiftContent() state = %#v", state)
	}
	if call := output[2].(llm.ToolCall); string(call.Arguments) != `{}` {
		t.Fatalf("LiftContent() tool call arguments = %s, want {}", call.Arguments)
	}
	for _, test := range []struct {
		block anthropic.ContentBlockUnion
		want  string
	}{
		{block: anthropic.ContentBlockUnion{Type: "server_tool_use"}, want: `content block 0 has unsupported type "server_tool_use"`},
		{block: anthropic.ContentBlockUnion{Type: "tool_use", Name: "lookup"}, want: "content block 0 tool_use is missing id or name"},
		{block: anthropic.ContentBlockUnion{Type: "tool_use", ID: "t", Name: "lookup", Input: json.RawMessage(`{`)}, want: "content block 0 tool_use input is invalid JSON"},
	} {
		if _, _, _, err := LiftContent([]anthropic.ContentBlockUnion{test.block}, "bedrock", []string{"thinking"}); err == nil || err.Error() != test.want {
			t.Errorf("LiftContent(%s) = %v, want %q", test.block.Type, err, test.want)
		}
	}
}

func TestValidateFinalJSONOnlyChecksCompletedText(t *testing.T) {
	call := provider.Call{OutputSchema: json.RawMessage(`{"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}`)}
	invalid := []llm.Item{llm.ModelText(`{}`)}
	testassert.ErrorContains(t, ValidateFinalJSON(call, invalid, llm.ResponseStatusCompleted, false, false), "does not satisfy schema")
	if err := ValidateFinalJSON(call, invalid, llm.ResponseStatusLength, false, false); err != nil {
		t.Fatalf("ValidateFinalJSON(truncated) = %v", err)
	}
	if err := ValidateFinalJSON(call, invalid, llm.ResponseStatusCompleted, true, false); err != nil {
		t.Fatalf("ValidateFinalJSON(tool calls) = %v", err)
	}
}

func TestLowerErrorClassifiesLoweringFailures(t *testing.T) {
	strict := provider.NewStrictPortabilityError("strict")
	if got := LowerError(strict); got != strict {
		t.Fatalf("LowerError(provider error) = %#v, want it unchanged", got)
	}
	if got := LowerError(&anthropicschema.UnsupportedError{Reason: "x"}); got.Code != provider.CodeUnsupportedCapability {
		t.Fatalf("LowerError(unsupported schema) = %#v", got)
	}
	if got := LowerError(json.Unmarshal([]byte(`{`), new(any))); got.Code != provider.CodeInvalidArgument {
		t.Fatalf("LowerError(other) = %#v", got)
	}
}
