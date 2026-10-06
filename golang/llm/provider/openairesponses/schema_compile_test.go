package openairesponses

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// A schema outside the subset the lift validates against is rejected at
// compile time, before anything is dispatched or paid for (#1100).
func TestCompileRejectsAnUnsupportedOutputSchemaBeforeDispatch(t *testing.T) {
	_, err := newFixtureAdapter(t, []byte(`{"id":"unused"}`)).Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-schema", Model: "gpt-contract", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hi"}}}},
			Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Schema: json.RawMessage(`{"type":"object","patternProperties":{"^x":{"type":"string"}}}`)}}},
		Query: provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
	})
	if err == nil || !strings.Contains(err.Error(), "output schema") {
		t.Fatalf("compile = %v, want an output schema rejection", err)
	}
}
