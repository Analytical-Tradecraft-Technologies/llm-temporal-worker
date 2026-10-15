package openaichat

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// A schema outside the subset the lift validates against is rejected at
// compile time, before anything is dispatched or paid for (#1100).
func TestCompileRejectsAnUnsupportedOutputSchemaBeforeDispatch(t *testing.T) {
	_, err := testAdapter(t).Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-schema", Model: "chat-model", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hi"}}}},
			Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Schema: json.RawMessage(`{"type":"object","patternProperties":{"^x":{"type":"string"}}}`)}}},
		Query: provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
	})
	if err == nil || !strings.Contains(err.Error(), "output schema") {
		t.Fatalf("compile = %v, want an output schema rejection", err)
	}
}

// A strict schema is closed and fully required on the wire, and requests
// OpenAI documents as errors fail at compile time as unsupported (#1353).
func TestStrictOutputSchemaIsLoweredForOpenAI(t *testing.T) {
	request := llm.Request{Model: "chat-model", Input: wireAuditUserText("hi"), Output: &llm.OutputSpec{Format: llm.OutputFormat{
		Kind: llm.OutputKindJSONSchema, Name: "invoice", Strict: true,
		Schema: json.RawMessage(`{"type":"object","properties":{"total":{"type":"number"},"lines":{"type":"array","items":{"type":"string"},"uniqueItems":true}},"required":["total"]}`),
	}}}
	requestMap, err := lowerRequestMap(request, testProfile(), "default")
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(requestMap["response_format"])
	if err != nil {
		t.Fatal(err)
	}
	want := `{"json_schema":{"name":"invoice","schema":{"additionalProperties":false,"properties":{"lines":{"description":"{uniqueItems: true}","items":{"type":"string"},"type":"array"},"total":{"type":"number"}},"required":["total","lines"],"type":"object"},"strict":true},"type":"json_schema"}`
	if string(got) != want {
		t.Fatalf("response_format\n got: %s\nwant: %s", got, want)
	}
}

func TestCompileRejectsUnrepresentableOpenAIStructuredOutput(t *testing.T) {
	for name, request := range map[string]llm.Request{
		"strict open object": {Input: wireAuditUserText("hi"), Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true,
			Schema: json.RawMessage(`{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":true}`)}}},
		"json mode without json": {Input: wireAuditUserText("List three primary colours."), Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSON}}},
	} {
		t.Run(name, func(t *testing.T) {
			request.OperationKey = "op-structured"
			request.Model = "chat-model"
			_, err := testAdapter(t).Compile(context.Background(), provider.CompileInput{
				Request: request,
				Query:   provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
			})
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability || mapped.Phase != provider.PhaseCompile || mapped.Dispatch != provider.DispatchNotDispatched {
				t.Fatalf("compile = %#v, want an unsupported compile rejection", err)
			}
		})
	}
	// An instruction mentioning the word satisfies the provider's rule.
	request := llm.Request{Model: "chat-model", Instructions: []llm.Instruction{{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: "Reply with JSON."}},
		Input: wireAuditUserText("List three primary colours."), Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSON}}}
	if _, err := lowerRequestMap(request, testProfile(), "default"); err != nil {
		t.Fatalf("json mode mentioning json: %v", err)
	}
}
