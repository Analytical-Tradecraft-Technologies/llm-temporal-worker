package openairesponses

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
	_, err := newFixtureAdapter(t, []byte(`{"id":"unused"}`)).Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-schema", Model: "gpt-contract", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hi"}}}},
			Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Schema: json.RawMessage(`{"type":"object","patternProperties":{"^x":{"type":"string"}}}`)}}},
		Query: provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
	})
	if err == nil || !strings.Contains(err.Error(), "output schema") {
		t.Fatalf("compile = %v, want an output schema rejection", err)
	}
}

// A strict schema is closed and fully required on the wire, and requests
// OpenAI documents as errors fail at compile time as unsupported (#1353).
func TestStrictOutputSchemaIsLoweredForOpenAI(t *testing.T) {
	request := llm.Request{Model: "gpt-contract", Input: wireAuditUserText("hi"), Output: &llm.OutputSpec{Format: llm.OutputFormat{
		Kind: llm.OutputKindJSONSchema, Name: "invoice", Strict: true,
		Schema: json.RawMessage(`{"type":"object","properties":{"total":{"type":"number"},"note":{"type":"string","minLength":1}},"required":["total"]}`),
	}}}
	requestMap, _, err := lowerRequestMap(request, llm.ServiceClassStandard, false, supportedExtensionFields)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(requestMap["text"])
	if err != nil {
		t.Fatal(err)
	}
	want := `{"format":{"name":"invoice","schema":{"additionalProperties":false,"properties":{"note":{"description":"{minLength: 1}","type":"string"},"total":{"type":"number"}},"required":["total","note"],"type":"object"},"strict":true,"type":"json_schema"}}`
	if string(got) != want {
		t.Fatalf("text format\n got: %s\nwant: %s", got, want)
	}
}

// Constraints moved out of the wire schema are still enforced by the lift.
func TestStrictOutputValidatesAgainstCallerSchema(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"code":{"type":"string","minLength":3}},"required":["code"],"additionalProperties":false}`)
	call, err := newFixtureAdapter(t, []byte(`{"id":"unused"}`)).Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-strict", Model: "gpt-contract", Input: wireAuditUserText("hi"),
			Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: schema}}},
		Query: provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for text, valid := range map[string]bool{`{"code":"abc"}`: true, `{"code":"ab"}`: false} {
		output := []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: text}}}}
		if err := validateFinalJSON(call, output, llm.ResponseStatusCompleted, false, false); (err == nil) != valid {
			t.Fatalf("%s valid=%v: %v", text, valid, err)
		}
	}
}

// A number the schema validator cannot convert is an invalid response, not an
// Activity panic after payment (#1522).
func TestOutputWithAnUnboundedNumberFailsValidationWithoutPanicking(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"amount":{"type":"number","minimum":0}},"required":["amount"],"additionalProperties":false}`)
	call, err := newFixtureAdapter(t, []byte(`{"id":"unused"}`)).Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-number", Model: "gpt-contract", Input: wireAuditUserText("hi"),
			Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: schema}}},
		Query: provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for text, valid := range map[string]bool{`{"amount":1e3}`: true, `{"amount":1e3000000}`: false, `{"amount":-1e3000000}`: false} {
		output := []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: text}}}}
		if err := validateFinalJSON(call, output, llm.ResponseStatusCompleted, false, false); (err == nil) != valid {
			t.Fatalf("%s valid=%v: %v", text, valid, err)
		}
	}
}

func TestCompileRejectsUnrepresentableOpenAIStructuredOutput(t *testing.T) {
	for name, request := range map[string]llm.Request{
		"strict allOf": {Input: wireAuditUserText("hi"), Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true,
			Schema: json.RawMessage(`{"type":"object","properties":{"a":{"allOf":[{"type":"string"}]}}}`)}}},
		"json mode without json": {Input: wireAuditUserText("List three primary colours."), Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSON}}},
	} {
		t.Run(name, func(t *testing.T) {
			request.OperationKey = "op-structured"
			request.Model = "gpt-contract"
			_, err := newFixtureAdapter(t, []byte(`{"id":"unused"}`)).Compile(context.Background(), provider.CompileInput{
				Request: request,
				Query:   provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
			})
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability || mapped.Phase != provider.PhaseCompile || mapped.Dispatch != provider.DispatchNotDispatched {
				t.Fatalf("compile = %#v, want an unsupported compile rejection", err)
			}
			// The engine asks before admission, so nothing is reserved.
			err = newFixtureAdapter(t, []byte(`{"id":"unused"}`)).ValidateRequest(context.Background(), request)
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability {
				t.Fatalf("validate = %#v, want an unsupported rejection", err)
			}
		})
	}
	// Any case of the word satisfies the provider's rule.
	request := llm.Request{Model: "gpt-contract", Input: wireAuditUserText("answer as a Json object"), Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSON}}}
	if _, _, err := lowerRequestMap(request, llm.ServiceClassStandard, false, supportedExtensionFields); err != nil {
		t.Fatalf("json mode mentioning json: %v", err)
	}
}
