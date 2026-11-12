package provider_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

type sdkParams struct{ Model string }

func TestDispatchParamsAcceptsValueOrPointer(t *testing.T) {
	for name, params := range map[string]any{"value": sdkParams{Model: "m"}, "pointer": &sdkParams{Model: "m"}} {
		call := provider.Call{Family: provider.FamilyOpenAIChat, EndpointID: "e", SDKParams: params}
		got, err := provider.DispatchParams[sdkParams](call, provider.FamilyOpenAIChat, "e")
		if err != nil || got.Model != "m" {
			t.Fatalf("%s: DispatchParams = %+v, %v", name, got, err)
		}
	}
}

func TestDispatchParamsRefusesForeignOrMistypedCalls(t *testing.T) {
	var nilPointer *sdkParams
	for _, test := range []struct {
		name    string
		call    provider.Call
		message string
	}{
		{"other family", provider.Call{Family: provider.FamilyOpenAIResponses, EndpointID: "e", SDKParams: sdkParams{}}, "call does not belong to this adapter"},
		{"other endpoint", provider.Call{Family: provider.FamilyOpenAIChat, EndpointID: "x", SDKParams: sdkParams{}}, "call does not belong to this adapter"},
		{"nil pointer", provider.Call{Family: provider.FamilyOpenAIChat, EndpointID: "e", SDKParams: nilPointer}, "call SDK parameters have unexpected type"},
		{"missing", provider.Call{Family: provider.FamilyOpenAIChat, EndpointID: "e"}, "call SDK parameters have unexpected type"},
		{"wrong type", provider.Call{Family: provider.FamilyOpenAIChat, EndpointID: "e", SDKParams: "params"}, "call SDK parameters have unexpected type"},
	} {
		_, err := provider.DispatchParams[sdkParams](test.call, provider.FamilyOpenAIChat, "e")
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.SafeMessage != test.message || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Phase != provider.PhaseDispatch {
			t.Fatalf("%s: DispatchParams error = %#v", test.name, err)
		}
	}
}

func TestOutputSchemaCopiesOnlyJSONSchemaOutput(t *testing.T) {
	schema := json.RawMessage(`{"type":"object"}`)
	request := llm.Request{Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Schema: schema}}}
	got := provider.OutputSchema(request)
	if string(got) != string(schema) {
		t.Fatalf("OutputSchema = %s", got)
	}
	got[0] = '['
	if schema[0] != '{' {
		t.Fatal("OutputSchema shares the request's schema bytes")
	}
	if got := provider.OutputSchema(llm.Request{}); got != nil {
		t.Fatalf("OutputSchema without output = %s", got)
	}
	if got := provider.OutputSchema(llm.Request{Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindText}}}); got != nil {
		t.Fatalf("OutputSchema for text output = %s", got)
	}
}
