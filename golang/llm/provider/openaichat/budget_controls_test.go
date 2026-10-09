package openaichat

import (
	"context"
	"encoding/json"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"testing"
)

func TestProfileRejectsRequestOwnedWireControls(t *testing.T) {
	for _, field := range []string{"model", "messages", "service_tier", "max_tokens", "max_completion_tokens", "response_format", "tools", "tool_choice", "parallel_tool_calls", "max_tool_calls", "container", "temperature", "top_p", "seed", "presence_penalty", "frequency_penalty", "stop", "reasoning_effort", "stream"} {
		t.Run(field, func(t *testing.T) {
			for _, alias := range []string{field, ""} {
				p := testProfile()
				p.AllowedExtensions["unsafe"] = ExtensionSpec{Fields: map[string]string{field: alias}}
				if _, err := NewProfile(p); err == nil {
					t.Fatalf("accepted extension targeting %s with alias %q", field, alias)
				}
			}
			p := testProfile()
			p.AllowedExtensions["unsafe"] = ExtensionSpec{Fields: map[string]string{"alias": field}}
			if _, err := NewProfile(p); err == nil {
				t.Fatalf("accepted alias targeting %s", field)
			}
			p = testProfile()
			p.WireDefaults = map[string]json.RawMessage{field: json.RawMessage(`1`)}
			if _, err := NewProfile(p); err == nil {
				t.Fatalf("accepted wire default for %s", field)
			}
		})
	}
}

func TestChatRejectsUnsupportedChoiceCountsBeforeDispatch(t *testing.T) {
	for _, raw := range []string{`0`, `2`, `4`, `-1`, `null`, `true`, `"1"`, `1.5`} {
		t.Run(raw, func(t *testing.T) {
			p := testProfile()
			p.WireDefaults = map[string]json.RawMessage{"n": json.RawMessage(raw)}
			if _, err := NewProfile(p); err == nil {
				t.Error("accepted unsupported default choice count")
			}
			p.WireDefaults = nil
			p.AllowedExtensions["choices"] = ExtensionSpec{Fields: map[string]string{"count": "n"}}
			validated, err := NewProfile(p)
			if err != nil {
				t.Fatal(err)
			}
			a := &Adapter{endpointID: "chat-prod", profile: validated}
			req := boundedChatRequest()
			req.Extensions = map[string]json.RawMessage{"choices": json.RawMessage(`{"count":` + raw + `}`)}
			if _, err := a.Compile(context.Background(), provider.CompileInput{Request: req, Query: provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"}, Strict: true}); err == nil {
				t.Fatal("compiled unsupported choice count")
			}
		})
	}
}

func boundedChatRequest() llm.Request {
	return llm.Request{OperationKey: "bounded", Model: "chat-model", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}}
}

func TestChatSingleChoicePreservesBudgetedExplicitAndDefaultCap(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, asDefault := range []bool{false, true} {
			p := testProfile()
			p.AllowedExtensions["choices"] = ExtensionSpec{Fields: map[string]string{"n": ""}}
			if asDefault {
				p.WireDefaults = map[string]json.RawMessage{"n": json.RawMessage(`1`)}
			}
			p, err := NewProfile(p)
			if err != nil {
				t.Fatal(err)
			}
			request := boundedChatRequest()
			if explicit {
				cap := 16
				request.Output = &llm.OutputSpec{MaxTokens: &cap}
			}
			if !asDefault {
				request.Extensions = map[string]json.RawMessage{"choices": json.RawMessage(`{"n":1}`)}
			}
			prepared, err := (budget.Estimator{MaxOutput: 16}).PrepareRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			call, err := (&Adapter{endpointID: "chat-prod", profile: p}).Compile(context.Background(), provider.CompileInput{Request: prepared, Query: provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"}, Strict: true})
			if err != nil {
				t.Fatal(err)
			}
			wire := marshalWire(t, call.SDKParams)
			if wire["max_completion_tokens"] != float64(16) || wire["n"] != float64(1) {
				t.Fatalf("wire limits = %v, %v", wire["max_completion_tokens"], wire["n"])
			}
			if _, ok := wire["max_tokens"]; ok {
				t.Fatal("conflicting output limit on wire")
			}
		}
	}
}

// Lowering also enforces the boundary if an internal caller supplies a profile
// without going through its public constructor.
func TestChatCompileRejectsOutputCapOverrides(t *testing.T) {
	for _, field := range []string{"max_tokens", "max_completion_tokens"} {
		for _, explicit := range []bool{false, true} {
			for _, asDefault := range []bool{false, true} {
				p := testProfile()
				request := boundedChatRequest()
				if explicit {
					cap := 16
					request.Output = &llm.OutputSpec{MaxTokens: &cap}
				}
				if asDefault {
					p.WireDefaults = map[string]json.RawMessage{field: json.RawMessage(`4096`)}
				} else {
					p.AllowedExtensions["override"] = ExtensionSpec{Fields: map[string]string{"limit": field}}
					request.Extensions = map[string]json.RawMessage{"override": json.RawMessage(`{"limit":4096}`)}
				}
				prepared, err := (budget.Estimator{MaxOutput: 16}).PrepareRequest(request)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := (&Adapter{endpointID: "chat-prod", profile: p}).Compile(context.Background(), provider.CompileInput{Request: prepared, Query: provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"}, Strict: true}); err == nil {
					t.Fatalf("compiled output override: field=%s explicit=%v default=%v", field, explicit, asDefault)
				}
			}
		}
	}
}

func TestChatCompileRejectsUnsupportedChoiceDefault(t *testing.T) {
	p := testProfile()
	p.WireDefaults = map[string]json.RawMessage{"n": json.RawMessage(`4`)}
	if _, err := (&Adapter{endpointID: "chat-prod", profile: p}).Compile(context.Background(), provider.CompileInput{Request: boundedChatRequest(), Query: provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"}, Strict: true}); err == nil {
		t.Fatal("compiled unsupported default choice count")
	}
}
