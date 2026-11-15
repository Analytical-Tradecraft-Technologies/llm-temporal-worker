package anthropicmessages

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

var requestOwnedFields = []string{
	"model", "messages", "service_tier", "max_tokens", "system", "tools",
	"tool_choice", "output_config", "thinking", "temperature", "top_p", "top_k",
	"stop_sequences", "container", "mcp_servers", "metadata", "stream", "max_tool_calls",
	"inference_geo", "speed",
}

// An endpoint's extension allowlist identity-maps each configured key, so a
// request-owned key must be refused when the profile is built.
func TestNewProfileRejectsRequestOwnedExtensionFields(t *testing.T) {
	for _, wire := range requestOwnedFields {
		for _, mapped := range []string{wire, ""} {
			profile := testProfile()
			profile.AllowedExtensions["anthropic.owned"] = ExtensionSpec{Fields: map[string]string{wire: mapped}}
			testassert.ErrorContains(t, testassert.Err(NewProfile(profile)), "cannot override", fmt.Sprintf("NewProfile(%q -> %q)", wire, mapped))
		}
	}
}

// A profile that bypasses NewProfile must still not let an extension replace
// a request-owned control during lowering, in either portability mode.
func TestCompileRejectsRequestOwnedExtensionFields(t *testing.T) {
	values := map[string]string{
		"output_config": `{"effort":"high"}`,
		"system":        `""`,
		"temperature":   `1.7`,
		"tools":         `[{"type":"web_search_20250305","name":"web_search","max_uses":50}]`,
	}
	for _, wire := range requestOwnedFields {
		value, ok := values[wire]
		if !ok {
			value = `null`
		}
		for _, strict := range []bool{false, true} {
			t.Run(wire+messagestest.PortabilityName(strict), func(t *testing.T) {
				profile := testProfile()
				profile.AllowedExtensions["anthropic.owned"] = ExtensionSpec{Fields: map[string]string{wire: wire}}
				adapter := &Adapter{endpointID: "anthropic-test", profile: profile}
				maxTokens := 64
				call, err := adapter.Compile(context.Background(), provider.CompileInput{
					Request: llm.Request{
						OperationKey: "owned-extension",
						Model:        "claude-contract",
						Instructions: []llm.Instruction{{Text: "answer"}},
						Input:        []llm.Item{llm.HumanText("hello")},
						Output:       &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}`)}},
						Extensions:   map[string]json.RawMessage{"anthropic.owned": json.RawMessage(`{"` + wire + `":` + value + `}`)},
					},
					Query:  provider.CapabilityQuery{EndpointID: "anthropic-test", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
					Strict: strict,
				})
				testassert.ErrorContains(t, err, "cannot override")
				if call.SDKParams != nil {
					t.Fatal("rejected extension produced dispatch parameters")
				}
			})
		}
	}
}

func TestCompileAcceptsAllowedNonOwnedExtensionField(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(messagestest.PortabilityName(strict), func(t *testing.T) {
			adapter := &Adapter{endpointID: "anthropic-test", profile: mustProfile(t, testProfile())}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{
				Request: llm.Request{
					OperationKey: "allowed-extension",
					Model:        "claude-contract",
					Input:        []llm.Item{llm.HumanText("hello")},
					Extensions:   map[string]json.RawMessage{"anthropic.contract": json.RawMessage(`{"tag":"us"}`)},
				},
				Query:  provider.CapabilityQuery{EndpointID: "anthropic-test", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
				Strict: strict,
			})
			testassert.NoError(t, err)
			if wire := contract.JSONObject(t, call.SDKParams); wire["contract_tag"] != "us" {
				t.Fatalf("wire = %#v", wire)
			}
		})
	}
}
