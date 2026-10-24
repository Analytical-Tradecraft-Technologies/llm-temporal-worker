package bedrockconverse

import (
	"context"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Hosted-tool flags are refused at compile, never silently dropped (#1506).
func TestCompileRejectsHostedToolFlags(t *testing.T) {
	adapter := &Adapter{endpointID: bedrockFixtureEndpoint, profile: mustBedrockConverseProfile(t)}
	for name, mutate := range map[string]func(*llm.Request){
		"web_search":     func(r *llm.Request) { r.WebSearch = true },
		"web_fetch":      func(r *llm.Request) { r.WebFetch = true },
		"code_execution": func(r *llm.Request) { r.CodeExecution = true },
	} {
		for _, strict := range []bool{true, false} {
			request := llm.Request{OperationKey: "op-hosted", Model: bedrockFixtureModel, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hi"}}}}}
			mutate(&request)
			_, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Strict: strict,
				Query: provider.CapabilityQuery{EndpointID: bedrockFixtureEndpoint, Family: provider.FamilyBedrockConverse, Model: bedrockFixtureModel}})
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeUnsupportedCapability || mapped.Dispatch != provider.DispatchNotDispatched {
				t.Fatalf("%s strict=%v: compile = %#v, want unsupported_capability", name, strict, err)
			}
		}
	}
}
