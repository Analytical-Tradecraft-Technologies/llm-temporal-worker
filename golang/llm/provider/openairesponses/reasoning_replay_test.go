package openairesponses

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestLiftedReasoningOutputReplaysAsReasoningInput(t *testing.T) {
	var response responses.Response
	if err := json.Unmarshal(readContractFixture(t, "openai-responses", "response.completed.json"), &response); err != nil {
		t.Fatal(err)
	}
	call := provider.Call{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract", OperationKey: "fixture-op", ServiceClass: llm.ServiceClassStandard}
	lifted, err := liftResponse(call, &response, "req")
	if err != nil {
		t.Fatal(err)
	}
	var state *llm.ProviderState
	var produced []llm.Item
	for _, item := range lifted.Output {
		if value, ok := item.(llm.ProviderState); ok {
			state = &value
			continue
		}
		produced = append(produced, item)
	}
	if state == nil {
		t.Fatal("fixture did not lift a reasoning provider state")
	}
	// The fixture lists its reasoning item last. The provider emits one ahead
	// of the output it produced, and a dangling one is not replayed.
	input := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "question"}}}, *state}
	input = append(input, produced...)
	input = append(input, llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "follow-up"}}})
	params, err := lowerRequest(llm.Request{Model: "gpt-contract", OperationKey: "replay", Input: input}, llm.ServiceClassStandard)
	if err != nil {
		t.Fatalf("replaying lifted output: %v", err)
	}
	wire := marshalParams(t, params)
	found := false
	for _, raw := range wire["input"].([]any) {
		if item, ok := raw.(map[string]any); ok && item["type"] == "reasoning" {
			found = true
		}
	}
	if !found {
		t.Fatalf("reasoning item missing from replayed input: %s", mustJSON(t, params))
	}

	for _, foreign := range []llm.ProviderState{
		{Provider: "anthropic", EndpointFamily: "messages", MediaType: reasoningStateMediaType, Opaque: state.Opaque},
		{Provider: "openai", EndpointFamily: "responses", MediaType: reasoningStateMediaType, Opaque: []byte(`{"type":"message"}`)},
	} {
		_, err := lowerRequest(llm.Request{Model: "gpt-contract", OperationKey: "replay", Input: []llm.Item{foreign}}, llm.ServiceClassStandard)
		if err == nil || !(strings.Contains(err.Error(), "not accepted") || strings.Contains(err.Error(), "want reasoning")) {
			t.Fatalf("foreign provider state error = %v", err)
		}
	}
}

func TestStrictCompileAcceptsReplayedReasoning(t *testing.T) {
	response := loadContractResponseFixture(t, "openai-responses", "response.completed.json")
	call := provider.Call{EndpointID: "openai-fixture", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract", OperationKey: "fixture-op", ServiceClass: llm.ServiceClassStandard}
	lifted, err := liftResponse(call, &response, "req")
	if err != nil {
		t.Fatal(err)
	}
	request := loadContractRequestFixture(t, "openai-responses", "request.semantic.json")
	request.Input = append(append(request.Input, lifted.Output...), llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "follow-up"}}})
	if _, err := fixtureAdapterForProfile(t, responsesFixtureProfiles[0]).Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "openai-fixture", Family: provider.FamilyOpenAIResponses, Model: request.Model},
		Strict:  true,
	}); err != nil {
		t.Fatalf("strict compile of replayed reasoning: %v", err)
	}
}
