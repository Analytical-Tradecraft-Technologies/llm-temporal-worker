package openairesponses

import (
	"strings"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func TestLiftedReasoningOutputReplaysAsReasoningInput(t *testing.T) {
	response := testassert.UnmarshalJSON[responses.Response](t, readContractFixture(t, "openai-responses", "response.completed.json"))
	lifted := mustLift(t, liftCall("fixture-op", llm.ServiceClassStandard), &response, "req")
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
	input := []llm.Item{llm.HumanText("question"), *state}
	input = append(input, produced...)
	input = append(input, llm.HumanText("follow-up"))
	params, err := lowerRequest(llm.Request{Model: "gpt-contract", OperationKey: "replay", Input: input}, llm.ServiceClassStandard)
	testassert.NoError(t, err, "replaying lifted output")
	wire := contract.JSONObject(t, params)
	found := false
	for _, raw := range wire["input"].([]any) {
		if item, ok := raw.(map[string]any); ok && item["type"] == "reasoning" {
			found = true
		}
	}
	if !found {
		t.Fatalf("reasoning item missing from replayed input: %s", testassert.MarshalJSON(t, params))
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
	lifted := mustLift(t, call, &response, "req")
	request := loadContractRequestFixture(t, "openai-responses", "request.semantic.json")
	request.Input = append(append(request.Input, lifted.Output...), llm.HumanText("follow-up"))
	testassert.NoError(t, testassert.Err(adaptertest.Compile(fixtureAdapterForProfile(t, responsesFixtureProfiles[0]), "openai-fixture", provider.FamilyOpenAIResponses, request, true)), "strict compile of replayed reasoning")
}
