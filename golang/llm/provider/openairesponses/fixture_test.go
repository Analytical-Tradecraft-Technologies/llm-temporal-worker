package openairesponses

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestWireFixtureMatchesNormalizedSDKParams(t *testing.T) {
	request := loadContractRequestFixture(t, "openai-responses", "request.semantic.json")
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
	if err != nil {
		t.Fatal(err)
	}
	params, err := lowerRequest(normalized, serviceClass)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalFixtureJSON(t, got, "openai-responses", "request.wire.json")
}

func TestSemanticFixtureMatchesLiftedResponse(t *testing.T) {
	request := loadContractRequestFixture(t, "openai-responses", "request.semantic.json")
	call, err := fixtureAdapterForProfile(t, responsesFixtureProfiles[0]).Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "openai-fixture", Family: provider.FamilyOpenAIResponses, Model: request.Model},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	response := loadContractResponseFixture(t, "openai-responses", "response.completed.json")
	lifted, err := liftResponse(call, &response, "req-openai-fixture")
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(lifted)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalFixtureJSON(t, got, "openai-responses", "response.semantic.json")
}
