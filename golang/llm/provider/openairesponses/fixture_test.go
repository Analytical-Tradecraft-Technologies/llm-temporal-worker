package openairesponses

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func TestWireFixtureMatchesNormalizedSDKParams(t *testing.T) {
	request := loadContractRequestFixture(t, "openai-responses", "request.semantic.json")
	normalized, serviceClass := contract.NormalizeRequest(t, request)
	params, err := lowerRequest(normalized, serviceClass)
	testassert.NoError(t, err)
	got := contract.MarshalJSON(t, params)
	assertCanonicalFixtureJSON(t, got, "openai-responses", "request.wire.json")
}

func TestSemanticFixtureMatchesLiftedResponse(t *testing.T) {
	request := loadContractRequestFixture(t, "openai-responses", "request.semantic.json")
	call := adaptertest.CompileStrict(t, fixtureAdapterForProfile(t, responsesFixtureProfiles[0]), "openai-fixture", provider.FamilyOpenAIResponses, request)
	response := loadContractResponseFixture(t, "openai-responses", "response.completed.json")
	lifted := mustLift(t, call, &response, "req-openai-fixture")
	got := contract.MarshalJSON(t, lifted)
	assertCanonicalFixtureJSON(t, got, "openai-responses", "response.semantic.json")
}
