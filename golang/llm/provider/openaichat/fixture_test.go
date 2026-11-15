package openaichat

import (
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func TestWireFixtureMatchesNormalizedSDKParams(t *testing.T) {
	params, err := lowerRequest(llm.Request{
		OperationKey: "fixture-op",
		Model:        "chat-contract",
		ServiceClass: llm.ServiceClassStandard,
		Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.TextPart{Text: "hello"},
		}}},
	}, testProfile(), "default")
	if err != nil {
		t.Fatal(err)
	}
	got := contract.MarshalJSON(t, params)
	assertCanonicalFixture(t, got, "request.wire.json")
}

func TestSemanticFixtureMatchesLiftedResponse(t *testing.T) {
	response := openai.ChatCompletion{
		ID: "chat-fixture", Model: "chat-contract", ServiceTier: openai.ChatCompletionServiceTierDefault,
		Choices: []openai.ChatCompletionChoice{{
			Index: 0, FinishReason: "stop",
			Message: openai.ChatCompletionMessage{Role: "assistant", Content: "hello"},
		}},
	}
	call := provider.Call{
		EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-contract",
		OperationKey: "fixture-op", ServiceClass: llm.ServiceClassStandard,
	}
	lifted, err := testProfile().liftResponse(call, &response, "req-fixture")
	if err != nil {
		t.Fatal(err)
	}
	got := contract.MarshalJSON(t, lifted)
	assertCanonicalFixture(t, got, "response.semantic.json")
}

func assertCanonicalFixture(t *testing.T, got []byte, name string) {
	t.Helper()
	contract.AssertCanonicalJSON(t, got, readFixture(t, name), "fixture "+name)
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	return contract.ReadFixture(t, "testdata/unit/chat/"+name)
}
