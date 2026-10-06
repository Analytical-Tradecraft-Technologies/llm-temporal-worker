package openaichat

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
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
	got, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := json.Marshal(lifted)
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalFixture(t, got, "response.semantic.json")
}

func assertCanonicalFixture(t *testing.T, got []byte, name string) {
	t.Helper()
	want := readFixture(t, name)
	gotCanonical, err := llm.CanonicalJSON(got)
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical, err := llm.CanonicalJSON(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotCanonical, wantCanonical) {
		t.Fatalf("fixture %s mismatch\n got: %s\nwant: %s", name, gotCanonical, wantCanonical)
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/unit/chat/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
