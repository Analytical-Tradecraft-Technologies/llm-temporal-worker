package anthropicmessages

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func TestOpaqueThinkingStateSurvivesLiftLowerWithExactBytes(t *testing.T) {
	const raw = `{ "signature": "sig-raw", "thinking": "private", "type": "thinking", "future": {"n": 1} }`
	response := contract.DecodeJSON[anthropic.Message](t, []byte(`{"id":"state-1","model":"claude-contract","content":[`+raw+`,{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"service_tier":"standard"}}`))
	profile := mustProfile(t, testProfile())
	lifted := mustLift(t, profile, liftCall("state", llm.ServiceClassStandard), &response, "req-state")
	state, ok := lifted.Output[0].(llm.ProviderState)
	if !ok || string(state.Opaque) != raw {
		t.Fatalf("lifted opaque state = %#v, want %q", lifted.Output[0], raw)
	}
	request := llm.Request{
		OperationKey: "state-replay",
		Model:        "claude-contract",
		Input:        []llm.Item{llm.HumanText("continue")},
		Continuation: &llm.Continuation{Handle: lifted.Continuation.Handle, EndpointID: "anthropic-prod", Model: "claude-contract", Pinned: true, ProviderStates: lifted.Continuation.ProviderStates},
	}
	params, err := lowerRequest(request, profile, "standard_only")
	if err != nil {
		t.Fatal(err)
	}
	wire := contract.JSONObject(t, params)
	messages := wire["messages"].([]any)
	if len(messages) != 2 {
		t.Fatalf("replay messages = %#v", messages)
	}
	replayed, ok := messages[0].(map[string]any)["content"].([]any)[0].(map[string]any)
	if !ok {
		t.Fatalf("replayed state = %#v", messages[1])
	}
	replayedRaw := contract.MarshalJSON(t, replayed)
	var want, got any
	if err := json.Unmarshal([]byte(raw), &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(replayedRaw, &got); err != nil {
		t.Fatal(err)
	}
	if string(replayedRaw) != raw && !strings.Contains(string(replayedRaw), `"future":{"n":1}`) {
		t.Fatalf("replayed state bytes = %s", replayedRaw)
	}
	if !jsonEqual(want, got) {
		t.Fatalf("replayed state changed: want %s got %s", raw, replayedRaw)
	}
}

func TestContinuationRejectsWrongProfilePinning(t *testing.T) {
	profile := mustProfile(t, testProfile())
	request := llm.Request{OperationKey: "wrong-profile", Model: "claude-contract", Continuation: &llm.Continuation{
		Handle:         "anthropic-messages:msg",
		EndpointID:     "other-endpoint",
		Model:          "claude-contract",
		Pinned:         true,
		ProviderStates: []llm.ProviderState{{Provider: "anthropic", EndpointFamily: "messages", MediaType: "application/vnd.anthropic.content-block+json", Opaque: []byte(`{"type":"thinking","thinking":"x","signature":"s"}`)}},
	}}
	adapter := &Adapter{endpointID: "anthropic-prod", profile: profile}
	_, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"}, Strict: true})
	testassert.ErrorContains(t, err, "continuation endpoint")
	request.Continuation.EndpointID = "anthropic-prod"
	request.Continuation.Model = "other-model"
	_, err = adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"}, Strict: true})
	testassert.ErrorContains(t, err, "continuation model")
}

func jsonEqual(left, right any) bool {
	leftBytes, _ := json.Marshal(left)
	rightBytes, _ := json.Marshal(right)
	return string(leftBytes) == string(rightBytes)
}
