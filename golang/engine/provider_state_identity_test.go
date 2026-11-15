package engine

import (
	"bytes"
	"context"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// TestGenerateStoresAdapterProviderStateUnderRoutePinning writes a
// continuation whose provider state carries the adapter label (openai/
// responses) on an openai_responses route through the real memory store, then
// reloads it and expects the adapter label back for lowering.
func TestGenerateStoresAdapterProviderStateUnderRoutePinning(t *testing.T) {
	stateValue := llm.ProviderState{Provider: "openai", EndpointFamily: "responses", MediaType: "application/vnd.openai.reasoning+json", Opaque: []byte(`{"id":"rs_1"}`)}
	response := successfulResponse()
	response.Continuation = &llm.Continuation{Handle: "provider-opaque-handle", ProviderStates: []llm.ProviderState{stateValue}}
	harness := newHarness(t, &fakeAdapter{name: "provider-state-identity", response: response})
	harness.useMemoryContinuations(t, 3)

	first, err := harness.engine.Generate(context.Background(), baseRequest("provider-state-identity"))
	if err != nil {
		t.Fatalf("Generate() error = %v, want the continuation to be stored", err)
	}
	if first.Continuation == nil || first.Continuation.Handle == "" {
		t.Fatalf("continuation = %#v, want a secure handle", first.Continuation)
	}
	next := baseRequest("provider-state-identity-next")
	next.Continuation = &llm.Continuation{Handle: first.Continuation.Handle}
	loaded, _, _, err := harness.engine.loadContinuation(context.Background(), next, harness.clock)
	if err != nil {
		t.Fatal(err)
	}
	states := loaded.Continuation.ProviderStates
	if len(states) != 1 || states[0].Provider != stateValue.Provider || states[0].EndpointFamily != stateValue.EndpointFamily || !bytes.Equal(states[0].Opaque, stateValue.Opaque) {
		t.Fatalf("reloaded provider states = %#v, want the adapter label %s/%s", states, stateValue.Provider, stateValue.EndpointFamily)
	}
}

func TestGenerateRejectsProviderStateFromAnotherFamily(t *testing.T) {
	response := successfulResponse()
	response.Continuation = &llm.Continuation{Handle: "provider-opaque-handle", ProviderStates: []llm.ProviderState{{Provider: "anthropic", EndpointFamily: "messages", MediaType: "application/vnd.anthropic.content-block+json", Opaque: []byte(`{}`)}}}
	harness := newHarness(t, &fakeAdapter{name: "provider-state-mismatch", response: response})
	harness.useMemoryContinuations(t, 5)
	if _, err := harness.engine.Generate(context.Background(), baseRequest("provider-state-mismatch")); err == nil {
		t.Fatal("Messages provider state on an openai_responses route was stored")
	}
}

func TestAdapterStateLabelsCoverEveryStatefulMessagesFamily(t *testing.T) {
	for family, want := range map[string][2]string{"anthropic_messages": {"anthropic", "messages"}, "bedrock_messages": {"bedrock", "messages"}, "openai_responses": {"openai", "responses"}} {
		if !adapterStateMatchesRoute(family, want[0], want[1]) {
			t.Fatalf("%s does not accept %s/%s", family, want[0], want[1])
		}
		if provider, label := adapterStateLabel(family, "configured"); provider != want[0] || label != want[1] {
			t.Fatalf("%s restores %s/%s, want %s/%s", family, provider, label, want[0], want[1])
		}
	}
}
