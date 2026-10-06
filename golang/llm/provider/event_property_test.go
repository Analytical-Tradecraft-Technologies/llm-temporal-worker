package provider_test

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestAssemblerEventSequenceInvariants(t *testing.T) {
	assembler := provider.NewAssembler("event-invariant")
	for _, event := range []provider.Event{
		provider.OutputStarted{Index: 0},
		provider.TextDelta{Index: 0, Text: "semantic "},
		provider.TextDelta{Index: 0, Text: "stream"},
		provider.OutputFinished{Index: 0},
		provider.StreamCompleted{},
	} {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("Add(%T): %v", event, err)
		}
	}
	response, err := assembler.Result()
	if err != nil {
		t.Fatal(err)
	}
	if response.OperationKey != "event-invariant" || response.Status != llm.ResponseStatusCompleted || len(response.Output) != 1 {
		t.Fatalf("assembled response = %#v", response)
	}
	message, ok := response.Output[0].(llm.Message)
	if !ok || len(message.Content) != 1 || message.Content[0] != (llm.TextPart{Text: "semantic stream"}) {
		t.Fatalf("assembled output = %#v", response.Output)
	}
}
