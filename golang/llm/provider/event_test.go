package provider_test

import (
	"reflect"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestAssemblerBuildsTerminalResponseInOrder(t *testing.T) {
	assembler := provider.NewAssembler("operation-1")
	message := llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "hello"}}}
	for _, event := range []provider.Event{
		provider.OutputStarted{Index: 0},
		provider.TextDelta{Index: 0, Text: "he"},
		provider.TextDelta{Index: 0, Text: "llo"},
		provider.OutputFinished{Index: 0, Item: message},
		provider.UsageUpdated{Usage: llm.Usage{InputTokens: 2, OutputTokens: 1}},
		provider.StreamCompleted{},
	} {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("add %T: %v", event, err)
		}
	}
	response, err := assembler.Result()
	if err != nil {
		t.Fatal(err)
	}
	if response.OperationKey != "operation-1" || response.Status != llm.ResponseStatusCompleted {
		t.Fatalf("response identity = %#v", response)
	}
	if len(response.Output) != 1 || response.Usage.InputTokens != 2 {
		t.Fatalf("assembled response = %#v", response)
	}
}

func TestAssemblerKeepsResponseOutputOrderWhenCompletionsAreInterleaved(t *testing.T) {
	assembler := provider.NewAssembler("operation-interleaved")
	first := llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "first"}}}
	second := llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "second"}}}
	for _, event := range []provider.Event{
		provider.OutputStarted{Index: 0},
		provider.OutputStarted{Index: 1},
		// The provider may finish a later output before an earlier one.
		provider.OutputFinished{Index: 1, Item: second},
		provider.OutputFinished{Index: 0, Item: first},
		provider.StreamCompleted{},
	} {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("add %T: %v", event, err)
		}
	}
	response, err := assembler.Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Output) != 2 || !reflect.DeepEqual(response.Output[0], first) || !reflect.DeepEqual(response.Output[1], second) {
		t.Fatalf("response output order = %#v, want first then second", response.Output)
	}
}

func TestAssemblerMergesPartialUsageUpdates(t *testing.T) {
	assembler := provider.NewAssembler("operation-usage")
	for _, event := range []provider.Event{
		provider.UsageUpdated{Usage: llm.Usage{InputTokens: 4}},
		provider.UsageUpdated{Usage: llm.Usage{OutputTokens: 2}},
		provider.StreamCompleted{},
	} {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("add %T: %v", event, err)
		}
	}
	response, err := assembler.Result()
	if err != nil {
		t.Fatal(err)
	}
	if response.Usage.InputTokens != 4 || response.Usage.OutputTokens != 2 {
		t.Fatalf("merged usage = %#v", response.Usage)
	}
}

func TestAssemblerRejectsInvalidOrderAndTerminalReuse(t *testing.T) {
	for _, events := range [][]provider.Event{
		{provider.TextDelta{Index: 0, Text: "before start"}},
		{provider.OutputStarted{Index: 0}, provider.OutputStarted{Index: 0}},
		{provider.OutputStarted{Index: 0}, provider.StreamCompleted{}},
		{provider.OutputStarted{Index: 0}, provider.OutputFinished{Index: 0}, provider.StreamCompleted{}, provider.UsageUpdated{}},
	} {
		assembler := provider.NewAssembler("operation-1")
		failed := false
		for _, event := range events {
			if err := assembler.Add(event); err != nil {
				failed = true
				break
			}
		}
		if !failed {
			t.Errorf("accepted invalid event sequence %#v", events)
		}
	}
	if _, err := provider.NewAssembler("operation-1").Result(); err == nil {
		t.Fatal("returned a result before a terminal event")
	}
}

func TestAssemblerRejectsDuplicateTerminalAndPreservesOpaqueState(t *testing.T) {
	assembler := provider.NewAssembler("operation-1")
	opaque := []byte{0x00, 0xff, 0x7f}
	for _, event := range []provider.Event{
		provider.OutputStarted{Index: 0},
		provider.ProviderStateDelta{Index: 0, State: llm.ProviderState{Provider: "provider-1", EndpointFamily: "family-1", MediaType: "application/octet-stream", Opaque: opaque}},
		provider.OutputFinished{Index: 0},
		provider.StreamCompleted{},
	} {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("add %T: %v", event, err)
		}
	}
	if err := assembler.Add(provider.StreamCompleted{}); err == nil {
		t.Fatal("accepted duplicate terminal")
	}
	response, err := assembler.Result()
	if err != nil {
		t.Fatal(err)
	}
	message, ok := response.Output[0].(llm.Message)
	if !ok || len(message.Content) != 1 {
		t.Fatalf("assembled opaque output = %#v", response.Output)
	}
	state, ok := message.Content[0].(llm.ProviderStatePart)
	if !ok || string(state.Opaque) != string(opaque) {
		t.Fatalf("opaque state = %#v, want exact bytes %#v", state, opaque)
	}
}

func TestAssemblerAllowsDeferredToolIdentityOutsideStreamingPort(t *testing.T) {
	assembler := provider.NewAssembler("operation-1")
	for _, event := range []provider.Event{
		provider.OutputStarted{Index: 0},
		provider.ToolArgumentsDelta{Index: 0, Fragment: `{"argument":`},
		provider.ToolArgumentsDelta{Index: 0, CallID: "call-1", Name: "lookup", Fragment: "true}"},
		provider.OutputFinished{Index: 0},
		provider.StreamCompleted{},
	} {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("add %T: %v", event, err)
		}
	}
	response, err := assembler.Result()
	if err != nil {
		t.Fatal(err)
	}
	tool, ok := response.Output[0].(llm.ToolCall)
	if !ok || tool.ID != "call-1" || tool.Name != "lookup" || string(tool.Arguments) != `{"argument":true}` {
		t.Fatalf("assembled deferred-identity tool = %#v", response.Output[0])
	}
}

func TestAssemblerRejectsInvalidUTF8AndUnfinishedOutput(t *testing.T) {
	assembler := provider.NewAssembler("operation-1")
	if err := assembler.Add(provider.OutputStarted{Index: 0}); err != nil {
		t.Fatal(err)
	}
	if err := assembler.Add(provider.TextDelta{Index: 0, Text: string([]byte{0xff})}); err == nil {
		t.Fatal("accepted invalid UTF-8 delta")
	}
	if err := assembler.Add(provider.StreamCompleted{}); err == nil {
		t.Fatal("accepted terminal event with unfinished output")
	}
}
