package bedrockmessages

import (
	"bytes"
	"os"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func FuzzDecodeStream(f *testing.F) {
	fixture, err := os.ReadFile("testdata/contracts/events.wire")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(fixture)
	f.Add([]byte("event: message_stop\ndata: {}\n\n"))
	f.Fuzz(func(t *testing.T, wire []byte) {
		if len(wire) > 2<<20 {
			return
		}
		events, err := DecodeStream(bytes.NewReader(wire))
		if err != nil {
			return
		}
		assertDecodedEventsAssemble(t, events)
	})
}

func TestDecodedProviderTerminalErrorIsValidFuzzOutcome(t *testing.T) {
	events, err := DecodeStream(bytes.NewReader([]byte("event: error\ndata: {}\n\n")))
	if err != nil {
		t.Fatal(err)
	}
	assertDecodedEventsAssemble(t, events)
}

func assertDecodedEventsAssemble(t *testing.T, events []provider.Event) {
	t.Helper()
	assembler := provider.NewAssembler("bedrock-fuzz")
	for index, event := range events {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("decoded event %d (%T) violates assembler contract: %v", index, event, err)
		}
	}
	response, err := assembler.Result()
	if hasProviderTerminalError(events) {
		if err == nil {
			t.Fatal("decoded provider terminal error unexpectedly assembled a response")
		}
		return
	}
	if err != nil {
		t.Fatalf("decoded terminal stream did not assemble: %v", err)
	}
	if response.OperationKey != "bedrock-fuzz" || !response.Status.Valid() {
		t.Fatalf("assembled response is not semantic: %#v", response)
	}
}

func hasProviderTerminalError(events []provider.Event) bool {
	if len(events) == 0 {
		return false
	}
	_, ok := events[len(events)-1].(provider.StreamErrored)
	return ok
}
