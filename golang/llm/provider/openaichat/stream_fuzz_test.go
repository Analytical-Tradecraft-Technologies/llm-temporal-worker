package openaichat

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
	f.Add([]byte("data: {\"choices\":[]}\n\ndata: [DONE]\n\n"))
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

func assertDecodedEventsAssemble(t *testing.T, events []provider.Event) {
	t.Helper()
	assembler := provider.NewAssembler("openai-chat-fuzz")
	for index, event := range events {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("decoded event %d (%T) violates assembler contract: %v", index, event, err)
		}
	}
	response, err := assembler.Result()
	if err != nil {
		t.Fatalf("decoded terminal stream did not assemble: %v", err)
	}
	if response.OperationKey != "openai-chat-fuzz" || !response.Status.Valid() {
		t.Fatalf("assembled response is not semantic: %#v", response)
	}
}
