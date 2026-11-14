package anthropicmessages

import (
	"bytes"
	"os"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamtest"
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
		streamtest.AssertEventsAssemble(t, "anthropic-fuzz", events)
	})
}

func TestDecodedProviderTerminalErrorIsValidFuzzOutcome(t *testing.T) {
	events, err := DecodeStream(bytes.NewReader([]byte("event: error\ndata: {}\n\n")))
	if err != nil {
		t.Fatal(err)
	}
	streamtest.AssertEventsAssemble(t, "anthropic-fuzz", events)
}
