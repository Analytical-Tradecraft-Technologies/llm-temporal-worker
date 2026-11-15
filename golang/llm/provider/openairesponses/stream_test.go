package openairesponses

import (
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

// streamWire is a complete stream that decodes and assembles.
var streamWire = []byte("event: response.created\ndata: {\"type\":\"response.created\"}\n\nevent: response.output_item.added\ndata: {\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\nevent: response.output_text.delta\ndata: {\"output_index\":0,\"delta\":\"hello\"}\n\nevent: response.output_item.done\ndata: {\"output_index\":0}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\"}\n\n")

func TestDecodeStreamIsFragmentationInvariant(t *testing.T) {
	streamtest.FragmentsAssemble(t, streamWire, 4, 9, "responses-stream", DecodeStream)
}

func TestDecodeStreamEnumeratesSplitPointsEmptyReadsAndCRLF(t *testing.T) {
	streamtest.SplitInvariant(t, streamWire, DecodeStream)
}

func TestDecodeStreamRejectsUnknownEvent(t *testing.T) {
	if _, err := DecodeStream(strings.NewReader("event: response.unknown\ndata: {}\n\n")); err == nil {
		t.Fatal("unknown event unexpectedly succeeded")
	}
}

func TestDecodeStreamRejectsTerminalWithUnfinishedOutput(t *testing.T) {
	wire := "event: response.output_item.added\ndata: {}\n\nevent: response.completed\ndata: {}\n\n"
	if _, err := DecodeStream(strings.NewReader(wire)); err == nil {
		t.Fatal("terminal stream with unfinished output unexpectedly succeeded")
	}
}
