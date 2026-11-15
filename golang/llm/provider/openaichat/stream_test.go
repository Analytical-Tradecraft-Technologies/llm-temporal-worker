package openaichat

import (
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

// streamWire is a complete stream that decodes and assembles.
var streamWire = []byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hel\"}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")

func TestDecodeStreamIsFragmentationInvariant(t *testing.T) {
	streamtest.FragmentsAssemble(t, streamWire, 17, 5, "chat-stream", DecodeStream)
}

func TestDecodeStreamEnumeratesSplitPointsEmptyReadsAndCRLF(t *testing.T) {
	streamtest.SplitInvariant(t, streamWire, DecodeStream)
}

func TestDecodeStreamRejectsTruncatedTerminal(t *testing.T) {
	if _, err := DecodeStream(strings.NewReader("data: {\"choices\":[]}\n\n")); err == nil {
		t.Fatal("truncated stream unexpectedly succeeded")
	}
}

func TestDecodeStreamRejectsTerminalWithUnfinishedOutput(t *testing.T) {
	wire := "data: {\"choices\":[{}]}\n\ndata: [DONE]\n\n"
	if _, err := DecodeStream(strings.NewReader(wire)); err == nil {
		t.Fatal("terminal stream with unfinished output unexpectedly succeeded")
	}
}
