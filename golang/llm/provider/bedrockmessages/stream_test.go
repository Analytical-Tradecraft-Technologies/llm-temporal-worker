package bedrockmessages

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

// streamWire is a complete stream that decodes and assembles.
var streamWire = []byte("event: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\nevent: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"bedrock\"}}\n\nevent: content_block_stop\ndata: {\"index\":0}\n\nevent: message_stop\ndata: {}\n\n")

func TestDecodeStreamMatchesAnthropicMiddlewareEventSemantics(t *testing.T) {
	streamtest.FragmentsAssemble(t, streamWire, 99, 7, "bedrock-stream", DecodeStream)
}

func TestDecodeStreamEnumeratesSplitPointsEmptyReadsAndCRLF(t *testing.T) {
	streamtest.SplitInvariant(t, streamWire, DecodeStream)
}
