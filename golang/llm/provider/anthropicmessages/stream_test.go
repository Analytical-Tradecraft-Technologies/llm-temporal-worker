package anthropicmessages

import (
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

func TestDecodeStreamPreservesTextToolAndReasoningAcrossFragments(t *testing.T) {
	wire := []byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":3}}}\n\nevent: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\nevent: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\nevent: content_block_stop\ndata: {\"index\":0}\n\nevent: content_block_start\ndata: {\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call-1\",\"name\":\"lookup\"}}\n\nevent: content_block_delta\ndata: {\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"q\\\":\\\"sydney\\\"}\"}}\n\nevent: content_block_stop\ndata: {\"index\":1}\n\nevent: message_delta\ndata: {\"usage\":{\"output_tokens\":2}}\n\nevent: message_stop\ndata: {}\n\n")
	streamtest.FragmentsAssemble(t, wire, 3, 11, "anthropic-stream", DecodeStream)
}

func TestDecodeStreamEnumeratesSplitPointsEmptyReadsAndCRLF(t *testing.T) {
	wire := []byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":3}}}\n\nevent: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\nevent: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\nevent: content_block_stop\ndata: {\"index\":0}\n\nevent: message_delta\ndata: {\"usage\":{\"output_tokens\":2}}\n\nevent: message_stop\ndata: {}\n\n")
	streamtest.SplitInvariant(t, wire, DecodeStream)
}

func TestDecodeStreamRejectsEventAfterTerminal(t *testing.T) {
	wire := "event: message_stop\ndata: {}\n\nevent: ping\ndata: {}\n\n"
	if _, err := DecodeStream(strings.NewReader(wire)); err == nil {
		t.Fatal("post-terminal event unexpectedly succeeded")
	}
}
