package anthropicmessages

import (
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

func TestDecodeStreamPreservesTextToolAndReasoningAcrossFragments(t *testing.T) {
	wire := []byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":3}}}\n\nevent: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\nevent: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\nevent: content_block_stop\ndata: {\"index\":0}\n\nevent: content_block_start\ndata: {\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call-1\",\"name\":\"lookup\"}}\n\nevent: content_block_delta\ndata: {\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"q\\\":\\\"sydney\\\"}\"}}\n\nevent: content_block_stop\ndata: {\"index\":1}\n\nevent: message_delta\ndata: {\"usage\":{\"output_tokens\":2}}\n\nevent: message_stop\ndata: {}\n\n")
	want, err := DecodeStream(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	for _, chunks := range [][][]byte{streamtest.Fragment(wire, 1), streamtest.RandomFragment(wire, 3, 11)} {
		got, err := DecodeStream(&anthropicChunkReader{chunks: chunks})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("fragmented events differ\n got: %#v\nwant: %#v", got, want)
		}
	}
	assembler := provider.NewAssembler("anthropic-stream")
	for _, event := range want {
		if err := assembler.Add(event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := assembler.Result(); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeStreamEnumeratesSplitPointsEmptyReadsAndCRLF(t *testing.T) {
	wire := []byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":3}}}\n\nevent: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\nevent: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\nevent: content_block_stop\ndata: {\"index\":0}\n\nevent: message_delta\ndata: {\"usage\":{\"output_tokens\":2}}\n\nevent: message_stop\ndata: {}\n\n")
	for _, variant := range []struct {
		name string
		wire []byte
	}{
		{name: "lf", wire: wire},
		{name: "crlf", wire: streamtest.CRLF(wire)},
	} {
		t.Run(variant.name, func(t *testing.T) {
			want, err := DecodeStream(bytes.NewReader(variant.wire))
			if err != nil {
				t.Fatal(err)
			}
			for split, chunks := range streamtest.TwoPartFragments(variant.wire) {
				got, err := DecodeStream(&anthropicChunkReader{chunks: streamtest.WithEmptyChunks(chunks)})
				if err != nil {
					t.Fatalf("split %d: %v", split, err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("split %d differs\n got: %#v\nwant: %#v", split, got, want)
				}
			}
		})
	}
}

func TestDecodeStreamRejectsEventAfterTerminal(t *testing.T) {
	wire := "event: message_stop\ndata: {}\n\nevent: ping\ndata: {}\n\n"
	if _, err := DecodeStream(strings.NewReader(wire)); err == nil {
		t.Fatal("post-terminal event unexpectedly succeeded")
	}
}

type anthropicChunkReader struct {
	chunks [][]byte
	index  int
}

func (reader *anthropicChunkReader) Read(buffer []byte) (int, error) {
	if reader.index >= len(reader.chunks) {
		return 0, io.EOF
	}
	chunk := reader.chunks[reader.index]
	if len(chunk) == 0 {
		reader.index++
		return 0, nil
	}
	n := copy(buffer, chunk)
	reader.chunks[reader.index] = chunk[n:]
	if len(reader.chunks[reader.index]) == 0 {
		reader.index++
	}
	return n, nil
}
