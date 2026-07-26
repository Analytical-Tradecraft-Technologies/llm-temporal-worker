package bedrockmessages

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

func TestDecodeStreamMatchesAnthropicMiddlewareEventSemantics(t *testing.T) {
	wire := []byte("event: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\nevent: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"bedrock\"}}\n\nevent: content_block_stop\ndata: {\"index\":0}\n\nevent: message_stop\ndata: {}\n\n")
	want, err := DecodeStream(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeStream(&bedrockChunkReader{chunks: streamtest.RandomFragment(wire, 99, 7)})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fragmented events differ\n got: %#v\nwant: %#v", got, want)
	}
	assembler := provider.NewAssembler("bedrock-stream")
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
	wire := []byte("event: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\nevent: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"bedrock\"}}\n\nevent: content_block_stop\ndata: {\"index\":0}\n\nevent: message_stop\ndata: {}\n\n")
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
				got, err := DecodeStream(&bedrockChunkReader{chunks: streamtest.WithEmptyChunks(chunks)})
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

type bedrockChunkReader struct {
	chunks [][]byte
	index  int
}

func (reader *bedrockChunkReader) Read(buffer []byte) (int, error) {
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
