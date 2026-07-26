package openaichat

import (
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

func TestDecodeStreamIsFragmentationInvariant(t *testing.T) {
	wire := []byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hel\"}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")
	want, err := DecodeStream(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	for _, chunks := range [][][]byte{streamtest.Fragment(wire, 1), streamtest.RandomFragment(wire, 17, 5)} {
		got, err := DecodeStream(&chunkReader{chunks: chunks})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("fragmented events differ\n got: %#v\nwant: %#v", got, want)
		}
	}
	assembler := provider.NewAssembler("chat-stream")
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
	wire := []byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hel\"}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")
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
				got, err := DecodeStream(&chunkReader{chunks: streamtest.WithEmptyChunks(chunks)})
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

type chunkReader struct {
	chunks [][]byte
	index  int
}

func (reader *chunkReader) Read(buffer []byte) (int, error) {
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
