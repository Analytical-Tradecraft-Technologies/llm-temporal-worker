package streamtest

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// ChunkReader returns its chunks one Read at a time, splitting a chunk only
// when the buffer is smaller. An empty chunk is a Read of zero bytes with no
// error, as a slow transport can return.
type ChunkReader struct {
	chunks [][]byte
	index  int
}

// NewChunkReader reads chunks in order, then io.EOF.
func NewChunkReader(chunks [][]byte) *ChunkReader {
	return &ChunkReader{chunks: chunks}
}

func (reader *ChunkReader) Read(buffer []byte) (int, error) {
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

// SplitInvariant checks that decode returns the same value for wire, with LF
// and with CRLF line endings, whether it reads the stream whole or in two
// parts at every split point with empty reads around the split.
func SplitInvariant[T any](t *testing.T, wire []byte, decode func(io.Reader) (T, error)) {
	t.Helper()
	for _, variant := range []struct {
		name string
		wire []byte
	}{
		{name: "lf", wire: wire},
		{name: "crlf", wire: CRLF(wire)},
	} {
		t.Run(variant.name, func(t *testing.T) {
			want, err := decode(bytes.NewReader(variant.wire))
			if err != nil {
				t.Fatal(err)
			}
			for split, chunks := range TwoPartFragments(variant.wire) {
				got, err := decode(NewChunkReader(WithEmptyChunks(chunks)))
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

// AssertEventsAssemble checks that decoded events satisfy the assembler
// contract: a stream ending in a provider terminal error assembles no
// response, and any other stream assembles a valid one for operationKey.
func AssertEventsAssemble(t *testing.T, operationKey string, events []provider.Event) {
	t.Helper()
	assembler := provider.NewAssembler(operationKey)
	for index, event := range events {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("decoded event %d (%T) violates assembler contract: %v", index, event, err)
		}
	}
	response, err := assembler.Result()
	if len(events) > 0 {
		if _, ok := events[len(events)-1].(provider.StreamErrored); ok {
			if err == nil {
				t.Fatal("decoded provider terminal error unexpectedly assembled a response")
			}
			return
		}
	}
	if err != nil {
		t.Fatalf("decoded terminal stream did not assemble: %v", err)
	}
	if response.OperationKey != operationKey || !response.Status.Valid() {
		t.Fatalf("assembled response is not semantic: %#v", response)
	}
}
