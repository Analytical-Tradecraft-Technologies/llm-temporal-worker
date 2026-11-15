package streamtest

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
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

// Decode decodes a provider stream into events.
type Decode func(io.Reader) ([]provider.Event, error)

// FuzzDecode seeds f with the fixture at fixturePath and the extra seeds,
// and checks that every stream of up to 2 MiB that decode accepts satisfies
// AssertEventsAssemble for operationKey.
func FuzzDecode(f *testing.F, fixturePath, operationKey string, decode Decode, seeds ...string) {
	fixture, err := os.ReadFile(fixturePath)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(fixture)
	for _, seed := range seeds {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, wire []byte) {
		if len(wire) > 2<<20 {
			return
		}
		events, err := decode(bytes.NewReader(wire))
		if err != nil {
			return
		}
		AssertEventsAssemble(t, operationKey, events)
	})
}

// FragmentationInvariant checks that decode returns the same events for
// wire read whole, split at every point with an empty read at the split,
// one byte at a time, and in chunks of up to maxChunk bytes from seed, and returns
// those events.
func FragmentationInvariant(t *testing.T, wire []byte, seed int64, maxChunk int, decode Decode) []provider.Event {
	t.Helper()
	want, err := decode(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	for split := 1; split < len(wire); split++ {
		got, err := decode(NewChunkReader([][]byte{wire[:split], {}, wire[split:]}))
		if err != nil {
			t.Fatalf("split %d: %v", split, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("split %d changed decoded events\n got: %#v\nwant: %#v", split, got, want)
		}
	}
	assertFragmentsDecodeTo(t, want, wire, seed, maxChunk, decode)
	return want
}

// FragmentsAssemble checks that decode returns the same events for wire read
// whole, one byte at a time, and in chunks of up to maxChunk bytes from seed,
// and that those events assemble into a response for operationKey.
func FragmentsAssemble(t *testing.T, wire []byte, seed int64, maxChunk int, operationKey string, decode Decode) {
	t.Helper()
	want, err := decode(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	assertFragmentsDecodeTo(t, want, wire, seed, maxChunk, decode)
	if _, err := Assemble(operationKey, want); err != nil {
		t.Fatal(err)
	}
}

// assertFragmentsDecodeTo checks that decode returns want for wire read one
// byte at a time and in chunks of up to maxChunk bytes from seed.
func assertFragmentsDecodeTo(t *testing.T, want []provider.Event, wire []byte, seed int64, maxChunk int, decode Decode) {
	t.Helper()
	for _, chunks := range [][][]byte{Fragment(wire, 1), RandomFragment(wire, seed, maxChunk)} {
		got, err := decode(NewChunkReader(chunks))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("fragmented events differ\n got: %#v\nwant: %#v", got, want)
		}
	}
}

// Assemble assembles events into the response for operationKey.
func Assemble(operationKey string, events []provider.Event) (llm.Response, error) {
	assembler := provider.NewAssembler(operationKey)
	for _, event := range events {
		if err := assembler.Add(event); err != nil {
			return llm.Response{}, err
		}
	}
	return assembler.Result()
}

// AssembleJSON decodes wire and returns the JSON encoding of the response
// its events assemble into for operationKey.
func AssembleJSON(wire []byte, operationKey string, decode Decode) ([]byte, error) {
	events, err := decode(bytes.NewReader(wire))
	if err != nil {
		return nil, err
	}
	response, err := Assemble(operationKey, events)
	if err != nil {
		return nil, err
	}
	return json.Marshal(response)
}

// AssertDecodedAssembles decodes wire and checks the events with
// AssertEventsAssemble for operationKey.
func AssertDecodedAssembles(t *testing.T, operationKey, wire string, decode Decode) {
	t.Helper()
	events, err := decode(strings.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	AssertEventsAssemble(t, operationKey, events)
}
