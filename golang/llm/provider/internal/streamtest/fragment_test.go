package streamtest

import (
	"bytes"
	"reflect"
	"testing"
)

func TestFragmentUsesFixedNonEmptyChunksAndCopiesInput(t *testing.T) {
	data := []byte("abcdef")
	tests := []struct {
		name      string
		chunkSize int
		want      [][]byte
	}{
		{name: "single byte", chunkSize: 1, want: chunks("a", "b", "c", "d", "e", "f")},
		{name: "even split", chunkSize: 2, want: chunks("ab", "cd", "ef")},
		{name: "partial final chunk", chunkSize: 4, want: chunks("abcd", "ef")},
		{name: "zero defaults to bytes", chunkSize: 0, want: chunks("a", "b", "c", "d", "e", "f")},
		{name: "negative defaults to bytes", chunkSize: -1, want: chunks("a", "b", "c", "d", "e", "f")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := Fragment(data, test.chunkSize)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("fragments = %#v, want %#v", got, test.want)
			}
			if len(got) == 0 {
				t.Fatal("returned no fragments for non-empty input")
			}
			got[0][0] = 'X'
			if data[0] != 'a' {
				t.Fatal("fragment aliases input data")
			}
		})
	}
	if got := Fragment(nil, 4); len(got) != 0 {
		t.Fatalf("empty input produced %d fragments", len(got))
	}
}

func TestRandomFragmentIsDeterministicBoundedAndReconstructsInput(t *testing.T) {
	data := []byte("the quick brown fox jumps over the lazy dog")
	first := RandomFragment(data, 42, 7)
	second := RandomFragment(data, 42, 7)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different fragments\nfirst: %#v\nsecond: %#v", first, second)
	}
	if len(first) == 0 {
		t.Fatal("returned no fragments for non-empty input")
	}
	for index, fragment := range first {
		if len(fragment) == 0 || len(fragment) > 7 {
			t.Fatalf("fragment %d length = %d, want 1..7", index, len(fragment))
		}
	}
	if got := join(first); !bytes.Equal(got, data) {
		t.Fatalf("reconstructed data = %q, want %q", got, data)
	}

	for _, maxChunk := range []int{0, -1} {
		fragments := RandomFragment(data, 42, maxChunk)
		for index, fragment := range fragments {
			if len(fragment) == 0 || len(fragment) > 16 {
				t.Fatalf("default fragment %d length = %d, want 1..16", index, len(fragment))
			}
		}
		if got := join(fragments); !bytes.Equal(got, data) {
			t.Fatalf("default reconstructed data = %q, want %q", got, data)
		}
	}
}

func TestTwoPartFragmentsEnumeratesEverySplitAndEmptyReadsPreserveInput(t *testing.T) {
	data := []byte("abcdef")
	fragments := TwoPartFragments(data)
	if len(fragments) != len(data)+1 {
		t.Fatalf("split count = %d, want %d", len(fragments), len(data)+1)
	}
	for split, chunks := range fragments {
		if got := join(chunks); !bytes.Equal(got, data) {
			t.Fatalf("split %d reconstructed %q, want %q", split, got, data)
		}
		withEmpty := WithEmptyChunks(chunks)
		if len(withEmpty) != len(chunks)*2+1 {
			t.Fatalf("split %d empty chunk count = %d, want %d", split, len(withEmpty), len(chunks)*2+1)
		}
		if withEmpty[0] != nil || withEmpty[len(withEmpty)-1] != nil {
			t.Fatalf("split %d missing boundary empty reads: %#v", split, withEmpty)
		}
		if got := join(withEmpty); !bytes.Equal(got, data) {
			t.Fatalf("split %d with empty reads reconstructed %q, want %q", split, got, data)
		}
	}
}

func TestCRLFConvertsLineEndings(t *testing.T) {
	if got := string(CRLF([]byte("a\nb\n"))); got != "a\r\nb\r\n" {
		t.Fatalf("CRLF conversion = %q", got)
	}
}

func chunks(values ...string) [][]byte {
	result := make([][]byte, len(values))
	for index, value := range values {
		result[index] = []byte(value)
	}
	return result
}

func join(chunks [][]byte) []byte {
	return bytes.Join(chunks, nil)
}
