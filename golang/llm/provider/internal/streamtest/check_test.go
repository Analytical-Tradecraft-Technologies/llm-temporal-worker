package streamtest

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"io"
	"testing"
)

func TestChunkReaderReturnsEachChunkThenEOF(t *testing.T) {
	reader := NewChunkReader([][]byte{[]byte("ab"), {}, []byte("c")})
	buffer := make([]byte, 1)
	var reads []string
	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			break
		}
		testassert.NoError(t, err)
		reads = append(reads, string(buffer[:n]))
	}
	if got := len(reads); got != 4 || reads[0] != "a" || reads[1] != "b" || reads[2] != "" || reads[3] != "c" {
		t.Fatalf("reads = %q", reads)
	}
}

func TestSplitInvariantAcceptsAWholeStreamDecoder(t *testing.T) {
	SplitInvariant(t, []byte("data: a\n\ndata: b\n\n"), func(reader io.Reader) (int, error) {
		data, err := io.ReadAll(reader)
		return len(data), err
	})
}
