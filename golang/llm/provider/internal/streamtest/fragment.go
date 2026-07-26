package streamtest

import (
	"bytes"
	"math/rand"
)

// Fragment returns deterministic transport chunks. A zero or negative chunk
// size uses one byte per fragment; callers can use the same input with every
// split point and seeded random chunk sizes.
func Fragment(data []byte, chunkSize int) [][]byte {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	result := make([][]byte, 0, (len(data)+chunkSize-1)/chunkSize)
	for len(data) > 0 {
		n := chunkSize
		if n > len(data) {
			n = len(data)
		}
		result = append(result, append([]byte(nil), data[:n]...))
		data = data[n:]
	}
	return result
}

// RandomFragment uses a local deterministic PRNG and never returns an empty
// chunk, making it suitable for repeatable decoder fuzz seeds.
func RandomFragment(data []byte, seed int64, maxChunk int) [][]byte {
	if maxChunk <= 0 {
		maxChunk = 16
	}
	random := rand.New(rand.NewSource(seed))
	result := make([][]byte, 0)
	for len(data) > 0 {
		n := 1 + random.Intn(maxChunk)
		if n > len(data) {
			n = len(data)
		}
		result = append(result, append([]byte(nil), data[:n]...))
		data = data[n:]
	}
	return result
}

// TwoPartFragments returns a fresh pair (or a single boundary chunk) for
// every possible split point in data. It is useful for proving that a
// decoder does not depend on a particular network read boundary.
func TwoPartFragments(data []byte) [][][]byte {
	result := make([][][]byte, 0, len(data)+1)
	for split := 0; split <= len(data); split++ {
		chunks := make([][]byte, 0, 2)
		if split > 0 {
			chunks = append(chunks, append([]byte(nil), data[:split]...))
		}
		if split < len(data) {
			chunks = append(chunks, append([]byte(nil), data[split:]...))
		}
		result = append(result, chunks)
	}
	return result
}

// WithEmptyChunks places a zero-length read before, between, and after the
// supplied transport chunks. Readers are allowed to return (0, nil), so this
// ensures decoders make progress without treating an empty read as EOF.
func WithEmptyChunks(chunks [][]byte) [][]byte {
	result := make([][]byte, 0, len(chunks)*2+1)
	result = append(result, nil)
	for _, chunk := range chunks {
		result = append(result, append([]byte(nil), chunk...))
		result = append(result, nil)
	}
	return result
}

// CRLF converts LF-delimited fixtures to the equivalent CRLF wire form.
func CRLF(data []byte) []byte {
	return bytes.ReplaceAll(data, []byte("\n"), []byte("\r\n"))
}
