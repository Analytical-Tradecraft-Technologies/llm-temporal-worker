package openaichat

import (
	"io"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamdecode"
)

// DecodeStream consumes an OpenAI-compatible Chat Completions SSE stream and
// returns provider-neutral events. The reader may return arbitrary transport
// fragments; SSE framing is reconstructed before JSON decoding.
func DecodeStream(reader io.Reader) ([]provider.Event, error) {
	return streamdecode.ParseAndDecode(reader, streamdecode.DecodeChat)
}

// Decode is an alias for DecodeStream used by generic stream harnesses.
func Decode(reader io.Reader) ([]provider.Event, error) { return DecodeStream(reader) }
