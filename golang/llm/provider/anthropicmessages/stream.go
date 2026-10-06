package anthropicmessages

import (
	"io"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamdecode"
)

// DecodeStream consumes the Anthropic Messages SSE event stream. Thinking and
// signature fragments stay opaque provider-neutral reasoning deltas.
func DecodeStream(reader io.Reader) ([]provider.Event, error) {
	events, err := streamdecode.Parse(reader)
	if err != nil {
		return nil, err
	}
	return streamdecode.DecodeAnthropic(events)
}

func Decode(reader io.Reader) ([]provider.Event, error) { return DecodeStream(reader) }
