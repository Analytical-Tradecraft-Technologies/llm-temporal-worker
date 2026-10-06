package bedrockmessages

import (
	"io"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamdecode"
)

// Bedrock middleware normalizes EventStream responses into Anthropic-shaped
// SSE, so the same strict decoder applies after official SDK translation.
func DecodeStream(reader io.Reader) ([]provider.Event, error) {
	events, err := streamdecode.Parse(reader)
	if err != nil {
		return nil, err
	}
	return streamdecode.DecodeAnthropic(events)
}

func Decode(reader io.Reader) ([]provider.Event, error) { return DecodeStream(reader) }
