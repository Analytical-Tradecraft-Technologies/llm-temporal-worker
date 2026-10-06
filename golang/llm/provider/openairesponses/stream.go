package openairesponses

import (
	"io"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamdecode"
)

// DecodeStream consumes an OpenAI Responses SSE stream and returns neutral
// events after reconstructing arbitrary transport fragmentation.
func DecodeStream(reader io.Reader) ([]provider.Event, error) {
	events, err := streamdecode.Parse(reader)
	if err != nil {
		return nil, err
	}
	return streamdecode.DecodeResponses(events)
}

func Decode(reader io.Reader) ([]provider.Event, error) { return DecodeStream(reader) }
