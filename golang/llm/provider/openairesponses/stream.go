package openairesponses

import (
	"io"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamdecode"
)

// DecodeStream consumes an OpenAI Responses SSE stream and returns neutral
// events after reconstructing arbitrary transport fragmentation.
func DecodeStream(reader io.Reader) ([]provider.Event, error) {
	return streamdecode.ParseAndDecode(reader, streamdecode.DecodeResponses)
}

func Decode(reader io.Reader) ([]provider.Event, error) { return DecodeStream(reader) }
