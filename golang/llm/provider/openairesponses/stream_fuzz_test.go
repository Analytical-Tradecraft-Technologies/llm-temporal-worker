package openairesponses

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

func FuzzDecodeStream(f *testing.F) {
	streamtest.FuzzDecode(f, "testdata/contracts/events.wire", "openai-responses-fuzz", DecodeStream, "event: response.completed\ndata: {}\n\n")
}

func TestDecodedProviderTerminalErrorIsValidFuzzOutcome(t *testing.T) {
	streamtest.AssertDecodedAssembles(t, "openai-responses-fuzz", "event: response.failed\ndata: {}\n\n", DecodeStream)
}
