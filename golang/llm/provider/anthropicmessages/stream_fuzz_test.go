package anthropicmessages

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

func FuzzDecodeStream(f *testing.F) {
	streamtest.FuzzDecode(f, "testdata/contracts/events.wire", "anthropic-fuzz", DecodeStream, "event: message_stop\ndata: {}\n\n")
}

func TestDecodedProviderTerminalErrorIsValidFuzzOutcome(t *testing.T) {
	streamtest.AssertDecodedAssembles(t, "anthropic-fuzz", "event: error\ndata: {}\n\n", DecodeStream)
}
