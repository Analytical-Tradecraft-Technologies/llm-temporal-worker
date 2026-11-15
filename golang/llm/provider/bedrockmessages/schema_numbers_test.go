package bedrockmessages

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

func TestInvokePreservesExactSchemaNumbers(t *testing.T) {
	messagestest.InvokePreservesExactSchemaNumbers(t, func(t *testing.T, schema string) []byte {
		var wire []byte
		adapter := recordingBedrockAdapter(t, `{}`, &wire)
		input := bedrockCompileInput(structuredOutputRequest(schema), true)
		input.Request.Tools = []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(schema)}}
		call, err := adapter.Compile(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		// The synthetic output intentionally does not satisfy the schema; inspect the request.
		_, _ = adapter.Invoke(context.Background(), call, nil)
		return wire
	})
}
