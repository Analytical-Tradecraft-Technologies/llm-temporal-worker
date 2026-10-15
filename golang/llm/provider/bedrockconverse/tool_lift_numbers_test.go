package bedrockconverse

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// The SDK decodes tool input into a float64 document, so the lift must take
// the arguments from the response body to keep integers beyond 2^53 exact.
func TestToolUseLiftPreservesJSONNumbers(t *testing.T) {
	for _, input := range []string{
		`{"account_id":9007199254740993,"snowflake":1790000000000000001,"price":0.1}`,
		`{"negative":-9007199254740993,"unsigned":18446744073709551615}`,
		`{"nested":[{"decimal":0.123456789012345678901,"exponent":1.234567890123456789e+25},true,null,"9007199254740993"]}`,
	} {
		t.Run(input, func(t *testing.T) {
			body := `{"output":{"message":{"role":"assistant","content":[{"text":"checking"},{"toolUse":{"toolUseId":"tooluse_1","name":"lookup","input":` + input + `}}]}},"stopReason":"tool_use","usage":{"inputTokens":4,"outputTokens":3,"totalTokens":7}}`
			result, err := invokeCannedConverse(t, body)
			if err != nil {
				t.Fatal(err)
			}
			if result.Response.Status != llm.ResponseStatusToolCalls || len(result.Response.Output) != 2 {
				t.Fatalf("response = %#v", result.Response)
			}
			call, ok := result.Response.Output[1].(llm.ToolCall)
			if !ok || call.ID != "tooluse_1" || call.Name != "lookup" {
				t.Fatalf("tool call = %#v", result.Response.Output[1])
			}
			got, err := llm.CanonicalJSON(call.Arguments)
			if err != nil {
				t.Fatal(err)
			}
			want, err := llm.CanonicalJSON([]byte(input))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("tool arguments = %s, want %s", got, want)
			}
		})
	}
}

// Raw arguments are kept byte for byte, so input the normalized tool call
// cannot carry is an accepted invalid response.
func TestToolUseLiftRejectsInvalidRawInput(t *testing.T) {
	for name, input := range map[string]string{
		"duplicate key": `{"q":"a","q":"b"}`,
		"invalid utf-8": "{\"q\":\"a\xffb\"}",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := invokeCannedConverse(t, `{"output":{"message":{"role":"assistant","content":[{"toolUse":{"toolUseId":"tooluse_1","name":"lookup","input":`+input+`}}]}},"stopReason":"tool_use","usage":{"inputTokens":4,"outputTokens":3,"totalTokens":7}}`)
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Phase != provider.PhaseLift || mapped.Dispatch != provider.DispatchAccepted {
				t.Fatalf("tool input error = %#v", err)
			}
		})
	}
}
