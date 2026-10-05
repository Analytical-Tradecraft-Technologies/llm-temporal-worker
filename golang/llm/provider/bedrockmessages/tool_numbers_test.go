package bedrockmessages

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// Assert the encoded provider document, not a float64-decoded approximation.
func TestToolHistoryPreservesJSONNumbers(t *testing.T) {
	for _, arguments := range []string{
		`{"account_id":9007199254740993}`,
		`{"negative":-9007199254740993,"unsigned":18446744073709551615}`,
		`{"nested":[{"decimal":0.123456789012345678901,"exponent":1.234567890123456789e+25},true,null,"9007199254740993"]}`,
	} {
		t.Run(arguments, func(t *testing.T) {
			request := llm.Request{Model: "contract-model", Input: []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(arguments)}}}
			params, err := lowerRequest(request, mustBedrockProfile(t, ""))
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(params)
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				Messages []struct {
					Content []struct {
						Input json.RawMessage `json:"input"`
					} `json:"content"`
				} `json:"messages"`
			}
			if err := json.Unmarshal(encoded, &wire); err != nil {
				t.Fatal(err)
			}
			actual := []byte(wire.Messages[0].Content[0].Input)
			decode := func(data []byte) any {
				t.Helper()
				decoder := json.NewDecoder(bytes.NewReader(data))
				decoder.UseNumber()
				var value any
				if err := decoder.Decode(&value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			if !reflect.DeepEqual(decode(actual), decode([]byte(arguments))) {
				t.Fatalf("tool input changed: got %s, want %s", actual, arguments)
			}
		})
	}
}
