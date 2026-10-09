package anthropicmessages

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestInvokePreservesExactSchemaNumbers(t *testing.T) {
	const schema = `{"type":"object","properties":{"alpha":{"type":"number","const":0.100000000000000000001,"maximum":1.234567890123456789e+20},"zeta":{"type":"integer","enum":[9007199254740993,-9007199254740993,18446744073709551615],"minimum":9007199254740993}},"required":["zeta","alpha"],"additionalProperties":false}`
	var wire []byte
	adapter := structuredOutputAdapter(t, `{}`, &wire)
	input := structuredOutputInput(schema, true)
	input.Request.Tools = []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(schema)}}
	call, err := adapter.Compile(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	// The synthetic output intentionally does not satisfy the schema; inspect the request.
	_, _ = adapter.Invoke(context.Background(), call, nil)
	var body struct {
		Tools []struct {
			InputSchema json.RawMessage `json:"input_schema"`
		} `json:"tools"`
		OutputConfig struct {
			Format struct {
				Schema json.RawMessage `json:"schema"`
			} `json:"format"`
		} `json:"output_config"`
	}
	if err := json.Unmarshal(wire, &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Tools) != 1 {
		t.Fatalf("tool schemas missing: %s", wire)
	}
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
	if !reflect.DeepEqual(decode(body.Tools[0].InputSchema), decode([]byte(schema))) {
		t.Fatalf("tool schema changed on transport: %s", body.Tools[0].InputSchema)
	}
	for _, raw := range []json.RawMessage{body.Tools[0].InputSchema, body.OutputConfig.Format.Schema} {
		for _, literal := range []string{"9007199254740993", "-9007199254740993", "18446744073709551615", "0.100000000000000000001", "1.234567890123456789e+20"} {
			if !bytes.Contains(raw, []byte(literal)) {
				t.Fatalf("schema lost exact number %s: %s", literal, raw)
			}
		}
		if strings.Index(string(raw), `"zeta":`) > strings.Index(string(raw), `"alpha":`) {
			t.Fatalf("required property order changed: %s", raw)
		}
	}
}
