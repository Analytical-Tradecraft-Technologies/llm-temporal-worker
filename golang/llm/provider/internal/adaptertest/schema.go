package adaptertest

import (
	"encoding/json"
	"strings"
	"testing"
)

// Schemas whose properties are listed out of "required" order, so a test
// can check the provider receives them in required order (#1096). The
// exact-number variants also carry numbers a float64 cannot hold.
const (
	OutOfOrderToolSchema    = `{"type":"object","required":["zeta","alpha"],"properties":{"alpha":{"type":"string"},"zeta":{"type":"string"}}}`
	OutOfOrderOutputSchema  = `{"type":"object","required":["reasoning","answer"],"properties":{"answer":{"type":"string"},"reasoning":{"type":"string"}},"additionalProperties":false}`
	ExactNumberToolSchema   = `{"type":"object","required":["zeta","alpha"],"properties":{"alpha":{"type":"number","const":0.100000000000000000001,"maximum":1.234567890123456789e+20},"zeta":{"type":"integer","enum":[9007199254740993],"minimum":9007199254740993}}}`
	ExactNumberOutputSchema = `{"type":"object","required":["reasoning","answer"],"properties":{"answer":{"type":"number","const":0.100000000000000000001,"maximum":1.234567890123456789e+20},"reasoning":{"type":"integer","enum":[9007199254740993],"minimum":9007199254740993}},"additionalProperties":false}`
)

// PropertiesInRequiredOrder checks that a request body built from the
// out-of-order or exact-number schemas lists every property in "required"
// order.
func PropertiesInRequiredOrder(t *testing.T, body []byte) {
	t.Helper()
	for _, pair := range [][2]string{{`"reasoning":`, `"answer":`}, {`"zeta":`, `"alpha":`}} {
		first, second := strings.Index(string(body), pair[0]), strings.Index(string(body), pair[1])
		if first < 0 || second < 0 || first > second {
			t.Fatalf("request body does not list %s before %s:\n%s", pair[0], pair[1], body)
		}
	}
}

// StrictOutputValidation checks that the constraints a strict output schema
// moves out of the wire schema are still enforced on the output, and that a
// number the schema validator cannot convert is an invalid output, not an
// Activity panic after payment (#1522). validate compiles a strict
// structured-output request for schema and validates a final model message
// of text against the call.
func StrictOutputValidation(t *testing.T, validate func(t *testing.T, schema, text string) error) {
	for _, test := range []struct {
		name   string
		schema string
		texts  map[string]bool
	}{
		{name: "caller schema constraint", schema: `{"type":"object","properties":{"code":{"type":"string","minLength":3}},"required":["code"],"additionalProperties":false}`, texts: map[string]bool{`{"code":"abc"}`: true, `{"code":"ab"}`: false}},
		{name: "unbounded number", schema: `{"type":"object","properties":{"amount":{"type":"number","minimum":0}},"required":["amount"],"additionalProperties":false}`, texts: map[string]bool{`{"amount":1e3}`: true, `{"amount":1e3000000}`: false, `{"amount":-1e3000000}`: false}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for text, valid := range test.texts {
				if err := validate(t, test.schema, text); (err == nil) != valid {
					t.Fatalf("%s valid=%v: %v", text, valid, err)
				}
			}
		})
	}
}

// ExactSchemaNumbers checks that every schema sent for an exact-number
// schema keeps each constraint's exact number. A strict output schema
// carries bounds in its description note (#1353); the number must stay
// exact either way.
func ExactSchemaNumbers(t *testing.T, schemas ...json.RawMessage) {
	t.Helper()
	for _, raw := range schemas {
		for _, literal := range [][2]string{{`"enum":[9007199254740993]`, ""}, {`"minimum":9007199254740993`, "minimum: 9007199254740993"}, {`"const":0.100000000000000000001`, ""}, {`"maximum":1.234567890123456789e+20`, "maximum: 1.234567890123456789e+20"}} {
			if !strings.Contains(string(raw), literal[0]) && (literal[1] == "" || !strings.Contains(string(raw), literal[1])) {
				t.Fatalf("schema constraint changed in transport (%s): %s", literal[0], raw)
			}
		}
	}
}
