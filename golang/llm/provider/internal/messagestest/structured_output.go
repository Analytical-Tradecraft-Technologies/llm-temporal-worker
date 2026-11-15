package messagestest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

// StructuredOutputAdapter is a Messages adapter under a structured-output
// test.
type StructuredOutputAdapter interface {
	adaptertest.Compiler
	adaptertest.Invoker
}

// NewStructuredOutputAdapter returns an adapter whose provider answers every
// request with one text block holding text and records the request body the
// SDK sent in wire.
type NewStructuredOutputAdapter func(t *testing.T, text string, wire *[]byte) StructuredOutputAdapter

// StructuredOutputInput returns the compile input for a request asking for
// strict JSON-schema output with schema; strict selects strict portability.
type StructuredOutputInput func(schema string, strict bool) provider.CompileInput

const constrainedOutputSchema = `{"type":"object","properties":{"name":{"type":"string","minLength":3},"age":{"type":"integer","minimum":0},"tags":{"type":"array","items":{"type":"string"},"maxItems":2}},"required":["name"]}`

func StructuredOutputSendsProviderValidSchemaAndValidatesCallerSchema(t *testing.T, newAdapter NewStructuredOutputAdapter, input StructuredOutputInput) {
	// Required properties come first, in "required" order (#1096).
	const wantSchema = `{"additionalProperties":false,"properties":{` +
		`"name":{"description":"{minLength: 3}","type":"string"},` +
		`"age":{"description":"{minimum: 0}","type":"integer"},` +
		`"tags":{"description":"{maxItems: 2}","items":{"type":"string"},"type":"array"}},` +
		`"required":["name"],"type":"object"}`
	for _, test := range []struct {
		name, text string
		valid      bool
	}{
		{name: "conforming", text: `{"name":"abc","age":1,"tags":["a"]}`, valid: true},
		{name: "string shorter than minLength", text: `{"name":"ab"}`},
		{name: "number below minimum", text: `{"name":"abc","age":-1}`},
		// The validator would dereference a nil big.Rat for this number (#1522).
		{name: "number with an unbounded exponent", text: `{"name":"abc","age":1e3000000}`},
		{name: "array longer than maxItems", text: `{"name":"abc","tags":["a","b","c"]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var wire []byte
			adapter := newAdapter(t, test.text, &wire)
			call, err := adapter.Compile(context.Background(), input(constrainedOutputSchema, true))
			if err != nil {
				t.Fatal(err)
			}
			result, err := adapter.Invoke(context.Background(), call, nil)
			var body struct {
				OutputConfig struct {
					Format struct {
						Type   string          `json:"type"`
						Schema json.RawMessage `json:"schema"`
					} `json:"format"`
				} `json:"output_config"`
			}
			if decodeErr := json.Unmarshal(wire, &body); decodeErr != nil {
				t.Fatalf("wire body %s: %v", wire, decodeErr)
			}
			if body.OutputConfig.Format.Type != "json_schema" || string(body.OutputConfig.Format.Schema) != wantSchema {
				t.Errorf("wire schema\n got: %s\nwant: %s", body.OutputConfig.Format.Schema, wantSchema)
			}
			if test.valid {
				if err != nil || result.Response.Status != llm.ResponseStatusCompleted {
					t.Fatalf("conforming response = %#v, %v", result.Response, err)
				}
				return
			}
			adaptertest.RequireLiftInvalidResponse(t, err, "non-conforming response error")
		})
	}
}

func StructuredOutputRejectsUnrepresentableSchemasAtCompile(t *testing.T, newAdapter NewStructuredOutputAdapter, input StructuredOutputInput) {
	var wire []byte
	adapter := newAdapter(t, `{}`, &wire)
	for _, test := range []struct {
		name, schema string
		strict       bool
	}{
		{name: "recursive root", schema: `{"type":"object","properties":{"child":{"$ref":"#"}}}`, strict: true},
		{name: "recursive definition", schema: `{"type":"object","properties":{"node":{"$ref":"#/$defs/node"}},"$defs":{"node":{"type":"object","properties":{"next":{"$ref":"#/$defs/node"}}}}}`, strict: false},
		{name: "open object in strict mode", schema: `{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":true}`, strict: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := adapter.Compile(context.Background(), input(test.schema, test.strict))
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeUnsupportedCapability || providerErr.Phase != provider.PhaseCompile || providerErr.Dispatch != provider.DispatchNotDispatched {
				t.Fatalf("compile error = %#v", err)
			}
		})
	}
	if wire != nil {
		t.Fatalf("rejected schema reached the provider: %s", wire)
	}
	// Best-effort may narrow an open object that declares its properties.
	call, err := adapter.Compile(context.Background(), input(`{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":true}`, false))
	if err != nil {
		t.Fatal(err)
	}
	schema := contract.JSONObject(t, call.SDKParams)["output_config"].(map[string]any)["format"].(map[string]any)["schema"].(map[string]any)
	if schema["additionalProperties"] != false {
		t.Fatalf("best-effort schema = %#v", schema)
	}
}

// A schema that is not recursive but is exponential to validate after payment
// is an invalid argument at compile, before anything is reserved or sent (#1521).
func StructuredOutputRejectsExponentialSchemaAtCompile(t *testing.T, newAdapter NewStructuredOutputAdapter, input StructuredOutputInput) {
	var wire []byte
	adapter := newAdapter(t, `{}`, &wire)
	_, err := adapter.Compile(context.Background(), input(adaptertest.ExponentialDAGSchema, false))
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeInvalidArgument || providerErr.Phase != provider.PhaseCompile || providerErr.Dispatch != provider.DispatchNotDispatched {
		t.Fatalf("compile error = %#v", err)
	}
	if wire != nil {
		t.Fatalf("rejected schema reached the provider: %s", wire)
	}
}
