package anthropicmessages

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

const constrainedOutputSchema = `{"type":"object","properties":{"name":{"type":"string","minLength":3},"age":{"type":"integer","minimum":0},"tags":{"type":"array","items":{"type":"string"},"maxItems":2}},"required":["name"]}`

// structuredOutputAdapter answers every request with one text block and
// records the request body the SDK put on the wire.
func structuredOutputAdapter(t *testing.T, text string, wire *[]byte) *Adapter {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"id": "msg_structured", "type": "message", "role": "assistant", "model": "claude-contract",
		"content": []any{map[string]any{"type": "text", "text": text}}, "stop_reason": "end_turn",
		"usage": map[string]any{"input_tokens": 2, "output_tokens": 1, "service_tier": "standard"},
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(ClientConfig{
		BaseURL: "http://127.0.0.1/contract",
		APIKey:  "test-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			*wire, _ = io.ReadAll(request.Body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}, "Request-Id": []string{"req-structured"}},
				Body:       io.NopCloser(strings.NewReader(string(body))),
				Request:    request,
			}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := testProfile()
	profile.ExpectedBaseURL = "http://127.0.0.1/contract"
	adapter, err := New(client, "anthropic-prod", profile)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func structuredOutputInput(schema string, strict bool) provider.CompileInput {
	return provider.CompileInput{
		Request: llm.Request{
			OperationKey: "structured", Model: "claude-contract",
			Input:  []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "extract"}}}},
			Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Strict: true, Schema: json.RawMessage(schema)}},
		},
		Query:  provider.CapabilityQuery{EndpointID: "anthropic-prod", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict: strict,
	}
}

func TestStructuredOutputSendsProviderValidSchemaAndValidatesCallerSchema(t *testing.T) {
	const wantSchema = `{"additionalProperties":false,"properties":{` +
		`"age":{"description":"{minimum: 0}","type":"integer"},` +
		`"name":{"description":"{minLength: 3}","type":"string"},` +
		`"tags":{"description":"{maxItems: 2}","items":{"type":"string"},"type":"array"}},` +
		`"required":["name"],"type":"object"}`
	for _, test := range []struct {
		name, text string
		valid      bool
	}{
		{name: "conforming", text: `{"name":"abc","age":1,"tags":["a"]}`, valid: true},
		{name: "string shorter than minLength", text: `{"name":"ab"}`},
		{name: "number below minimum", text: `{"name":"abc","age":-1}`},
		{name: "array longer than maxItems", text: `{"name":"abc","tags":["a","b","c"]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var wire []byte
			adapter := structuredOutputAdapter(t, test.text, &wire)
			call, err := adapter.Compile(context.Background(), structuredOutputInput(constrainedOutputSchema, true))
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
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse || providerErr.Phase != provider.PhaseLift || providerErr.Dispatch != provider.DispatchAccepted {
				t.Fatalf("non-conforming response error = %#v", err)
			}
		})
	}
}

func TestStructuredOutputRejectsUnrepresentableSchemasAtCompile(t *testing.T) {
	var wire []byte
	adapter := structuredOutputAdapter(t, `{}`, &wire)
	for _, test := range []struct {
		name, schema string
		strict       bool
	}{
		{name: "recursive root", schema: `{"type":"object","properties":{"child":{"$ref":"#"}}}`, strict: true},
		{name: "recursive definition", schema: `{"type":"object","properties":{"node":{"$ref":"#/$defs/node"}},"$defs":{"node":{"type":"object","properties":{"next":{"$ref":"#/$defs/node"}}}}}`, strict: false},
		{name: "open object in strict mode", schema: `{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":true}`, strict: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := adapter.Compile(context.Background(), structuredOutputInput(test.schema, test.strict))
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
	call, err := adapter.Compile(context.Background(), structuredOutputInput(`{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":true}`, false))
	if err != nil {
		t.Fatal(err)
	}
	schema := marshalWire(t, call.SDKParams)["output_config"].(map[string]any)["format"].(map[string]any)["schema"].(map[string]any)
	if schema["additionalProperties"] != false {
		t.Fatalf("best-effort schema = %#v", schema)
	}
}
