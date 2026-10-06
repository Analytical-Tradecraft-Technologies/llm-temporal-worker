package bedrockmessages

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

const constrainedOutputSchema = `{"type":"object","properties":{"name":{"type":"string","minLength":3},"age":{"type":"integer","minimum":0},"tags":{"type":"array","items":{"type":"string"},"maxItems":2}},"required":["name"]}`

// recordingBedrockAdapter answers every InvokeModel request with one text
// block and records the request body the SDK put on the wire.
func recordingBedrockAdapter(t *testing.T, text string, wire *[]byte) *Adapter {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"id": "msg-bedrock-structured", "type": "message", "role": "assistant", "model": "claude-contract",
		"content": []any{map[string]any{"type": "text", "text": text}}, "stop_reason": "end_turn",
		"usage": map[string]any{"input_tokens": 2, "output_tokens": 1, "service_tier": "default"},
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(context.Background(), ClientConfig{
		BaseURL: "http://127.0.0.1",
		HTTPClient: &http.Client{Transport: bedrockRoundTrip(func(request *http.Request) (*http.Response, error) {
			*wire, _ = io.ReadAll(request.Body)
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}, "X-Amzn-Requestid": []string{"bedrock-structured"}}, Body: io.NopCloser(strings.NewReader(string(body))), Request: request}, nil
		})},
		AWSConfig: aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("contract-access", "contract-secret", "")},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "bedrock-prod", mustBedrockProfile(t, "http://127.0.0.1"))
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func bedrockCompileInput(request llm.Request, strict bool) provider.CompileInput {
	request.OperationKey, request.Model = "structured", "claude-contract"
	return provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"},
		Strict:  strict,
	}
}

func structuredOutputRequest(schema string) llm.Request {
	return llm.Request{
		Input:  []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "extract"}}}},
		Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Strict: true, Schema: json.RawMessage(schema)}},
	}
}

func TestStructuredOutputSendsProviderValidSchemaAndValidatesCallerSchema(t *testing.T) {
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
		{name: "array longer than maxItems", text: `{"name":"abc","tags":["a","b","c"]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var wire []byte
			adapter := recordingBedrockAdapter(t, test.text, &wire)
			call, err := adapter.Compile(context.Background(), bedrockCompileInput(structuredOutputRequest(constrainedOutputSchema), true))
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
	adapter := recordingBedrockAdapter(t, `{}`, &wire)
	for _, test := range []struct {
		name, schema string
		strict       bool
	}{
		{name: "recursive root", schema: `{"type":"object","properties":{"child":{"$ref":"#"}}}`, strict: true},
		{name: "recursive definition", schema: `{"type":"object","properties":{"node":{"$ref":"#/$defs/node"}},"$defs":{"node":{"type":"object","properties":{"next":{"$ref":"#/$defs/node"}}}}}`, strict: false},
		{name: "open object in strict mode", schema: `{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":true}`, strict: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := adapter.Compile(context.Background(), bedrockCompileInput(structuredOutputRequest(test.schema), test.strict))
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeUnsupportedCapability || providerErr.Phase != provider.PhaseCompile || providerErr.Dispatch != provider.DispatchNotDispatched {
				t.Fatalf("compile error = %#v", err)
			}
		})
	}
	if wire != nil {
		t.Fatalf("rejected schema reached the provider: %s", wire)
	}
}

func TestCompileRejectsURLMediaAndKeepsInlineBytes(t *testing.T) {
	var wire []byte
	adapter := recordingBedrockAdapter(t, "seen", &wire)
	media := func(part llm.Part) llm.Request {
		return llm.Request{Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "describe"}, part}}}}
	}
	for _, test := range []struct {
		name string
		part llm.Part
	}{
		{name: "image", part: llm.ImagePart{URL: "https://example.test/image.png", MediaType: "image/png"}},
		{name: "document", part: llm.DocumentPart{URL: "https://example.test/report.pdf", MediaType: "application/pdf"}},
	} {
		for _, strict := range []bool{true, false} {
			_, err := adapter.Compile(context.Background(), bedrockCompileInput(media(test.part), strict))
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeUnsupportedCapability || providerErr.Phase != provider.PhaseCompile || providerErr.Dispatch != provider.DispatchNotDispatched || !strings.Contains(providerErr.Error(), "URL sources are not supported") {
				t.Fatalf("%s URL strict=%t compile error = %#v", test.name, strict, err)
			}
		}
	}
	call, err := adapter.Compile(context.Background(), bedrockCompileInput(media(llm.ImagePart{Bytes: []byte("image-bytes"), MediaType: "image/png"}), true))
	if err != nil {
		t.Fatalf("inline image = %v", err)
	}
	if _, err := adapter.Invoke(context.Background(), call, nil); err != nil {
		t.Fatalf("inline image invoke = %v", err)
	}
	if !strings.Contains(string(wire), `"type":"base64"`) || strings.Contains(string(wire), `"type":"url"`) {
		t.Fatalf("inline image wire body = %s", wire)
	}
}
