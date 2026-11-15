package bedrockmessages

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

// recordingBedrockAdapter answers every InvokeModel request with one text
// block and records the request body the SDK put on the wire.
func recordingBedrockAdapter(t *testing.T, text string, wire *[]byte) *Adapter {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"id": "msg-bedrock-structured", "type": "message", "role": "assistant", "model": "claude-contract",
		"content": []any{map[string]any{"type": "text", "text": text}}, "stop_reason": "end_turn",
		"usage": map[string]any{"input_tokens": 2, "output_tokens": 1, "service_tier": "default"},
	})
	testassert.NoError(t, err)
	client := newBedrockTestClient(t, "http://127.0.0.1", &http.Client{Transport: adaptertest.RoundTripFunc(func(request *http.Request) (*http.Response, error) {
		*wire, _ = io.ReadAll(request.Body)
		return adaptertest.JSONResponse(request, http.StatusOK, string(body), "X-Amzn-Requestid", "bedrock-structured"), nil
	})}, nil)
	adapter, err := New(client, "bedrock-prod", mustBedrockProfile(t, "http://127.0.0.1"))
	testassert.NoError(t, err)
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
		Input:  []llm.Item{llm.HumanText("extract")},
		Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Strict: true, Schema: json.RawMessage(schema)}},
	}
}

func TestStructuredOutputSendsProviderValidSchemaAndValidatesCallerSchema(t *testing.T) {
	messagestest.StructuredOutputSendsProviderValidSchemaAndValidatesCallerSchema(t, newStructuredOutputAdapter, structuredOutputInput)
}

func TestStructuredOutputRejectsUnrepresentableSchemasAtCompile(t *testing.T) {
	messagestest.StructuredOutputRejectsUnrepresentableSchemasAtCompile(t, newStructuredOutputAdapter, structuredOutputInput)
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
	testassert.NoError(t, err, "inline image")
	testassert.NoError(t, testassert.Err(adapter.Invoke(context.Background(), call, nil)), "inline image invoke")
	if !strings.Contains(string(wire), `"type":"base64"`) || strings.Contains(string(wire), `"type":"url"`) {
		t.Fatalf("inline image wire body = %s", wire)
	}
}

func TestStructuredOutputRejectsExponentialSchemaAtCompile(t *testing.T) {
	messagestest.StructuredOutputRejectsExponentialSchemaAtCompile(t, newStructuredOutputAdapter, structuredOutputInput)
}

func newStructuredOutputAdapter(t *testing.T, text string, wire *[]byte) messagestest.StructuredOutputAdapter {
	return recordingBedrockAdapter(t, text, wire)
}

func structuredOutputInput(schema string, strict bool) provider.CompileInput {
	return bedrockCompileInput(structuredOutputRequest(schema), strict)
}
