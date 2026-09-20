package openaichat

import (
	"encoding/json"
	"errors"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestLiftCompletedToolResponsePreservesUsageAndIDs(t *testing.T) {
	var response openai.ChatCompletion
	err := json.Unmarshal([]byte(`{
      "id":"chatcmpl-1",
      "object":"chat.completion",
      "created":1700000000,
      "model":"chat-model-resolved",
      "service_tier":"priority",
      "choices":[{
        "index":0,
        "finish_reason":"tool_calls",
        "message":{
          "role":"assistant",
          "content":"hello",
          "refusal":"",
          "tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"sydney\"}"}}]
        }
      }],
      "usage":{"prompt_tokens":10,"completion_tokens":7,"total_tokens":17,"prompt_tokens_details":{"cached_tokens":3,"cache_write_tokens":1},"completion_tokens_details":{"reasoning_tokens":2}}
    }`), &response)
	if err != nil {
		t.Fatal(err)
	}
	profile := testProfile()
	call := provider.Call{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model", OperationKey: "op-lift", ServiceClass: llm.ServiceClassPriority}
	lifted, err := profile.liftResponse(call, &response, "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if lifted.Status != llm.ResponseStatusToolCalls || lifted.Service.Actual == nil || *lifted.Service.Actual != llm.ServiceClassPriority {
		t.Fatalf("status/service = %#v %#v", lifted.Status, lifted.Service)
	}
	if lifted.Provider.ResponseID != "chatcmpl-1" || lifted.Provider.RequestID != "req-1" || lifted.Route.ResolvedModel != "chat-model-resolved" {
		t.Fatalf("identity = %#v %#v", lifted.Provider, lifted.Route)
	}
	if lifted.Usage.InputTokens != 6 || lifted.Usage.OutputTokens != 7 || lifted.Usage.ReasoningTokens != 2 || lifted.Usage.CacheReadTokens != 3 || lifted.Usage.CacheWriteTokens != 1 {
		t.Fatalf("usage = %#v", lifted.Usage)
	}
	if len(lifted.Output) != 2 {
		t.Fatalf("output length = %d", len(lifted.Output))
	}
	if message, ok := lifted.Output[0].(llm.Message); !ok || message.Content[0].(llm.TextPart).Text != "hello" {
		t.Fatalf("message = %#v", lifted.Output[0])
	}
	if call, ok := lifted.Output[1].(llm.ToolCall); !ok || call.ID != "call-1" || call.Name != "lookup" || !json.Valid(call.Arguments) {
		t.Fatalf("tool call = %#v", lifted.Output[1])
	}
	if string(lifted.Usage.ProviderRaw["total_tokens"]) != "17" {
		t.Fatalf("provider usage = %#v", lifted.Usage.ProviderRaw)
	}
}

func TestLiftMapsFinishReasonsAndRejectsUnknownTier(t *testing.T) {
	for _, test := range []struct {
		reason string
		want   llm.ResponseStatus
	}{
		{reason: "stop", want: llm.ResponseStatusCompleted},
		{reason: "length", want: llm.ResponseStatusLength},
		{reason: "content_filter", want: llm.ResponseStatusContentFiltered},
	} {
		response := openai.ChatCompletion{ID: "id", Model: "model", ServiceTier: openai.ChatCompletionServiceTierDefault, Choices: []openai.ChatCompletionChoice{{FinishReason: test.reason, Message: openai.ChatCompletionMessage{Role: "assistant"}}}}
		got, err := testProfile().liftResponse(provider.Call{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "model", OperationKey: "op", ServiceClass: llm.ServiceClassStandard}, &response, "req")
		if err != nil || got.Status != test.want {
			t.Fatalf("reason %q = %#v, %v", test.reason, got, err)
		}
	}
	unknown := openai.ChatCompletion{ID: "id", Model: "model", ServiceTier: "scale", Choices: []openai.ChatCompletionChoice{{FinishReason: "stop", Message: openai.ChatCompletionMessage{Role: "assistant"}}}}
	_, err := testProfile().liftResponse(provider.Call{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "model", OperationKey: "op", ServiceClass: llm.ServiceClassStandard}, &unknown, "req")
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse || providerErr.Dispatch != provider.DispatchAccepted {
		t.Fatalf("unknown tier error = %#v", err)
	}
}

func TestLiftRejectsMalformedToolArguments(t *testing.T) {
	response := openai.ChatCompletion{ID: "id", Model: "model", ServiceTier: openai.ChatCompletionServiceTierDefault, Choices: []openai.ChatCompletionChoice{{FinishReason: "tool_calls", Message: openai.ChatCompletionMessage{Role: "assistant", ToolCalls: []openai.ChatCompletionMessageToolCallUnion{{ID: "call", Type: "function", Function: openai.ChatCompletionMessageFunctionToolCallFunction{Name: "lookup", Arguments: "{"}}}}}}}
	_, err := testProfile().liftResponse(provider.Call{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "model", OperationKey: "op", ServiceClass: llm.ServiceClassStandard}, &response, "req")
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse {
		t.Fatalf("malformed args error = %#v", err)
	}
}

func TestLiftLocallyValidatesRequestedJSONSchema(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}`)
	params, err := lowerRequest(llm.Request{
		Model: "chat-model",
		Output: &llm.OutputSpec{Format: llm.OutputFormat{
			Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: schema,
		}},
	}, testProfile(), "default")
	if err != nil {
		t.Fatal(err)
	}
	call := provider.Call{
		EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model",
		OperationKey: "op-json", ServiceClass: llm.ServiceClassStandard, SDKParams: params,
	}
	valid := openai.ChatCompletion{
		ID: "valid", Model: "chat-model", ServiceTier: openai.ChatCompletionServiceTierDefault,
		Choices: []openai.ChatCompletionChoice{{FinishReason: "stop", Message: openai.ChatCompletionMessage{Content: `{"answer":"ok"}`}}},
	}
	if _, err := testProfile().liftResponse(call, &valid, "req"); err != nil {
		t.Fatalf("valid JSON response = %v", err)
	}
	invalid := valid
	invalid.ID = "invalid"
	invalid.Choices[0].Message.Content = `{"answer":3}`
	_, err = testProfile().liftResponse(call, &invalid, "req")
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse || providerErr.Dispatch != provider.DispatchAccepted {
		t.Fatalf("invalid JSON response error = %#v", err)
	}
}
