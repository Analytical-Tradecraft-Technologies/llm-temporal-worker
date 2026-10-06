package streamdecode

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestDecodeChatAssemblesTextToolAndUsage(t *testing.T) {
	events, err := DecodeChat([]SSE{
		{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"hello"}}]}`)},
		{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"!"},"finish_reason":"stop"}]}`)},
		{Data: []byte(`{"choices":[{"index":1,"delta":{"tool_calls":[{"id":"call-1","index":0,"function":{"name":"lookup","arguments":"{\"q\":\"sydney\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":2,"completion_tokens":3}}`)},
		{Data: []byte("[DONE]")},
	})
	if err != nil {
		t.Fatal(err)
	}
	assembler := provider.NewAssembler("chat-operation")
	for _, event := range events {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("assemble %T: %v", event, err)
		}
	}
	response, err := assembler.Result()
	if err != nil {
		t.Fatal(err)
	}
	if response.Usage.InputTokens != 2 || response.Usage.OutputTokens != 3 {
		t.Fatalf("usage = %#v", response.Usage)
	}
	if len(response.Output) != 2 {
		t.Fatalf("output length = %d, want 2", len(response.Output))
	}
	message, ok := response.Output[0].(llm.Message)
	if !ok || len(message.Content) != 1 || message.Content[0].(llm.TextPart).Text != "hello!" {
		t.Fatalf("text output = %#v", response.Output[0])
	}
	call, ok := response.Output[1].(llm.ToolCall)
	if !ok || call.ID != "call-1" || call.Name != "lookup" || string(call.Arguments) != `{"q":"sydney"}` {
		t.Fatalf("tool output = %#v", response.Output[1])
	}
}

func TestDecodeAnthropicPreservesPartialUsageToolAndReasoning(t *testing.T) {
	events, err := DecodeAnthropic([]SSE{
		{Event: "message_start", Data: []byte(`{"message":{"usage":{"input_tokens":4}}}`)},
		{Event: "content_block_start", Data: []byte(`{"index":0,"content_block":{"type":"text"}}`)},
		{Event: "content_block_delta", Data: []byte(`{"index":0,"delta":{"type":"text_delta","text":"hello"}}`)},
		{Event: "content_block_stop", Data: []byte(`{"index":0}`)},
		{Event: "content_block_start", Data: []byte(`{"index":1,"content_block":{"type":"tool_use","id":"call-1","name":"lookup"}}`)},
		{Event: "content_block_delta", Data: []byte(`{"index":1,"delta":{"type":"input_json_delta","partial_json":"{\"q\":\"sydney\"}"}}`)},
		{Event: "content_block_stop", Data: []byte(`{"index":1}`)},
		{Event: "content_block_start", Data: []byte(`{"index":2,"content_block":{"type":"thinking"}}`)},
		{Event: "content_block_delta", Data: []byte(`{"index":2,"delta":{"type":"thinking_delta","thinking":"because"}}`)},
		{Event: "content_block_stop", Data: []byte(`{"index":2}`)},
		{Event: "message_delta", Data: []byte(`{"usage":{"output_tokens":2}}`)},
		{Event: "message_stop", Data: []byte(`{}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	assembler := provider.NewAssembler("anthropic-operation")
	for _, event := range events {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("assemble %T: %v", event, err)
		}
	}
	response, err := assembler.Result()
	if err != nil {
		t.Fatal(err)
	}
	if response.Usage.InputTokens != 4 || response.Usage.OutputTokens != 2 || len(response.Output) != 3 {
		t.Fatalf("response facts = %#v", response)
	}
	call, ok := response.Output[1].(llm.ToolCall)
	if !ok || call.ID != "call-1" || call.Name != "lookup" {
		t.Fatalf("tool output = %#v", response.Output[1])
	}
	reasoning, ok := response.Output[2].(llm.Message)
	if !ok || len(reasoning.Content) != 1 || reasoning.Content[0].(llm.TextPart).Text != "because" {
		t.Fatalf("reasoning output = %#v", response.Output[2])
	}
}

func TestDecodeResponsesRejectsInvalidOrderAndCompletes(t *testing.T) {
	if _, err := DecodeResponses([]SSE{
		{Event: "response.output_text.delta", Data: []byte(`{"output_index":0,"delta":"early"}`)},
	}); err == nil {
		t.Fatal("accepted Responses delta before output start")
	}

	events, err := DecodeResponses([]SSE{
		{Event: "response.created", Data: []byte(`{"type":"response.created"}`)},
		{Event: "response.output_item.added", Data: []byte(`{"output_index":0,"item":{"type":"message"}}`)},
		{Event: "response.output_text.delta", Data: []byte(`{"output_index":0,"delta":"hello"}`)},
		{Event: "response.output_item.done", Data: []byte(`{"output_index":0}`)},
		{Event: "response.completed", Data: []byte(`{"type":"response.completed"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	assembler := provider.NewAssembler("responses-operation")
	for _, event := range events {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("assemble %T: %v", event, err)
		}
	}
	response, err := assembler.Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Output) != 1 {
		t.Fatalf("output length = %d, want 1", len(response.Output))
	}
	message, ok := response.Output[0].(llm.Message)
	if !ok || message.Content[0].(llm.TextPart).Text != "hello" {
		t.Fatalf("response output = %#v", response.Output[0])
	}
}

func TestDecodeResponsesAssemblesToolCall(t *testing.T) {
	events, err := DecodeResponses([]SSE{
		{Event: "response.output_item.added", Data: []byte(`{"output_index":0,"item":{"type":"function_call"}}`)},
		{Event: "response.function_call_arguments.delta", Data: []byte(`{"output_index":0,"delta":"{\"city\":\"sydney\"}","item":{"call_id":"call-2","name":"lookup"}}`)},
		{Event: "response.output_item.done", Data: []byte(`{"output_index":0}`)},
		{Event: "response.completed", Data: []byte(`{"type":"response.completed"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	assembler := provider.NewAssembler("responses-tool-operation")
	for _, event := range events {
		if err := assembler.Add(event); err != nil {
			t.Fatalf("assemble %T: %v", event, err)
		}
	}
	response, err := assembler.Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Output) != 1 {
		t.Fatalf("output length = %d, want 1", len(response.Output))
	}
	call, ok := response.Output[0].(llm.ToolCall)
	if !ok || call.ID != "call-2" || call.Name != "lookup" || string(call.Arguments) != `{"city":"sydney"}` {
		t.Fatalf("tool output = %#v", response.Output[0])
	}
}

func TestDecodersRejectPostTerminalAndUnknownEvents(t *testing.T) {
	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "chat post terminal",
			call: func() error {
				_, err := DecodeChat([]SSE{{Data: []byte("[DONE]")}, {Data: []byte(`{"choices":[]}`)}})
				return err
			},
		},
		{
			name: "anthropic unknown event",
			call: func() error {
				_, err := DecodeAnthropic([]SSE{{Event: "unknown", Data: []byte(`{}`)}})
				return err
			},
		},
		{
			name: "responses unknown event",
			call: func() error {
				_, err := DecodeResponses([]SSE{{Event: "response.unknown", Data: []byte(`{}`)}})
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil {
				t.Fatal("invalid stream unexpectedly succeeded")
			}
		})
	}
}

func TestDecodersRejectSequencesTheAssemblerWouldReject(t *testing.T) {
	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "anthropic unfinished output",
			call: func() error {
				_, err := DecodeAnthropic([]SSE{
					{Event: "content_block_start", Data: []byte(`{"index":0,"content_block":{"type":"text"}}`)},
					{Event: "message_stop", Data: []byte(`{}`)},
				})
				return err
			},
		},
		{
			name: "anthropic out of order output",
			call: func() error {
				_, err := DecodeAnthropic([]SSE{
					{Event: "content_block_start", Data: []byte(`{"index":1,"content_block":{"type":"text"}}`)},
					{Event: "content_block_stop", Data: []byte(`{"index":1}`)},
					{Event: "message_stop", Data: []byte(`{}`)},
				})
				return err
			},
		},
		{
			name: "chat out of order output",
			call: func() error {
				_, err := DecodeChat([]SSE{
					{Data: []byte(`{"choices":[{"index":1,"delta":{},"finish_reason":"stop"}]}`)},
					{Data: []byte("[DONE]")},
				})
				return err
			},
		},
		{
			name: "chat delta after output finish",
			call: func() error {
				_, err := DecodeChat([]SSE{
					{Data: []byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)},
					{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"late"}}]}`)},
					{Data: []byte("[DONE]")},
				})
				return err
			},
		},
		{
			name: "responses out of order output",
			call: func() error {
				_, err := DecodeResponses([]SSE{
					{Event: "response.output_item.added", Data: []byte(`{"output_index":1}`)},
					{Event: "response.output_item.done", Data: []byte(`{"output_index":1}`)},
					{Event: "response.completed", Data: []byte(`{}`)},
				})
				return err
			},
		},
		{
			name: "responses delta after output finish",
			call: func() error {
				_, err := DecodeResponses([]SSE{
					{Event: "response.output_item.added", Data: []byte(`{"output_index":0}`)},
					{Event: "response.output_item.done", Data: []byte(`{"output_index":0}`)},
					{Event: "response.output_text.delta", Data: []byte(`{"output_index":0,"delta":"late"}`)},
					{Event: "response.completed", Data: []byte(`{}`)},
				})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil {
				t.Fatal("decoder accepted a sequence the event assembler would reject")
			}
		})
	}
}
