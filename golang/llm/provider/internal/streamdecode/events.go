package streamdecode

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func DecodeAnthropic(events []SSE) ([]provider.Event, error) {
	result := make([]provider.Event, 0)
	tools := map[int]struct{ id, name string }{}
	started := map[int]bool{}
	finished := map[int]bool{}
	nextIndex := 0
	terminal := false
	for _, event := range events {
		data := bytes.TrimSpace(event.Data)
		if bytes.Equal(data, []byte("[DONE]")) {
			if terminal {
				return nil, fmt.Errorf("stream has more than one terminal event")
			}
			if unfinished, ok := firstUnfinishedOutput(started, finished); ok {
				return nil, fmt.Errorf("Anthropic stream ended with unfinished output index %d", unfinished)
			}
			result = append(result, provider.StreamCompleted{})
			terminal = true
			continue
		}
		if terminal {
			return nil, fmt.Errorf("event %q occurred after terminal event", event.Event)
		}
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(data, &envelope); err != nil {
			return nil, fmt.Errorf("anthropic event %q: %w", event.Event, err)
		}
		index := -1
		_ = json.Unmarshal(envelope["index"], &index)
		switch event.Event {
		case "message_start":
			var message struct {
				Usage struct {
					InputTokens int64 `json:"input_tokens"`
				} `json:"usage"`
			}
			if raw := envelope["message"]; len(raw) > 0 {
				_ = json.Unmarshal(raw, &message)
			}
			if message.Usage.InputTokens != 0 {
				result = append(result, provider.UsageUpdated{Usage: llm.Usage{InputTokens: message.Usage.InputTokens}})
			}
		case "content_block_start":
			if index < 0 {
				return nil, fmt.Errorf("content_block_start has invalid index")
			}
			if index != nextIndex {
				return nil, fmt.Errorf("content_block_start index %d is out of order", index)
			}
			result = append(result, provider.OutputStarted{Index: index})
			started[index] = true
			nextIndex++
			var block struct {
				Type string `json:"type"`
				ID   string `json:"id"`
				Name string `json:"name"`
			}
			if raw := envelope["content_block"]; len(raw) > 0 {
				_ = json.Unmarshal(raw, &block)
			}
			if block.Type == "tool_use" {
				tools[index] = struct{ id, name string }{id: block.ID, name: block.Name}
			}
		case "content_block_delta":
			if index < 0 {
				return nil, fmt.Errorf("content_block_delta has invalid index")
			}
			if !started[index] || finished[index] {
				return nil, fmt.Errorf("content_block_delta index %d has no active output", index)
			}
			var delta struct {
				Type        string `json:"type"`
				Text        string `json:"text"`
				PartialJSON string `json:"partial_json"`
				Thinking    string `json:"thinking"`
				Signature   string `json:"signature"`
			}
			if err := json.Unmarshal(envelope["delta"], &delta); err != nil {
				return nil, fmt.Errorf("anthropic content delta: %w", err)
			}
			switch delta.Type {
			case "text_delta":
				result = append(result, provider.TextDelta{Index: index, Text: delta.Text})
			case "input_json_delta":
				call := tools[index]
				result = append(result, provider.ToolArgumentsDelta{Index: index, CallID: call.id, Name: call.name, Fragment: delta.PartialJSON})
			case "thinking_delta":
				result = append(result, provider.ReasoningDelta{Index: index, Opaque: []byte(delta.Thinking)})
			case "signature_delta":
				result = append(result, provider.ReasoningDelta{Index: index, Opaque: []byte(delta.Signature)})
			default:
				return nil, fmt.Errorf("unsupported Anthropic content delta %q", delta.Type)
			}
		case "content_block_stop":
			if index < 0 {
				return nil, fmt.Errorf("content_block_stop has invalid index")
			}
			if !started[index] || finished[index] {
				return nil, fmt.Errorf("content_block_stop index %d has no active output", index)
			}
			result = append(result, provider.OutputFinished{Index: index})
			finished[index] = true
		case "message_delta":
			var delta struct {
				Usage struct {
					OutputTokens int64 `json:"output_tokens"`
				} `json:"usage"`
			}
			if raw := envelope["usage"]; len(raw) > 0 {
				_ = json.Unmarshal(raw, &delta.Usage)
			}
			if delta.Usage.OutputTokens != 0 {
				result = append(result, provider.UsageUpdated{Usage: llm.Usage{OutputTokens: delta.Usage.OutputTokens}})
			}
		case "message_stop":
			if unfinished, ok := firstUnfinishedOutput(started, finished); ok {
				return nil, fmt.Errorf("Anthropic stream ended with unfinished output index %d", unfinished)
			}
			result = append(result, provider.StreamCompleted{})
			terminal = true
		case "error":
			result = append(result, provider.StreamErrored{Err: fmt.Errorf("Anthropic stream returned provider error")})
			terminal = true
		case "ping", "message_ping", "content_block_delta_done", "message_delta_done":
			// Keep-alive and provider-specific completion markers carry no
			// semantic output; the terminal message_stop remains authoritative.
		default:
			return nil, fmt.Errorf("unsupported Anthropic stream event %q", event.Event)
		}
	}
	if !terminal {
		return nil, fmt.Errorf("stream ended without terminal event")
	}
	return result, nil
}

func DecodeChat(events []SSE) ([]provider.Event, error) {
	result := make([]provider.Event, 0)
	started := map[int]bool{}
	finished := map[int]bool{}
	nextIndex := 0
	terminal := false
	for _, event := range events {
		data := bytes.TrimSpace(event.Data)
		if bytes.Equal(data, []byte("[DONE]")) {
			if terminal {
				return nil, fmt.Errorf("stream has more than one terminal event")
			}
			if unfinished, ok := firstUnfinishedOutput(started, finished); ok {
				return nil, fmt.Errorf("chat stream ended with unfinished output index %d", unfinished)
			}
			result = append(result, provider.StreamCompleted{})
			terminal = true
			continue
		}
		if terminal {
			return nil, fmt.Errorf("event occurred after terminal event")
		}
		var chunk struct {
			Choices []struct {
				Index int `json:"index"`
				Delta struct {
					Content   string `json:"content"`
					ToolCalls []struct {
						ID       string `json:"id"`
						Index    int    `json:"index"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
			Usage *struct {
				PromptTokens     int64 `json:"prompt_tokens"`
				CompletionTokens int64 `json:"completion_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(data, &chunk); err != nil {
			return nil, fmt.Errorf("chat stream event: %w", err)
		}
		if chunk.Usage != nil {
			result = append(result, provider.UsageUpdated{Usage: llm.Usage{InputTokens: chunk.Usage.PromptTokens, OutputTokens: chunk.Usage.CompletionTokens}})
		}
		for _, choice := range chunk.Choices {
			if choice.Index < 0 {
				return nil, fmt.Errorf("chat choice index %d is invalid", choice.Index)
			}
			if !started[choice.Index] {
				if choice.Index != nextIndex {
					return nil, fmt.Errorf("chat choice index %d is out of order", choice.Index)
				}
				result = append(result, provider.OutputStarted{Index: choice.Index})
				started[choice.Index] = true
				nextIndex++
			}
			if finished[choice.Index] {
				return nil, fmt.Errorf("chat output index %d is already finished", choice.Index)
			}
			if choice.Delta.Content != "" {
				result = append(result, provider.TextDelta{Index: choice.Index, Text: choice.Delta.Content})
			}
			for _, tool := range choice.Delta.ToolCalls {
				result = append(result, provider.ToolArgumentsDelta{Index: choice.Index, CallID: tool.ID, Name: tool.Function.Name, Fragment: tool.Function.Arguments})
			}
			if choice.FinishReason != nil && *choice.FinishReason != "" {
				if finished[choice.Index] {
					return nil, fmt.Errorf("chat output index %d finished twice", choice.Index)
				}
				result = append(result, provider.OutputFinished{Index: choice.Index})
				finished[choice.Index] = true
			}
		}
	}
	if !terminal {
		return nil, fmt.Errorf("stream ended without terminal event")
	}
	return result, nil
}

func firstUnfinishedOutput(started, finished map[int]bool) (int, bool) {
	first, found := 0, false
	for index := range started {
		if finished[index] || (found && index >= first) {
			continue
		}
		first, found = index, true
	}
	return first, found
}

func DecodeResponses(events []SSE) ([]provider.Event, error) {
	result := make([]provider.Event, 0)
	started := map[int]bool{}
	finished := map[int]bool{}
	nextIndex := 0
	terminal := false
	for _, event := range events {
		if terminal {
			return nil, fmt.Errorf("event %q occurred after terminal event", event.Event)
		}
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(bytes.TrimSpace(event.Data), &envelope); err != nil {
			return nil, fmt.Errorf("Responses event %q: %w", event.Event, err)
		}
		var index int
		_ = json.Unmarshal(envelope["output_index"], &index)
		switch event.Event {
		case "response.output_item.added":
			if index < 0 {
				return nil, fmt.Errorf("Responses output index is invalid")
			}
			if started[index] || index != nextIndex {
				return nil, fmt.Errorf("Responses output index %d is out of order", index)
			}
			result = append(result, provider.OutputStarted{Index: index})
			started[index] = true
			nextIndex++
		case "response.output_text.delta":
			if !started[index] || finished[index] {
				return nil, fmt.Errorf("Responses text delta has no active output")
			}
			var delta string
			if err := json.Unmarshal(envelope["delta"], &delta); err != nil {
				return nil, err
			}
			result = append(result, provider.TextDelta{Index: index, Text: delta})
		case "response.function_call_arguments.delta":
			if !started[index] || finished[index] {
				return nil, fmt.Errorf("Responses tool delta has no active output")
			}
			var delta string
			_ = json.Unmarshal(envelope["delta"], &delta)
			var item struct {
				CallID string `json:"call_id"`
				Name   string `json:"name"`
			}
			_ = json.Unmarshal(envelope["item"], &item)
			result = append(result, provider.ToolArgumentsDelta{Index: index, CallID: item.CallID, Name: item.Name, Fragment: delta})
		case "response.output_item.done":
			if !started[index] {
				return nil, fmt.Errorf("Responses output finish precedes output start")
			}
			if finished[index] {
				return nil, fmt.Errorf("Responses output index %d finished twice", index)
			}
			result = append(result, provider.OutputFinished{Index: index})
			finished[index] = true
		case "response.completed":
			if unfinished, ok := firstUnfinishedOutput(started, finished); ok {
				return nil, fmt.Errorf("Responses stream ended with unfinished output index %d", unfinished)
			}
			result = append(result, provider.StreamCompleted{})
			terminal = true
		case "response.failed", "response.incomplete":
			result = append(result, provider.StreamErrored{Err: fmt.Errorf("OpenAI Responses stream did not complete")})
			terminal = true
		case "response.created", "response.in_progress", "response.content_part.added", "response.content_part.done", "response.output_text.done", "response.function_call_arguments.done", "response.reasoning_summary_text.delta", "response.reasoning_summary_text.done":
			// Metadata and explicit done markers are represented by the output
			// item event; no duplicate neutral event is emitted here.
		default:
			return nil, fmt.Errorf("unsupported Responses stream event %q", event.Event)
		}
	}
	if !terminal {
		return nil, fmt.Errorf("stream ended without terminal event")
	}
	return result, nil
}
