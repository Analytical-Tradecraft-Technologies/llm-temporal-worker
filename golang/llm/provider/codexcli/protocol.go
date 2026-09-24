package codexcli

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// stream consumes the pinned CLI JSONL protocol. Unknown events, warnings,
// model reroutes and every native tool item are fatal, not ignored telemetry.
// No raw error or reasoning content leaves this parser.
type stream struct {
	maxBytes  int
	total     int
	pending   []byte
	threadID  string
	started   bool
	completed bool
	final     []byte
	usage     llm.Usage
	items     map[string]bool
	fail      error
	cancel    func()
}

func (stream *stream) Write(data []byte) (int, error) {
	if stream.fail != nil {
		return 0, stream.fail
	}
	stream.total += len(data)
	if stream.total > stream.maxBytes {
		return 0, stream.stop("codex_cli output exceeded its admitted byte limit")
	}
	stream.pending = append(stream.pending, data...)
	for {
		end := bytes.IndexByte(stream.pending, '\n')
		if end < 0 {
			break
		}
		line := stream.pending[:end]
		if err := stream.event(line); err != nil {
			return 0, stream.stop(err.Error())
		}
		stream.pending = stream.pending[end+1:]
	}
	return len(data), nil
}

func (stream *stream) stop(message string) error {
	stream.fail = fmt.Errorf("%s", message)
	if stream.cancel != nil {
		stream.cancel()
	}
	return stream.fail
}

func (stream *stream) finish() error {
	if stream.fail != nil {
		return stream.fail
	}
	if len(stream.pending) != 0 {
		return fmt.Errorf("codex_cli stream ended with an unterminated event")
	}
	if !stream.completed || len(stream.final) == 0 {
		return fmt.Errorf("codex_cli terminal response or usage is missing")
	}
	return nil
}

func (stream *stream) event(line []byte) error {
	if len(line) == 0 || stream.completed {
		return fmt.Errorf("codex_cli unexpected event after terminal or empty event")
	}
	canonical, err := llm.CanonicalJSON(line)
	if err != nil {
		return fmt.Errorf("codex_cli malformed or duplicate-key event")
	}
	var event struct {
		Type     string          `json:"type"`
		ThreadID string          `json:"thread_id"`
		Model    string          `json:"model"`
		Item     json.RawMessage `json:"item"`
		Usage    json.RawMessage `json:"usage"`
	}
	if err := json.Unmarshal(canonical, &event); err != nil || event.Model != "" {
		return fmt.Errorf("codex_cli invalid event or unexpected model metadata")
	}
	switch event.Type {
	case "thread.started":
		if stream.threadID != "" || stream.started || event.ThreadID == "" || len(event.ThreadID) > 128 {
			return fmt.Errorf("codex_cli invalid or duplicate thread")
		}
		stream.threadID = event.ThreadID
	case "turn.started":
		if stream.threadID == "" || stream.started {
			return fmt.Errorf("codex_cli invalid or duplicate turn")
		}
		stream.started = true
	case "item.started", "item.updated", "item.completed":
		if !stream.started {
			return fmt.Errorf("codex_cli item before turn")
		}
		var item struct {
			ID   string `json:"id"`
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(event.Item, &item); err != nil || item.ID == "" {
			return fmt.Errorf("codex_cli invalid item")
		}
		// The pinned protocol emits reasoning and agent messages only at
		// completion. Everything else indicates a tool, reroute or warning.
		if event.Type != "item.completed" || (item.Type != "agent_message" && item.Type != "reasoning") {
			return fmt.Errorf("codex_cli forbidden tool, model change or warning")
		}
		if stream.items == nil {
			stream.items = make(map[string]bool)
		}
		if stream.items[item.ID] {
			return fmt.Errorf("codex_cli duplicate completed item")
		}
		stream.items[item.ID] = true
		if item.Type == "agent_message" {
			if stream.final != nil {
				return fmt.Errorf("codex_cli duplicate final message")
			}
			stream.final = []byte(item.Text)
		}
	case "turn.completed":
		if !stream.started || len(stream.final) == 0 {
			return fmt.Errorf("codex_cli terminal before final message")
		}
		var usage struct {
			Input      *int64 `json:"input_tokens"`
			Cached     *int64 `json:"cached_input_tokens"`
			CacheWrite *int64 `json:"cache_write_input_tokens"`
			Output     *int64 `json:"output_tokens"`
			Reasoning  *int64 `json:"reasoning_output_tokens"`
		}
		if err := json.Unmarshal(event.Usage, &usage); err != nil || usage.Input == nil || usage.Cached == nil || usage.CacheWrite == nil || usage.Output == nil || usage.Reasoning == nil {
			return fmt.Errorf("codex_cli complete token usage is required")
		}
		// The CLI synthesizes all-zero usage when it never received usage;
		// a nonempty structured generation cannot truthfully use zero tokens.
		if *usage.Input <= 0 || *usage.Output <= 0 || *usage.Cached < 0 || *usage.Cached > *usage.Input || *usage.CacheWrite < 0 || *usage.Reasoning < 0 || *usage.Reasoning > *usage.Output {
			return fmt.Errorf("codex_cli token usage is absent or inconsistent")
		}
		stream.usage = llm.Usage{InputTokens: *usage.Input, OutputTokens: *usage.Output, CacheReadTokens: *usage.Cached, CacheWriteTokens: *usage.CacheWrite, ReasoningTokens: *usage.Reasoning,
			ProviderRaw: map[string]json.RawMessage{"codex_cli_turn_completed": append(json.RawMessage(nil), event.Usage...)}}
		stream.completed = true
	default:
		return fmt.Errorf("codex_cli failed or unsupported stream event")
	}
	return nil
}

type boundedStderr struct {
	data     []byte
	max      int
	cancel   func()
	overflow bool
}

func (output *boundedStderr) Write(data []byte) (int, error) {
	if len(data) > output.max-len(output.data) {
		output.overflow = true
		output.cancel()
		return 0, fmt.Errorf("codex_cli diagnostic output exceeded its bound")
	}
	output.data = append(output.data, data...)
	return len(data), nil
}
