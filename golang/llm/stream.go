package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// MaxStreamEventBytes bounds one incremental or provider-state event before it
// can cross a caller boundary. Larger generated values must be split by the
// provider adapter. Completed semantic values use the ordinary response and
// Activity payload limits so a valid Generate response cannot be finalized
// successfully and then become undeliverable solely because it is terminal.
const MaxStreamEventBytes = 64 * 1024

// Event is one typed, ordered unit of a provider-neutral response stream.
// Implementations are deliberately closed so callers can rely on exhaustive
// handling of the semantic event union.
type Event interface {
	event()
	Header() EventHeader
}

// EventStream is a one-way, pull-based source of ordered semantic events.
// Consumers must Close it when they stop before a terminal event so the
// producer can cancel any in-flight provider read.
type EventStream interface {
	Next(context.Context) (Event, error)
	Close() error
}

// EventHeader accompanies every stream event. Sequence starts at one and is
// assigned by the engine after admission gives the operation an identity.
type EventHeader struct {
	Sequence     uint64
	OperationID  string
	OutputIndex  *int
	ContentIndex *int
}

// Header returns a defensive copy of the header, including optional indexes.
func (header EventHeader) Header() EventHeader {
	result := header
	result.OutputIndex = copyPointer(header.OutputIndex)
	result.ContentIndex = copyPointer(header.ContentIndex)
	return result
}

// TextDelta is an ordered UTF-8 fragment of one output item's text content.
type TextDelta struct {
	EventHeader
	Text string
}

func (TextDelta) event() {}

// ResponseStarted is emitted after admission assigns the durable operation ID
// and before the first provider output is delivered.
type ResponseStarted struct{ EventHeader }

func (ResponseStarted) event() {}

// ContentStarted marks the beginning of one ordered output item.
type ContentStarted struct{ EventHeader }

func (ContentStarted) event() {}

// JSONDelta is an ordered UTF-8 fragment of incremental JSON content. The
// fragment need not be complete JSON until its ContentCompleted event.
type JSONDelta struct {
	EventHeader
	Fragment string
}

func (JSONDelta) event() {}

// ToolCallStarted introduces a tool call before its argument fragments.
type ToolCallStarted struct {
	EventHeader
	CallID string
	Name   string
}

func (ToolCallStarted) event() {}

// ToolArgumentsDelta is an ordered UTF-8 fragment of a tool call's JSON
// arguments. The complete value is validated when the content finishes.
type ToolArgumentsDelta struct {
	EventHeader
	CallID   string
	Name     string
	Fragment string
}

func (ToolArgumentsDelta) event() {}

// ContentCompleted marks one output item complete. Item is present when the
// provider supplied a complete semantic item; otherwise the engine's bounded
// assembler derives it from preceding deltas.
type ContentCompleted struct {
	EventHeader
	Item Item
}

func (ContentCompleted) event() {}

// UsageUpdated carries the latest normalized usage observation.
type UsageUpdated struct {
	EventHeader
	Usage Usage
}

func (UsageUpdated) event() {}

// ProviderStateDelta preserves opaque provider bytes byte-for-byte and in
// order. The bytes are never decoded by the engine merely to make them
// portable.
type ProviderStateDelta struct {
	EventHeader
	State ProviderState
}

func (ProviderStateDelta) event() {}

// ResponseCompleted is the sole successful terminal event. Its response has
// already passed the engine's continuation, result-store, and ledger
// finalization steps.
type ResponseCompleted struct {
	EventHeader
	Response Response
}

func (ResponseCompleted) event() {}

// StreamErrored is the sole unsuccessful terminal event. Its error is the
// same classified, safe error returned by Generate; raw provider payloads are
// never placed in the event.
type StreamErrored struct {
	EventHeader
	Err error
}

func (StreamErrored) event() {}

// StreamFailed is kept as a descriptive alias for callers that use failure
// rather than error terminology for terminal stream outcomes.
type StreamFailed = StreamErrored

// IsTerminalEvent reports whether an event closes the semantic stream. The
// error terminal is added alongside the rest of the event union below.
func IsTerminalEvent(value Event) bool {
	switch value.(type) {
	case ResponseCompleted, StreamErrored:
		return true
	default:
		return false
	}
}

// ValidateStreamEvent verifies the stable event identity and the per-event
// safety limit before delivery to a stream consumer.
func ValidateStreamEvent(value Event) error {
	if value == nil {
		return fmt.Errorf("stream event is nil")
	}
	header := value.Header()
	if header.Sequence == 0 {
		return fmt.Errorf("stream event sequence must be positive")
	}
	if header.OperationID == "" {
		return fmt.Errorf("stream event operation ID is empty")
	}
	if err := validateStreamText("operation ID", header.OperationID); err != nil {
		return err
	}
	for name, index := range map[string]*int{"output": header.OutputIndex, "content": header.ContentIndex} {
		if index != nil && *index < 0 {
			return fmt.Errorf("stream event %s index must not be negative", name)
		}
	}
	switch event := value.(type) {
	case TextDelta:
		return validateStreamText("text delta", event.Text)
	case ResponseStarted, ContentStarted:
		return nil
	case JSONDelta:
		return validateStreamText("JSON delta", event.Fragment)
	case ToolCallStarted:
		if event.CallID == "" || event.Name == "" {
			return fmt.Errorf("stream tool call start requires call ID and name")
		}
		if err := validateStreamText("tool call ID", event.CallID); err != nil {
			return err
		}
		return validateStreamText("tool call name", event.Name)
	case ToolArgumentsDelta:
		if event.CallID == "" || event.Name == "" {
			return fmt.Errorf("stream tool argument delta requires call ID and name")
		}
		if err := validateStreamText("tool call ID", event.CallID); err != nil {
			return err
		}
		if err := validateStreamText("tool call name", event.Name); err != nil {
			return err
		}
		return validateStreamText("tool argument delta", event.Fragment)
	case ContentCompleted:
		if event.Item == nil {
			return nil
		}
		return validateCompletedStreamJSON("completed content", event.Item)
	case UsageUpdated:
		return validateStreamJSON("usage update", event.Usage)
	case ProviderStateDelta:
		return validateStreamJSON("provider state", event.State)
	case ResponseCompleted:
		return validateCompletedStreamJSON("completed response", event.Response)
	case StreamErrored:
		if event.Err == nil {
			return fmt.Errorf("stream terminal error is nil")
		}
		if len(event.Err.Error()) > MaxStreamEventBytes {
			return fmt.Errorf("stream terminal error is %d bytes; limit is %d", len(event.Err.Error()), MaxStreamEventBytes)
		}
	default:
		return fmt.Errorf("unsupported stream event %T", value)
	}
	return nil
}

func validateStreamText(name, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("stream %s is not valid UTF-8", name)
	}
	if len(value) > MaxStreamEventBytes {
		return fmt.Errorf("stream %s is %d bytes; limit is %d", name, len(value), MaxStreamEventBytes)
	}
	return nil
}

func validateStreamJSON(name string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("stream %s is invalid: %w", name, err)
	}
	if len(encoded) > MaxStreamEventBytes {
		return fmt.Errorf("stream %s is %d bytes; limit is %d", name, len(encoded), MaxStreamEventBytes)
	}
	return nil
}

// validateCompletedStreamJSON keeps terminal and per-content values
// serializable without applying the incremental-event cap. They are the final
// semantic response already subject to the caller's response/payload limits.
func validateCompletedStreamJSON(name string, value any) error {
	if _, err := json.Marshal(value); err != nil {
		return fmt.Errorf("stream %s is invalid: %w", name, err)
	}
	return nil
}
