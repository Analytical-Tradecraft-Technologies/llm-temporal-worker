package provider

import "context"

type Family string

const (
	FamilyOpenAIResponses   Family = "openai_responses"
	FamilyOpenAIChat        Family = "openai_chat"
	FamilyAnthropicMessages Family = "anthropic_messages"
	FamilyBedrockMessages   Family = "bedrock_messages"
	FamilyBedrockConverse   Family = "bedrock_converse"
)

func (family Family) Valid() bool {
	switch family {
	case FamilyOpenAIResponses, FamilyOpenAIChat, FamilyAnthropicMessages, FamilyBedrockMessages, FamilyBedrockConverse:
		return true
	default:
		return false
	}
}

// Adapter is the provider boundary. Implementations own one official SDK and
// endpoint family; route selection, retries, budgets, and state remain outside.
type Adapter interface {
	Name() string
	Capabilities(context.Context, CapabilityQuery) (CapabilitySet, error)
	Compile(context.Context, CompileInput) (Call, error)
	Invoke(context.Context, Call, Observer) (Result, error)
}

// EventSource is a one-way source of provider-neutral stream events. It owns
// the provider response body and must stop promptly when either context is
// canceled or Close is called. Once it returns a provider terminal event, its
// next Next call must return io.EOF immediately; emitting another event or
// blocking after a terminal is a provider protocol violation. Every emitted
// ToolArgumentsDelta must contain a nonempty CallID and Name; adapters must
// buffer or normalize provider fragments that discover tool identity later.
type EventSource interface {
	Next(context.Context) (Event, error)
	Close() error
}

// StreamResult records the dispatch observation made while opening a real
// provider stream. Metadata is safe response-header evidence only; event
// payloads continue through EventSource in their original order.
type StreamResult struct {
	Source   EventSource
	Metadata ResponseMetadata
	Dispatch DispatchCertainty
}

// StreamingAdapter is an optional extension of Adapter. It deliberately does
// not provide a default implementation: an Adapter without this port cannot
// be used by Engine.Stream and is rejected before dispatch. Implementations
// must call Observer.BeforePossibleWrite immediately before their first
// possible provider write and return DispatchAccepted only after that call;
// otherwise the engine records the result as ambiguous and refuses replay.
type StreamingAdapter interface {
	Adapter
	OpenStream(context.Context, Call, Observer) (StreamResult, error)
}

// ResumableAdapter is the optional provider boundary for APIs that accept an
// operation and return a provider-owned identifier that can be polled. The
// operation id is deliberately passed only to Poll; callers must persist it
// before making that call and must never turn a pending result into Submit on
// a retry. Providers that do not expose a documented idempotency lookup do not
// implement RecoverByIdempotencyKey and an acceptance/persistence gap remains
// ambiguous.
type ResumableAdapter interface {
	Adapter
	Submit(context.Context, Call, Observer) (ResumableResult, error)
	Poll(context.Context, Call, string, Observer) (ResumableResult, error)
}

// IdempotencyRecovery is an optional extension for providers that document a
// lookup by the caller's operation key. The engine invokes this lookup at most
// once after an accepted or ambiguous Submit failure. The returned
// ResumableResult is validated before a pending ID is persisted or a terminal
// outcome is finalized. Without this extension, an acceptance/persistence gap
// is ambiguous and must not trigger a second Submit.
type IdempotencyRecovery interface {
	RecoverByIdempotencyKey(context.Context, Call, Observer) (ResumableResult, error)
}
