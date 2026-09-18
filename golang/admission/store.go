package admission

import (
	"context"
	"time"
)

type AdmissionStore interface {
	Begin(context.Context, BeginRequest) (BeginResult, error)
	MarkDispatching(context.Context, DispatchRequest) error
	Continue(context.Context, ContinueRequest) (ContinueResult, error)
	Complete(context.Context, CompleteRequest) error
	Fail(context.Context, FailRequest) error
	Get(context.Context, string) (Operation, error)
}

// PreWriteFailureStore atomically records an attempt that is proven not to
// have written to a provider and closes its reserved operation. Callers must
// not emulate this boundary with MarkDispatching followed by Fail: a crash
// between those calls would turn a known no-write outcome into dispatching.
type PreWriteFailureStore interface {
	FailBeforeDispatch(context.Context, FailRequest) error
}

// ProviderPendingStore is an optional extension implemented by durable
// operation repositories that can persist and recover a provider-owned
// operation identifier. Engines must never require this extension for
// adapters that only support one-shot Invoke calls.
type ProviderPendingStore interface {
	AdmissionStore
	MarkProviderPending(context.Context, ProviderPendingRequest) error
	ProviderOperation(context.Context, string) (string, error)
}

// ProviderPendingSchedule is an optional extension for durable repositories
// that retain provider-provided next-poll guidance. Engines use it when a
// retry resumes an already accepted operation; stores that do not expose the
// schedule remain safe but may poll immediately.
type ProviderPendingSchedule interface {
	ProviderPollAfter(context.Context, string) (time.Time, error)
}
