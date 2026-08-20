package provider

import (
	"context"
	"errors"
	"sync"
)

type egressOutcomeKind uint8

const (
	egressOutcomeNone egressOutcomeKind = iota
	egressOutcomePolicyDenied
	egressOutcomePreDispatchUnavailable
	egressOutcomePreDispatchContext
)

// EgressOutcome carries the certified pre-dispatch result for one provider
// call. Some SDKs replace a guarded HTTP transport error with ctx.Err() after
// RoundTrip returns. The first certified outcome seals the result so a late
// detached dial cannot turn caller cancellation into a route fallback.
type EgressOutcome struct {
	callerContext context.Context

	mu    sync.RWMutex
	kind  egressOutcomeKind
	cause error
}

type egressOutcomeContextKey struct{}

// WithEgressOutcome attaches an empty egress outcome to a child of ctx. The
// returned context must be passed to the provider SDK for the guarded
// RoundTripper to record the result for this call. The original caller context
// is retained so a client-local timeout can be distinguished from caller
// cancellation.
func WithEgressOutcome(ctx context.Context) (context.Context, *EgressOutcome) {
	if ctx == nil {
		ctx = context.Background()
	}
	outcome := &EgressOutcome{callerContext: ctx}
	return context.WithValue(ctx, egressOutcomeContextKey{}, outcome), outcome
}

// CallerContextForEgressOutcome returns the outer caller context associated
// with ctx. It falls back to ctx when no egress outcome is attached.
func CallerContextForEgressOutcome(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	outcome, _ := ctx.Value(egressOutcomeContextKey{}).(*EgressOutcome)
	if outcome == nil || outcome.callerContext == nil {
		return ctx
	}
	return outcome.callerContext
}

// RecordEgressDenied preserves an explicit policy rejection on ctx. Other
// transport failures must use one of the pre-dispatch recorders instead.
func RecordEgressDenied(ctx context.Context, cause error) {
	if ctx == nil || !errors.Is(cause, ErrProviderEgressDenied) {
		return
	}
	egressOutcomeForContext(ctx).record(egressOutcomePolicyDenied, cause)
}

// RecordPreDispatchFailure records a guarded DNS, TCP, TLS, or client-local
// timeout failure only when no writable provider connection was acquired.
func RecordPreDispatchFailure(ctx context.Context, cause error) {
	if ctx == nil || !errors.Is(cause, ErrProviderPreDispatch) {
		return
	}
	egressOutcomeForContext(ctx).record(egressOutcomePreDispatchUnavailable, cause)
}

// RecordPreDispatchContext records caller cancellation or deadline only when
// the guarded transport proved it happened before a writable connection was
// acquired.
func RecordPreDispatchContext(ctx context.Context, cause error) {
	if ctx == nil || (!errors.Is(cause, context.Canceled) && !errors.Is(cause, context.DeadlineExceeded)) {
		return
	}
	egressOutcomeForContext(ctx).record(egressOutcomePreDispatchContext, cause)
}

func egressOutcomeForContext(ctx context.Context) *EgressOutcome {
	if ctx == nil {
		return nil
	}
	outcome, _ := ctx.Value(egressOutcomeContextKey{}).(*EgressOutcome)
	return outcome
}

func (outcome *EgressOutcome) record(kind egressOutcomeKind, cause error) {
	if outcome == nil || kind == egressOutcomeNone || cause == nil {
		return
	}
	outcome.mu.Lock()
	defer outcome.mu.Unlock()
	if outcome.kind != egressOutcomeNone {
		return
	}
	outcome.kind = kind
	outcome.cause = cause
}

func (outcome *EgressOutcome) result() (egressOutcomeKind, error) {
	if outcome == nil {
		return egressOutcomeNone, nil
	}
	outcome.mu.RLock()
	defer outcome.mu.RUnlock()
	return outcome.kind, outcome.cause
}

// Denial returns the recorded policy denial, if any. It remains available for
// callers that need to log or diagnose only policy rejections.
func (outcome *EgressOutcome) Denial() error {
	kind, cause := outcome.result()
	if kind != egressOutcomePolicyDenied {
		return nil
	}
	return cause
}

// ClassifyEgressOutcome returns a safe common error only for a direct guarded
// marker or when an SDK replaced the guarded transport error with cancellation.
// Unmarked errors retain normal adapter-specific conservative classification.
func ClassifyEgressOutcome(outcome *EgressOutcome, err error) *Error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrProviderEgressDenied) {
		return NewEgressDeniedError(err)
	}
	if errors.Is(err, ErrProviderPreDispatch) {
		return NewPreDispatchUnavailableError(err)
	}
	if errors.Is(err, ErrProviderResponseTooLarge) {
		return NewProviderResponseTooLargeError(err)
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	kind, cause := outcome.result()
	switch kind {
	case egressOutcomePolicyDenied:
		return NewEgressDeniedError(cause)
	case egressOutcomePreDispatchUnavailable:
		return NewPreDispatchUnavailableError(cause)
	case egressOutcomePreDispatchContext:
		return NewPreDispatchContextError(cause)
	default:
		return nil
	}
}

// EgressDenialForContextError is retained for source compatibility with
// callers that only need policy denials. New call sites should use
// ClassifyEgressOutcome so certified availability and caller cancellation are
// not conflated.
func EgressDenialForContextError(outcome *EgressOutcome, err error) error {
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return outcome.Denial()
}
