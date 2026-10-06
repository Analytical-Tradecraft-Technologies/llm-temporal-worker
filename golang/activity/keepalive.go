package activity

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// DefaultHeartbeatKeepaliveInterval is deliberately independent of any
// provider request deadline. Workflow callers must use an Activity heartbeat
// timeout of at least three times this cadence (or their configured override).
const DefaultHeartbeatKeepaliveInterval = time.Second

const heartbeatProviderWaitPhase = "provider_wait"

// heartbeatTicker is small so tests can advance bounded provider waits without
// wall-clock sleeps. Production uses a time.Ticker.
type heartbeatTicker interface {
	C() <-chan time.Time
	Stop()
}

type timeHeartbeatTicker struct {
	ticker *time.Ticker
}

func newTimeHeartbeatTicker(interval time.Duration) heartbeatTicker {
	return &timeHeartbeatTicker{ticker: time.NewTicker(interval)}
}

func (ticker *timeHeartbeatTicker) C() <-chan time.Time { return ticker.ticker.C }

func (ticker *timeHeartbeatTicker) Stop() { ticker.ticker.Stop() }

// heartbeatKeepalive sends a fixed, redacted fact only while a one-shot
// Engine.Generate call is blocked in a provider wait. It has no stream or
// response payload access.
type heartbeatKeepalive struct {
	parent context.Context
	cancel context.CancelFunc
	ticker heartbeatTicker
	done   chan struct{}

	mu       sync.Mutex
	stopping bool
	err      error
}

func startHeartbeatKeepalive(parent context.Context, target Heartbeater, interval time.Duration, tickerFactory func(time.Duration) heartbeatTicker) (context.Context, *heartbeatKeepalive) {
	if tickerFactory == nil {
		tickerFactory = newTimeHeartbeatTicker
	}
	child, cancel := context.WithCancel(parent)
	keepalive := &heartbeatKeepalive{
		parent: parent,
		cancel: cancel,
		ticker: tickerFactory(interval),
		done:   make(chan struct{}),
	}
	go keepalive.run(child, target)
	return child, keepalive
}

func (keepalive *heartbeatKeepalive) run(ctx context.Context, target Heartbeater) {
	defer close(keepalive.done)
	defer keepalive.ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-keepalive.ticker.C():
		}
		if !keepalive.beginBeat() || keepalive.parent.Err() != nil {
			return
		}
		if err := target.Beat(ctx, engine.Progress{Phase: heartbeatProviderWaitPhase}); err != nil {
			if keepalive.recordFailure(ctx, err) {
				keepalive.cancel()
			}
			return
		}
	}
}

// beginBeat reserves the tick before calling the target. A Stop that starts
// after this reservation waits for the in-flight call, so a heartbeat failure
// cannot race a successful provider return into a false Activity success.
func (keepalive *heartbeatKeepalive) beginBeat() bool {
	keepalive.mu.Lock()
	defer keepalive.mu.Unlock()
	return !keepalive.stopping
}

// recordFailure deliberately preserves a failure from a heartbeat that was
// already reserved when Stop began. Parent cancellation is the sole normal
// cancellation path; it must remain cancellation rather than ambiguity.
func (keepalive *heartbeatKeepalive) recordFailure(ctx context.Context, err error) bool {
	keepalive.mu.Lock()
	defer keepalive.mu.Unlock()
	if keepalive.parent.Err() != nil {
		return false
	}
	// Stop cancels the child context after Generate has already returned. A
	// Heartbeater that observes that cancellation is not a failed heartbeat;
	// only a transport or other genuine pre-stop error makes the provider
	// outcome ambiguous.
	if keepalive.stopping && errors.Is(err, context.Canceled) && errors.Is(ctx.Err(), context.Canceled) {
		return false
	}
	if keepalive.err == nil {
		keepalive.err = err
	}
	return true
}

func (keepalive *heartbeatKeepalive) stop() error {
	if keepalive == nil {
		return nil
	}
	keepalive.mu.Lock()
	keepalive.stopping = true
	keepalive.mu.Unlock()
	keepalive.cancel()
	<-keepalive.done
	keepalive.mu.Lock()
	defer keepalive.mu.Unlock()
	return keepalive.err
}

func heartbeatKeepaliveFailure(cause error) error {
	failure := provider.NewError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "Activity heartbeat keepalive failed during provider wait")
	failure.Cause = cause
	return failure
}
