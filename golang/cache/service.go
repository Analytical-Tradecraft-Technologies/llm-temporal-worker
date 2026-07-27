package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// FillCoordinator collapses identical in-process cache fills. It is only a
// latency/cost optimization: callers must still perform the authoritative
// PostgreSQL lookup, fill-lease acquisition, publication, and failure update.
// A Fingerprint is comparable and already domain-separated by Input.Operation;
// callers must not derive it from raw prompt text or use one key across
// operation domains.
type FillCoordinator struct {
	mu               sync.Mutex
	flights          map[Fingerprint]*fillCall
	maxResponseBytes int
}

type fillCall struct {
	done     chan struct{}
	response []byte
	err      error
}

// FillResult reports the normalized response template. Shared is true when
// this invocation waited for another local owner; it has no bearing on
// durable cache provenance.
type FillResult struct {
	Response []byte
	Shared   bool
}

// FillFunc performs one provider call and returns the operation-kind-neutral,
// normalized response template. It must not mutate the returned bytes after
// returning them.
type FillFunc func(context.Context) ([]byte, error)

// NewFillCoordinator creates a coordinator with an optional response bound.
// A positive bound prevents an accidental unbounded in-memory response; zero
// uses no additional bound because the durable repository remains responsible
// for its own inline/blob policy.
func NewFillCoordinator(maxResponseBytes int) (*FillCoordinator, error) {
	if maxResponseBytes < 0 {
		return nil, fmt.Errorf("cache response limit must not be negative")
	}
	return &FillCoordinator{flights: make(map[Fingerprint]*fillCall), maxResponseBytes: maxResponseBytes}, nil
}

// Fill runs fn once for concurrent callers sharing key. Waiters can cancel
// independently without canceling the owner's provider call. Failed fills are
// never retained, so a subsequent request can retry after a transient error.
func (coordinator *FillCoordinator) Fill(ctx context.Context, key Fingerprint, fn FillFunc) (result FillResult, err error) {
	if coordinator == nil {
		return result, errors.New("cache fill coordinator is nil")
	}
	if ctx == nil {
		return result, errors.New("cache fill context is nil")
	}
	if fn == nil {
		return result, errors.New("cache fill function is nil")
	}
	if key == (Fingerprint{}) {
		return result, errors.New("cache fill fingerprint is required")
	}

	coordinator.mu.Lock()
	call, existing := coordinator.flights[key]
	if !existing {
		call = &fillCall{done: make(chan struct{})}
		coordinator.flights[key] = call
	}
	coordinator.mu.Unlock()

	if existing {
		return coordinator.wait(ctx, call)
	}

	return coordinator.runOwner(ctx, key, call, fn)
}

func (coordinator *FillCoordinator) wait(ctx context.Context, call *fillCall) (FillResult, error) {
	select {
	case <-call.done:
		if call.err != nil {
			return FillResult{}, call.err
		}
		return FillResult{Response: append([]byte(nil), call.response...), Shared: true}, nil
	case <-ctx.Done():
		return FillResult{}, ctx.Err()
	}
}

func (coordinator *FillCoordinator) runOwner(ctx context.Context, key Fingerprint, call *fillCall, fn FillFunc) (result FillResult, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("cache fill panicked: %v", recovered)
			result = FillResult{}
		}
		coordinator.mu.Lock()
		call.response = append([]byte(nil), result.Response...)
		call.err = err
		delete(coordinator.flights, key)
		close(call.done)
		coordinator.mu.Unlock()
	}()

	response, err := fn(ctx)
	if err != nil {
		return FillResult{}, err
	}
	if coordinator.maxResponseBytes > 0 && len(response) > coordinator.maxResponseBytes {
		return FillResult{}, fmt.Errorf("cache fill response exceeds %d bytes", coordinator.maxResponseBytes)
	}
	return FillResult{Response: append([]byte(nil), response...)}, nil
}
