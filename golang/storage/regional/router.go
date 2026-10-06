// Package regional supplies bounded, conservative regional storage routing.
package regional

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/mfow/llm-temporal-worker/golang/storage/blob"
)

// Router remembers the last healthy endpoint, without sharing mutable SDK clients.
// A request visits each endpoint at most once. An attempt reserves deadline time
// for the remaining endpoints so a failed region cannot consume the entire call.
type Router struct {
	Active  atomic.Int32
	Count   int
	Timeout time.Duration
}

func AttemptContext(ctx context.Context, timeout time.Duration, remaining int) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok {
		budget := time.Until(deadline) / time.Duration(remaining)
		if budget < timeout {
			timeout = budget
		}
	}
	return context.WithTimeout(ctx, timeout)
}

// Unavailable excludes permanent authorization, validation, corruption and
// conditional failures. Caller cancellation is checked separately by Run.
func Unavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, contracts.ErrPermissionDenied) || errors.Is(err, contracts.ErrUnauthenticated) || errors.Is(err, contracts.ErrInvalidArgument) || errors.Is(err, contracts.ErrUnsupported) {
		return false
	}
	if errors.Is(err, contracts.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var response *smithyhttp.ResponseError
	if errors.As(err, &response) {
		return response.HTTPStatusCode() >= 500
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "ServiceUnavailable", "InternalError", "InternalServerError", "RequestTimeout":
			return true
		default:
			return false
		}
	}
	var network net.Error
	return errors.As(err, &network)
}

func Missing(err error) bool {
	if errors.Is(err, contracts.ErrNotFound) || errors.Is(err, blob.ErrNotFound) {
		return true
	}
	var api smithy.APIError
	return errors.As(err, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound")
}

// Run retries only explicitly eligible failures. An ambiguous mutation is
// returned unchanged and advances the endpoint for subsequent reconciliation.
func Run[T any](ctx context.Context, r *Router, retry func(error) bool, call func(context.Context, int) (T, error)) (T, error) {
	var zero T
	start := int(r.Active.Load()) % r.Count
	var last error
	var unavailable error
	for n := 0; n < r.Count; n++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		index := (start + n) % r.Count
		attempt, cancel := AttemptContext(ctx, r.Timeout, r.Count-n)
		value, err := call(attempt, index)
		// Do not cancel an opened streaming reader's context before it is consumed.
		// Calls returning readers must consume them within call.
		cancel()
		if err == nil {
			r.Active.Store(int32(index))
			return value, nil
		}
		last = err
		if Unavailable(err) && unavailable == nil {
			unavailable = err
		}
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		if !retry(err) {
			if Unavailable(err) {
				r.Active.Store(int32((index + 1) % r.Count))
			}
			return zero, err
		}
	}
	if Missing(last) && unavailable != nil {
		return zero, unavailable
	}
	return zero, last
}

func RetryRead(err error) bool { return Unavailable(err) }

// No uncertain write is automatically replayed, even if its primary kind is
// unavailable. Repository reconciliation owns that decision.
func RetryMutation(err error) bool {
	return Unavailable(err) && !errors.Is(err, contracts.ErrOutcomeUnknown)
}
