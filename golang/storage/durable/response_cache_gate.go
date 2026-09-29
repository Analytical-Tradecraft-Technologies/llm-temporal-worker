package durable

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

var (
	ErrCacheWait             = errors.New("response cache fill is in progress")
	ErrCacheRecoveryRequired = errors.New("response cache attempt requires recovery")
)

// This value stays inside one runner invocation. It contains no dispatch
// grant and must never be serialized or passed back as an activity result.
type responseCachePreparation struct {
	cache       *ResponseCache
	lease       cache.FillLease
	fill        cache.FillDecision
	entry       *cache.ResponseEntry
	disposition CacheDisposition
	preparedAt  time.Time
}

func (p *responseCachePreparation) validate(disposition CacheDisposition) error {
	if p == nil {
		if disposition == CacheWait || disposition == CacheRecoveryRequired {
			return errors.New("cache ownership decision requires response cache preparation")
		}
		return nil // Existing deployments may supply their own non-cloud ports.
	}
	if p.cache == nil || disposition != p.disposition {
		return errors.New("cache decision changed after preparation")
	}
	return nil
}

func (p *responseCachePreparation) stop() error {
	if p == nil {
		return nil
	}
	switch p.disposition {
	case CacheWait:
		mapped := provider.NewError(provider.CodeStateUnavailable, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryAfter, "response cache fill is in progress")
		mapped.OperationID = string(p.lease.OperationID)
		mapped.RetryAfter = p.fill.Record.Lease.ExpiresAt.Sub(p.preparedAt)
		return fmt.Errorf("%w: %w", ErrCacheWait, mapped)
	case CacheRecoveryRequired:
		return p.recoveryError(nil)
	}
	return nil
}

func (p *responseCachePreparation) validateRoute(route RoutePlan) error {
	if p == nil {
		return nil
	}
	key := p.lease.Key
	if p.disposition != CacheMiss || string(p.lease.OperationID) != string(route.OperationID) || p.lease.Attempt != string(route.GenerationID) ||
		key.Route != route.CacheIdentity || string(key.Route.Provider) != route.Provider || string(key.Route.Endpoint) != route.EndpointID || string(key.Route.Model) != route.Model {
		return errors.New("dispatch route or budget generation differs from cache preparation")
	}
	return nil
}

// A denied reservation leaves the fill HELD; it can be retried or expire.
// After accepted reservation, start fences duplicate attempts BEFORE Redis
// claim. If start fails, the unclaimed budget authorization retains its normal
// expiry; never refund it here, since another invocation may be using it.
// Once start may have committed, failures require durable attempt recovery;
// neither a timer nor activity retry grants permission to submit again.
func (p *responseCachePreparation) start(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := p.cache.now().UTC()
	if now.Before(p.preparedAt) || !now.Before(p.lease.ExpiresAt) {
		return p.recoveryError(errors.New("cache start clock is outside the lease"))
	}
	started, err := p.cache.fills.Start(ctx, p.lease, now)
	if err != nil || !started {
		return p.recoveryError(err)
	}
	return nil
}

func (p *responseCachePreparation) recoveryError(cause error) error {
	mapped := provider.NewError(provider.CodeStateUnavailable, provider.PhaseAdmission, provider.DispatchAmbiguous, provider.RetrySameOperation, "response cache attempt requires recovery")
	mapped.OperationID = string(p.lease.OperationID)
	mapped.Cause = cause
	return fmt.Errorf("%w: %w", ErrCacheRecoveryRequired, mapped)
}
