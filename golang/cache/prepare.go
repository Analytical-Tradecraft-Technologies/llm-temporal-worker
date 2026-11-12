package cache

import (
	"context"
	"errors"
)

// Preparation returns immediately. Entry is an origin template which still
// needs a distinct zero-cost consuming operation/checkpoint and use receipt.
// With no Entry, Fill describes ownership, waiting or recovery. Even an owner
// must successfully Start and obtain Redis authorization before dispatch.
type Preparation struct {
	Entry *ResponseEntry
	Fill  FillDecision
}

// Prepare is the storage-neutral cache phase for a NEW, undispatched attempt.
// The operation's durable replay/recovery phase must run first. Do not use a
// cache hit to abandon a previously started paid attempt. Workflow timers, not
// sleeping activities, should retry waiting decisions with a fresh lookup time.
// The proposed lease stays identical when retrying an uncertain acquisition.
func Prepare(ctx context.Context, responses ResponseRepository, fills FillRepository, lookup ResponseLookup, lease FillLease) (Preparation, error) {
	if ctx == nil || responses == nil || fills == nil || lookup.Key != lease.Key || lookup.Now.Before(lease.AcquiredAt) || !lookup.Now.Before(lease.ExpiresAt) {
		return Preparation{}, errors.New("invalid cache preparation")
	}
	if err := ctx.Err(); err != nil {
		return Preparation{}, err
	}
	entry, err := responses.Lookup(ctx, lookup)
	if err != nil || entry != nil {
		return Preparation{Entry: entry}, err
	}
	return acquireFill(ctx, responses, fills, lookup, lease)
}

// PrepareAfterMiss is Prepare for a caller whose own Lookup of the same key
// found no entry earlier in the same invocation; it skips that first lookup.
// A success published since is still found: an owner or waiter rechecks after
// acquisition as Prepare does, and any other decision, which observes another
// attempt's fill without taking it, is followed by the lookup Prepare would
// have made first.
func PrepareAfterMiss(ctx context.Context, responses ResponseRepository, fills FillRepository, lookup ResponseLookup, lease FillLease) (Preparation, error) {
	if ctx == nil || responses == nil || fills == nil || lookup.Key != lease.Key || lookup.Now.Before(lease.AcquiredAt) || !lookup.Now.Before(lease.ExpiresAt) {
		return Preparation{}, errors.New("invalid cache preparation")
	}
	if err := ctx.Err(); err != nil {
		return Preparation{}, err
	}
	result, err := acquireFill(ctx, responses, fills, lookup, lease)
	if err != nil || result.Entry != nil || result.Fill.Disposition == FillOwned || result.Fill.Disposition == FillWait {
		return result, err
	}
	entry, err := responses.Lookup(ctx, lookup)
	if err != nil || entry == nil {
		return result, err
	}
	return Preparation{Entry: entry}, nil
}

func acquireFill(ctx context.Context, responses ResponseRepository, fills FillRepository, lookup ResponseLookup, lease FillLease) (Preparation, error) {
	decision, err := fills.Acquire(ctx, lease, lookup.Now)
	if err != nil {
		return Preparation{}, err
	}
	result := Preparation{Fill: decision}
	// An existing dispatch or completed attempt must go back through durable
	// operation recovery, even if another success is now available.
	if decision.Disposition != FillOwned && decision.Disposition != FillWait {
		return result, nil
	}
	// Publication can race the first lookup and acquisition. An owner must
	// recheck before spending budget, and a waiter can consume the new success.
	entry, err := responses.Lookup(ctx, lookup)
	if err != nil || entry == nil {
		return result, err
	}
	if decision.Disposition == FillOwned {
		if err := fills.Release(ctx, lease, lookup.Now); err != nil {
			return Preparation{}, err
		}
	}
	return Preparation{Entry: entry}, nil
}
