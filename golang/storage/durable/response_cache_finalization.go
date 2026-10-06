package durable

import (
	"context"
	"errors"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
)

// CompleteAttempt finishes a started Generate or Compact fill after its origin
// checkpoint has committed. The caller supplies the same lease, completion,
// entry and idempotent budget-settlement callback on every retry, including a
// replay of an uncertain publication or completion write. The callback must
// settle the original Redis generation; it must not acquire another budget.
//
// A publishable success becomes visible before budget settlement. If any step
// fails, the caller must retry this whole sequence without assuming which
// writes committed. An
// incomplete response, provider failure or unknown outcome supplies no entry;
// its original budget is still settled before the fill can be finished.
func (c *ResponseCache) CompleteAttempt(ctx context.Context, lease cache.FillLease, completion cache.FillCompletion, entry *cache.ResponseEntry, settleBudget func(context.Context) error) error {
	if c == nil || nilCacheRepository(c.responses) || nilCacheRepository(c.fills) || ctx == nil || settleBudget == nil {
		return errors.New("response cache completion requires repositories, context and budget settlement")
	}
	if err := ValidateAttemptCompletion(lease, completion, entry); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if entry != nil {
		if err := c.responses.Publish(ctx, *entry); err != nil {
			return err
		}
	}
	if err := settleBudget(ctx); err != nil {
		return err
	}
	return c.fills.Complete(ctx, lease, completion)
}

// RecordUse writes the receipt for a cache hit after its distinct, zero-cost
// consumer checkpoint has committed. A lost acknowledgement is retried with
// the identical use, including CompletedAt; the repository verifies both
// checkpoints and makes the receipt unique per consuming operation.
func (c *ResponseCache) RecordUse(ctx context.Context, origin cache.ResponseEntry, use cache.ResponseUse) error {
	if c == nil || nilCacheRepository(c.responses) || ctx == nil {
		return errors.New("response cache use requires a repository and context")
	}
	if err := ValidateResponseUse(origin, use); err != nil {
		return err
	}
	return c.responses.RecordUse(ctx, use)
}

// ValidateAttemptCompletion validates immutable replay inputs before persisting them.
// It performs no writes and does not authorize dispatch.
func ValidateAttemptCompletion(lease cache.FillLease, completion cache.FillCompletion, entry *cache.ResponseEntry) error {
	if lease.Key.ScopeID == "" || lease.OperationID == "" || lease.Attempt == "" || lease.AcquiredAt.IsZero() ||
		lease.ExpiresAt.IsZero() || !lease.ExpiresAt.After(lease.AcquiredAt) || lease.ExpiresAt.Sub(lease.AcquiredAt) > cache.MaxFillLease ||
		completion.CompletedAt.IsZero() || completion.CompletedAt.Before(lease.AcquiredAt) {
		return errors.New("invalid response cache completion identity or time")
	}
	switch completion.Outcome {
	case cache.FillPublished:
		if entry == nil || entry.ID == "" || entry.ID != completion.EntryID || entry.Key != lease.Key ||
			entry.OriginOperationID != lease.OperationID || entry.OriginCheckpointID == "" || entry.CompletedAt.IsZero() ||
			entry.CompletedAt.Before(lease.AcquiredAt) || entry.CompletedAt.After(completion.CompletedAt) {
			return errors.New("published response does not match the started fill")
		}
	case cache.FillNotCacheable, cache.FillFailed, cache.FillUnknown:
		if entry != nil || completion.EntryID != "" {
			return errors.New("non-published fill cannot include a cache entry")
		}
	default:
		return errors.New("invalid response cache fill outcome")
	}
	return nil
}

// ValidateResponseUse validates the immutable origin and consumer receipt without writes.
func ValidateResponseUse(origin cache.ResponseEntry, use cache.ResponseUse) error {
	if origin.ID == "" || origin.Key.ScopeID == "" || origin.OriginOperationID == "" || origin.OriginCheckpointID == "" ||
		origin.CompletedAt.IsZero() || use.ScopeID != origin.Key.ScopeID || use.EntryID != origin.ID ||
		use.OperationID == "" || use.OperationID == origin.OriginOperationID || use.CheckpointID == "" ||
		use.CheckpointID == origin.OriginCheckpointID || use.CompletedAt.IsZero() || use.CompletedAt.Before(origin.CompletedAt) {
		return errors.New("cache use does not match its origin or consumer")
	}
	return nil
}
