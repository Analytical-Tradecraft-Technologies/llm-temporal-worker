package cloudstate

import "github.com/mfow/llm-temporal-worker/golang/cache"

func normalizeFillLease(lease cache.FillLease) (cache.FillLease, error) {
	if !validCacheKey(lease.Key) || !safeText(string(lease.OperationID), 4096) || !safeText(lease.Attempt, 256) || !validTime(lease.AcquiredAt) || !validTime(lease.ExpiresAt) ||
		!lease.ExpiresAt.After(lease.AcquiredAt) || lease.ExpiresAt.Sub(lease.AcquiredAt) > cache.MaxFillLease {
		return cache.FillLease{}, ErrInvalid
	}
	lease.AcquiredAt, lease.ExpiresAt = lease.AcquiredAt.UTC(), lease.ExpiresAt.UTC()
	return lease, nil
}

func validFillCompletion(c cache.FillCompletion) bool {
	if !validTime(c.CompletedAt) {
		return false
	}
	switch c.Outcome {
	case cache.FillPublished:
		return safeText(string(c.EntryID), 256)
	case cache.FillFailed, cache.FillNotCacheable, cache.FillUnknown:
		return c.EntryID == ""
	}
	return false
}

func validFillRecord(r cache.FillRecord) bool {
	if _, err := normalizeFillLease(r.Lease); err != nil || !validTime(r.UpdatedAt) || r.UpdatedAt.Before(r.Lease.AcquiredAt) {
		return false
	}
	switch r.State {
	case cache.FillHeld:
		return r.UpdatedAt.Equal(r.Lease.AcquiredAt) && r.Completion == (cache.FillCompletion{})
	case cache.FillStarted:
		return r.UpdatedAt.Before(r.Lease.ExpiresAt) && r.Completion == (cache.FillCompletion{})
	case cache.FillReleased:
		return r.Completion == (cache.FillCompletion{})
	case cache.FillFinished:
		return validFillCompletion(r.Completion) && r.Completion.CompletedAt.Equal(r.UpdatedAt)
	}
	return false
}
