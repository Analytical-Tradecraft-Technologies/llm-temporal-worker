package cache

import (
	"context"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

// MaxFillLease bounds the time to start a fill, not the duration of paid work.
// Redis budget authorization is separate and must still be checked at dispatch.
const MaxFillLease = 15 * time.Minute

// FillLease is a stable attempt identity. Persist it before using it and retain
// every field unchanged across retries. Attempt must be unique for each new
// attempt, including retries of paid work whose outcome is unknown.
type FillLease struct {
	Key         ResponseKey
	OperationID state.OperationID
	Attempt     string
	AcquiredAt  time.Time
	ExpiresAt   time.Time
}

type FillState string

const (
	FillHeld     FillState = "held"
	FillStarted  FillState = "started"
	FillReleased FillState = "released"
	FillFinished FillState = "finished"
)

type FillOutcome string

const (
	FillPublished    FillOutcome = "published"
	FillNotCacheable FillOutcome = "not_cacheable"
	FillFailed       FillOutcome = "failed"
	FillUnknown      FillOutcome = "outcome_unknown"
)

// FillCompletion is supplied by the durable finalizer after resolving provider
// state and reconciling the original budget. Unknown work must remain charged;
// finishing it permits a NEW attempt that must acquire fresh Redis budget.
// Only Published accepts EntryID and requires an already published success.
type FillCompletion struct {
	Outcome     FillOutcome
	EntryID     state.CacheEntryID
	CompletedAt time.Time
}

type FillRecord struct {
	Lease      FillLease
	State      FillState
	UpdatedAt  time.Time
	Completion FillCompletion
}

type FillDisposition string

const (
	FillOwned           FillDisposition = "owned"
	FillWait            FillDisposition = "wait"
	FillRecoveryNeeded  FillDisposition = "recovery_needed"
	FillAttemptFinished FillDisposition = "attempt_finished"
)

type FillDecision struct {
	Disposition FillDisposition
	Record      FillRecord
}

// FillRepository coordinates independent workers through conditional writes.
// It never sleeps, calls a provider or reserves/refunds budget. All times come
// from a trusted worker clock. A read failure is an error, never permission to
// dispatch. Implementations retain tombstones; do not independently TTL fills.
type FillRepository interface {
	// Acquire may take over a HELD lease that has expired at the given time,
	// since Start then fences the previous owner. STARTED work never expires:
	// it requires explicit recovery. Retry unknown writes with the identical
	// proposed lease and a fresh time. A lease fenced by another attempt's
	// record conflicts and returns that record.
	Acquire(context.Context, FillLease, time.Time) (FillDecision, error)
	// Start returns true ONLY to the call that commits held -> started with a
	// definite acknowledgement. Only that invocation may dispatch (after Redis
	// authorization). False means recover the existing attempt, never resubmit.
	// An unknown write must also be recovered; retrying cannot return true for
	// a transition that already committed. This is not provider exactly-once.
	// Call Start inside the same activity invocation that submits; do not
	// return its boolean from a separate activity as a reusable workflow token.
	Start(context.Context, FillLease, time.Time) (bool, error)
	// Release abandons only an unstarted attempt, e.g. a second lookup hit.
	Release(context.Context, FillLease, time.Time) error
	// Complete records a resolved started attempt. It cannot recover provider
	// state or verify Redis settlement; those are required caller obligations.
	Complete(context.Context, FillLease, FillCompletion) error
}
