package control

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// SpendSummaryListOptions selects a bounded read for an already-authorized
// opaque scope. Time bounds are half-open: StartTime <= completion < EndTime.
// ScopeID is supplied by the authenticated scope resolver, never guessed by
// the reader. Implementations must preserve exact and unknown cost separately.
type SpendSummaryListOptions struct {
	ScopeID        uuid.UUID
	StartTime      time.Time
	EndTime        time.Time
	GroupBy        []SpendDimension
	OperationKinds []OperationKind
}

// SpendSummaryReader supplies typed spend aggregates without exposing a
// database connection or provider-specific repository to query composition.
type SpendSummaryReader interface {
	ListSpendSummary(context.Context, SpendSummaryListOptions) (SpendSummaryResult, error)
}
