package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
)

// PendingRole says which part of a public operation a discovery row belongs to.
type PendingRole string

const (
	// PendingRoleRoot is a public operation's request: it holds the caller's
	// manifest, its preparation and its active attempt.
	PendingRoleRoot PendingRole = "root"
	// PendingRoleAttempt is one paid attempt of a root.
	PendingRoleAttempt PendingRole = "attempt"
)

// PendingExecution is the content-free accounting state of one provider
// execution. It never carries the provider job ID, response or receipts.
type PendingExecution struct {
	AttemptID RequestID
	Stage     ExecutionStage
	// Paid is true when the attempt's plan required a Redis reservation.
	Paid    bool
	Claimed bool
	// Pollable is true when the provider returned a job that can be polled.
	Pollable        bool
	SettlementSaved bool
	Settled         bool
	// Retryable is true for a known failure the request may retry with a new
	// attempt and fresh budget.
	Retryable    bool
	RecoverAfter time.Time
	PollAfter    time.Time
}

// PendingInspection describes one discovery row's authoritative record without
// exposing its encrypted payload. For a root, Execution is its active
// attempt's; for an attempt, its own.
type PendingInspection struct {
	ID    RequestID
	Shard int
	// Initializing means the discovery row exists but Create never published
	// the request's first event. Nothing else is known about it.
	Initializing bool
	Kind         string
	Status       Status
	Revision     uint64
	CreatedAt    time.Time
	UpdatedAt    time.Time
	// Role is empty for a record still in StatusPending, which holds no
	// progress yet.
	Role PendingRole
	// RootID is an attempt's root.
	RootID RequestID
	// ActiveAttemptID is the root's active attempt, for a root and for an
	// attempt alike. An attempt that is not its root's active attempt has no
	// workflow left that will advance it.
	ActiveAttemptID RequestID
	// Retired is true when the root replaced this attempt.
	Retired bool
	// Finalizing is true while a root holds a committed finalization step
	// that only completion finishes.
	Finalizing bool
	Execution  *PendingExecution
}

// Terminal reports a completed or failed record.
func (inspection PendingInspection) Terminal() bool { return inspection.Status.terminal() }

// InspectPending is a privileged, read-only recovery query for one discovered
// request ID, such as one returned by ListPending. It reads the authoritative
// record (and, for an attempt, its root) and never writes: it does not repair
// a lagging discovery row, retire an attempt or touch the terminal rows that
// budget recovery reads. Like ReadForRecovery, it must never be exposed to an
// untrusted caller; an ID is not authorization.
func (r *Repository) InspectPending(ctx context.Context, id RequestID) (PendingInspection, error) {
	shard, err := PendingShard(id)
	if err != nil {
		return PendingInspection{}, err
	}
	record, err := r.ReadForRecovery(ctx, id)
	if errors.Is(err, contracts.ErrNotFound) {
		// An interrupted Create leaves only its revision-zero discovery row.
		row, getErr := r.table.Get(ctx, r.indexKey(id))
		if getErr != nil {
			return PendingInspection{}, getErr
		}
		index, decodeErr := r.decodeIndex(row)
		if decodeErr != nil {
			return PendingInspection{}, decodeErr
		}
		if index.Revision != 0 {
			return PendingInspection{}, ErrCorrupt
		}
		return PendingInspection{ID: id, Shard: shard, Initializing: true, Status: index.Status, UpdatedAt: index.UpdatedAt}, nil
	}
	if err != nil {
		return PendingInspection{}, err
	}
	result := PendingInspection{ID: id, Shard: shard, Kind: record.Request.Kind, Status: record.Status, Revision: record.Revision, CreatedAt: record.Request.CreatedAt, UpdatedAt: record.UpdatedAt, Role: PendingRoleRoot}
	if record.Status == StatusPending {
		// Created but never prepared or initialized as an attempt: it holds
		// no progress, so neither its role nor any paid work exists yet.
		result.Role = ""
		return result, nil
	}
	var progress map[string]json.RawMessage
	if json.Unmarshal(record.Progress, &progress) != nil || progress == nil {
		return PendingInspection{}, ErrCorrupt
	}
	if data := progress["attempt_parent"]; data != nil {
		return r.inspectAttempt(ctx, record, progress, result)
	}
	if record.Status.terminal() {
		// Completion and failure drop a root's execution metadata.
		return result, nil
	}
	result.Finalizing = executionFinalizing(progress)
	active, err := r.decodeRequestAttempt(record, progress["request_attempt"])
	if err != nil {
		return PendingInspection{}, err
	}
	if active == nil {
		// A root from before independent attempts may hold its own execution.
		result.Execution, err = pendingExecution(record)
		return result, err
	}
	result.ActiveAttemptID = active.ID
	child, err := r.Read(ctx, record.Request.Scope, active.ID)
	if errors.Is(err, contracts.ErrNotFound) {
		// The root names its child only after the child's first event.
		return PendingInspection{}, ErrCorrupt
	}
	if err != nil {
		return PendingInspection{}, err
	}
	result.Execution, err = pendingExecution(child)
	return result, err
}

func (r *Repository) inspectAttempt(ctx context.Context, record Record, progress map[string]json.RawMessage, result PendingInspection) (PendingInspection, error) {
	var link RequestAttempt
	if json.Unmarshal(progress["attempt_parent"], &link) != nil || link.Validate() != nil || link.ID != record.Request.ID {
		return PendingInspection{}, ErrCorrupt
	}
	result.Role, result.RootID, result.Retired = PendingRoleAttempt, link.RootID, progress["attempt_retired"] != nil
	root, err := r.Read(ctx, record.Request.Scope, link.RootID)
	if errors.Is(err, contracts.ErrNotFound) {
		// A child is created only from a committed root in the same scope.
		return PendingInspection{}, ErrCorrupt
	}
	if err != nil {
		return PendingInspection{}, err
	}
	if !root.Status.terminal() {
		var rootProgress map[string]json.RawMessage
		if json.Unmarshal(root.Progress, &rootProgress) != nil || rootProgress == nil {
			return PendingInspection{}, ErrCorrupt
		}
		active, err := r.decodeRequestAttempt(root, rootProgress["request_attempt"])
		if err != nil {
			return PendingInspection{}, err
		}
		if active != nil {
			result.ActiveAttemptID = active.ID
		}
	}
	result.Execution, err = pendingExecution(record)
	return result, err
}

// pendingExecution summarizes record's own provider execution, or returns nil
// when it has no budget plan or no execution yet.
func pendingExecution(record Record) (*PendingExecution, error) {
	_, plan, execution, err := executionProgress(record)
	if errors.Is(err, ErrBudgetPlanMissing) || (err == nil && execution == nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &PendingExecution{
		AttemptID:       record.Request.ID,
		Stage:           execution.Stage,
		Paid:            plan.RequiresReservation(),
		Claimed:         execution.Claim != nil,
		Pollable:        execution.ProviderOperationID != "",
		SettlementSaved: execution.Settlement != nil,
		Settled:         execution.Settled,
		Retryable:       execution.Failure != nil && execution.Failure.Retryable,
		RecoverAfter:    execution.RecoverAfter,
		PollAfter:       execution.PollAfter,
	}, nil
}
