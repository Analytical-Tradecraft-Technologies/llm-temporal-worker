package cloudstate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
	"github.com/google/uuid"
)

var ErrProviderExecutionMissing = errors.New("provider execution missing")

type ExecutionStage string

const (
	ExecutionClaiming   ExecutionStage = "claiming"
	ExecutionSubmitting ExecutionStage = "submitting"
	ExecutionPending    ExecutionStage = "pending"
	ExecutionSucceeded  ExecutionStage = "succeeded"
	ExecutionFailed     ExecutionStage = "failed"
	ExecutionUnknown    ExecutionStage = "outcome_unknown"
)

// ExecutionFailure deliberately excludes messages, diagnostic causes and raw
// provider metadata. The encrypted response/job ID remains internal too.
type ExecutionFailure struct {
	Code           provider.Code              `json:"code"`
	Dispatch       provider.DispatchCertainty `json:"dispatch"`
	Retryable      bool                       `json:"retryable,omitempty"`
	RetryNotBefore time.Time                  `json:"retry_not_before,omitempty"`
}

// ProviderExecution is one provider attempt, never permission to submit it again.
// The original immutable BudgetPlan remains alongside it. A replacement paid
// attempt needs a new request/operation identity and a fresh Redis reservation.
type ProviderExecution struct {
	StartToken          string                    `json:"start_token"`
	Version             int                       `json:"version"`
	Revision            uint64                    `json:"revision"`
	Stage               ExecutionStage            `json:"stage"`
	Reservation         durable.ReserveResult     `json:"reservation"`
	Claim               *durable.ClaimReceipt     `json:"claim,omitempty"`
	StartedAt           time.Time                 `json:"started_at"`
	RecoverAfter        time.Time                 `json:"recover_after"`
	UpdatedAt           time.Time                 `json:"updated_at"`
	CompletedAt         time.Time                 `json:"completed_at,omitempty"`
	ProviderOperationID string                    `json:"provider_operation_id,omitempty"`
	PollAfter           time.Time                 `json:"poll_after,omitempty"`
	Response            *llm.Response             `json:"response,omitempty"`
	Failure             *ExecutionFailure         `json:"failure,omitempty"`
	Settlement          *durable.ReconcileRequest `json:"settlement,omitempty"`
	Settled             bool                      `json:"settled"`
}

type SavedProviderExecution struct {
	Plan      BudgetPlan
	Execution ProviderExecution
}

func (execution ProviderExecution) Validate(plan BudgetPlan) error {
	startToken, tokenErr := uuid.Parse(execution.StartToken)
	if plan.Validate() != nil || execution.Version != 1 || execution.Revision == 0 || execution.Revision > maxRevisions ||
		tokenErr != nil || startToken == uuid.Nil || startToken.String() != execution.StartToken ||
		!validTime(execution.StartedAt) || execution.StartedAt.Before(plan.QuotedAt) || !validTime(execution.UpdatedAt) || execution.UpdatedAt.Before(execution.StartedAt) ||
		!execution.RecoverAfter.Equal(execution.StartedAt.Add(durable.BudgetStartLease)) {
		return ErrInvalid
	}
	paid := plan.RequiresReservation()
	if paid {
		if !execution.Reservation.Accepted || execution.Reservation.Validate(plan.Reservation) != nil || !executionReservationMatches(plan, execution.Reservation) {
			return ErrInvalid
		}
	} else if !equalExecutionJSON(execution.Reservation, durable.ReserveResult{}) || execution.Claim != nil || execution.Settlement != nil {
		return ErrInvalid
	}
	if execution.Claim != nil && execution.Claim.Validate(execution.Reservation) != nil {
		return ErrInvalid
	}
	if execution.ProviderOperationID != "" && !safeText(execution.ProviderOperationID, 4096) {
		return ErrInvalid
	}
	switch execution.Stage {
	case ExecutionClaiming:
		if execution.Claim != nil || execution.Revision != 1 {
			return ErrInvalid
		}
	case ExecutionSubmitting:
		if paid && execution.Claim == nil {
			return ErrInvalid
		}
	case ExecutionPending:
		if (paid && execution.Claim == nil) || execution.ProviderOperationID == "" || !validTime(execution.PollAfter) {
			return ErrInvalid
		}
	case ExecutionSucceeded:
		if (paid && execution.Claim == nil) || execution.Response == nil || execution.Failure != nil {
			return ErrInvalid
		}
		if _, err := execution.Response.MarshalJSON(); err != nil {
			return ErrInvalid
		}
	case ExecutionFailed, ExecutionUnknown:
		if execution.Failure == nil || !execution.Failure.Code.Valid() || !execution.Failure.Dispatch.Valid() {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	if execution.Failure != nil {
		failure := execution.Failure
		if failure.Retryable {
			if execution.Stage != ExecutionFailed || !validTime(failure.RetryNotBefore) || failure.RetryNotBefore.Before(execution.CompletedAt) {
				return ErrInvalid
			}
		} else if !failure.RetryNotBefore.IsZero() {
			return ErrInvalid
		}
	}
	if execution.Stage != ExecutionSucceeded && execution.Response != nil {
		return ErrInvalid
	}
	if execution.Stage != ExecutionFailed && execution.Stage != ExecutionUnknown && execution.Failure != nil {
		return ErrInvalid
	}
	if execution.Stage != ExecutionPending && !execution.PollAfter.IsZero() {
		return ErrInvalid
	}
	if execution.Settled && paid && execution.Settlement == nil {
		return ErrInvalid
	}
	terminal := execution.Stage == ExecutionSucceeded || execution.Stage == ExecutionFailed
	if terminal {
		if !validTime(execution.CompletedAt) || execution.CompletedAt.Before(execution.StartedAt) || execution.CompletedAt.After(execution.UpdatedAt) {
			return ErrInvalid
		}
	} else if !execution.CompletedAt.IsZero() || (execution.Settled && execution.Stage != ExecutionUnknown) {
		return ErrInvalid
	}
	if !paid && terminal && !execution.Settled {
		return ErrInvalid
	}
	if execution.Settlement != nil {
		settlement := execution.Settlement
		// An unknown outcome may only be settled at its full reservation, and
		// only once its recovery window has elapsed.
		unknown := execution.Stage == ExecutionUnknown
		if execution.Claim == nil || (!terminal && !unknown) || (unknown && execution.UpdatedAt.Before(execution.RecoverAfter)) || settlement.Validate() != nil ||
			settlement.OperationID != plan.Route.OperationID || settlement.GenerationID != plan.Route.GenerationID || settlement.IncarnationID != execution.Reservation.IncarnationID ||
			len(settlement.Events) != len(execution.Reservation.Events) {
			return ErrInvalid
		}
		for i, event := range settlement.Events {
			reserved := execution.Reservation.Events[i]
			if event.WindowID != reserved.WindowID || !event.BucketStart.Equal(reserved.BucketStart) || event.ReservationRevision != reserved.ReservationRevision+1 ||
				event.ReservedDecreaseUSD.Cmp(reserved.AmountUSD) != 0 || (unknown && event.Kind != budget.JournalFinalizeUnknown) {
				return ErrInvalid
			}
		}
	}
	return nil
}

func executionReservationMatches(plan BudgetPlan, result durable.ReserveResult) bool {
	if len(result.Events) != len(plan.Reservation.Reservations) || result.Denial != nil || result.RetryAfter != 0 {
		return false
	}
	seen := make(map[string]bool)
	windows := make(map[string]bool)
	for _, event := range result.Events {
		if seen[event.EventID] || windows[event.WindowID] {
			return false
		}
		seen[event.EventID] = true
		windows[event.WindowID] = true
		matched := false
		for _, window := range plan.Reservation.Reservations {
			if event.WindowID == window.WindowID && event.BucketStart.Equal(time.Unix(0, window.Bucket*window.BucketNanos)) && event.AmountUSD.Cmp(window.AmountUSD) == 0 {
				matched = true
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// BeginProviderExecution fences initial admission before consuming its claim.
// Only a confirmed fresh write returns true. An uncertain acknowledgement or
// an existing record never authorizes submission, even by the same caller.
func (r *Repository) BeginProviderExecution(ctx context.Context, scope Scope, id RequestID, reservation durable.ReserveResult, now time.Time) (SavedProviderExecution, bool, error) {
	// Distinct proposal tokens stop identical concurrent CAS retries from both
	// observing the event stream's idempotent success as a fresh start grant.
	startToken, err := uuid.NewRandom()
	if err != nil {
		return SavedProviderExecution{}, false, err
	}
	for attempt := 0; attempt < 16; attempt++ {
		record, err := r.Read(ctx, scope, id)
		if err != nil {
			return SavedProviderExecution{}, false, err
		}
		progress, plan, existing, err := executionProgress(record)
		if err != nil {
			return SavedProviderExecution{}, false, err
		}
		if existing != nil {
			return SavedProviderExecution{*plan, *existing}, false, r.repairBudgetPlanIndex(ctx, record)
		}
		if record.Status != StatusRunning || executionFinalizing(progress) {
			return SavedProviderExecution{}, false, contracts.ErrConflict
		}
		// The quoting worker's clock may be ahead of this one. The execution
		// starts no earlier than the record it extends, never before its quote.
		if now.Before(record.UpdatedAt) {
			now = record.UpdatedAt
		}
		execution := ProviderExecution{Version: 1, StartToken: startToken.String(), Revision: 1, Stage: ExecutionClaiming, Reservation: reservation, StartedAt: now.UTC(), UpdatedAt: now.UTC(), RecoverAfter: now.UTC().Add(durable.BudgetStartLease)}
		if execution.Validate(*plan) != nil {
			return SavedProviderExecution{}, false, ErrInvalid
		}
		err = r.writeExecution(ctx, record, progress, execution)
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		return SavedProviderExecution{*plan, execution}, err == nil, err
	}
	return SavedProviderExecution{}, false, contracts.ErrConflict
}

func (r *Repository) LoadProviderExecution(ctx context.Context, scope Scope, id RequestID) (SavedProviderExecution, error) {
	record, err := r.Read(ctx, scope, id)
	if err != nil {
		return SavedProviderExecution{}, err
	}
	_, plan, execution, err := executionProgress(record)
	if err != nil {
		return SavedProviderExecution{}, err
	}
	if execution == nil {
		return SavedProviderExecution{}, ErrProviderExecutionMissing
	}
	return SavedProviderExecution{*plan, *execution}, r.repairBudgetPlanIndex(ctx, record)
}

// SaveProviderExecution uses an explicit revision and immutable attempt/lease
// bindings. Concurrent polling can only advance one view; stale workers reload.
func (r *Repository) SaveProviderExecution(ctx context.Context, scope Scope, id RequestID, previous uint64, execution ProviderExecution) error {
	record, err := r.Read(ctx, scope, id)
	if err != nil {
		return err
	}
	progress, plan, existing, err := executionProgress(record)
	if err != nil {
		return err
	}
	if existing == nil {
		return ErrProviderExecutionMissing
	}
	if execution.Validate(*plan) != nil || execution.Revision != previous+1 {
		return ErrInvalid
	}
	if equalExecution(*existing, execution) {
		return r.repairBudgetPlanIndex(ctx, record)
	}
	if existing.Revision != previous || executionFinalizing(progress) || !validExecutionTransition(*existing, execution) {
		return contracts.ErrConflict
	}
	return r.writeExecution(ctx, record, progress, execution)
}

func executionProgress(record Record) (map[string]json.RawMessage, *BudgetPlan, *ProviderExecution, error) {
	progress, plan, err := budgetPlanProgress(record.Progress)
	if err != nil {
		return nil, nil, nil, err
	}
	if plan == nil {
		return nil, nil, nil, ErrBudgetPlanMissing
	}
	if plan.Kind != record.Request.Kind || plan.QuotedAt.Before(record.Request.CreatedAt) {
		return nil, nil, nil, ErrCorrupt
	}
	encoded, exists := progress["provider_execution"]
	if !exists {
		return progress, plan, nil, nil
	}
	var execution ProviderExecution
	if json.Unmarshal(encoded, &execution) != nil || execution.Validate(*plan) != nil {
		return nil, nil, nil, ErrCorrupt
	}
	return progress, plan, &execution, nil
}

func executionFinalizing(progress map[string]json.RawMessage) bool {
	return progress["checkpoint_finalization"] != nil || progress["finalization_handoff"] != nil
}

func (r *Repository) writeExecution(ctx context.Context, record Record, progress map[string]json.RawMessage, execution ProviderExecution) error {
	encoded, err := json.Marshal(execution)
	if err != nil {
		return ErrInvalid
	}
	progress["provider_execution"] = encoded
	data, err := json.Marshal(progress)
	if err != nil || len(data) > maxPayloadBytes {
		return ErrInvalid
	}
	status := StatusProviderPending
	if execution.Stage == ExecutionSucceeded || execution.Stage == ExecutionFailed {
		status = StatusRunning
	}
	if execution.Stage == ExecutionUnknown {
		status = StatusOutcomeUnknown
	}
	digest := sha256.Sum256(encoded)
	now := execution.UpdatedAt
	if now.Before(record.UpdatedAt) {
		now = record.UpdatedAt
	}
	_, err = r.TryUpdate(ctx, record.Request.Scope, record.Request.ID, Update{ExpectedRevision: record.Revision, Token: "provider-" + hex.EncodeToString(digest[:]), Status: status, Progress: data, UpdatedAt: now})
	return err
}

func validExecutionTransition(old, next ProviderExecution) bool {
	if old.StartToken != next.StartToken || !old.StartedAt.Equal(next.StartedAt) || !old.RecoverAfter.Equal(next.RecoverAfter) || next.UpdatedAt.Before(old.UpdatedAt) ||
		!equalExecutionJSON(old.Reservation, next.Reservation) || (old.Claim != nil && !equalExecutionJSON(old.Claim, next.Claim)) ||
		(old.ProviderOperationID != "" && old.ProviderOperationID != next.ProviderOperationID) || (!old.CompletedAt.IsZero() && !old.CompletedAt.Equal(next.CompletedAt)) {
		return false
	}
	if old.Stage == ExecutionSucceeded || old.Stage == ExecutionFailed {
		// A terminal result is immutable; only acknowledge the saved settlement.
		copy := next
		copy.Revision = old.Revision
		copy.UpdatedAt = old.UpdatedAt
		copy.Settled = old.Settled
		return !old.Settled && next.Settled && equalExecution(old, copy)
	}
	if old.Stage == ExecutionUnknown && old.Settlement != nil {
		// A settled unknown outcome is closed at its conservative charge. Only
		// the settlement acknowledgement may follow; an exact outcome learned
		// later is an authorized correction, never a second settlement.
		return next.Stage == ExecutionUnknown && equalExecutionJSON(old.Settlement, next.Settlement) && (next.Settled || !old.Settled)
	}
	switch old.Stage {
	case ExecutionClaiming:
		return next.Stage == ExecutionSubmitting || next.Stage == ExecutionUnknown || next.Stage == ExecutionFailed
	case ExecutionSubmitting, ExecutionPending, ExecutionUnknown:
		return next.Stage == ExecutionPending || next.Stage == ExecutionSucceeded || next.Stage == ExecutionFailed || next.Stage == ExecutionUnknown
	}
	return false
}

func equalExecution(left, right ProviderExecution) bool { return equalExecutionJSON(left, right) }

// Saved progress is canonical JSON, while a value still in memory keeps the
// provider's spelling of raw JSON such as tool-call arguments. Compare the
// canonical forms so key order alone is never a different terminal result.
func equalExecutionJSON(left, right any) bool {
	l, le := canonicalExecutionJSON(left)
	r, re := canonicalExecutionJSON(right)
	return le == nil && re == nil && bytes.Equal(l, r)
}

func canonicalExecutionJSON(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return llm.CanonicalJSONWithLimits(encoded, maxPayloadBytes, llm.DefaultCanonicalMaxDepth)
}
