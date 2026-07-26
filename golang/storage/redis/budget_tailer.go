package redis

import (
	"context"
	"errors"
	"fmt"
)

// ErrBudgetStreamGenerationChanged means a Stream hint belongs to a
// different immutable generation than the worker checkpoint. The caller must
// reload the active Redis manifest before applying any further hints.
var ErrBudgetStreamGenerationChanged = errors.New("budget stream generation changed")

// BudgetStreamCursorState is the worker-local checkpoint for the broadcast
// budget Stream. The generation ID is part of the checkpoint so a worker can
// never apply a hint from a previous immutable generation to the current one.
// Cursor is a Redis Stream ID, or empty before the first read.
type BudgetStreamCursorState struct {
	GenerationID BudgetGenerationID
	Cursor       string
}

// BudgetStreamReload is returned by the authoritative Redis-only reload path
// after a disabled tailer, a retained-stream gap, or a generation switch.
// The reload must read the active pointer and manifest directly from Redis and
// return the manifest's high-water mark as Cursor. PostgreSQL is intentionally
// not part of this callback.
type BudgetStreamReload func(context.Context) (BudgetStreamCursorState, error)

// BudgetStreamApply receives a validated coordination hint. Hints may
// invalidate local plans or wake waiters, but they must never authorize work;
// the atomic Redis admission Function remains the authority. Apply should be
// idempotent because a worker may persist its cursor after a successful hint
// and retry a partially processed batch after a restart.
type BudgetStreamApply func(context.Context, BudgetStreamRecord) error

// BudgetStreamTailerOptions configures one worker's independent broadcast
// reader. Every worker gets its own instance and cursor; no consumer group or
// shared in-memory cursor is used.
type BudgetStreamTailerOptions struct {
	Port      BudgetEventPort
	Initial   BudgetStreamCursorState
	BatchSize int
	Enabled   bool
	Reload    BudgetStreamReload
	Apply     BudgetStreamApply
}

// BudgetStreamTailerResult describes one bounded poll. Cursor is the latest
// successfully observed Stream ID and must be persisted in the worker lease
// by the runtime owner. Reloaded is true when no Stream hint was applied and
// the authoritative Redis-only reload callback supplied a new checkpoint.
type BudgetStreamTailerResult struct {
	State    BudgetStreamCursorState
	Records  int
	Reloaded bool
}

// BudgetStreamTailer is a small, storage-neutral worker-side Stream loop. It
// centralizes the fail-closed behavior required by the control-plane design:
// disabled consumption and retained-stream gaps discard local hints and
// reload Redis, while malformed Stream state or callback failures stop the
// poll without advancing past unprocessed records.
type BudgetStreamTailer struct {
	port      BudgetEventPort
	state     BudgetStreamCursorState
	batchSize int
	enabled   bool
	reload    BudgetStreamReload
	apply     BudgetStreamApply
}

// NewBudgetStreamTailer validates and constructs one independent worker
// tailer. Reload is required even when the Stream is enabled because a gap or
// generation switch must have an authoritative recovery path.
func NewBudgetStreamTailer(options BudgetStreamTailerOptions) (*BudgetStreamTailer, error) {
	if options.Port == nil {
		return nil, errors.New("budget stream port is required")
	}
	if err := validateBudgetStreamCursorState(options.Initial); err != nil {
		return nil, fmt.Errorf("initial budget stream checkpoint: %w", err)
	}
	if options.Reload == nil {
		return nil, errors.New("budget stream reload callback is required")
	}
	if options.BatchSize <= 0 || options.BatchSize > 10_000 {
		return nil, errors.New("budget stream batch size must be between 1 and 10000")
	}
	return &BudgetStreamTailer{
		port: options.Port, state: options.Initial, batchSize: options.BatchSize,
		enabled: options.Enabled, reload: options.Reload, apply: options.Apply,
	}, nil
}

// State returns a copy of the current checkpoint. The caller owns lease
// persistence; this method never writes Redis or PostgreSQL.
func (tailer *BudgetStreamTailer) State() BudgetStreamCursorState {
	if tailer == nil {
		return BudgetStreamCursorState{}
	}
	return tailer.state
}

// SetEnabled changes only local Stream consumption. Disabling the tailer does
// not leave stale hints active: the next Poll performs the required
// authoritative Redis-only reload and replaces the checkpoint.
func (tailer *BudgetStreamTailer) SetEnabled(enabled bool) {
	if tailer != nil {
		tailer.enabled = enabled
	}
}

// Poll performs at most one bounded XREAD-equivalent operation. It advances
// the checkpoint after each successfully applied record, so a callback error
// leaves the failing record available for retry while preserving progress for
// earlier idempotently applied hints.
func (tailer *BudgetStreamTailer) Poll(ctx context.Context) (BudgetStreamTailerResult, error) {
	if tailer == nil {
		return BudgetStreamTailerResult{}, errors.New("budget stream tailer is nil")
	}
	if err := ctx.Err(); err != nil {
		return BudgetStreamTailerResult{State: tailer.state}, err
	}
	if !tailer.enabled {
		return tailer.reloadCheckpoint(ctx)
	}
	records, err := tailer.port.Read(ctx, tailer.state.Cursor, tailer.batchSize)
	if errors.Is(err, ErrBudgetStreamGap) {
		return tailer.reloadCheckpoint(ctx)
	}
	if err != nil {
		return BudgetStreamTailerResult{State: tailer.state}, err
	}
	result := BudgetStreamTailerResult{State: tailer.state}
	for _, record := range records {
		if err := validateBudgetStreamRecord(record, tailer.state); err != nil {
			// A generation switch is intentionally handled as a reload rather
			// than an apply: the old local plan is no longer trustworthy.
			if errors.Is(err, ErrBudgetStreamGenerationChanged) {
				return tailer.reloadCheckpoint(ctx)
			}
			return result, err
		}
		if !budgetStreamIDAdvances(tailer.state.Cursor, record.ID) {
			return result, fmt.Errorf("%w: Stream cursor did not advance", ErrBudgetStreamInvalid)
		}
		if tailer.apply != nil {
			if err := tailer.apply(ctx, record); err != nil {
				return result, fmt.Errorf("apply budget stream hint %s: %w", record.ID, err)
			}
		}
		tailer.state.Cursor = record.ID
		result.Records++
		result.State = tailer.state
	}
	return result, nil
}

func budgetStreamIDAdvances(previous, next string) bool {
	if previous == "" {
		return true
	}
	previousMajor, previousMinor, previousErr := parseRedisStreamID(previous)
	nextMajor, nextMinor, nextErr := parseRedisStreamID(next)
	if previousErr != nil || nextErr != nil {
		return false
	}
	return nextMajor > previousMajor || (nextMajor == previousMajor && nextMinor > previousMinor)
}

func (tailer *BudgetStreamTailer) reloadCheckpoint(ctx context.Context) (BudgetStreamTailerResult, error) {
	state, err := tailer.reload(ctx)
	if err != nil {
		return BudgetStreamTailerResult{State: tailer.state}, fmt.Errorf("reload budget generation: %w", err)
	}
	if err := validateBudgetStreamCursorState(state); err != nil {
		return BudgetStreamTailerResult{State: tailer.state}, fmt.Errorf("reload budget checkpoint: %w", err)
	}
	tailer.state = state
	return BudgetStreamTailerResult{State: state, Reloaded: true}, nil
}

func validateBudgetStreamCursorState(state BudgetStreamCursorState) error {
	if err := validateOpaqueID("generation_id", string(state.GenerationID)); err != nil {
		return err
	}
	if state.Cursor != "" {
		if len(state.Cursor) > MaxBudgetStreamIDBytes {
			return errors.New("cursor exceeds bounded Redis Stream ID length")
		}
		if _, _, err := parseRedisStreamID(state.Cursor); err != nil {
			return fmt.Errorf("invalid cursor: %w", err)
		}
	}
	return nil
}

func validateBudgetStreamRecord(record BudgetStreamRecord, state BudgetStreamCursorState) error {
	if len(record.ID) == 0 || len(record.ID) > MaxBudgetStreamIDBytes {
		return fmt.Errorf("%w: Stream record ID is invalid", ErrBudgetStreamInvalid)
	}
	if _, _, err := parseRedisStreamID(record.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrBudgetStreamInvalid, err)
	}
	if err := record.Event.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrBudgetStreamInvalid, err)
	}
	if record.Event.GenerationID != state.GenerationID {
		return fmt.Errorf("%w: Stream generation %q differs from checkpoint %q", ErrBudgetStreamGenerationChanged, record.Event.GenerationID, state.GenerationID)
	}
	return nil
}
