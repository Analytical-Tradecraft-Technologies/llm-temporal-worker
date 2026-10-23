package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	durable "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// ErrBudgetRecoveryBusy reports that another process holds the recovery lease.
var ErrBudgetRecoveryBusy = errors.New("Redis budget recovery is running in another process")

// ErrBudgetRecoveryConflict reports a surviving Redis operation that disagrees
// with its durable record. Recovery stops rather than overwrite either side.
var ErrBudgetRecoveryConflict = errors.New("Redis budget recovery found a conflicting operation record")

const (
	budgetRecoveryLeaseSuffix    = "budget:recovery-lease"
	budgetRecoveryProgressSuffix = "budget:recovery-progress"

	// BudgetRecoveryClockSkew bounds how far Redis TIME at acceptance may run
	// ahead of the worker clock that recorded the execution start. Only
	// records saved before acceptance expiries were recorded durably use it.
	BudgetRecoveryClockSkew = 5 * time.Minute
)

func (keys BudgetKeySpace) RecoveryLeaseKey() string {
	return keys.space.admissionPrefix() + budgetRecoveryLeaseSuffix
}

func (keys BudgetKeySpace) RecoveryProgressKey() string {
	return keys.space.admissionPrefix() + budgetRecoveryProgressSuffix
}

// BudgetRecoveryOperation is one durably recorded Redis acceptance and its
// furthest durable progress. Every field comes from the cloud execution record
// written before the corresponding Redis mutation could authorize paid work.
type BudgetRecoveryOperation struct {
	Request durable.ReserveRequest
	// Reservation is the accepted result saved before any claim.
	Reservation durable.ReserveResult
	// Claimed is true unless the durable record proves no claim applied.
	Claimed bool
	// StartedAt is the worker time the execution was saved, after acceptance.
	StartedAt time.Time
	// StartBy bounds the unclaimed lease; it is never earlier than the
	// deadline Redis set at acceptance.
	StartBy time.Time
	// Settlement is the exact event batch saved before Redis settlement.
	Settlement *durable.ReconcileRequest
}

// recoveredReservation adds the settlement projection that the Function keeps
// beside the canonical reservation. The canonical part is never changed, so
// the request fingerprint and reservation event IDs stay identical.
type recoveredReservation struct {
	durableReservation
	ReservedNano        string `json:"reserved_nano"`
	AccountedNano       string `json:"accounted_nano"`
	ReservationRevision int    `json:"reservation_revision"`
	Status              string `json:"status"`
}

type recoveredOperation struct {
	Schema        string                 `json:"schema"`
	OperationID   string                 `json:"operation_id"`
	GenerationID  string                 `json:"generation_id"`
	IncarnationID string                 `json:"incarnation_id"`
	Fingerprint   string                 `json:"fingerprint"`
	Status        string                 `json:"status"`
	OccurredAt    time.Time              `json:"occurred_at"`
	StartByMillis int64                  `json:"start_by_millis"`
	Claimed       bool                   `json:"claimed"`
	Reservations  []recoveredReservation `json:"reservations"`
	Events        map[string]string      `json:"events,omitempty"`
}

// recoveredRecord rebuilds the exact operation record Redis held, advanced to
// the durable record's progress. Acceptance expiries saved with the result are
// exact; older results use a conservative bound that keeps spend counted at
// least as long as Redis did.
func (keys BudgetKeySpace) recoveredRecord(operation BudgetRecoveryOperation) (recoveredOperation, []string, error) {
	request, result := operation.Request, operation.Reservation
	if err := result.Validate(request); err != nil || !result.Accepted || operation.StartedAt.IsZero() || operation.StartBy.IsZero() {
		return recoveredOperation{}, nil, fmt.Errorf("%w: invalid durable recovery record", ErrBudgetRecoveryConflict)
	}
	reservations, err := canonicalDurableReservations(request.OperationID, request.GenerationID, request.Reservations, request.ExpiresAt, operation.StartedAt)
	if err != nil {
		return recoveredOperation{}, nil, err
	}
	fingerprint, err := durableRequestFingerprint(request, reservations)
	if err != nil {
		return recoveredOperation{}, nil, err
	}
	events := make(map[string]budget.ReservationEvent, len(result.Events))
	for _, event := range result.Events {
		events[event.EventID] = event
	}
	settled := map[string]budget.CompletionEvent{}
	record := recoveredOperation{
		Schema: "durable-budget/v1", OperationID: string(request.OperationID), GenerationID: string(request.GenerationID),
		IncarnationID: string(result.IncarnationID), Fingerprint: fingerprint, Status: "accepted",
		StartByMillis: operation.StartBy.UnixMilli(), Claimed: operation.Claimed,
	}
	if settlement := operation.Settlement; settlement != nil {
		if settlement.OperationID != request.OperationID || settlement.GenerationID != request.GenerationID || settlement.IncarnationID != result.IncarnationID || settlement.Validate() != nil {
			return recoveredOperation{}, nil, fmt.Errorf("%w: settlement does not match its reservation", ErrBudgetRecoveryConflict)
		}
		record.Events = make(map[string]string, len(settlement.Events))
		for _, event := range settlement.Events {
			settled[event.WindowID+"\x00"+strconv.FormatInt(event.BucketStart.UTC().UnixNano(), 10)] = event
			record.Events[event.EventID] = durableEventFingerprint(event)
		}
	}
	keyList := []string{keys.space.durableBudgetOperationKey(string(request.GenerationID), string(request.OperationID))}
	for _, value := range reservations {
		reserved, ok := events[value.EventID]
		if !ok {
			return recoveredOperation{}, nil, fmt.Errorf("%w: reservation event is missing", ErrBudgetRecoveryConflict)
		}
		if record.OccurredAt.IsZero() || reserved.OccurredAt.Before(record.OccurredAt) {
			record.OccurredAt = reserved.OccurredAt
		}
		value.WindowExpiresMillis = recoveredWindowExpiry(value, result.WindowExpiries[value.EventID], operation.StartedAt)
		value.ExpiresMillis = operation.StartBy.UnixMilli()
		restored := recoveredReservation{durableReservation: value, ReservedNano: value.AmountNano, AccountedNano: "0", ReservationRevision: 1, Status: "reserved"}
		if event, ok := settled[value.WindowID+"\x00"+value.BucketStart]; ok {
			if !operation.Claimed && event.Kind != budget.JournalRelease {
				return recoveredOperation{}, nil, fmt.Errorf("%w: unclaimed settlement", ErrBudgetRecoveryConflict)
			}
			accounted, err := pricing.CeilNanoUSD(event.AccountedIncreaseUSD)
			if err != nil {
				return recoveredOperation{}, nil, err
			}
			restored.ReservedNano, restored.AccountedNano = "0", accounted.String()
			restored.ReservationRevision = event.ReservationRevision
			restored.Status = "finalized"
			if event.Kind == budget.JournalFinalizeUnknown || event.Kind == budget.JournalRetainAmbiguous {
				restored.Status = "ambiguous"
			}
			if event.Kind == budget.JournalRetainAmbiguous {
				restored.ReservedNano = value.AmountNano
			}
		} else if operation.Settlement != nil {
			return recoveredOperation{}, nil, fmt.Errorf("%w: settlement does not cover every reservation", ErrBudgetRecoveryConflict)
		}
		record.Reservations = append(record.Reservations, restored)
		keyList = append(keyList, keys.space.durableBudgetKey(value.PolicyID, value.WindowID), keys.space.durableBudgetExpiryKey(value.PolicyID, value.WindowID))
	}
	return record, keyList, nil
}

// recoveredWindowExpiry mirrors durable_reserve: spend counts until the end of
// the acceptance bucket plus the window. Without a recorded expiry, acceptance
// is bounded by the execution start plus the permitted clock skew.
func recoveredWindowExpiry(value durableReservation, recorded time.Time, startedAt time.Time) int64 {
	floor := value.WindowExpiresMillis
	if !recorded.IsZero() {
		if recorded.UnixMilli() > floor {
			return recorded.UnixMilli()
		}
		return floor
	}
	bucket, errBucket := strconv.ParseInt(value.BucketNanos, 10, 64)
	duration, errDuration := strconv.ParseInt(value.DurationNanos, 10, 64)
	if errBucket != nil || errDuration != nil || bucket <= 0 {
		return floor
	}
	bucketMillis, durationMillis := bucket/int64(time.Millisecond), duration/int64(time.Millisecond)
	if bucketMillis <= 0 {
		return floor
	}
	accepted := startedAt.Add(BudgetRecoveryClockSkew).UnixMilli()
	bound := (accepted/bucketMillis+1)*bucketMillis + durationMillis
	if bound > floor {
		return bound
	}
	return floor
}

func (initializer *RedisBudgetInitializer) recoveryRun(ctx context.Context, keys []string, args ...string) (string, string, error) {
	if ctx == nil {
		return "", "", ErrBudgetAuthorityUnavailable
	}
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	result, err := initializer.invoke.Run(ctx, initializer.version, keys, args...)
	if err != nil {
		return "", "", err
	}
	return durableFunctionRecord(result)
}

func (initializer *RedisBudgetInitializer) validReceipt(value budget.Initialization) error {
	if value.Validate() != nil || !value.Ready || value.Identity != initializer.Identity() {
		return ErrBudgetAuthorityUnavailable
	}
	return nil
}

// BeginRecovery fences budget mutations behind a persistent recovering marker.
// Without force it acts only on a lost or interrupted recovery marker; with
// force it also fences a ready marker, for suspected partial loss or failover.
// It reports whether recovery is required (false: the authority is ready).
func (initializer *RedisBudgetInitializer) BeginRecovery(ctx context.Context, value budget.Initialization, force bool) (bool, error) {
	if err := initializer.validReceipt(value); err != nil {
		return false, err
	}
	mode := "missing"
	if force {
		mode = "force"
	}
	status, _, err := initializer.recoveryRun(ctx, []string{initializer.keys.AuthorityKey(), initializer.keys.EventsKey(), initializer.keys.RecoveryProgressKey()},
		"durable_authority_recover_begin", value.Marker(), mode)
	if err != nil {
		return false, err
	}
	switch status {
	case "recovering":
		return true, nil
	case "ready":
		return false, nil
	default:
		return false, mapDurableStatus(status)
	}
}

// HoldRecoveryLease acquires or renews the disposable recovery lease and
// optionally stores progress. It returns the stored progress for resumption.
// ErrBudgetRecoveryBusy means another live process holds the lease; a nil
// error with ready true means recovery already completed elsewhere.
func (initializer *RedisBudgetInitializer) HoldRecoveryLease(ctx context.Context, value budget.Initialization, token string, ttl time.Duration, progress string) (string, bool, error) {
	return initializer.recoveryLease(ctx, value, token, ttl, "hold", progress)
}

// ReleaseRecoveryLease lets a waiting process take over immediately.
func (initializer *RedisBudgetInitializer) ReleaseRecoveryLease(ctx context.Context, value budget.Initialization, token string) error {
	_, _, err := initializer.recoveryLease(ctx, value, token, time.Second, "release", "")
	return err
}

func (initializer *RedisBudgetInitializer) recoveryLease(ctx context.Context, value budget.Initialization, token string, ttl time.Duration, action, progress string) (string, bool, error) {
	if err := initializer.validReceipt(value); err != nil {
		return "", false, err
	}
	status, stored, err := initializer.recoveryRun(ctx, []string{initializer.keys.AuthorityKey(), initializer.keys.RecoveryLeaseKey(), initializer.keys.RecoveryProgressKey()},
		"durable_recovery_lease", value.Marker(), token, strconv.FormatInt(ttl.Milliseconds(), 10), action, progress)
	if err != nil {
		return "", false, err
	}
	switch status {
	case "held", "released":
		return stored, false, nil
	case "ready":
		return "", true, nil
	case "busy":
		return "", false, ErrBudgetRecoveryBusy
	default:
		return "", false, mapDurableStatus(status)
	}
}

// RecoverOperation merges one durable record into Redis atomically. It is
// idempotent: a surviving or already restored record at the same or later
// progress is left unchanged, and only the difference is applied to balances.
func (initializer *RedisBudgetInitializer) RecoverOperation(ctx context.Context, value budget.Initialization, token string, operation BudgetRecoveryOperation) (bool, error) {
	if err := initializer.validReceipt(value); err != nil {
		return false, err
	}
	record, keys, err := initializer.keys.recoveredRecord(operation)
	if err != nil {
		return false, err
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return false, err
	}
	keys = append([]string{initializer.keys.AuthorityKey(), initializer.keys.RecoveryLeaseKey()}, keys...)
	status, _, err := initializer.recoveryRun(ctx, keys, "durable_recover_operation", value.Marker(), token, string(payload))
	if err != nil {
		return false, err
	}
	switch status {
	case "restored":
		return true, nil
	case "unchanged":
		return false, nil
	case "busy":
		return false, ErrBudgetRecoveryBusy
	case "conflict":
		return false, ErrBudgetRecoveryConflict
	case "ready":
		return false, ErrBudgetAuthorityUnavailable
	default:
		return false, mapDurableStatus(status)
	}
}

// CommitRecovery publishes readiness only after every durable record merged.
// It is idempotent: a ready marker for the same receipt reports success.
func (initializer *RedisBudgetInitializer) CommitRecovery(ctx context.Context, value budget.Initialization, now time.Time) error {
	if err := initializer.validReceipt(value); err != nil {
		return err
	}
	event, err := (BudgetStreamEvent{Schema: budgetStreamEventSchema, Kind: BudgetEventInitialize, GenerationID: "redis-budget-v1", OccurredAt: now.UTC()}).Marshal()
	if err != nil {
		return err
	}
	status, _, err := initializer.recoveryRun(ctx, []string{initializer.keys.AuthorityKey(), initializer.keys.EventsKey(), initializer.keys.RecoveryProgressKey()},
		"durable_authority_recover_commit", value.Marker(), string(event))
	if err != nil {
		return err
	}
	if status != "ready" {
		return mapDurableStatus(status)
	}
	return nil
}
