package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	durable "github.com/mfow/llm-temporal-worker/golang/storage/durable"
	redisclient "github.com/redis/go-redis/v9"
)

var (
	ErrRedisBudgetConflict             = errors.New("Redis durable budget idempotency conflict")
	ErrRedisBudgetGenerationMismatch   = errors.New("Redis durable budget generation mismatch")
	ErrRedisBudgetIncarnationMismatch  = errors.New("Redis durable budget incarnation mismatch")
	ErrRedisBudgetReservationNotFound  = errors.New("Redis durable budget reservation is not known")
	ErrRedisBudgetReservationFinalized = errors.New("Redis durable budget reservation is already finalized")
)

// RedisBudgetMaterializer is the production Redis implementation of the
// durable BudgetMaterializer port. It uses the existing versioned admission
// Function/Lua execution seam, but a separate key family and nano-USD wire
// representation. Legacy admission records and micro-USD buckets are never
// mixed with these records.
//
// A materializer instance is bound to one immutable generation/incarnation.
// Requests from another snapshot fail closed before invoking Redis.
type RedisBudgetMaterializer struct {
	space       keySpace
	invoke      FunctionInvoker
	reader      StringReader
	function    string
	generation  durable.GenerationID
	incarnation durable.IncarnationID
	clock       func() time.Time
}

// RedisBudgetMaterializerOptions configures one snapshot-owned materializer.
// Client is used only when Invoker is omitted; production callers should use
// the same Function/Lua mode and version as AdmissionStore.
type RedisBudgetMaterializerOptions struct {
	Client          redisclient.Scripter
	Invoker         FunctionInvoker
	Reader          StringReader
	Keys            KeyOptions
	Mode            AdmissionMode
	FunctionVersion string
	GenerationID    durable.GenerationID
	IncarnationID   durable.IncarnationID
	Clock           func() time.Time
}

func NewRedisBudgetMaterializer(options RedisBudgetMaterializerOptions) (*RedisBudgetMaterializer, error) {
	space, err := newKeySpace(options.Keys)
	if err != nil {
		return nil, err
	}
	if err := options.GenerationID.Validate(); err != nil {
		return nil, fmt.Errorf("generation: %w", err)
	}
	if err := options.IncarnationID.Validate(); err != nil {
		return nil, fmt.Errorf("incarnation: %w", err)
	}
	function := options.FunctionVersion
	if function == "" {
		function = AdmissionFunctionVersion
	}
	invoke := options.Invoker
	if invoke == nil {
		if options.Client == nil {
			return nil, errors.New("Redis durable budget Function client is required")
		}
		invoke = redisInvoker{client: options.Client, mode: options.Mode, version: function}
	}
	reader := options.Reader
	if reader == nil && options.Client != nil {
		client, ok := options.Client.(interface {
			Get(context.Context, string) *redisclient.StringCmd
		})
		if ok {
			reader = redisReader{client: client}
		}
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	return &RedisBudgetMaterializer{
		space: space, invoke: invoke, reader: reader, function: function,
		generation: options.GenerationID, incarnation: options.IncarnationID,
		clock: options.Clock,
	}, nil
}

var _ durable.BudgetMaterializer = (*RedisBudgetMaterializer)(nil)

type durableReservation struct {
	PolicyID      string `json:"policy_id"`
	WindowID      string `json:"window_id"`
	Bucket        string `json:"bucket"`
	AmountNano    string `json:"amount_nano"`
	LimitNano     string `json:"limit_nano"`
	BucketNanos   string `json:"bucket_nanos"`
	DurationNanos string `json:"duration_nanos"`
	BucketStart   string `json:"bucket_start_nanos"`
	ExpiresMillis int64  `json:"expires_millis"`
	EventID       string `json:"event_id"`
	AmountUSD     string `json:"amount_usd"`
	LimitUSD      string `json:"limit_usd"`
}

type durableOperation struct {
	Schema        string               `json:"schema"`
	OperationID   string               `json:"operation_id"`
	GenerationID  string               `json:"generation_id"`
	IncarnationID string               `json:"incarnation_id"`
	Fingerprint   string               `json:"fingerprint"`
	Status        string               `json:"status"`
	OccurredAt    time.Time            `json:"occurred_at"`
	ExpiresAt     time.Time            `json:"expires_at,omitempty"`
	Reservations  []durableReservation `json:"reservations"`
	Events        map[string]string    `json:"events,omitempty"`
	Denial        *durableDenial       `json:"denial,omitempty"`
}

type durableDenial struct {
	PolicyID      string `json:"policy_id"`
	WindowID      string `json:"window_id"`
	LimitNano     string `json:"limit_nano"`
	ActiveNano    string `json:"active_nano"`
	RequestedNano string `json:"requested_nano"`
}

type durableEvent struct {
	EventID               string `json:"event_id"`
	WindowID              string `json:"window_id"`
	BucketStart           string `json:"bucket_start_nanos"`
	ReservationRevision   int    `json:"reservation_revision"`
	Kind                  string `json:"kind"`
	ReservedDecreaseNano  string `json:"reserved_decrease_nano"`
	AccountedIncreaseNano string `json:"accounted_increase_nano"`
	AccountedDecreaseNano string `json:"accounted_decrease_nano"`
	Fingerprint           string `json:"fingerprint"`
}

func (m *RedisBudgetMaterializer) Accept(ctx context.Context, request durable.ReserveRequest) (durable.ReserveResult, error) {
	if m == nil {
		return durable.ReserveResult{}, errors.New("Redis durable budget materializer is nil")
	}
	if ctx == nil {
		return durable.ReserveResult{}, errors.New("Redis durable budget materializer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return durable.ReserveResult{}, err
	}
	if err := request.OperationID.Validate(); err != nil {
		return durable.ReserveResult{}, err
	}
	if request.GenerationID != m.generation {
		return durable.ReserveResult{}, ErrRedisBudgetGenerationMismatch
	}
	if len(request.Reservations) == 0 {
		return durable.ReserveResult{}, errors.New("Redis durable budget reservation list must not be empty")
	}
	reservations, err := canonicalDurableReservations(request.OperationID, request.GenerationID, request.Reservations, request.ExpiresAt, m.clock())
	if err != nil {
		return durable.ReserveResult{}, err
	}
	fingerprint, err := durableRequestFingerprint(request, reservations)
	if err != nil {
		return durable.ReserveResult{}, err
	}
	operation := string(request.OperationID)
	keys := []string{m.space.durableBudgetOperationKey(string(request.GenerationID), operation)}
	for _, reservation := range reservations {
		keys = append(keys,
			m.space.durableBudgetKey(reservation.PolicyID, reservation.WindowID),
			m.space.durableBudgetExpiryKey(reservation.PolicyID, reservation.WindowID))
	}
	ttl := durableTTLSeconds(request.ExpiresAt, m.clock(), reservations)
	if ttl <= 0 {
		return durable.ReserveResult{}, errors.New("Redis durable budget reservation expiry must be in the future")
	}
	wire, err := json.Marshal(reservations)
	if err != nil {
		return durable.ReserveResult{}, fmt.Errorf("marshal Redis durable budget reservations: %w", err)
	}
	result, err := m.invoke.Run(ctx, m.function, keys,
		"durable_reserve", string(request.GenerationID), string(m.incarnation), operation,
		fingerprint, strconv.FormatInt(ttl, 10), m.clock().UTC().Format(time.RFC3339Nano), string(wire))
	if err != nil {
		return durable.ReserveResult{}, resolveMutationError(ctx, err)
	}
	status, recordData, err := durableFunctionRecord(result)
	if err != nil {
		return durable.ReserveResult{}, err
	}
	if status == "conflict" {
		return durable.ReserveResult{}, ErrRedisBudgetConflict
	}
	if status != "created" && status != "existing" {
		return durable.ReserveResult{}, mapDurableStatus(status)
	}
	var record durableOperation
	if err := json.Unmarshal([]byte(recordData), &record); err != nil {
		return durable.ReserveResult{}, fmt.Errorf("decode Redis durable budget operation: %w", err)
	}
	if record.GenerationID != string(m.generation) {
		return durable.ReserveResult{}, ErrRedisBudgetGenerationMismatch
	}
	if record.IncarnationID != string(m.incarnation) {
		return durable.ReserveResult{}, ErrRedisBudgetIncarnationMismatch
	}
	if record.Status == "denied" {
		return durableDeniedResult(request, record)
	}
	if record.Status != "accepted" {
		return durable.ReserveResult{}, fmt.Errorf("invalid Redis durable budget operation status %q", record.Status)
	}
	reserve := durable.ReserveResult{
		OperationID: request.OperationID, Accepted: true,
		GenerationID: request.GenerationID, IncarnationID: m.incarnation,
		Events: make([]budget.ReservationEvent, 0, len(record.Reservations)),
	}
	for _, value := range record.Reservations {
		amount, err := pricing.ParseUSD(value.AmountUSD)
		if err != nil {
			return durable.ReserveResult{}, fmt.Errorf("decode reservation amount: %w", err)
		}
		bucketStart, err := parseInt64String(value.BucketStart)
		if err != nil {
			return durable.ReserveResult{}, err
		}
		event := budget.ReservationEvent{
			EventID: value.EventID, GenerationID: string(request.GenerationID),
			OperationID: string(request.OperationID), WindowID: value.WindowID,
			BucketStart: time.Unix(0, bucketStart).UTC(), ReservationRevision: 1,
			AmountUSD: amount, OccurredAt: record.OccurredAt,
		}
		if err := event.Validate(); err != nil {
			return durable.ReserveResult{}, fmt.Errorf("Redis reservation event: %w", err)
		}
		reserve.Events = append(reserve.Events, event)
	}
	if err := reserve.Validate(request); err != nil {
		return durable.ReserveResult{}, err
	}
	return reserve, nil
}

func (m *RedisBudgetMaterializer) Reconcile(ctx context.Context, request durable.ReconcileRequest) error {
	if m == nil {
		return errors.New("Redis durable budget materializer is nil")
	}
	if ctx == nil {
		return errors.New("Redis durable budget materializer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return err
	}
	if request.GenerationID != m.generation {
		return ErrRedisBudgetGenerationMismatch
	}
	if request.IncarnationID != m.incarnation {
		return ErrRedisBudgetIncarnationMismatch
	}
	operationKey := m.space.durableBudgetOperationKey(string(request.GenerationID), string(request.OperationID))
	if m.reader == nil {
		return errors.New("Redis durable budget record reader is required for reconciliation")
	}
	raw, err := m.reader.Get(ctx, operationKey)
	if errors.Is(err, redisclient.Nil) {
		return ErrRedisBudgetReservationNotFound
	}
	if err != nil {
		return resolveMutationError(ctx, err)
	}
	var record durableOperation
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return fmt.Errorf("decode Redis durable budget operation: %w", err)
	}
	if record.GenerationID != string(request.GenerationID) {
		return ErrRedisBudgetGenerationMismatch
	}
	if record.IncarnationID != string(request.IncarnationID) {
		return ErrRedisBudgetIncarnationMismatch
	}
	if record.Status != "accepted" {
		return ErrRedisBudgetReservationNotFound
	}
	keys := []string{operationKey}
	for _, reservation := range record.Reservations {
		keys = append(keys,
			m.space.durableBudgetKey(reservation.PolicyID, reservation.WindowID),
			m.space.durableBudgetExpiryKey(reservation.PolicyID, reservation.WindowID))
	}
	events := make([]durableEvent, 0, len(request.Events))
	for _, event := range request.Events {
		reservedDecrease, err := pricing.CeilNanoUSD(event.ReservedDecreaseUSD)
		if err != nil {
			return fmt.Errorf("reserved decrease materialization: %w", err)
		}
		accountedIncrease, err := pricing.CeilNanoUSD(event.AccountedIncreaseUSD)
		if err != nil {
			return fmt.Errorf("accounted increase materialization: %w", err)
		}
		accountedDecrease, err := pricing.CeilNanoUSD(event.AccountedDecreaseUSD)
		if err != nil {
			return fmt.Errorf("accounted decrease materialization: %w", err)
		}
		bucketStart := event.BucketStart.UTC().UnixNano()
		wire := durableEvent{
			EventID: event.EventID, WindowID: event.WindowID,
			BucketStart:         strconv.FormatInt(bucketStart, 10),
			ReservationRevision: event.ReservationRevision, Kind: string(event.Kind),
			ReservedDecreaseNano:  reservedDecrease.String(),
			AccountedIncreaseNano: accountedIncrease.String(),
			AccountedDecreaseNano: accountedDecrease.String(),
			Fingerprint:           durableEventFingerprint(event),
		}
		events = append(events, wire)
	}
	payload, err := json.Marshal(events)
	if err != nil {
		return fmt.Errorf("marshal Redis durable budget events: %w", err)
	}
	result, err := m.invoke.Run(ctx, m.function, keys,
		"durable_reconcile", string(request.GenerationID), string(request.IncarnationID), string(request.OperationID), string(payload))
	if err != nil {
		return resolveMutationError(ctx, err)
	}
	status, _, err := durableFunctionRecord(result)
	if err != nil {
		return err
	}
	switch status {
	case "ok", "existing":
		return nil
	case "conflict":
		return ErrRedisBudgetConflict
	case "generation_mismatch":
		return ErrRedisBudgetGenerationMismatch
	case "incarnation_mismatch":
		return ErrRedisBudgetIncarnationMismatch
	case "not_found":
		return ErrRedisBudgetReservationNotFound
	case "finalized":
		return ErrRedisBudgetReservationFinalized
	default:
		return mapDurableStatus(status)
	}
}

func canonicalDurableReservations(operation durable.OperationID, generation durable.GenerationID, values []admission.WindowReservation, expiresAt, now time.Time) ([]durableReservation, error) {
	result := make([]durableReservation, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	seenWindowBucket := make(map[string]struct{}, len(values))
	for index, value := range values {
		if value.PolicyID == "" || value.WindowID == "" || len(value.PolicyID) > 128 || len(value.WindowID) > 128 {
			return nil, fmt.Errorf("reservation %d has unsafe policy/window identity", index)
		}
		if value.Bucket < 0 || value.BucketNanos <= 0 || value.DurationNanos < value.BucketNanos {
			return nil, fmt.Errorf("reservation %d has invalid bucket bounds", index)
		}
		if value.Bucket > (1<<62)/value.BucketNanos {
			return nil, fmt.Errorf("reservation %d bucket overflows timestamp", index)
		}
		amount, err := durableUSD(value.AmountUSD, value.Amount)
		if err != nil || amount.IsZero() {
			if err == nil {
				err = errors.New("amount must be positive")
			}
			return nil, fmt.Errorf("reservation %d amount: %w", index, err)
		}
		limit, err := durableUSD(value.LimitUSD, value.Limit)
		if err != nil || limit.IsZero() || amount.Cmp(limit) > 0 {
			if err == nil {
				err = errors.New("limit must be positive and no less than amount")
			}
			return nil, fmt.Errorf("reservation %d limit: %w", index, err)
		}
		amountNano, err := pricing.CeilNanoUSD(amount)
		if err != nil {
			return nil, fmt.Errorf("reservation %d amount materialization: %w", index, err)
		}
		limitNano, err := pricing.FloorNanoUSD(limit)
		if err != nil || amountNano > limitNano {
			if err == nil {
				err = errors.New("conservative materialization makes amount exceed limit")
			}
			return nil, fmt.Errorf("reservation %d limit materialization: %w", index, err)
		}
		bucketStart := value.Bucket * value.BucketNanos
		windowExpiry := time.Unix(0, bucketStart).Add(time.Duration(value.DurationNanos)).UTC()
		reservationExpiry := windowExpiry
		if !expiresAt.IsZero() && expiresAt.Before(reservationExpiry) {
			reservationExpiry = expiresAt.UTC()
		}
		if !reservationExpiry.After(now.UTC()) {
			return nil, fmt.Errorf("reservation %d expiry must be in the future", index)
		}
		bucketKey := value.WindowID + "\x00" + strconv.FormatInt(bucketStart, 10)
		if _, ok := seenWindowBucket[bucketKey]; ok {
			return nil, fmt.Errorf("reservation %d duplicates window/bucket start", index)
		}
		seenWindowBucket[bucketKey] = struct{}{}
		identity := value.PolicyID + "\x00" + value.WindowID + "\x00" + strconv.FormatInt(value.Bucket, 10)
		if _, ok := seen[identity]; ok {
			return nil, fmt.Errorf("reservation %d duplicates policy/window/bucket", index)
		}
		seen[identity] = struct{}{}
		result = append(result, durableReservation{
			PolicyID: value.PolicyID, WindowID: value.WindowID,
			Bucket: strconv.FormatInt(value.Bucket, 10), AmountNano: amountNano.String(), LimitNano: limitNano.String(),
			BucketNanos: strconv.FormatInt(value.BucketNanos, 10), DurationNanos: strconv.FormatInt(value.DurationNanos, 10),
			BucketStart: strconv.FormatInt(bucketStart, 10), ExpiresMillis: reservationExpiry.UnixMilli(),
			EventID:   durableReservationEventID(string(operation), string(generation), value.PolicyID, value.WindowID, bucketStart),
			AmountUSD: amount.String(), LimitUSD: limit.String(),
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].PolicyID != result[j].PolicyID {
			return result[i].PolicyID < result[j].PolicyID
		}
		if result[i].WindowID != result[j].WindowID {
			return result[i].WindowID < result[j].WindowID
		}
		return result[i].Bucket < result[j].Bucket
	})
	return result, nil
}

func durableUSD(exact pricing.USD, legacy pricing.MicroUSD) (pricing.USD, error) {
	if !exact.IsZero() {
		return exact, exact.Validate()
	}
	return pricing.USDFromMicro(legacy)
}

func durableTTLSeconds(expiresAt, now time.Time, reservations []durableReservation) int64 {
	if !expiresAt.IsZero() {
		delta := expiresAt.Sub(now)
		if delta <= 0 {
			return 0
		}
		return int64((delta + time.Second - 1) / time.Second)
	}
	max := int64(1)
	for _, reservation := range reservations {
		seconds := (reservation.ExpiresMillis - now.UnixMilli() + 999) / 1000
		if seconds > max {
			max = seconds
		}
	}
	return max
}

func durableRequestFingerprint(request durable.ReserveRequest, reservations []durableReservation) (string, error) {
	wire := struct {
		OperationID  string               `json:"operation_id"`
		GenerationID string               `json:"generation_id"`
		ExpiresAt    time.Time            `json:"expires_at"`
		Reservations []durableReservation `json:"reservations"`
	}{string(request.OperationID), string(request.GenerationID), request.ExpiresAt.UTC(), reservations}
	data, err := json.Marshal(wire)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func durableEventFingerprint(event budget.CompletionEvent) string {
	data, _ := json.Marshal(event)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func durableReservationEventID(operation, generation, policy, window string, bucket int64) string {
	digest := sha256.Sum256([]byte(operation + "\x00" + generation + "\x00" + policy + "\x00" + window + "\x00" + strconv.FormatInt(bucket, 10) + "\x00revision-1"))
	return hex.EncodeToString(digest[:])
}

func durableDeniedResult(request durable.ReserveRequest, record durableOperation) (durable.ReserveResult, error) {
	result := durable.ReserveResult{OperationID: request.OperationID, GenerationID: request.GenerationID, Accepted: false}
	if record.Denial != nil {
		limit, err := parseNanoUSD(record.Denial.LimitNano)
		if err != nil {
			return durable.ReserveResult{}, err
		}
		active, err := parseNanoUSD(record.Denial.ActiveNano)
		if err != nil {
			return durable.ReserveResult{}, err
		}
		requested, err := parseNanoUSD(record.Denial.RequestedNano)
		if err != nil {
			return durable.ReserveResult{}, err
		}
		limitUSD, err := pricing.USDFromNano(limit)
		if err != nil {
			return durable.ReserveResult{}, err
		}
		activeUSD, err := pricing.USDFromNano(active)
		if err != nil {
			return durable.ReserveResult{}, err
		}
		requestedUSD, err := pricing.USDFromNano(requested)
		if err != nil {
			return durable.ReserveResult{}, err
		}
		limitMicro, err := pricing.MicroFromUSD(limitUSD)
		if err != nil {
			return durable.ReserveResult{}, err
		}
		activeMicro, err := pricing.MicroFromUSD(activeUSD)
		if err != nil {
			return durable.ReserveResult{}, err
		}
		requestedMicro, err := pricing.MicroFromUSD(requestedUSD)
		if err != nil {
			return durable.ReserveResult{}, err
		}
		result.Denial = &admission.Denial{PolicyID: record.Denial.PolicyID, WindowID: record.Denial.WindowID, Limit: limitMicro, Active: activeMicro, Requested: requestedMicro, LimitUSD: limitUSD, ActiveUSD: activeUSD, RequestedUSD: requestedUSD}
	}
	return result, nil
}

func parseNanoUSD(value string) (pricing.NanoUSD, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || !pricing.NanoUSD(parsed).Valid() {
		return 0, fmt.Errorf("invalid Redis nano-USD value %q", value)
	}
	return pricing.NanoUSD(parsed), nil
}

func parseInt64String(value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid Redis durable integer %q", value)
	}
	return parsed, nil
}

func durableFunctionRecord(result []any) (string, string, error) {
	if len(result) < 1 {
		return "", "", errors.New("Redis durable budget Function returned an empty result")
	}
	status, ok := result[0].(string)
	if !ok {
		return "", "", errors.New("Redis durable budget Function returned an invalid status")
	}
	if len(result) == 1 {
		return status, "", nil
	}
	record, ok := result[1].(string)
	if !ok {
		return "", "", errors.New("Redis durable budget Function returned an invalid record")
	}
	return status, record, nil
}

func mapDurableStatus(status string) error {
	switch status {
	case "generation_mismatch":
		return ErrRedisBudgetGenerationMismatch
	case "incarnation_mismatch":
		return ErrRedisBudgetIncarnationMismatch
	case "not_found":
		return ErrRedisBudgetReservationNotFound
	case "finalized":
		return ErrRedisBudgetReservationFinalized
	case "state_unavailable":
		return ErrUnavailable
	default:
		return fmt.Errorf("Redis durable budget Function returned status %q", status)
	}
}
