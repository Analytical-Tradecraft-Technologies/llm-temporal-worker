package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/redis/go-redis/v9"
)

var (
	// ErrUnavailable is returned when Redis contains malformed state or a
	// Function cannot safely prove whether a mutation committed.
	ErrUnavailable = errors.New("shared Redis state unavailable")
)

// AdmissionOptions configures the durable, cross-replica admission store.
// Client is a go-redis Scripter plus Get method (redis.Client and
// redis.ClusterClient both satisfy it). Invoker and Reader are injectable
// seams for offline command/function harnesses.
type AdmissionOptions struct {
	Client          redis.Scripter
	Reader          StringReader
	Invoker         FunctionInvoker
	Mode            AdmissionMode
	FunctionVersion string
	Keys            KeyOptions
	Clock           func() time.Time
	MaxRecordBytes  int
}

type AdmissionStore struct {
	space          keySpace
	clock          func() time.Time
	invoke         FunctionInvoker
	function       string
	reader         StringReader
	maxRecordBytes int
}

var _ admission.AdmissionStore = (*AdmissionStore)(nil)

// NewAdmissionStore constructs a Redis-backed AdmissionStore. The Function
// script is immutable and versioned; all mutations execute atomically in one
// Redis command. No client retry is performed after a transport error.
func NewAdmissionStore(options AdmissionOptions) (*AdmissionStore, error) {
	space, err := newKeySpace(options.Keys)
	if err != nil {
		return nil, err
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.MaxRecordBytes <= 0 {
		options.MaxRecordBytes = 256 << 10
	}
	invoke := options.Invoker
	function := options.FunctionVersion
	if function == "" {
		function = AdmissionFunctionVersion
	}
	if invoke == nil {
		client, ok := options.Client.(redis.Scripter)
		if !ok || client == nil {
			return nil, fmt.Errorf("Redis Function client is required")
		}
		switch options.Mode {
		case AdmissionModeFunction, AdmissionModeLua:
		default:
			return nil, fmt.Errorf("Redis admission mode must be function or lua")
		}
		invoke = redisInvoker{client: client, mode: options.Mode, version: function}
	}
	reader := options.Reader
	if reader == nil {
		client, ok := options.Client.(interface {
			Get(context.Context, string) *redis.StringCmd
		})
		if !ok || client == nil {
			return nil, fmt.Errorf("Redis record reader is required")
		}
		reader = redisReader{client: client}
	}
	return &AdmissionStore{space: space, clock: options.Clock, invoke: invoke, function: function, reader: reader, maxRecordBytes: options.MaxRecordBytes}, nil
}

func (store *AdmissionStore) Begin(ctx context.Context, request admission.BeginRequest) (admission.BeginResult, error) {
	if err := ctx.Err(); err != nil {
		return admission.BeginResult{}, err
	}
	if err := validateBegin(request); err != nil {
		return admission.BeginResult{}, err
	}
	operation := admission.Operation{ID: request.ID, ScopeKey: request.ScopeKey, RequestDigest: request.RequestDigest, State: admission.StateReserved, ReservedMicroUSD: request.Reservation, Reservations: cloneReservations(request.Reservations), ConfigVersion: request.ConfigVersion, PriceVersion: request.PriceVersion, DispatchToken: request.ID, LeaseUntil: request.LeaseUntil, ExpiresAt: request.ExpiresAt}
	payload, err := encodeOperation(operation)
	if err != nil {
		return admission.BeginResult{}, err
	}
	if err := validJSONSize(payload, store.maxRecordBytes); err != nil {
		return admission.BeginResult{}, err
	}
	ttl := ttlSeconds(store.clock(), request.ExpiresAt)
	keys := make([]string, 3+len(request.Reservations))
	keys[0] = store.space.scopeKey(request.ScopeKey)
	keys[1] = store.space.operationIndexKey(request.ID)
	keys[2] = store.space.operationKey(request.ScopeKey, request.ID)
	for index, reservation := range request.Reservations {
		keys[index+3] = store.space.budgetKey(reservation.PolicyID, reservation.WindowID)
	}
	result, err := store.invoke.Run(ctx, store.function, keys, "begin", string(payload), strconv.FormatInt(int64(ttl), 10))
	if err != nil {
		return admission.BeginResult{}, resolveMutationError(ctx, err)
	}
	status, recordData, denialData, err := parseFunctionResult(result)
	if err != nil {
		return admission.BeginResult{}, err
	}
	switch status {
	case "created", "existing":
		decoded, err := store.decodeRecord(recordData)
		if err != nil {
			return admission.BeginResult{}, err
		}
		return admission.BeginResult{Operation: decoded, Existing: status == "existing"}, nil
	case "denied":
		denial, err := decodeDenial(denialData)
		if err != nil {
			return admission.BeginResult{}, err
		}
		return admission.BeginResult{Denied: denial}, nil
	case "conflict":
		return admission.BeginResult{}, admission.ErrOperationConflict
	default:
		return admission.BeginResult{}, mapStatus(status)
	}
}

func (store *AdmissionStore) MarkDispatching(ctx context.Context, request admission.DispatchRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if request.OperationID == "" || request.DispatchToken == "" {
		return fmt.Errorf("invalid dispatch request")
	}
	if request.Attempt.AttemptNumber >= maxAttemptNumber {
		return fmt.Errorf("attempt number exceeds dispatch increment limit")
	}
	attempt, err := encodeAttempt(request.Attempt)
	if err != nil {
		return err
	}
	operationKey, err := store.operationKeyForID(ctx, request.OperationID)
	if err != nil {
		return err
	}
	keys := []string{store.space.operationIndexKey(request.OperationID), operationKey}
	result, err := store.invoke.Run(ctx, store.function, keys, "mark_dispatching", request.DispatchToken, string(attempt), request.LeaseUntil.Format(time.RFC3339Nano), "0")
	if err != nil {
		return resolveMutationError(ctx, err)
	}
	status, _, _, err := parseFunctionResult(result)
	if err != nil {
		return err
	}
	return mapStatus(status)
}

func (store *AdmissionStore) Continue(ctx context.Context, request admission.ContinueRequest) (admission.ContinueResult, error) {
	if err := ctx.Err(); err != nil {
		return admission.ContinueResult{}, err
	}
	if request.OperationID == "" || request.DispatchToken == "" || request.Remaining < 0 || !request.Remaining.Valid() {
		return admission.ContinueResult{}, fmt.Errorf("invalid continuation admission request")
	}
	if err := admission.ValidateOutcome(request.Outcome); err != nil {
		return admission.ContinueResult{}, err
	}
	if err := validateReservations(request.Reservations); err != nil {
		return admission.ContinueResult{}, err
	}
	outcome, err := encodeOutcome(request.Outcome)
	if err != nil {
		return admission.ContinueResult{}, err
	}
	reservations, err := encodeReservations(request.Reservations)
	if err != nil {
		return admission.ContinueResult{}, err
	}
	operationKey, err := store.operationKeyForID(ctx, request.OperationID)
	if err != nil {
		return admission.ContinueResult{}, err
	}
	// We need the old reservation vector to lay out the budget keys. Reading
	// first is safe: the Function rechecks the token and operation atomically.
	operation, err := store.Get(ctx, request.OperationID)
	if err != nil {
		return admission.ContinueResult{}, err
	}
	if err := validateContinuationReservationEnvelope(len(operation.Reservations), len(request.Reservations)); err != nil {
		return admission.ContinueResult{}, err
	}
	keys := make([]string, 2+len(operation.Reservations)+len(request.Reservations))
	keys[0] = store.space.operationIndexKey(request.OperationID)
	keys[1] = operationKey
	for index, reservation := range operation.Reservations {
		keys[index+2] = store.space.budgetKey(reservation.PolicyID, reservation.WindowID)
	}
	base := 2 + len(operation.Reservations)
	for index, reservation := range request.Reservations {
		keys[base+index] = store.space.budgetKey(reservation.PolicyID, reservation.WindowID)
	}
	ttl := ttlSeconds(store.clock(), request.ExpiresAt)
	result, err := store.invoke.Run(ctx, store.function, keys, "continue", request.DispatchToken, string(outcome), strconv.FormatInt(int64(request.Remaining), 10), string(reservations), request.LeaseUntil.Format(time.RFC3339Nano), request.ExpiresAt.Format(time.RFC3339Nano), strconv.FormatInt(int64(ttl), 10))
	if err != nil {
		return admission.ContinueResult{}, resolveMutationError(ctx, err)
	}
	status, recordData, denialData, err := parseFunctionResult(result)
	if err != nil {
		return admission.ContinueResult{}, err
	}
	if status == "denied" {
		operation, err := store.decodeRecord(recordData)
		if err != nil {
			return admission.ContinueResult{}, err
		}
		denial, err := decodeDenial(denialData)
		if err != nil {
			return admission.ContinueResult{}, err
		}
		return admission.ContinueResult{Operation: operation, Denied: denial}, nil
	}
	if status != "ok" {
		return admission.ContinueResult{}, mapStatus(status)
	}
	operation, err = store.decodeRecord(recordData)
	if err != nil {
		return admission.ContinueResult{}, err
	}
	return admission.ContinueResult{Operation: operation}, nil
}

func validateContinuationReservationEnvelope(existing, next int) error {
	if existing < 0 || next < 0 || existing > maxRedisReservations || next > maxRedisReservations || existing+next+2 > maxRedisFunctionKeys {
		return fmt.Errorf("continuation reservation envelope exceeds Redis key limit")
	}
	return nil
}

func (store *AdmissionStore) Complete(ctx context.Context, request admission.CompleteRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if request.OperationID == "" || request.DispatchToken == "" || request.Actual < 0 || !request.Actual.Valid() {
		return fmt.Errorf("invalid complete request")
	}
	attempt, err := encodeAttempt(request.Attempt)
	if err != nil {
		return err
	}
	resultRef, err := encodeBlobRef(request.ResultRef)
	if err != nil {
		return err
	}
	operationKey, err := store.operationKeyForID(ctx, request.OperationID)
	if err != nil {
		return err
	}
	operation, err := store.Get(ctx, request.OperationID)
	if err != nil {
		return err
	}
	keys := make([]string, 2+len(operation.Reservations))
	keys[0] = store.space.operationIndexKey(request.OperationID)
	keys[1] = operationKey
	for index, reservation := range operation.Reservations {
		keys[index+2] = store.space.budgetKey(reservation.PolicyID, reservation.WindowID)
	}
	result, err := store.invoke.Run(ctx, store.function, keys, "complete", request.DispatchToken, strconv.FormatInt(int64(request.Actual), 10), string(resultRef), string(attempt), "0")
	if err != nil {
		return resolveMutationError(ctx, err)
	}
	status, _, _, err := parseFunctionResult(result)
	if err != nil {
		return err
	}
	return mapStatus(status)
}

func (store *AdmissionStore) Fail(ctx context.Context, request admission.FailRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if request.OperationID == "" || request.DispatchToken == "" || request.Incurred < 0 || !request.Incurred.Valid() {
		return fmt.Errorf("invalid fail request")
	}
	attempt, err := encodeAttempt(request.Attempt)
	if err != nil {
		return err
	}
	operationKey, err := store.operationKeyForID(ctx, request.OperationID)
	if err != nil {
		return err
	}
	operation, err := store.Get(ctx, request.OperationID)
	if err != nil {
		return err
	}
	keys := make([]string, 2+len(operation.Reservations))
	keys[0] = store.space.operationIndexKey(request.OperationID)
	keys[1] = operationKey
	for index, reservation := range operation.Reservations {
		keys[index+2] = store.space.budgetKey(reservation.PolicyID, reservation.WindowID)
	}
	result, err := store.invoke.Run(ctx, store.function, keys, "fail", request.DispatchToken, string(request.Certainty), strconv.FormatInt(int64(request.Incurred), 10), string(attempt), strconv.FormatInt(int64(0), 10), "0")
	if err != nil {
		return resolveMutationError(ctx, err)
	}
	status, _, _, err := parseFunctionResult(result)
	if err != nil {
		return err
	}
	return mapStatus(status)
}

func (store *AdmissionStore) Get(ctx context.Context, id string) (admission.Operation, error) {
	if err := ctx.Err(); err != nil {
		return admission.Operation{}, err
	}
	if id == "" {
		return admission.Operation{}, admission.ErrOperationNotFound
	}
	operationKey, err := store.operationKeyForID(ctx, id)
	if err != nil {
		return admission.Operation{}, err
	}
	record, err := store.reader.Get(ctx, operationKey)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return admission.Operation{}, admission.ErrOperationNotFound
		}
		return admission.Operation{}, resolveMutationError(ctx, err)
	}
	return store.decodeRecord([]byte(record))
}

func (store *AdmissionStore) operationKeyForID(ctx context.Context, id string) (string, error) {
	indexKey := store.space.operationIndexKey(id)
	value, err := store.reader.Get(ctx, indexKey)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", admission.ErrOperationNotFound
		}
		return "", resolveMutationError(ctx, err)
	}
	if value == "" || len(value) > 512 {
		return "", ErrUnavailable
	}
	return value, nil
}

func (store *AdmissionStore) decodeRecord(data []byte) (admission.Operation, error) {
	if len(data) == 0 || len(data) > store.maxRecordBytes {
		return admission.Operation{}, ErrUnavailable
	}
	operation, err := decodeOperation(data)
	if err != nil {
		return admission.Operation{}, ErrUnavailable
	}
	if !operation.ExpiresAt.IsZero() && !store.clock().Before(operation.ExpiresAt) {
		return admission.Operation{}, admission.ErrOperationNotFound
	}
	return operation, nil
}

func validateBegin(request admission.BeginRequest) error {
	if request.ID == "" || request.ScopeKey == "" || request.Reservation < 0 || !request.Reservation.Valid() {
		return fmt.Errorf("invalid admission begin request")
	}
	if err := validateMicro(request.Reservation); err != nil {
		return err
	}
	return validateReservations(request.Reservations)
}

func validateReservations(reservations []admission.WindowReservation) error {
	if len(reservations) > maxRedisReservations {
		return fmt.Errorf("admission reservation count exceeds Redis key limit")
	}
	for _, reservation := range reservations {
		if reservation.PolicyID == "" || reservation.WindowID == "" || reservation.Limit <= 0 || reservation.BucketNanos <= 0 || reservation.DurationNanos <= 0 || reservation.Amount < 0 || !reservation.Amount.Valid() || !reservation.Limit.Valid() {
			return fmt.Errorf("invalid admission reservation")
		}
		if err := validateMicro(reservation.Amount); err != nil {
			return err
		}
		if err := validateMicro(reservation.Limit); err != nil {
			return err
		}
		if err := validateRedisInteger(reservation.Bucket); err != nil {
			return err
		}
		if reservation.BucketNanos > maxRedisInteger || reservation.DurationNanos > maxRedisInteger {
			return fmt.Errorf("reservation duration exceeds Redis safe range")
		}
	}
	return nil
}

func cloneReservations(values []admission.WindowReservation) []admission.WindowReservation {
	return append([]admission.WindowReservation(nil), values...)
}

func ttlSeconds(now time.Time, expires time.Time) int64 {
	if expires.IsZero() {
		return 0
	}
	seconds := int64(expires.Sub(now) / time.Second)
	if seconds <= 0 {
		return 1
	}
	return seconds
}

func encodeBlobRef(ref *state.BlobRef) ([]byte, error) {
	if ref == nil {
		return []byte("null"), nil
	}
	if !ref.Valid() {
		return nil, fmt.Errorf("invalid result reference")
	}
	return json.Marshal(blobRefWire{Digest: fmt.Sprintf("%x", ref.Digest[:]), Size: ref.Size, Media: ref.Media})
}

func parseFunctionResult(result []any) (status string, record, denial []byte, err error) {
	if len(result) < 1 {
		return "", nil, nil, ErrUnavailable
	}
	status, ok := resultString(result[0])
	if !ok || status == "" {
		return "", nil, nil, ErrUnavailable
	}
	if len(result) > 1 {
		record, _ = resultBytes(result[1])
	}
	if status == "denied" && len(result) == 2 {
		return status, nil, record, nil
	}
	if len(result) > 2 {
		denial, _ = resultBytes(result[2])
	}
	return status, record, denial, nil
}

func resultString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}

func resultBytes(value any) ([]byte, bool) {
	switch typed := value.(type) {
	case string:
		return []byte(typed), true
	case []byte:
		return append([]byte(nil), typed...), true
	default:
		return nil, false
	}
}

func mapStatus(status string) error {
	switch status {
	case "ok", "created", "existing":
		return nil
	case "not_found":
		return admission.ErrOperationNotFound
	case "invalid_token":
		return admission.ErrInvalidToken
	case "invalid_transition":
		return admission.ErrInvalidTransition
	case "conflict":
		return admission.ErrOperationConflict
	case "state_unavailable", "NOSCRIPT":
		return ErrUnavailable
	case "invalid_request":
		return fmt.Errorf("invalid Redis admission request")
	default:
		return fmt.Errorf("Redis admission function failed")
	}
}

func resolveMutationError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, redis.Nil) {
		return admission.ErrOperationNotFound
	}
	return fmt.Errorf("Redis admission mutation outcome is unresolved: %w", ErrUnavailable)
}

// Ensure pricing remains a direct dependency for consumers that only compile
// this package with generated test doubles.
var _ pricing.MicroUSD
