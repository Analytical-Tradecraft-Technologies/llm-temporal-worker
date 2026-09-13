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

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	durable "github.com/mfow/llm-temporal-worker/golang/storage/durable"
	redisclient "github.com/redis/go-redis/v9"
)

var (
	ErrRedisBudgetConflict             = fmt.Errorf("%w: Redis durable budget idempotency conflict", durable.ErrBatchMaterializationConflict)
	ErrRedisBudgetGenerationMismatch   = errors.New("Redis durable budget generation mismatch")
	ErrRedisBudgetIncarnationMismatch  = errors.New("Redis durable budget incarnation mismatch")
	ErrRedisBudgetReservationNotFound  = fmt.Errorf("%w: Redis durable budget reservation is not known", durable.ErrReservationNotFound)
	ErrRedisBudgetReservationFinalized = errors.New("Redis durable budget reservation is already finalized")
)

// RedisBudgetMaterializer is the production Redis implementation of the
// durable BudgetMaterializer port. It uses the existing versioned admission
// Function/Lua execution seam, but a separate key family and nano-USD wire
// representation. Legacy admission records and micro-USD buckets are never
// mixed with these records.
//
// New reservations are bound to the materializer's immutable generation and
// incarnation. Reconciliation is fenced by the identity persisted with the
// generation-scoped reservation so it remains possible after snapshot rotation.
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
var _ durable.BatchBudgetMaterializer = (*RedisBudgetMaterializer)(nil)
var _ durable.BatchGrantMaterializer = (*RedisBudgetMaterializer)(nil)
var _ durable.BatchEscrowMaterializer = (*RedisBudgetMaterializer)(nil)
var _ durable.BatchMaterializationReader = (*RedisBudgetMaterializer)(nil)

// Batch operation keys have a separate bounded allowance; shared budget and
// expiry keys are deduplicated. Payload and per-operation limits stay unchanged.
const maxDurableBatchKeys = 1536
const maxDurableBatchPayloadBytes = 2 * 1024 * 1024

type durableReservation struct {
	PolicyID       string `json:"policy_id"`
	WindowID       string `json:"window_id"`
	Bucket         string `json:"bucket"`
	AmountNano     string `json:"amount_nano"`
	LimitNano      string `json:"limit_nano"`
	BucketNanos    string `json:"bucket_nanos"`
	DurationNanos  string `json:"duration_nanos"`
	BucketStart    string `json:"bucket_start_nanos"`
	ExpiresMillis  int64  `json:"expires_millis"`
	EventID        string `json:"event_id"`
	AmountUSD      string `json:"amount_usd"`
	LimitUSD       string `json:"limit_usd"`
	BucketKeyIndex int    `json:"bucket_key_index,omitempty"`
	ExpiryKeyIndex int    `json:"expiry_key_index,omitempty"`
}

type durableOperation struct {
	Schema              string                     `json:"schema"`
	OperationID         string                     `json:"operation_id"`
	GenerationID        string                     `json:"generation_id"`
	IncarnationID       string                     `json:"incarnation_id"`
	Fingerprint         string                     `json:"fingerprint"`
	Status              string                     `json:"status"`
	OccurredAt          time.Time                  `json:"occurred_at"`
	ExpiresAt           time.Time                  `json:"expires_at,omitempty"`
	Route               durable.DispatchRouteFacts `json:"route"`
	Bounds              durable.ReservationBounds  `json:"bounds,omitempty"`
	LogicalCostNano     string                     `json:"logical_cost_nano"`
	RemainingEscrowNano string                     `json:"remaining_escrow_nano,omitempty"`
	Reservations        []durableReservation       `json:"reservations"`
	Events              map[string]string          `json:"events,omitempty"`
	RefundNano          string                     `json:"refund_nano,omitempty"`
	Denial              *durableDenial             `json:"denial,omitempty"`
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

type durableBatchOperation struct {
	OperationKeyIndex int                        `json:"operation_key_index"`
	OperationID       string                     `json:"operation_id"`
	Fingerprint       string                     `json:"fingerprint"`
	OccurredAt        time.Time                  `json:"occurred_at"`
	ExpiresAt         time.Time                  `json:"expires_at"`
	Route             durable.DispatchRouteFacts `json:"route"`
	Bounds            durable.ReservationBounds  `json:"bounds,omitempty"`
	LogicalCostNano   string                     `json:"logical_cost_nano"`
	Reservations      []durableReservation       `json:"reservations"`
}

type durableBatchRecord struct {
	Schema              string             `json:"schema"`
	GenerationID        string             `json:"generation_id"`
	IncarnationID       string             `json:"incarnation_id"`
	EscrowID            string             `json:"escrow_id,omitempty"`
	EscrowFingerprint   string             `json:"escrow_fingerprint,omitempty"`
	AllocationSequence  int64              `json:"allocation_sequence,omitempty"`
	RemainingEscrowNano string             `json:"remaining_escrow_nano,omitempty"`
	ContentDigest       string             `json:"content_digest"`
	Fingerprint         string             `json:"fingerprint"`
	Status              string             `json:"status"`
	Operations          []durableOperation `json:"operations"`
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
	now := m.clock().UTC()
	if request.IncarnationID == "" {
		request.IncarnationID = m.incarnation
	}
	if request.OccurredAt.IsZero() {
		request.OccurredAt = now
	}
	if request.Route != (durable.DispatchRouteFacts{}) {
		if err := request.Route.Validate(); err != nil {
			return durable.ReserveResult{}, fmt.Errorf("Redis durable budget route: %w", err)
		}
	}
	if err := request.OperationID.Validate(); err != nil {
		return durable.ReserveResult{}, err
	}
	if request.GenerationID != m.generation {
		return durable.ReserveResult{}, ErrRedisBudgetGenerationMismatch
	}
	if request.IncarnationID != m.incarnation {
		return durable.ReserveResult{}, ErrRedisBudgetIncarnationMismatch
	}
	if len(request.Reservations) == 0 {
		return durable.ReserveResult{}, errors.New("Redis durable budget reservation list must not be empty")
	}
	reservations, err := canonicalDurableReservations(request.OperationID, request.GenerationID, request.Reservations, request.ExpiresAt, now)
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
	ttl := durableTTLSeconds(request.ExpiresAt, now, reservations)
	if ttl <= 0 {
		return durable.ReserveResult{}, errors.New("Redis durable budget reservation expiry must be in the future")
	}
	wire, err := json.Marshal(reservations)
	if err != nil {
		return durable.ReserveResult{}, fmt.Errorf("marshal Redis durable budget reservations: %w", err)
	}
	routeWire, err := json.Marshal(request.Route)
	if err != nil {
		return durable.ReserveResult{}, fmt.Errorf("marshal Redis durable budget route: %w", err)
	}
	logicalNano, err := durableLogicalCostNano(request.LogicalCostUSD, false)
	if err != nil {
		return durable.ReserveResult{}, err
	}
	result, err := m.invoke.Run(ctx, m.function, keys,
		"durable_reserve", string(request.GenerationID), string(m.incarnation), operation,
		fingerprint, strconv.FormatInt(ttl, 10), request.OccurredAt.UTC().Format(time.RFC3339Nano), string(routeWire), string(wire), logicalNano)
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
	return reserveResultFromRecord(request, record)
}

func (m *RedisBudgetMaterializer) AcceptBatch(ctx context.Context, contentDigest [32]byte, requests []durable.ReserveRequest) ([]durable.ReserveResult, error) {
	if m == nil {
		return nil, errors.New("Redis durable budget materializer is nil")
	}
	if ctx == nil {
		return nil, errors.New("Redis durable budget materializer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := m.clock().UTC()
	normalized := append([]durable.ReserveRequest(nil), requests...)
	operations := make([]durableBatchOperation, len(normalized))
	keys := make([]string, 1, 1+len(normalized))
	digestHex := hex.EncodeToString(contentDigest[:])
	keys[0] = m.space.durableBudgetBatchKey(string(m.generation), digestHex)
	keyIndexes := make(map[string]int)
	for index := range normalized {
		normalized[index].Reservations = append([]admission.WindowReservation(nil), normalized[index].Reservations...)
		if normalized[index].IncarnationID == "" {
			normalized[index].IncarnationID = m.incarnation
		}
		if normalized[index].OccurredAt.IsZero() {
			normalized[index].OccurredAt = now
		}
		keys = append(keys, m.space.durableBudgetOperationKey(string(normalized[index].GenerationID), string(normalized[index].OperationID)))
	}
	if err := durable.ValidateBatchReserveRequest(contentDigest, normalized); err != nil {
		return nil, err
	}
	fingerprintHash := sha256.New()
	_, _ = fingerprintHash.Write(contentDigest[:])
	ttl := int64(0)
	for index, request := range normalized {
		if request.GenerationID != m.generation {
			return nil, ErrRedisBudgetGenerationMismatch
		}
		if request.IncarnationID != m.incarnation {
			return nil, ErrRedisBudgetIncarnationMismatch
		}
		if request.Route != (durable.DispatchRouteFacts{}) {
			if err := request.Route.Validate(); err != nil {
				return nil, fmt.Errorf("Redis durable batch route %d: %w", index, err)
			}
		}
		reservations, err := canonicalDurableReservations(request.OperationID, request.GenerationID, request.Reservations, request.ExpiresAt, now)
		if err != nil {
			return nil, fmt.Errorf("Redis durable batch operation %d: %w", index, err)
		}
		requestTTL := durableTTLSeconds(request.ExpiresAt, now, reservations)
		if requestTTL <= 0 {
			return nil, fmt.Errorf("Redis durable batch operation %d expiry must be in the future", index)
		}
		if requestTTL > ttl {
			ttl = requestTTL
		}
		requestFingerprint, err := durableRequestFingerprint(request, reservations)
		if err != nil {
			return nil, err
		}
		fingerprintBytes, err := hex.DecodeString(requestFingerprint)
		if err != nil {
			return nil, err
		}
		_, _ = fingerprintHash.Write(fingerprintBytes)
		for reservationIndex := range reservations {
			reservations[reservationIndex].BucketKeyIndex = durableBatchKeyIndex(&keys, keyIndexes, m.space.durableBudgetKey(reservations[reservationIndex].PolicyID, reservations[reservationIndex].WindowID))
			reservations[reservationIndex].ExpiryKeyIndex = durableBatchKeyIndex(&keys, keyIndexes, m.space.durableBudgetExpiryKey(reservations[reservationIndex].PolicyID, reservations[reservationIndex].WindowID))
		}
		logicalNano, err := durableLogicalCostNano(request.LogicalCostUSD, false)
		if err != nil {
			return nil, err
		}
		operations[index] = durableBatchOperation{
			OperationKeyIndex: index + 2, OperationID: string(request.OperationID),
			LogicalCostNano: logicalNano,
			Fingerprint:     requestFingerprint, OccurredAt: request.OccurredAt.UTC(),
			ExpiresAt: request.ExpiresAt.UTC(), Route: request.Route, Bounds: request.Bounds, Reservations: reservations,
		}
	}
	if len(keys) > maxDurableBatchKeys {
		return nil, fmt.Errorf("Redis durable batch needs %d keys; atomic bound is %d", len(keys), maxDurableBatchKeys)
	}
	wire, err := json.Marshal(operations)
	if err != nil {
		return nil, fmt.Errorf("marshal Redis durable budget batch: %w", err)
	}
	if len(wire) > maxDurableBatchPayloadBytes {
		return nil, fmt.Errorf("Redis durable batch payload is %d bytes; bound is %d", len(wire), maxDurableBatchPayloadBytes)
	}
	fingerprint := hex.EncodeToString(fingerprintHash.Sum(nil))
	result, err := m.invoke.Run(ctx, m.function, keys,
		"durable_reserve_batch", string(m.generation), string(m.incarnation),
		digestHex, fingerprint, strconv.FormatInt(ttl, 10), string(wire))
	if err != nil {
		return nil, resolveMutationError(ctx, err)
	}
	status, recordData, err := durableFunctionRecord(result)
	if err != nil {
		return nil, err
	}
	if status == "conflict" {
		return nil, ErrRedisBudgetConflict
	}
	if status != "created" && status != "existing" {
		return nil, mapDurableStatus(status)
	}
	var record durableBatchRecord
	if err := json.Unmarshal([]byte(recordData), &record); err != nil {
		return nil, fmt.Errorf("decode Redis durable budget batch: %w", err)
	}
	if record.ContentDigest != digestHex || record.Fingerprint != fingerprint || len(record.Operations) != len(normalized) {
		return nil, ErrRedisBudgetConflict
	}
	results := make([]durable.ReserveResult, len(normalized))
	for index := range normalized {
		if record.Operations[index].OperationID != string(normalized[index].OperationID) {
			return nil, ErrRedisBudgetConflict
		}
		if record.Status == "denied" {
			results[index], err = durableDeniedResult(normalized[index], record.Operations[index])
		} else if record.Status == "accepted" {
			results[index], err = reserveResultFromRecord(normalized[index], record.Operations[index])
		} else {
			return nil, fmt.Errorf("invalid Redis durable budget batch status %q", record.Status)
		}
		if err != nil {
			return nil, err
		}
	}
	if err := durable.ValidateBatchReserveResult(normalized, results); err != nil {
		return nil, err
	}
	return results, nil
}

func (m *RedisBudgetMaterializer) LoadBatchMaterialization(ctx context.Context, contentDigest [32]byte) (durable.BatchMaterialization, bool, error) {
	if m == nil || ctx == nil || m.reader == nil {
		return durable.BatchMaterialization{}, false, errors.New("Redis durable batch materialization reader is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return durable.BatchMaterialization{}, false, err
	}
	if contentDigest == ([32]byte{}) {
		return durable.BatchMaterialization{}, false, errors.New("Redis durable batch content digest is required")
	}
	digestHex := hex.EncodeToString(contentDigest[:])
	raw, err := m.reader.Get(ctx, m.space.durableBudgetBatchKey(string(m.generation), digestHex))
	if errors.Is(err, redisclient.Nil) {
		return durable.BatchMaterialization{}, false, nil
	}
	if err != nil {
		return durable.BatchMaterialization{}, false, resolveMutationError(ctx, err)
	}
	var batch durableBatchRecord
	if err := json.Unmarshal([]byte(raw), &batch); err != nil {
		return durable.BatchMaterialization{}, false, fmt.Errorf("decode Redis durable budget batch: %w", err)
	}
	if batch.Schema != "durable-budget/v1" || batch.ContentDigest != digestHex || (batch.Status != "accepted" && batch.Status != "denied") || len(batch.Operations) == 0 || len(batch.Operations) > 1024 {
		return durable.BatchMaterialization{}, false, ErrRedisBudgetConflict
	}
	if batch.GenerationID != string(m.generation) {
		return durable.BatchMaterialization{}, false, ErrRedisBudgetGenerationMismatch
	}
	if batch.IncarnationID != string(m.incarnation) {
		return durable.BatchMaterialization{}, false, ErrRedisBudgetIncarnationMismatch
	}
	snapshot := durable.BatchMaterialization{
		Requests: make([]durable.ReserveRequest, len(batch.Operations)),
		Results:  make([]durable.ReserveResult, len(batch.Operations)),
	}
	hash := sha256.New()
	_, _ = hash.Write(contentDigest[:])
	for index, record := range batch.Operations {
		if record.Schema != batch.Schema || record.GenerationID != batch.GenerationID || record.IncarnationID != batch.IncarnationID || record.Status != batch.Status {
			return durable.BatchMaterialization{}, false, ErrRedisBudgetConflict
		}
		request, err := reserveRequestFromDurableOperation(record)
		if err != nil {
			return durable.BatchMaterialization{}, false, err
		}
		// Historical reconstruction is anchored to the original occurrence,
		// never the current clock or the mutable live operation record.
		reservations, err := canonicalDurableReservations(request.OperationID, request.GenerationID, request.Reservations, request.ExpiresAt, request.OccurredAt)
		if err != nil {
			return durable.BatchMaterialization{}, false, err
		}
		for ri, reservation := range record.Reservations {
			reservation.BucketKeyIndex, reservation.ExpiryKeyIndex = 0, 0
			if reservation != reservations[ri] {
				return durable.BatchMaterialization{}, false, ErrRedisBudgetConflict
			}
		}
		fingerprint, err := durableRequestFingerprint(request, reservations)
		if err != nil {
			return durable.BatchMaterialization{}, false, err
		}
		if fingerprint != record.Fingerprint {
			return durable.BatchMaterialization{}, false, ErrRedisBudgetConflict
		}
		decoded, _ := hex.DecodeString(fingerprint)
		_, _ = hash.Write(decoded)
		snapshot.Requests[index] = request
		if batch.Status == "accepted" {
			snapshot.Results[index], err = reserveResultFromRecord(request, record)
		} else {
			snapshot.Results[index], err = durableDeniedResult(request, record)
		}
		if err != nil {
			return durable.BatchMaterialization{}, false, err
		}
	}
	if batch.EscrowID != "" {
		if err := durable.OperationID(batch.EscrowID).Validate(); err != nil || len(batch.EscrowFingerprint) != 64 || batch.AllocationSequence <= 0 || batch.RemainingEscrowNano == "" {
			return durable.BatchMaterialization{}, false, ErrRedisBudgetConflict
		}
		_, _ = fmt.Fprintf(hash, "\x00%s\x00%s\x00%d", batch.EscrowID, batch.EscrowFingerprint, batch.AllocationSequence)
		remaining, err := parseNanoUSD(batch.RemainingEscrowNano)
		if err != nil {
			return durable.BatchMaterialization{}, false, err
		}
		snapshot.RemainingEscrowCostUSD, err = pricing.USDFromNano(remaining)
		if err != nil {
			return durable.BatchMaterialization{}, false, err
		}
	} else if batch.EscrowFingerprint != "" || batch.AllocationSequence != 0 {
		return durable.BatchMaterialization{}, false, ErrRedisBudgetConflict
	}
	if batch.Fingerprint != hex.EncodeToString(hash.Sum(nil)) {
		return durable.BatchMaterialization{}, false, ErrRedisBudgetConflict
	}
	if err := durable.ValidateBatchReserveRequest(contentDigest, snapshot.Requests); err != nil {
		return durable.BatchMaterialization{}, false, err
	}
	if err := durable.ValidateBatchReserveResult(snapshot.Requests, snapshot.Results); err != nil {
		return durable.BatchMaterialization{}, false, err
	}
	return snapshot, true, nil
}

func (m *RedisBudgetMaterializer) ConfirmBatchGrant(ctx context.Context, contentDigest [32]byte, operationID durable.OperationID) (durable.ReserveRequest, durable.ReserveResult, error) {
	if m == nil || ctx == nil || m.reader == nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, errors.New("Redis durable batch grant reader is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, err
	}
	if contentDigest == ([32]byte{}) {
		return durable.ReserveRequest{}, durable.ReserveResult{}, errors.New("Redis durable batch content digest is required")
	}
	if err := operationID.Validate(); err != nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, err
	}
	digestHex := hex.EncodeToString(contentDigest[:])
	raw, err := m.reader.Get(ctx, m.space.durableBudgetBatchKey(string(m.generation), digestHex))
	if errors.Is(err, redisclient.Nil) {
		return durable.ReserveRequest{}, durable.ReserveResult{}, ErrRedisBudgetReservationNotFound
	}
	if err != nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, resolveMutationError(ctx, err)
	}
	var batch durableBatchRecord
	if err := json.Unmarshal([]byte(raw), &batch); err != nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, fmt.Errorf("decode Redis durable budget batch: %w", err)
	}
	if batch.ContentDigest != digestHex || batch.Status != "accepted" {
		return durable.ReserveRequest{}, durable.ReserveResult{}, ErrRedisBudgetReservationNotFound
	}
	var batchOperation *durableOperation
	for index := range batch.Operations {
		if batch.Operations[index].OperationID == string(operationID) {
			batchOperation = &batch.Operations[index]
			break
		}
	}
	if batchOperation == nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, ErrRedisBudgetReservationNotFound
	}
	generationID := durable.GenerationID(batchOperation.GenerationID)
	record, err := m.readDurableOperation(ctx, generationID, operationID)
	if err != nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, err
	}
	if record.OperationID != batchOperation.OperationID ||
		record.GenerationID != batchOperation.GenerationID ||
		record.IncarnationID != batchOperation.IncarnationID ||
		record.Fingerprint != batchOperation.Fingerprint {
		return durable.ReserveRequest{}, durable.ReserveResult{}, ErrRedisBudgetConflict
	}
	request, err := reserveRequestFromDurableOperation(record)
	if err != nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, err
	}
	result, err := confirmDurableOperation(request, record, m.clock().UTC())
	return request, result, err
}

func reserveRequestFromDurableOperation(record durableOperation) (durable.ReserveRequest, error) {
	reservations := make([]admission.WindowReservation, len(record.Reservations))
	for index, value := range record.Reservations {
		bucket, err := strconv.ParseInt(value.Bucket, 10, 64)
		if err != nil {
			return durable.ReserveRequest{}, ErrUnavailable
		}
		bucketNanos, err := strconv.ParseInt(value.BucketNanos, 10, 64)
		if err != nil {
			return durable.ReserveRequest{}, ErrUnavailable
		}
		durationNanos, err := strconv.ParseInt(value.DurationNanos, 10, 64)
		if err != nil {
			return durable.ReserveRequest{}, ErrUnavailable
		}
		amount, err := pricing.ParseUSD(value.AmountUSD)
		if err != nil {
			return durable.ReserveRequest{}, ErrUnavailable
		}
		limit, err := pricing.ParseUSD(value.LimitUSD)
		if err != nil {
			return durable.ReserveRequest{}, ErrUnavailable
		}
		amountMicro, err := pricing.CeilMicroFromUSD(amount)
		if err != nil {
			return durable.ReserveRequest{}, ErrUnavailable
		}
		limitMicro, err := pricing.MicroFromUSD(limit)
		if err != nil {
			return durable.ReserveRequest{}, ErrUnavailable
		}
		reservations[index] = admission.WindowReservation{
			PolicyID: value.PolicyID, WindowID: value.WindowID, Bucket: bucket,
			Amount: amountMicro, Limit: limitMicro, AmountUSD: amount, LimitUSD: limit,
			BucketNanos: bucketNanos, DurationNanos: durationNanos,
		}
	}
	logicalNano, err := parseNanoUSD(record.LogicalCostNano)
	if err != nil {
		return durable.ReserveRequest{}, err
	}
	logicalCost, err := pricing.USDFromNano(logicalNano)
	if err != nil {
		return durable.ReserveRequest{}, err
	}
	return durable.ReserveRequest{
		OperationID: durable.OperationID(record.OperationID), GenerationID: durable.GenerationID(record.GenerationID),
		IncarnationID: durable.IncarnationID(record.IncarnationID), Reservations: reservations,
		ExpiresAt: record.ExpiresAt, OccurredAt: record.OccurredAt, Route: record.Route, Bounds: record.Bounds,
		LogicalCostUSD: logicalCost,
	}, nil
}

func (m *RedisBudgetMaterializer) readDurableOperation(ctx context.Context, generationID durable.GenerationID, operationID durable.OperationID) (durableOperation, error) {
	if err := generationID.Validate(); err != nil {
		return durableOperation{}, err
	}
	if err := operationID.Validate(); err != nil {
		return durableOperation{}, err
	}
	raw, err := m.reader.Get(ctx, m.space.durableBudgetOperationKey(string(generationID), string(operationID)))
	if errors.Is(err, redisclient.Nil) {
		return durableOperation{}, ErrRedisBudgetReservationNotFound
	}
	if err != nil {
		return durableOperation{}, resolveMutationError(ctx, err)
	}
	var record durableOperation
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return durableOperation{}, fmt.Errorf("decode Redis durable budget operation: %w", err)
	}
	return record, nil
}

func confirmDurableOperation(request durable.ReserveRequest, record durableOperation, now time.Time) (durable.ReserveResult, error) {
	if err := request.OperationID.Validate(); err != nil {
		return durable.ReserveResult{}, err
	}
	if err := request.GenerationID.Validate(); err != nil {
		return durable.ReserveResult{}, err
	}
	if err := request.IncarnationID.Validate(); err != nil {
		return durable.ReserveResult{}, err
	}
	if !request.ExpiresAt.After(now) {
		return durable.ReserveResult{}, ErrRedisBudgetReservationNotFound
	}
	reservations, err := canonicalDurableReservations(request.OperationID, request.GenerationID, request.Reservations, request.ExpiresAt, now)
	if err != nil {
		return durable.ReserveResult{}, err
	}
	fingerprint, err := durableRequestFingerprint(request, reservations)
	if err != nil {
		return durable.ReserveResult{}, err
	}
	if record.OperationID != string(request.OperationID) || record.Status != "accepted" {
		return durable.ReserveResult{}, ErrRedisBudgetReservationNotFound
	}
	if record.GenerationID != string(request.GenerationID) {
		return durable.ReserveResult{}, ErrRedisBudgetGenerationMismatch
	}
	if record.IncarnationID != string(request.IncarnationID) {
		return durable.ReserveResult{}, ErrRedisBudgetIncarnationMismatch
	}
	if record.Fingerprint != fingerprint {
		return durable.ReserveResult{}, fmt.Errorf(
			"%w: operation %s stored fingerprint %s does not match reconstructed %s",
			ErrRedisBudgetConflict, request.OperationID, record.Fingerprint, fingerprint,
		)
	}
	result, err := reserveResultFromRecord(request, record)
	if err != nil {
		return durable.ReserveResult{}, err
	}
	if err := durable.ValidatePlannedReserveResult(request, result); err != nil {
		return durable.ReserveResult{}, err
	}
	return result, nil
}

func (m *RedisBudgetMaterializer) AcceptBatchEscrow(ctx context.Context, contentDigest [32]byte, request durable.ReserveRequest) (durable.ReserveResult, error) {
	if _, err := durableLogicalCostNano(request.LogicalCostUSD, true); err != nil {
		return durable.ReserveResult{}, err
	}
	results, err := m.AcceptBatch(ctx, contentDigest, []durable.ReserveRequest{request})
	if err != nil {
		return durable.ReserveResult{}, err
	}
	if len(results) != 1 {
		return durable.ReserveResult{}, ErrUnavailable
	}
	return results[0], nil
}

func (m *RedisBudgetMaterializer) LoadBatchEscrow(ctx context.Context, contentDigest [32]byte, operationID durable.OperationID) (durable.ReserveRequest, durable.ReserveResult, error) {
	snapshot, found, err := m.LoadBatchMaterialization(ctx, contentDigest)
	if err != nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, err
	}
	if !found || len(snapshot.Requests) != 1 || snapshot.Requests[0].OperationID != operationID || !snapshot.Results[0].Accepted {
		return durable.ReserveRequest{}, durable.ReserveResult{}, ErrRedisBudgetReservationNotFound
	}
	request := snapshot.Requests[0]
	record, err := m.readDurableOperation(ctx, request.GenerationID, operationID)
	if err != nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, err
	}
	reservations, err := canonicalDurableReservations(request.OperationID, request.GenerationID, request.Reservations, request.ExpiresAt, request.OccurredAt)
	if err != nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, err
	}
	fingerprint, err := durableRequestFingerprint(request, reservations)
	if err != nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, err
	}
	if record.OperationID != string(operationID) || record.GenerationID != string(request.GenerationID) || record.IncarnationID != string(request.IncarnationID) || record.Fingerprint != fingerprint || (record.Status != "accepted" && record.Status != "closed") {
		return durable.ReserveRequest{}, durable.ReserveResult{}, ErrRedisBudgetConflict
	}
	remaining, err := parseNanoUSD(record.RemainingEscrowNano)
	if err != nil {
		return durable.ReserveRequest{}, durable.ReserveResult{}, err
	}
	request.RemainingEscrowCostUSD, err = pricing.USDFromNano(remaining)
	if err != nil || request.RemainingEscrowCostUSD.Cmp(request.LogicalCostUSD) > 0 {
		return durable.ReserveRequest{}, durable.ReserveResult{}, ErrUnavailable
	}
	return request, snapshot.Results[0], nil
}

func (m *RedisBudgetMaterializer) AllocateBatchGrants(ctx context.Context, contentDigest [32]byte, escrow durable.ReserveRequest, sequence int64, requests []durable.ReserveRequest) ([]durable.ReserveResult, error) {
	if m == nil || ctx == nil {
		return nil, errors.New("Redis durable batch escrow materializer is unavailable")
	}
	if err := durable.ValidateBatchReserveRequest(contentDigest, requests); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sequence <= 0 || escrow.GenerationID != m.generation || escrow.IncarnationID != m.incarnation {
		return nil, ErrRedisBudgetConflict
	}
	if _, err := durableLogicalCostNano(escrow.LogicalCostUSD, true); err != nil {
		return nil, err
	}
	now := m.clock().UTC()
	escrowReservations, err := canonicalDurableReservations(escrow.OperationID, escrow.GenerationID, escrow.Reservations, escrow.ExpiresAt, now)
	if err != nil {
		return nil, err
	}
	escrowFingerprint, err := durableRequestFingerprint(escrow, escrowReservations)
	if err != nil {
		return nil, err
	}
	digestHex := hex.EncodeToString(contentDigest[:])
	keys := []string{m.space.durableBudgetOperationKey(string(escrow.GenerationID), string(escrow.OperationID)), m.space.durableBudgetBatchKey(string(m.generation), digestHex)}
	keyIndexes := make(map[string]int)
	normalized := append([]durable.ReserveRequest(nil), requests...)
	for index := range normalized {
		keys = append(keys, m.space.durableBudgetOperationKey(string(normalized[index].GenerationID), string(normalized[index].OperationID)))
	}
	operations := make([]durableBatchOperation, len(normalized))
	fingerprintHash := sha256.New()
	_, _ = fingerprintHash.Write(contentDigest[:])
	ttl := durableTTLSeconds(escrow.ExpiresAt, now, escrowReservations)
	for index, request := range normalized {
		if request.GenerationID != m.generation || request.IncarnationID != m.incarnation {
			return nil, ErrRedisBudgetGenerationMismatch
		}
		reservations, e := canonicalDurableReservations(request.OperationID, request.GenerationID, request.Reservations, request.ExpiresAt, now)
		if e != nil {
			return nil, e
		}
		if durableTTLSeconds(request.ExpiresAt, now, reservations) > ttl {
			return nil, errors.New("allocated grant retention exceeds escrow retention")
		}
		fp, e := durableRequestFingerprint(request, reservations)
		if e != nil {
			return nil, e
		}
		decoded, _ := hex.DecodeString(fp)
		_, _ = fingerprintHash.Write(decoded)
		for ri := range reservations {
			reservations[ri].BucketKeyIndex = durableBatchKeyIndex(&keys, keyIndexes, m.space.durableBudgetKey(reservations[ri].PolicyID, reservations[ri].WindowID))
			reservations[ri].ExpiryKeyIndex = durableBatchKeyIndex(&keys, keyIndexes, m.space.durableBudgetExpiryKey(reservations[ri].PolicyID, reservations[ri].WindowID))
		}
		logicalNano, e := durableLogicalCostNano(request.LogicalCostUSD, true)
		if e != nil {
			return nil, e
		}
		operations[index] = durableBatchOperation{OperationKeyIndex: index + 3, OperationID: string(request.OperationID), Fingerprint: fp, OccurredAt: request.OccurredAt.UTC(), ExpiresAt: request.ExpiresAt.UTC(), Route: request.Route, Bounds: request.Bounds, LogicalCostNano: logicalNano, Reservations: reservations}
	}
	if len(keys) > maxDurableBatchKeys || ttl <= 0 {
		return nil, fmt.Errorf("Redis durable allocated batch needs %d keys (bound %d) and positive retention (got %d)", len(keys), maxDurableBatchKeys, ttl)
	}
	wire, e := json.Marshal(operations)
	if e != nil {
		return nil, e
	}
	if len(wire) > maxDurableBatchPayloadBytes {
		return nil, fmt.Errorf("Redis durable allocated batch payload is %d bytes; bound is %d", len(wire), maxDurableBatchPayloadBytes)
	}
	_, _ = fmt.Fprintf(fingerprintHash, "\x00%s\x00%s\x00%d", escrow.OperationID, escrowFingerprint, sequence)
	fingerprint := hex.EncodeToString(fingerprintHash.Sum(nil))
	result, e := m.invoke.Run(ctx, m.function, keys, "durable_allocate_batch_grants", string(m.generation), string(m.incarnation), string(escrow.OperationID), escrowFingerprint, digestHex, fingerprint, strconv.FormatInt(ttl, 10), string(wire), strconv.FormatInt(sequence, 10))
	if e != nil {
		return nil, resolveMutationError(ctx, e)
	}
	status, recordData, e := durableFunctionRecord(result)
	if e != nil {
		return nil, e
	}
	if status == "conflict" {
		return nil, ErrRedisBudgetConflict
	}
	if status != "created" && status != "existing" {
		return nil, mapDurableStatus(status)
	}
	var record durableBatchRecord
	if e = json.Unmarshal([]byte(recordData), &record); e != nil {
		return nil, e
	}
	if record.ContentDigest != digestHex || record.Fingerprint != fingerprint || record.EscrowID != string(escrow.OperationID) || record.EscrowFingerprint != escrowFingerprint || record.AllocationSequence != sequence || record.Status != "accepted" || len(record.Operations) != len(normalized) {
		return nil, ErrRedisBudgetConflict
	}
	results := make([]durable.ReserveResult, len(normalized))
	for index := range normalized {
		if record.Operations[index].OperationID != string(normalized[index].OperationID) || record.Operations[index].Fingerprint != operations[index].Fingerprint {
			return nil, ErrRedisBudgetConflict
		}
		results[index], e = reserveResultFromRecord(normalized[index], record.Operations[index])
		if e != nil {
			return nil, e
		}
	}

	if e = durable.ValidateBatchReserveResult(normalized, results); e != nil {
		return nil, e
	}
	return results, nil
}
func (m *RedisBudgetMaterializer) CloseBatchEscrow(ctx context.Context, contentDigest [32]byte, escrow durable.ReserveRequest, closeDigest [32]byte) (pricing.USD, bool, error) {
	if m == nil || ctx == nil || contentDigest == ([32]byte{}) || closeDigest == ([32]byte{}) {
		return pricing.USD{}, false, errors.New("Redis durable escrow close identity is required")
	}
	if err := ctx.Err(); err != nil {
		return pricing.USD{}, false, err
	}
	snapshot, found, err := m.LoadBatchMaterialization(ctx, contentDigest)
	if err != nil {
		return pricing.USD{}, false, err
	}
	if !found || len(snapshot.Requests) != 1 || snapshot.Requests[0].OperationID != escrow.OperationID {
		return pricing.USD{}, false, ErrRedisBudgetConflict
	}
	reservations, err := canonicalDurableReservations(escrow.OperationID, escrow.GenerationID, escrow.Reservations, escrow.ExpiresAt, escrow.OccurredAt)
	if err != nil {
		return pricing.USD{}, false, err
	}
	fingerprint, err := durableRequestFingerprint(escrow, reservations)
	if err != nil {
		return pricing.USD{}, false, err
	}
	keys := []string{m.space.durableBudgetOperationKey(string(escrow.GenerationID), string(escrow.OperationID))}
	for _, reservation := range reservations {
		keys = append(keys, m.space.durableBudgetKey(reservation.PolicyID, reservation.WindowID), m.space.durableBudgetExpiryKey(reservation.PolicyID, reservation.WindowID))
	}
	result, err := m.invoke.Run(ctx, m.function, keys, "durable_close_batch", string(escrow.GenerationID), string(escrow.IncarnationID), string(escrow.OperationID), fingerprint, hex.EncodeToString(closeDigest[:]))
	if err != nil {
		return pricing.USD{}, false, resolveMutationError(ctx, err)
	}
	status, raw, err := durableFunctionRecord(result)
	if err != nil {
		return pricing.USD{}, false, err
	}
	if status == "conflict" {
		return pricing.USD{}, false, ErrRedisBudgetConflict
	}
	if status != "closed" && status != "existing" {
		return pricing.USD{}, false, mapDurableStatus(status)
	}
	var record durableOperation
	if err = json.Unmarshal([]byte(raw), &record); err != nil {
		return pricing.USD{}, false, err
	}
	nano, err := parseNanoUSD(record.RefundNano)
	if err != nil {
		return pricing.USD{}, false, err
	}
	refund, err := pricing.USDFromNano(nano)
	return refund, status == "existing", err
}
func (m *RedisBudgetMaterializer) Confirm(ctx context.Context, request durable.ReserveRequest) (durable.ReserveResult, error) {
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
	if err := request.GenerationID.Validate(); err != nil {
		return durable.ReserveResult{}, err
	}
	if err := request.IncarnationID.Validate(); err != nil {
		return durable.ReserveResult{}, err
	}
	now := m.clock().UTC()
	if !request.ExpiresAt.After(now) {
		return durable.ReserveResult{}, ErrRedisBudgetReservationNotFound
	}
	if m.reader == nil {
		return durable.ReserveResult{}, errors.New("Redis durable budget record reader is required for confirmation")
	}
	record, err := m.readDurableOperation(ctx, request.GenerationID, request.OperationID)
	if err != nil {
		return durable.ReserveResult{}, err
	}
	return confirmDurableOperation(request, record, now)
}

func (m *RedisBudgetMaterializer) FenceDispatch(ctx context.Context, request durable.DispatchFenceRequest) error {
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
	reservationRequest := request.Reservation
	reservations, err := canonicalDurableReservations(
		reservationRequest.OperationID,
		reservationRequest.GenerationID,
		reservationRequest.Reservations,
		reservationRequest.ExpiresAt,
		reservationRequest.OccurredAt,
	)
	if err != nil {
		return err
	}
	fingerprint, err := durableRequestFingerprint(reservationRequest, reservations)
	if err != nil {
		return err
	}
	keys := []string{m.space.durableBudgetOperationKey(string(reservationRequest.GenerationID), string(reservationRequest.OperationID))}
	for _, reservation := range reservations {
		keys = append(keys,
			m.space.durableBudgetKey(reservation.PolicyID, reservation.WindowID),
			m.space.durableBudgetExpiryKey(reservation.PolicyID, reservation.WindowID))
	}
	routeWire, err := json.Marshal(reservationRequest.Route)
	if err != nil {
		return fmt.Errorf("marshal Redis dispatch fence route: %w", err)
	}
	reservationsWire, err := json.Marshal(reservations)
	if err != nil {
		return fmt.Errorf("marshal Redis dispatch fence reservations: %w", err)
	}
	result, err := m.invoke.Run(ctx, m.function, keys,
		"durable_fence", string(reservationRequest.GenerationID), string(reservationRequest.IncarnationID),
		string(reservationRequest.OperationID), fingerprint, strconv.FormatInt(request.RetainUntil.UTC().UnixMilli(), 10),
		string(routeWire), string(reservationsWire))
	if err != nil {
		return resolveMutationError(ctx, err)
	}
	status, _, err := durableFunctionRecord(result)
	if err != nil {
		return err
	}
	switch status {
	case "fenced", "existing":
		return nil
	case "conflict":
		return ErrRedisBudgetConflict
	case "generation_mismatch":
		return ErrRedisBudgetGenerationMismatch
	case "incarnation_mismatch":
		return ErrRedisBudgetIncarnationMismatch
	case "expired", "not_found":
		return ErrRedisBudgetReservationNotFound
	default:
		return mapDurableStatus(status)
	}
}

func reserveResultFromRecord(request durable.ReserveRequest, record durableOperation) (durable.ReserveResult, error) {
	reserve := durable.ReserveResult{
		OperationID: request.OperationID, Accepted: true,
		GenerationID: request.GenerationID, IncarnationID: request.IncarnationID,
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
	if record.OperationID != string(request.OperationID) {
		return ErrRedisBudgetReservationNotFound
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
	if len(values) == 0 || len(values) > 250 {
		return nil, errors.New("Redis durable operation must contain 1 to 250 reservations")
	}
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
		left, _ := strconv.ParseInt(result[i].Bucket, 10, 64)
		right, _ := strconv.ParseInt(result[j].Bucket, 10, 64)
		return left < right
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
		OperationID, GenerationID, IncarnationID string
		ExpiresAt, OccurredAt                    time.Time
		Route                                    durable.DispatchRouteFacts
		Bounds                                   durable.ReservationBounds
		LogicalCostUSD                           string
		Reservations                             []durableReservation
	}{
		string(request.OperationID), string(request.GenerationID), string(request.IncarnationID),
		request.ExpiresAt.UTC(), request.OccurredAt.UTC(), request.Route, request.Bounds, request.LogicalCostUSD.String(), reservations,
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func durableBatchKeyIndex(keys *[]string, indexes map[string]int, key string) int {
	if index, ok := indexes[key]; ok {
		return index
	}
	*keys = append(*keys, key)
	index := len(*keys)
	indexes[key] = index
	return index
}

func durableLogicalCostNano(cost pricing.USD, required bool) (string, error) {
	nano, err := pricing.CeilNanoUSD(cost)
	if err != nil {
		return "", err
	}
	exact, err := pricing.USDFromNano(nano)
	if err != nil || exact.Cmp(cost) != 0 || (required && nano == 0) {
		return "", errors.New("logical operation cost must be authoritative, nano-exact, and positive for escrow")
	}
	return nano.String(), nil
}

func durableEventFingerprint(event budget.CompletionEvent) string {
	data, _ := json.Marshal(event)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func durableReservationEventID(operation, generation, policy, window string, bucket int64) string {
	identity := fmt.Sprintf("llmtw/budget-reserve/v1\x00%s\x00%s\x00%s\x00%s\x00%d", operation, generation, policy, window, bucket)
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(identity)).String()
}

func durableDeniedResult(request durable.ReserveRequest, record durableOperation) (durable.ReserveResult, error) {
	result := durable.ReserveResult{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: request.IncarnationID, Accepted: false}
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
