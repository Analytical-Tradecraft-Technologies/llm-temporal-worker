package durable

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

var (
	ErrReferenceMaterializerConflict = fmt.Errorf("%w: reference budget materializer idempotency conflict", ErrBatchMaterializationConflict)
	ErrReferenceGenerationMismatch   = errors.New("reference budget materializer generation mismatch")
	ErrReferenceIncarnationMismatch  = errors.New("reference budget materializer incarnation mismatch")
	ErrReferenceReservationNotFound  = fmt.Errorf("%w: reference budget reservation is not known", ErrReservationNotFound)
	ErrReferenceReservationFinalized = errors.New("reference budget reservation is already finalized")
)

// ReferenceBudgetMaterializer is a storage-neutral model of the active budget
// port. It is deliberately intended for conformance tests and local reasoning;
// it is not wired into the production runtime or used as a Redis fallback.
//
// A single instance models one Redis generation/incarnation. Accept and
// Reconcile hold one mutex across validation and mutation, so a multi-window
// request is all-or-nothing and concurrent callers cannot oversubscribe a
// window.
type ReferenceBudgetMaterializer struct {
	mu          sync.Mutex
	generation  GenerationID
	incarnation IncarnationID
	now         func() time.Time
	buckets     map[referenceBucketKey]*referenceBucket
	operations  map[OperationID]referenceOperation
	batches     map[[32]byte]referenceBatch
}

type referenceBucketKey struct {
	policy string
	window string
	bucket int64
}

type referenceBucket struct {
	limit     pricing.USD
	amounts   map[OperationID]pricing.USD
	reserved  pricing.USD
	accounted pricing.USD
	entries   map[OperationID]referenceEntry
}

type referenceEntry struct {
	reserved     pricing.USD
	accounted    pricing.USD
	expiresAt    time.Time
	status       referenceReservationStatus
	lastRevision int
}

type referenceReservationStatus uint8

const (
	referenceReserved referenceReservationStatus = iota
	referenceAmbiguous
	referenceFinalized
)

type referenceOperation struct {
	fingerprint  [32]byte
	result       ReserveResult
	reservations map[referenceReservationKey]referenceReservation
	events       map[string][32]byte
	fenced       bool
	retainUntil  time.Time
}

type referenceBatch struct {
	fingerprint [32]byte
	requests    []ReserveRequest
	results     []ReserveResult
}

type referenceReservationKey struct {
	policy string
	window string
	bucket int64
}

type referenceReservation struct {
	key              referenceReservationKey
	amount           pricing.USD
	limit            pricing.USD
	bucketStartNanos int64
	expiresAt        time.Time
	revision         int
}

// NewReferenceBudgetMaterializer creates an isolated reference model for one
// immutable Redis generation and incarnation.
func NewReferenceBudgetMaterializer(generation GenerationID, incarnation IncarnationID, now func() time.Time) (*ReferenceBudgetMaterializer, error) {
	if err := generation.Validate(); err != nil {
		return nil, fmt.Errorf("generation: %w", err)
	}
	if err := incarnation.Validate(); err != nil {
		return nil, fmt.Errorf("incarnation: %w", err)
	}
	if now == nil {
		now = time.Now
	}
	return &ReferenceBudgetMaterializer{
		generation:  generation,
		incarnation: incarnation,
		now:         now,
		buckets:     make(map[referenceBucketKey]*referenceBucket),
		operations:  make(map[OperationID]referenceOperation),
		batches:     make(map[[32]byte]referenceBatch),
	}, nil
}

var _ BudgetMaterializer = (*ReferenceBudgetMaterializer)(nil)
var _ BatchBudgetMaterializer = (*ReferenceBudgetMaterializer)(nil)
var _ BatchGrantMaterializer = (*ReferenceBudgetMaterializer)(nil)
var _ BatchMaterializationReader = (*ReferenceBudgetMaterializer)(nil)

func (materializer *ReferenceBudgetMaterializer) Accept(ctx context.Context, request ReserveRequest) (ReserveResult, error) {
	if materializer == nil {
		return ReserveResult{}, errors.New("reference budget materializer is nil")
	}
	if ctx == nil {
		return ReserveResult{}, errors.New("reference budget materializer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return ReserveResult{}, err
	}
	now := materializer.clock()
	reservations, err := canonicalReferenceReservations(request.Reservations)
	if request.IncarnationID == "" {
		request.IncarnationID = materializer.incarnation
	}
	if request.OccurredAt.IsZero() {
		request.OccurredAt = now
	}
	if request.Route != (DispatchRouteFacts{}) {
		if err := request.Route.Validate(); err != nil {
			return ReserveResult{}, fmt.Errorf("reference budget route: %w", err)
		}
	}
	if err != nil {
		return ReserveResult{}, err
	}
	if err := request.OperationID.Validate(); err != nil {
		return ReserveResult{}, err
	}
	if err := request.GenerationID.Validate(); err != nil {
		return ReserveResult{}, err
	}
	if request.GenerationID != materializer.generation {
		return ReserveResult{}, ErrReferenceGenerationMismatch
	}
	if request.IncarnationID != materializer.incarnation {
		return ReserveResult{}, ErrReferenceIncarnationMismatch
	}
	if len(reservations) == 0 {
		return ReserveResult{}, errors.New("reference budget reservation list must not be empty")
	}
	if !request.ExpiresAt.IsZero() && !request.ExpiresAt.After(now) {
		return ReserveResult{}, errors.New("reference budget reservation expiry must be in the future")
	}
	planRequest := request
	if planRequest.ExpiresAt.IsZero() {
		for _, reservation := range reservations {
			if reservation.windowExpiresAt.After(planRequest.ExpiresAt) {
				planRequest.ExpiresAt = reservation.windowExpiresAt
			}
		}
	}
	planned, err := PlannedReserveResult(planRequest)
	if err != nil {
		return ReserveResult{}, err
	}
	fingerprint, err := referenceRequestFingerprint(request, reservations)
	if err != nil {
		return ReserveResult{}, err
	}

	materializer.mu.Lock()
	defer materializer.mu.Unlock()
	materializer.expire(now)
	if existing, ok := materializer.operations[request.OperationID]; ok {
		if existing.fingerprint != fingerprint {
			return ReserveResult{}, ErrReferenceMaterializerConflict
		}
		return cloneReserveResult(existing.result), nil
	}

	// Check every window before changing any state. This is the atomic
	// multi-window admission property that the Redis Function must preserve.
	for _, reservation := range reservations {
		key := referenceBucketKey{policy: reservation.policyID, window: reservation.windowID, bucket: reservation.bucket}
		bucket := materializer.buckets[key]
		active := pricing.MustUSD("0")
		if bucket != nil {
			if bucket.limit.Cmp(reservation.limitUSD) != 0 {
				return ReserveResult{}, ErrReferenceMaterializerConflict
			}
			active, err = bucket.reserved.Add(bucket.accounted)
			if err != nil {
				return ReserveResult{}, err
			}
		}
		remaining, err := reservation.limitUSD.Sub(active)
		if err != nil {
			return ReserveResult{}, err
		}
		if reservation.amountUSD.Cmp(remaining) > 0 {
			result := ReserveResult{
				OperationID:  request.OperationID,
				GenerationID: request.GenerationID,
				Accepted:     false,
				Denial: &admission.Denial{
					PolicyID:     reservation.policyID,
					WindowID:     reservation.windowID,
					LimitUSD:     reservation.limitUSD,
					ActiveUSD:    active,
					RequestedUSD: reservation.amountUSD,
				},
			}
			materializer.operations[request.OperationID] = referenceOperation{fingerprint: fingerprint, result: cloneReserveResult(result), reservations: make(map[referenceReservationKey]referenceReservation), events: make(map[string][32]byte)}
			return result, nil
		}
	}

	result := planned
	operation := referenceOperation{fingerprint: fingerprint, result: result, reservations: make(map[referenceReservationKey]referenceReservation, len(reservations)), events: make(map[string][32]byte)}
	for index, reservation := range reservations {
		key := referenceReservationKey{policy: reservation.policyID, window: reservation.windowID, bucket: reservation.bucket}
		bucketKey := referenceBucketKey(key)
		bucket := materializer.buckets[bucketKey]
		if bucket == nil {
			bucket = &referenceBucket{limit: reservation.limitUSD, amounts: make(map[OperationID]pricing.USD), entries: make(map[OperationID]referenceEntry)}
			materializer.buckets[bucketKey] = bucket
		}
		if bucket.limit.Cmp(reservation.limitUSD) != 0 {
			return ReserveResult{}, ErrReferenceMaterializerConflict
		}
		event := result.Events[index]
		if err := event.Validate(); err != nil {
			return ReserveResult{}, fmt.Errorf("reference reservation event: %w", err)
		}
		bucket.reserved, err = bucket.reserved.Add(reservation.amountUSD)
		if err != nil {
			return ReserveResult{}, err
		}
		bucket.amounts[request.OperationID], err = bucket.amounts[request.OperationID].Add(reservation.amountUSD)
		if err != nil {
			return ReserveResult{}, err
		}
		expiresAt := request.ExpiresAt
		if expiresAt.IsZero() || reservation.windowExpiresAt.Before(expiresAt) {
			expiresAt = reservation.windowExpiresAt
		}
		bucket.entries[request.OperationID] = referenceEntry{reserved: reservation.amountUSD, expiresAt: expiresAt, status: referenceReserved, lastRevision: 1}
		operation.reservations[key] = referenceReservation{key: key, amount: reservation.amountUSD, limit: reservation.limitUSD, bucketStartNanos: reservation.bucketStartNanos, expiresAt: expiresAt, revision: 1}
		operation.result.Events[index] = event
	}
	operation.result = cloneReserveResult(operation.result)
	materializer.operations[request.OperationID] = operation
	return cloneReserveResult(operation.result), nil
}

func (materializer *ReferenceBudgetMaterializer) AcceptBatch(ctx context.Context, contentDigest [32]byte, requests []ReserveRequest) ([]ReserveResult, error) {
	if materializer == nil {
		return nil, errors.New("reference budget materializer is nil")
	}
	if ctx == nil {
		return nil, errors.New("reference budget materializer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := materializer.clock()
	normalized := append([]ReserveRequest(nil), requests...)
	canonical := make([][]canonicalReferenceReservation, len(normalized))
	planned := make([]ReserveResult, len(normalized))
	fingerprints := make([][32]byte, len(normalized))
	batchHash := sha256.New()
	_, _ = batchHash.Write(contentDigest[:])
	for index := range normalized {
		if normalized[index].GenerationID != materializer.generation {
			return nil, ErrReferenceGenerationMismatch
		}
		normalized[index].Reservations = append([]admission.WindowReservation(nil), normalized[index].Reservations...)
		if normalized[index].IncarnationID == "" {
			normalized[index].IncarnationID = materializer.incarnation
		}
		if normalized[index].IncarnationID != materializer.incarnation {
			return nil, ErrReferenceIncarnationMismatch
		}
		if normalized[index].OccurredAt.IsZero() {
			normalized[index].OccurredAt = now
		}
		var err error
		canonical[index], err = canonicalReferenceReservations(normalized[index].Reservations)
		if err != nil {
			return nil, fmt.Errorf("batch reservation operation %d: %w", index, err)
		}
		if normalized[index].ExpiresAt.IsZero() {
			for _, reservation := range canonical[index] {
				if reservation.windowExpiresAt.After(normalized[index].ExpiresAt) {
					normalized[index].ExpiresAt = reservation.windowExpiresAt
				}
			}
		}
		planned[index], err = PlannedReserveResult(normalized[index])
		if err != nil {
			return nil, fmt.Errorf("batch reservation operation %d: %w", index, err)
		}
		fingerprints[index], err = referenceRequestFingerprint(normalized[index], canonical[index])
		if err != nil {
			return nil, err
		}
		_, _ = batchHash.Write(fingerprints[index][:])
	}
	if err := ValidateBatchReserveRequest(contentDigest, normalized); err != nil {
		return nil, err
	}
	var fingerprint [32]byte
	copy(fingerprint[:], batchHash.Sum(nil))

	materializer.mu.Lock()
	defer materializer.mu.Unlock()
	materializer.expire(now)
	if existing, ok := materializer.batches[contentDigest]; ok {
		if existing.fingerprint != fingerprint {
			return nil, ErrReferenceMaterializerConflict
		}
		return cloneReserveResults(existing.results), nil
	}
	for index, request := range normalized {
		if _, exists := materializer.operations[request.OperationID]; exists {
			return nil, fmt.Errorf("batch reservation operation %d: %w", index, ErrReferenceMaterializerConflict)
		}
	}

	type aggregateReservation struct {
		amount pricing.USD
		limit  pricing.USD
	}
	aggregate := make(map[referenceBucketKey]aggregateReservation)
	var denied *admission.Denial
	for operationIndex := range normalized {
		for _, reservation := range canonical[operationIndex] {
			key := referenceBucketKey{policy: reservation.policyID, window: reservation.windowID, bucket: reservation.bucket}
			value := aggregate[key]
			if !value.limit.IsZero() && value.limit.Cmp(reservation.limitUSD) != 0 {
				return nil, ErrReferenceMaterializerConflict
			}
			value.limit = reservation.limitUSD
			var err error
			value.amount, err = value.amount.Add(reservation.amountUSD)
			if err != nil {
				return nil, err
			}
			aggregate[key] = value
		}
	}
	for key, requested := range aggregate {
		active := pricing.MustUSD("0")
		if bucket := materializer.buckets[key]; bucket != nil {
			if bucket.limit.Cmp(requested.limit) != 0 {
				return nil, ErrReferenceMaterializerConflict
			}
			var err error
			active, err = bucket.reserved.Add(bucket.accounted)
			if err != nil {
				return nil, err
			}
		}
		remaining, err := requested.limit.Sub(active)
		if err != nil {
			return nil, err
		}
		if requested.amount.Cmp(remaining) > 0 {
			denied = &admission.Denial{PolicyID: key.policy, WindowID: key.window, LimitUSD: requested.limit, ActiveUSD: active, RequestedUSD: requested.amount}
			break
		}
	}
	if denied != nil {
		results := make([]ReserveResult, len(normalized))
		for index, request := range normalized {
			copyDenial := *denied
			results[index] = ReserveResult{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: request.IncarnationID, Denial: &copyDenial}
		}
		materializer.batches[contentDigest] = referenceBatch{fingerprint: fingerprint, requests: cloneReserveRequests(normalized), results: cloneReserveResults(results)}
		return results, nil
	}

	for operationIndex, request := range normalized {
		result := planned[operationIndex]
		operation := referenceOperation{fingerprint: fingerprints[operationIndex], result: result, reservations: make(map[referenceReservationKey]referenceReservation, len(canonical[operationIndex])), events: make(map[string][32]byte)}
		for reservationIndex, reservation := range canonical[operationIndex] {
			key := referenceReservationKey{policy: reservation.policyID, window: reservation.windowID, bucket: reservation.bucket}
			bucketKey := referenceBucketKey(key)
			bucket := materializer.buckets[bucketKey]
			if bucket == nil {
				bucket = &referenceBucket{limit: reservation.limitUSD, amounts: make(map[OperationID]pricing.USD), entries: make(map[OperationID]referenceEntry)}
				materializer.buckets[bucketKey] = bucket
			}
			var err error
			bucket.reserved, err = bucket.reserved.Add(reservation.amountUSD)
			if err != nil {
				return nil, err
			}
			bucket.amounts[request.OperationID], err = bucket.amounts[request.OperationID].Add(reservation.amountUSD)
			if err != nil {
				return nil, err
			}
			expiresAt := request.ExpiresAt
			if reservation.windowExpiresAt.Before(expiresAt) {
				expiresAt = reservation.windowExpiresAt
			}
			bucket.entries[request.OperationID] = referenceEntry{reserved: reservation.amountUSD, expiresAt: expiresAt, status: referenceReserved, lastRevision: 1}
			operation.reservations[key] = referenceReservation{key: key, amount: reservation.amountUSD, limit: reservation.limitUSD, bucketStartNanos: reservation.bucketStartNanos, expiresAt: expiresAt, revision: 1}
			operation.result.Events[reservationIndex] = result.Events[reservationIndex]
		}
		operation.result = cloneReserveResult(operation.result)
		materializer.operations[request.OperationID] = operation
	}
	materializer.batches[contentDigest] = referenceBatch{fingerprint: fingerprint, requests: cloneReserveRequests(normalized), results: cloneReserveResults(planned)}
	return cloneReserveResults(planned), nil
}

func (materializer *ReferenceBudgetMaterializer) LoadBatchMaterialization(ctx context.Context, contentDigest [32]byte) (BatchMaterialization, bool, error) {
	if materializer == nil || ctx == nil {
		return BatchMaterialization{}, false, errors.New("reference batch materialization reader is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return BatchMaterialization{}, false, err
	}
	if contentDigest == ([32]byte{}) {
		return BatchMaterialization{}, false, errors.New("batch reservation content digest is required")
	}
	materializer.mu.Lock()
	defer materializer.mu.Unlock()
	batch, ok := materializer.batches[contentDigest]
	if !ok {
		return BatchMaterialization{}, false, nil
	}
	if err := ValidateBatchReserveRequest(contentDigest, batch.requests); err != nil {
		return BatchMaterialization{}, false, err
	}
	hash := sha256.New()
	_, _ = hash.Write(contentDigest[:])
	for _, request := range batch.requests {
		if request.GenerationID != materializer.generation {
			return BatchMaterialization{}, false, ErrReferenceGenerationMismatch
		}
		if request.IncarnationID != materializer.incarnation {
			return BatchMaterialization{}, false, ErrReferenceIncarnationMismatch
		}
		reservations, err := canonicalReferenceReservations(request.Reservations)
		if err != nil {
			return BatchMaterialization{}, false, err
		}
		fingerprint, err := referenceRequestFingerprint(request, reservations)
		if err != nil {
			return BatchMaterialization{}, false, err
		}
		_, _ = hash.Write(fingerprint[:])
	}
	var fingerprint [32]byte
	copy(fingerprint[:], hash.Sum(nil))
	if fingerprint != batch.fingerprint {
		return BatchMaterialization{}, false, ErrReferenceMaterializerConflict
	}
	if err := ValidateBatchReserveResult(batch.requests, batch.results); err != nil {
		return BatchMaterialization{}, false, err
	}
	return BatchMaterialization{Requests: cloneReserveRequests(batch.requests), Results: cloneReserveResults(batch.results)}, true, nil
}

func (materializer *ReferenceBudgetMaterializer) ConfirmBatchGrant(ctx context.Context, contentDigest [32]byte, operationID OperationID) (ReserveRequest, ReserveResult, error) {
	if materializer == nil || ctx == nil {
		return ReserveRequest{}, ReserveResult{}, errors.New("reference batch grant materializer is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return ReserveRequest{}, ReserveResult{}, err
	}
	materializer.mu.Lock()
	defer materializer.mu.Unlock()
	materializer.expire(materializer.clock())
	batch, ok := materializer.batches[contentDigest]
	if !ok {
		return ReserveRequest{}, ReserveResult{}, ErrReferenceReservationNotFound
	}
	for index, request := range batch.requests {
		if request.OperationID != operationID || index >= len(batch.results) || !batch.results[index].Accepted {
			continue
		}
		operation, exists := materializer.operations[operationID]
		if !exists || operation.result.Accepted == false {
			return ReserveRequest{}, ReserveResult{}, ErrReferenceReservationNotFound
		}
		return cloneReserveRequest(request), cloneReserveResult(batch.results[index]), nil
	}
	return ReserveRequest{}, ReserveResult{}, ErrReferenceReservationNotFound
}

func (materializer *ReferenceBudgetMaterializer) Confirm(ctx context.Context, request ReserveRequest) (ReserveResult, error) {
	if materializer == nil {
		return ReserveResult{}, errors.New("reference budget materializer is nil")
	}
	if ctx == nil {
		return ReserveResult{}, errors.New("reference budget materializer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return ReserveResult{}, err
	}
	if request.GenerationID != materializer.generation {
		return ReserveResult{}, ErrReferenceGenerationMismatch
	}
	if request.IncarnationID != materializer.incarnation {
		return ReserveResult{}, ErrReferenceIncarnationMismatch
	}
	now := materializer.clock()
	if !request.ExpiresAt.After(now) {
		return ReserveResult{}, ErrReferenceReservationNotFound
	}
	reservations, err := canonicalReferenceReservations(request.Reservations)
	if err != nil {
		return ReserveResult{}, err
	}
	fingerprint, err := referenceRequestFingerprint(request, reservations)
	if err != nil {
		return ReserveResult{}, err
	}
	materializer.mu.Lock()
	defer materializer.mu.Unlock()
	materializer.expire(now)
	existing, ok := materializer.operations[request.OperationID]
	if !ok || !existing.result.Accepted {
		return ReserveResult{}, ErrReferenceReservationNotFound
	}
	if existing.fingerprint != fingerprint {
		return ReserveResult{}, ErrReferenceMaterializerConflict
	}
	result := cloneReserveResult(existing.result)
	if err := ValidatePlannedReserveResult(request, result); err != nil {
		return ReserveResult{}, err
	}
	return result, nil
}

// FenceDispatch is the in-memory model of the atomic Redis dispatch fence.
// The mutex covers identity verification and retention extension together.
func (materializer *ReferenceBudgetMaterializer) FenceDispatch(ctx context.Context, request DispatchFenceRequest) error {
	if materializer == nil {
		return errors.New("reference budget materializer is nil")
	}
	if ctx == nil {
		return errors.New("reference budget materializer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return err
	}
	reservationRequest := request.Reservation
	if reservationRequest.GenerationID != materializer.generation {
		return ErrReferenceGenerationMismatch
	}
	if reservationRequest.IncarnationID != materializer.incarnation {
		return ErrReferenceIncarnationMismatch
	}
	reservations, err := canonicalReferenceReservations(reservationRequest.Reservations)
	if err != nil {
		return err
	}
	fingerprint, err := referenceRequestFingerprint(reservationRequest, reservations)
	if err != nil {
		return err
	}
	now := materializer.clock()
	materializer.mu.Lock()
	defer materializer.mu.Unlock()
	materializer.expire(now)
	operation, ok := materializer.operations[reservationRequest.OperationID]
	if !ok || !operation.result.Accepted {
		return ErrReferenceReservationNotFound
	}
	if operation.fingerprint != fingerprint {
		return ErrReferenceMaterializerConflict
	}
	if !operation.fenced && !reservationRequest.ExpiresAt.After(now) {
		return ErrReferenceReservationNotFound
	}
	for key, reservation := range operation.reservations {
		bucket := materializer.buckets[referenceBucketKey(key)]
		if bucket == nil {
			return ErrReferenceReservationNotFound
		}
		entry, exists := bucket.entries[reservationRequest.OperationID]
		if !exists || entry.status != referenceReserved || entry.lastRevision != reservation.revision {
			return ErrReferenceReservationNotFound
		}
		if request.RetainUntil.After(entry.expiresAt) {
			entry.expiresAt = request.RetainUntil.UTC()
			bucket.entries[reservationRequest.OperationID] = entry
			reservation.expiresAt = request.RetainUntil.UTC()
			operation.reservations[key] = reservation
		}
	}
	operation.fenced = true
	if request.RetainUntil.After(operation.retainUntil) {
		operation.retainUntil = request.RetainUntil.UTC()
	}
	materializer.operations[reservationRequest.OperationID] = operation
	return nil
}

func (materializer *ReferenceBudgetMaterializer) Reconcile(ctx context.Context, request ReconcileRequest) error {
	if materializer == nil {
		return errors.New("reference budget materializer is nil")
	}
	if ctx == nil {
		return errors.New("reference budget materializer context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return err
	}
	if request.GenerationID != materializer.generation {
		return ErrReferenceGenerationMismatch
	}
	if request.IncarnationID != materializer.incarnation {
		return ErrReferenceIncarnationMismatch
	}
	materializer.mu.Lock()
	defer materializer.mu.Unlock()
	materializer.expire(materializer.clock())
	operation, ok := materializer.operations[request.OperationID]
	if !ok || !operation.result.Accepted {
		return ErrReferenceReservationNotFound
	}
	for _, event := range request.Events {
		fingerprint := referenceEventFingerprint(event)
		if existing, seen := operation.events[event.EventID]; seen {
			if existing != fingerprint {
				return ErrReferenceMaterializerConflict
			}
			continue
		}
		var reservationKey referenceReservationKey
		found := false
		for candidate := range operation.reservations {
			reservation := operation.reservations[candidate]
			if candidate.window == event.WindowID && reservation.bucketStartNanos == event.BucketStart.UnixNano() {
				if found {
					return ErrReferenceReservationNotFound
				}
				reservationKey = candidate
				found = true
			}
		}
		if !found {
			return ErrReferenceReservationNotFound
		}
		reservation := operation.reservations[reservationKey]
		if event.ReservationRevision <= reservation.revision {
			return ErrReferenceMaterializerConflict
		}
		bucket := materializer.buckets[referenceBucketKey(reservationKey)]
		if bucket == nil {
			return ErrReferenceReservationNotFound
		}
		entry, ok := bucket.entries[request.OperationID]
		if !ok {
			return ErrReferenceReservationNotFound
		}
		if entry.status == referenceFinalized {
			return ErrReferenceReservationFinalized
		}
		if entry.status == referenceAmbiguous && event.Kind != budget.JournalResolveUnknownExact {
			return ErrReferenceMaterializerConflict
		}
		if entry.status == referenceReserved && event.Kind == budget.JournalResolveUnknownExact {
			return ErrReferenceMaterializerConflict
		}
		if err := applyReferenceCompletion(bucket, request.OperationID, &entry, event); err != nil {
			return err
		}
		switch event.Kind {
		case budget.JournalRetainAmbiguous:
			entry.status = referenceAmbiguous
		case budget.JournalResolveUnknownExact, budget.JournalFinalizeExact, budget.JournalFinalizeUnknown, budget.JournalRelease:
			entry.status = referenceFinalized
		}
		entry.lastRevision = event.ReservationRevision
		bucket.entries[request.OperationID] = entry
		reservation.revision = event.ReservationRevision
		operation.reservations[reservationKey] = reservation
		operation.events[event.EventID] = fingerprint
	}
	materializer.operations[request.OperationID] = operation
	return nil
}

func applyReferenceCompletion(bucket *referenceBucket, operationID OperationID, entry *referenceEntry, event budget.CompletionEvent) error {
	if entry == nil {
		return ErrReferenceReservationNotFound
	}
	var err error
	if !event.ReservedDecreaseUSD.IsZero() {
		bucket.reserved, err = bucket.reserved.Sub(event.ReservedDecreaseUSD)
		if err != nil {
			return err
		}
		entry.reserved, err = entry.reserved.Sub(event.ReservedDecreaseUSD)
		if err != nil {
			return err
		}
	}
	if !event.AccountedIncreaseUSD.IsZero() {
		bucket.accounted, err = bucket.accounted.Add(event.AccountedIncreaseUSD)
		if err != nil {
			return err
		}
		entry.accounted, err = entry.accounted.Add(event.AccountedIncreaseUSD)
		if err != nil {
			return err
		}
	}
	if !event.AccountedDecreaseUSD.IsZero() {
		bucket.accounted, err = bucket.accounted.Sub(event.AccountedDecreaseUSD)
		if err != nil {
			return err
		}
		entry.accounted, err = entry.accounted.Sub(event.AccountedDecreaseUSD)
		if err != nil {
			return err
		}
	}
	if entry.reserved.IsZero() {
		delete(bucket.amounts, operationID)
	} else {
		bucket.amounts[operationID] = entry.reserved
	}
	return nil
}

func (materializer *ReferenceBudgetMaterializer) clock() time.Time {
	now := materializer.now()
	if now.IsZero() {
		return time.Now().UTC()
	}
	return now.UTC()
}

func (materializer *ReferenceBudgetMaterializer) expire(now time.Time) {
	for key, bucket := range materializer.buckets {
		for operationID, entry := range bucket.entries {
			if !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
				bucket.reserved, _ = bucket.reserved.Sub(entry.reserved)
				bucket.accounted, _ = bucket.accounted.Sub(entry.accounted)
				delete(bucket.entries, operationID)
				delete(bucket.amounts, operationID)
			}
		}
		if len(bucket.entries) == 0 {
			delete(materializer.buckets, key)
		}
	}
}

type canonicalReferenceReservation struct {
	policyID         string
	windowID         string
	bucket           int64
	amountUSD        pricing.USD
	limitUSD         pricing.USD
	bucketNanos      int64
	durationNanos    int64
	bucketStartNanos int64
	windowExpiresAt  time.Time
}

func canonicalReferenceReservations(values []admission.WindowReservation) ([]canonicalReferenceReservation, error) {
	result := make([]canonicalReferenceReservation, 0, len(values))
	seen := make(map[referenceReservationKey]struct{}, len(values))
	seenStarts := make(map[struct {
		window string
		start  int64
	}]struct{}, len(values))
	for index, value := range values {
		if value.PolicyID == "" || value.WindowID == "" || len(value.PolicyID) > 128 || len(value.WindowID) > 128 {
			return nil, fmt.Errorf("reservation %d has unsafe policy/window identity", index)
		}
		if value.Bucket < 0 || value.BucketNanos <= 0 || value.DurationNanos < value.BucketNanos || value.DurationNanos > int64(time.Duration(1<<62)) {
			return nil, fmt.Errorf("reservation %d has invalid bucket bounds", index)
		}
		if value.Bucket > (1<<62)/value.BucketNanos {
			return nil, fmt.Errorf("reservation %d bucket overflows timestamp", index)
		}
		amount, err := referenceAmount(value.AmountUSD, value.Amount)
		if err != nil {
			return nil, fmt.Errorf("reservation %d amount: %w", index, err)
		}
		limit, err := referenceAmount(value.LimitUSD, value.Limit)
		if err != nil || limit.IsZero() {
			if err == nil {
				err = errors.New("limit must be positive")
			}
			return nil, fmt.Errorf("reservation %d limit: %w", index, err)
		}
		if amount.IsZero() || amount.Cmp(limit) > 0 {
			return nil, fmt.Errorf("reservation %d amount must be positive and no greater than limit", index)
		}
		key := referenceReservationKey{policy: value.PolicyID, window: value.WindowID, bucket: value.Bucket}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("reservation %d duplicates policy/window/bucket", index)
		}
		for existing := range seen {
			if existing.window == key.window && existing.bucket == key.bucket {
				return nil, fmt.Errorf("reservation %d duplicates window/bucket without a policy discriminator", index)
			}
		}
		seen[key] = struct{}{}
		bucketStart := value.Bucket * value.BucketNanos
		startKey := struct {
			window string
			start  int64
		}{window: value.WindowID, start: bucketStart}
		if _, exists := seenStarts[startKey]; exists {
			return nil, fmt.Errorf("reservation %d duplicates window/bucket start", index)
		}
		seenStarts[startKey] = struct{}{}
		windowExpires := time.Unix(0, bucketStart).Add(time.Duration(value.DurationNanos)).UTC()
		result = append(result, canonicalReferenceReservation{policyID: value.PolicyID, windowID: value.WindowID, bucket: value.Bucket, amountUSD: amount, limitUSD: limit, bucketNanos: value.BucketNanos, durationNanos: value.DurationNanos, bucketStartNanos: bucketStart, windowExpiresAt: windowExpires})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].policyID != result[j].policyID {
			return result[i].policyID < result[j].policyID
		}
		if result[i].windowID != result[j].windowID {
			return result[i].windowID < result[j].windowID
		}
		return result[i].bucket < result[j].bucket
	})
	return result, nil
}

func referenceAmount(exact pricing.USD, legacy pricing.MicroUSD) (pricing.USD, error) {
	if !exact.IsZero() {
		if err := exact.Validate(); err != nil {
			return pricing.USD{}, err
		}
		return exact, nil
	}
	if !legacy.Valid() {
		return pricing.USD{}, errors.New("amount is outside Redis-safe range")
	}
	return pricing.USDFromMicro(legacy)
}

func referenceRequestFingerprint(request ReserveRequest, reservations []canonicalReferenceReservation) ([32]byte, error) {
	type wireReservation struct {
		PolicyID, WindowID         string
		Bucket                     int64
		Amount, Limit              string
		BucketNanos, DurationNanos int64
	}
	wire := struct {
		OperationID, GenerationID, IncarnationID string
		ExpiresAt, OccurredAt                    time.Time
		Route                                    DispatchRouteFacts
		Bounds                                   ReservationBounds
		LogicalCostUSD                           string
		Reservations                             []wireReservation
	}{OperationID: string(request.OperationID), GenerationID: string(request.GenerationID), IncarnationID: string(request.IncarnationID), ExpiresAt: request.ExpiresAt.UTC(), OccurredAt: request.OccurredAt.UTC(), Route: request.Route, Bounds: request.Bounds, LogicalCostUSD: request.LogicalCostUSD.String(), Reservations: make([]wireReservation, len(reservations))}
	for index, reservation := range reservations {
		wire.Reservations[index] = wireReservation{reservation.policyID, reservation.windowID, reservation.bucket, reservation.amountUSD.String(), reservation.limitUSD.String(), reservation.bucketNanos, reservation.durationNanos}
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(data), nil
}

func referenceEventFingerprint(event budget.CompletionEvent) [32]byte {
	data, _ := json.Marshal(event)
	return sha256.Sum256(data)
}

func cloneReserveResult(result ReserveResult) ReserveResult {
	result.Events = append([]budget.ReservationEvent(nil), result.Events...)
	if result.Denial != nil {
		denial := *result.Denial
		result.Denial = &denial
	}
	return result
}

func cloneReserveResults(results []ReserveResult) []ReserveResult {
	cloned := make([]ReserveResult, len(results))
	for index := range results {
		cloned[index] = cloneReserveResult(results[index])
	}
	return cloned
}

func cloneReserveRequest(request ReserveRequest) ReserveRequest {
	request.Reservations = append([]admission.WindowReservation(nil), request.Reservations...)
	return request
}

func cloneReserveRequests(requests []ReserveRequest) []ReserveRequest {
	cloned := make([]ReserveRequest, len(requests))
	for index := range requests {
		cloned[index] = cloneReserveRequest(requests[index])
	}
	return cloned
}
