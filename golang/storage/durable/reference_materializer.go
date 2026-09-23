package durable

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

var (
	ErrReferenceMaterializerConflict = errors.New("reference budget materializer idempotency conflict")
	ErrReferenceGenerationMismatch   = errors.New("reference budget materializer generation mismatch")
	ErrReferenceIncarnationMismatch  = errors.New("reference budget materializer incarnation mismatch")
	ErrReferenceReservationNotFound  = errors.New("reference budget reservation is not known")
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
	claimed      bool
	startBy      time.Time
	fingerprint  [32]byte
	result       ReserveResult
	reservations map[referenceReservationKey]referenceReservation
	events       map[string][32]byte
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
	windowExpiresAt  time.Time
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
	}, nil
}

var _ BudgetLeaser = (*ReferenceBudgetMaterializer)(nil)

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
	if len(reservations) == 0 {
		return ReserveResult{}, errors.New("reference budget reservation list must not be empty")
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

	if !request.ExpiresAt.IsZero() && !request.ExpiresAt.After(now) {
		return ReserveResult{}, ErrLeaseExpired
	}

	// Check every window before changing any state. This is the atomic
	// multi-window admission property that the Redis Function must preserve.
	type windowKey struct{ policy, window string }
	activeByWindow := make(map[windowKey]pricing.USD)
	limitByWindow := make(map[windowKey]pricing.USD)
	for _, reservation := range reservations {
		key := windowKey{reservation.policyID, reservation.windowID}
		active, staged := activeByWindow[key]
		if staged {
			if limitByWindow[key].Cmp(reservation.limitUSD) != 0 {
				return ReserveResult{}, ErrReferenceMaterializerConflict
			}
		} else {
			active = pricing.MustUSD("0")
			for bucketKey, bucket := range materializer.buckets {
				if bucketKey.policy != key.policy || bucketKey.window != key.window {
					continue
				}
				for _, amount := range []pricing.USD{bucket.reserved, bucket.accounted} {
					active, err = active.Add(amount)
					if err != nil {
						return ReserveResult{}, err
					}
				}
			}
		}
		remaining := reservation.limitUSD.SubOrZero(active)
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
			result.RetryAfter = time.Second
			return result, nil
		}
		// Stage this request's earlier buckets without publishing partial state.
		activeByWindow[key], err = active.Add(reservation.amountUSD)
		if err != nil {
			return ReserveResult{}, err
		}
		limitByWindow[key] = reservation.limitUSD
	}

	result := ReserveResult{OperationID: request.OperationID, Accepted: true, GenerationID: request.GenerationID, IncarnationID: materializer.incarnation, Events: make([]budget.ReservationEvent, 0, len(reservations))}
	operation := referenceOperation{startBy: now.Add(BudgetStartLease), fingerprint: fingerprint, result: result, reservations: make(map[referenceReservationKey]referenceReservation, len(reservations)), events: make(map[string][32]byte)}
	for _, reservation := range reservations {
		key := referenceReservationKey{policy: reservation.policyID, window: reservation.windowID, bucket: reservation.bucket}
		bucketKey := referenceBucketKey(key)
		bucket := materializer.buckets[bucketKey]
		if bucket == nil {
			bucket = &referenceBucket{limit: reservation.limitUSD, amounts: make(map[OperationID]pricing.USD), entries: make(map[OperationID]referenceEntry)}
			materializer.buckets[bucketKey] = bucket
		}
		event := budget.ReservationEvent{
			EventID:             referenceEventID(request.OperationID, request.GenerationID, key, 1),
			GenerationID:        string(request.GenerationID),
			OperationID:         string(request.OperationID),
			WindowID:            reservation.windowID,
			BucketStart:         time.Unix(0, reservation.bucketStartNanos).UTC(),
			ReservationRevision: 1,
			AmountUSD:           reservation.amountUSD,
			OccurredAt:          now,
		}
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
		expiresAt := operation.startBy
		if !request.ExpiresAt.IsZero() && request.ExpiresAt.Before(expiresAt) {
			expiresAt = request.ExpiresAt
		}
		operation.startBy = expiresAt
		bucket.entries[request.OperationID] = referenceEntry{reserved: reservation.amountUSD, expiresAt: expiresAt, status: referenceReserved, lastRevision: 1}
		operation.reservations[key] = referenceReservation{key: key, amount: reservation.amountUSD, limit: reservation.limitUSD, bucketStartNanos: reservation.bucketStartNanos, expiresAt: expiresAt, windowExpiresAt: reservation.windowExpiresAt, revision: 1}
		operation.result.Events = append(operation.result.Events, event)
	}
	operation.result = cloneReserveResult(operation.result)
	materializer.operations[request.OperationID] = operation
	return cloneReserveResult(operation.result), nil
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
	operation, ok := materializer.operations[request.OperationID]
	if !ok || !operation.result.Accepted {
		return ErrReferenceReservationNotFound
	}
	operation.events = maps.Clone(operation.events)
	operation.reservations = maps.Clone(operation.reservations)
	stagedBuckets := make(map[referenceBucketKey]*referenceBucket)
	seen := make(map[referenceReservationKey]bool)
	materializer.expire(materializer.clock())
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
		if seen[reservationKey] {
			return ErrReferenceMaterializerConflict
		}
		seen[reservationKey] = true
		reservation := operation.reservations[reservationKey]
		if event.ReservationRevision <= reservation.revision {
			return ErrReferenceMaterializerConflict
		}
		bucketKey := referenceBucketKey(reservationKey)
		bucket := stagedBuckets[bucketKey]
		if bucket == nil {
			original := materializer.buckets[bucketKey]
			if original == nil {
				return ErrReferenceReservationNotFound
			}
			copyBucket := *original
			copyBucket.entries = maps.Clone(original.entries)
			copyBucket.amounts = maps.Clone(original.amounts)
			bucket = &copyBucket
			stagedBuckets[bucketKey] = bucket
		}
		entry, ok := bucket.entries[request.OperationID]
		if !ok {
			return ErrReferenceReservationNotFound
		}
		if event.Kind != budget.JournalRelease && !operation.claimed {
			return ErrClaimRequired
		}
		if event.Kind == budget.JournalRelease && operation.claimed {
			return ErrReferenceMaterializerConflict
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
		if event.Kind != budget.JournalRetainAmbiguous && (event.ReservedDecreaseUSD.Cmp(entry.reserved) != 0 || event.AccountedDecreaseUSD.Cmp(entry.accounted) != 0) {
			return ErrReferenceMaterializerConflict
		}
		if err := applyReferenceCompletion(bucket, request.OperationID, &entry, event); err != nil {
			return err
		}
		switch event.Kind {
		case budget.JournalRetainAmbiguous:
			entry.status = referenceAmbiguous
			entry.expiresAt = time.Time{}
		case budget.JournalFinalizeUnknown:
			entry.status = referenceAmbiguous
			entry.expiresAt = reservation.windowExpiresAt
		case budget.JournalResolveUnknownExact, budget.JournalFinalizeExact, budget.JournalRelease:
			entry.status = referenceFinalized
			entry.expiresAt = reservation.windowExpiresAt
		}
		entry.lastRevision = event.ReservationRevision
		bucket.entries[request.OperationID] = entry
		reservation.revision = event.ReservationRevision
		operation.reservations[reservationKey] = reservation
		operation.events[event.EventID] = fingerprint
	}
	for key, bucket := range stagedBuckets {
		materializer.buckets[key] = bucket
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
		if amount.IsZero() {
			return nil, fmt.Errorf("reservation %d amount must be positive", index)
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
		windowExpires := time.Unix(0, bucketStart).Add(time.Duration(value.DurationNanos)).Add(time.Duration(value.BucketNanos)).UTC()
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
		OperationID, GenerationID string
		ExpiresAt                 time.Time
		Reservations              []wireReservation
	}{OperationID: string(request.OperationID), GenerationID: string(request.GenerationID), ExpiresAt: request.ExpiresAt.UTC(), Reservations: make([]wireReservation, len(reservations))}
	for index, reservation := range reservations {
		wire.Reservations[index] = wireReservation{reservation.policyID, reservation.windowID, reservation.bucket, reservation.amountUSD.String(), reservation.limitUSD.String(), reservation.bucketNanos, reservation.durationNanos}
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(data), nil
}

func referenceEventID(operation OperationID, generation GenerationID, key referenceReservationKey, revision int) string {
	data := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d\x00%d", operation, generation, key.policy, key.window, key.bucket, revision)
	digest := sha256.Sum256([]byte(data))
	return hex.EncodeToString(digest[:])
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

func (materializer *ReferenceBudgetMaterializer) Claim(ctx context.Context, request ClaimRequest) (ClaimReceipt, error) {
	if materializer == nil || ctx == nil {
		return ClaimReceipt{}, ErrBudgetBoundaryInvalid
	}
	if err := ctx.Err(); err != nil {
		return ClaimReceipt{}, err
	}
	if err := request.Validate(); err != nil {
		return ClaimReceipt{}, err
	}
	materializer.mu.Lock()
	defer materializer.mu.Unlock()
	if request.GenerationID != materializer.generation {
		return ClaimReceipt{}, ErrReferenceGenerationMismatch
	}
	if request.IncarnationID != materializer.incarnation {
		return ClaimReceipt{}, ErrReferenceIncarnationMismatch
	}
	operation, ok := materializer.operations[request.OperationID]
	if !ok || !operation.result.Accepted {
		return ClaimReceipt{}, ErrReferenceReservationNotFound
	}
	if operation.claimed {
		return ClaimReceipt{}, ErrAlreadyClaimed
	}
	if !materializer.clock().Before(operation.startBy) {
		return ClaimReceipt{}, ErrLeaseExpired
	}
	for key := range operation.reservations {
		bucket := materializer.buckets[referenceBucketKey(key)]
		if bucket == nil {
			return ClaimReceipt{}, ErrReferenceReservationFinalized
		}
		entry, exists := bucket.entries[request.OperationID]
		if !exists || entry.status != referenceReserved {
			return ClaimReceipt{}, ErrReferenceReservationFinalized
		}
	}
	for key := range operation.reservations {
		bucket := materializer.buckets[referenceBucketKey(key)]
		entry := bucket.entries[request.OperationID]
		entry.expiresAt = time.Time{}
		bucket.entries[request.OperationID] = entry
	}
	operation.claimed = true
	materializer.operations[request.OperationID] = operation
	return ClaimReceipt{OperationID: request.OperationID, GenerationID: request.GenerationID, IncarnationID: request.IncarnationID}, nil
}
