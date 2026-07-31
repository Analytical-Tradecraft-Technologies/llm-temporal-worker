package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

type budgetKey struct {
	policy string
	window string
}

type AdmissionStore struct {
	mu          sync.Mutex
	clock       func() time.Time
	operations  map[string]admission.Operation
	providerIDs map[string]string
	pollAfter   map[string]time.Time
	byScope     map[string]string
	buckets     map[budgetKey]map[int64]pricing.MicroUSD
}

type AdmissionOptions struct{ Clock func() time.Time }

func NewAdmissionStore(options AdmissionOptions) *AdmissionStore {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	return &AdmissionStore{clock: options.Clock, operations: make(map[string]admission.Operation), providerIDs: make(map[string]string), pollAfter: make(map[string]time.Time), byScope: make(map[string]string), buckets: make(map[budgetKey]map[int64]pricing.MicroUSD)}
}

// MarkProviderPending mirrors the durable repository transition used by
// resumable adapters. The memory store exists primarily for engine tests, but
// implementing the optional seam keeps those tests faithful to production.
func (store *AdmissionStore) MarkProviderPending(ctx context.Context, request admission.ProviderPendingRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if request.ProviderOperationID == "" || request.EndpointID == "" {
		return fmt.Errorf("provider operation id and endpoint are required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	operation, err := store.loadToken(request.OperationID, request.DispatchToken)
	if err != nil {
		return err
	}
	if operation.State == admission.StateProviderPending {
		if store.providerIDs[operation.ID] == request.ProviderOperationID {
			return nil
		}
		return admission.ErrOperationConflict
	}
	if operation.State != admission.StateDispatching {
		return admission.ErrInvalidTransition
	}
	operation.State = admission.StateProviderPending
	operation.Attempt.EndpointID = request.EndpointID
	operation.Attempt.Provider = request.Provider
	operation.Attempt.Dispatch = admission.Accepted
	operation.UpdatedAt = store.clock()
	store.operations[operation.ID] = operation
	store.providerIDs[operation.ID] = request.ProviderOperationID
	store.pollAfter[operation.ID] = request.PollAfter
	return nil
}

func (store *AdmissionStore) ProviderOperation(ctx context.Context, id string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.operations[id]; !ok {
		return "", admission.ErrOperationNotFound
	}
	providerID := store.providerIDs[id]
	if providerID == "" {
		return "", fmt.Errorf("provider operation envelope is incomplete")
	}
	return providerID, nil
}

// ProviderPollAfter returns the provider's persisted next-poll guidance for a
// pending operation. A zero time means that the provider supplied no delay.
func (store *AdmissionStore) ProviderPollAfter(ctx context.Context, id string) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.operations[id]; !ok {
		return time.Time{}, admission.ErrOperationNotFound
	}
	if _, ok := store.providerIDs[id]; !ok {
		return time.Time{}, fmt.Errorf("provider operation envelope is incomplete")
	}
	return store.pollAfter[id], nil
}

func (store *AdmissionStore) Begin(ctx context.Context, request admission.BeginRequest) (admission.BeginResult, error) {
	if err := ctx.Err(); err != nil {
		return admission.BeginResult{}, err
	}
	if request.ID == "" || request.ScopeKey == "" || request.Reservation < 0 || !request.Reservation.Valid() {
		return admission.BeginResult{}, fmt.Errorf("invalid admission begin request")
	}
	if err := admission.ValidateExactUSDReservationEnvelope(request.Reservations, request.Reservation, request.ReservationUSD); err != nil {
		return admission.BeginResult{}, fmt.Errorf("invalid admission begin request: %w", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if operationID, ok := store.byScope[request.ScopeKey]; ok {
		operation := store.operations[operationID]
		if operation.RequestDigest != request.RequestDigest {
			return admission.BeginResult{}, admission.ErrOperationConflict
		}
		return admission.BeginResult{Operation: operation.Clone(), Existing: true}, nil
	}
	if denial := store.checkReservations(request.Reservations, store.clock()); denial != nil {
		return admission.BeginResult{Denied: denial}, nil
	}
	for _, reservation := range request.Reservations {
		if err := store.addReservation(reservation, reservation.Amount); err != nil {
			return admission.BeginResult{}, err
		}
	}
	now := store.clock()
	token := request.ID
	if token == "" {
		digest := sha256.Sum256([]byte(request.ScopeKey))
		token = hex.EncodeToString(digest[:])
	}
	operation := admission.Operation{ID: request.ID, ScopeKey: request.ScopeKey, RequestDigest: request.RequestDigest, State: admission.StateReserved, ReservedMicroUSD: request.Reservation, Reservations: cloneReservations(request.Reservations), ConfigVersion: request.ConfigVersion, PriceVersion: request.PriceVersion, DispatchToken: token, LeaseUntil: request.LeaseUntil, CreatedAt: now, UpdatedAt: now, ExpiresAt: request.ExpiresAt}
	if !request.ReservationUSD.IsZero() || request.Reservation == 0 {
		reserved := request.ReservationUSD
		operation.ReservedCostUSD = &reserved
	}
	store.operations[operation.ID] = operation
	store.byScope[operation.ScopeKey] = operation.ID
	return admission.BeginResult{Operation: operation.Clone()}, nil
}

func (store *AdmissionStore) MarkDispatching(ctx context.Context, request admission.DispatchRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	operation, ok := store.operations[request.OperationID]
	if !ok {
		return admission.ErrOperationNotFound
	}
	if operation.DispatchToken != request.DispatchToken {
		return admission.ErrInvalidToken
	}
	if operation.State != admission.StateReserved {
		if operation.State.Terminal() {
			return nil
		}
		return admission.ErrInvalidTransition
	}
	operation.State = admission.StateDispatching
	operation.Attempt = request.Attempt
	operation.Attempt.AttemptNumber++
	operation.LeaseUntil = request.LeaseUntil
	operation.UpdatedAt = store.clock()
	store.operations[operation.ID] = operation
	return nil
}

func (store *AdmissionStore) Complete(ctx context.Context, request admission.CompleteRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	operation, err := store.loadToken(request.OperationID, request.DispatchToken)
	if err != nil {
		return err
	}
	if operation.State == admission.StateCompleted {
		return nil
	}
	if operation.State != admission.StateDispatching && operation.State != admission.StateProviderPending {
		return admission.ErrInvalidTransition
	}
	if request.Actual < 0 || !request.Actual.Valid() {
		return fmt.Errorf("invalid actual cost")
	}
	if err := store.reconcile(operation.Reservations, operation.ReservedMicroUSD, request.Actual); err != nil {
		return err
	}
	operation.State = admission.StateCompleted
	operation.IncurredMicroUSD = request.Actual
	operation.FinalMicroUSD = request.Actual
	if !request.ActualCostUSD.IsZero() || request.Actual == 0 {
		actual := request.ActualCostUSD
		operation.ActualCostUSD = &actual
		operation.IncurredCostUSD = &actual
	}
	operation.ReservedMicroUSD = 0
	operation.ResultRef = cloneBlobRef(request.ResultRef)
	operation.Attempt = request.Attempt
	operation.Attempt.Dispatch = admission.Accepted
	operation.UpdatedAt = store.clock()
	store.operations[operation.ID] = operation
	return nil
}

func (store *AdmissionStore) Fail(ctx context.Context, request admission.FailRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	operation, err := store.loadToken(request.OperationID, request.DispatchToken)
	if err != nil {
		return err
	}
	if operation.State.Terminal() {
		return nil
	}
	if err := admission.ValidateOutcome(admission.AttemptOutcome{Certainty: request.Certainty, Incurred: request.Incurred}); err != nil {
		return err
	}
	retain := request.Certainty == admission.Accepted || request.Certainty == admission.Ambiguous
	if retain {
		operation.State = admission.StateAmbiguous
		operation.IncurredMicroUSD = request.Incurred
		operation.FinalMicroUSD = operation.ReservedMicroUSD
		if !request.IncurredCostUSD.IsZero() || request.Incurred == 0 {
			incurred := request.IncurredCostUSD
			operation.IncurredCostUSD = &incurred
		}
	} else {
		if err := store.reconcile(operation.Reservations, operation.ReservedMicroUSD, request.Incurred); err != nil {
			return err
		}
		operation.State = admission.StateDefiniteFailed
		operation.IncurredMicroUSD = request.Incurred
		operation.FinalMicroUSD = request.Incurred
		if !request.IncurredCostUSD.IsZero() || request.Incurred == 0 {
			incurred := request.IncurredCostUSD
			operation.IncurredCostUSD = &incurred
			operation.ActualCostUSD = &incurred
		}
		operation.ReservedMicroUSD = 0
	}
	operation.Attempt = request.Attempt
	operation.Attempt.Dispatch = request.Certainty
	operation.UpdatedAt = store.clock()
	store.operations[operation.ID] = operation
	return nil
}

func (store *AdmissionStore) Continue(ctx context.Context, request admission.ContinueRequest) (admission.ContinueResult, error) {
	if err := ctx.Err(); err != nil {
		return admission.ContinueResult{}, err
	}
	if err := admission.ValidateExactUSDReservationEnvelope(request.Reservations, request.Remaining, request.RemainingUSD); err != nil {
		return admission.ContinueResult{}, fmt.Errorf("invalid continuation admission request: %w", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	operation, err := store.loadToken(request.OperationID, request.DispatchToken)
	if err != nil {
		return admission.ContinueResult{}, err
	}
	if operation.State != admission.StateDispatching {
		return admission.ContinueResult{}, admission.ErrInvalidTransition
	}
	if request.Outcome.Certainty == admission.Accepted || request.Outcome.Certainty == admission.Ambiguous {
		return admission.ContinueResult{}, fmt.Errorf("cannot continue an accepted or ambiguous attempt")
	}
	if err := store.reconcile(operation.Reservations, operation.ReservedMicroUSD, request.Outcome.Incurred); err != nil {
		return admission.ContinueResult{}, err
	}
	if denial := store.checkReservations(request.Reservations, store.clock()); denial != nil {
		operation.State = admission.StateDefiniteFailed
		operation.IncurredMicroUSD = request.Outcome.Incurred
		operation.FinalMicroUSD = request.Outcome.Incurred
		operation.ReservedMicroUSD = 0
		operation.UpdatedAt = store.clock()
		store.operations[operation.ID] = operation
		return admission.ContinueResult{Operation: operation.Clone(), Denied: denial}, nil
	}
	for _, reservation := range request.Reservations {
		if err := store.addReservation(reservation, reservation.Amount); err != nil {
			return admission.ContinueResult{}, err
		}
	}
	operation.State = admission.StateReserved
	operation.Reservations = cloneReservations(request.Reservations)
	operation.ReservedMicroUSD = request.Remaining
	if !request.RemainingUSD.IsZero() || request.Remaining == 0 {
		remaining := request.RemainingUSD
		operation.ReservedCostUSD = &remaining
	}
	operation.Attempt = request.Outcome.Attempt
	operation.Attempt.Dispatch = admission.NotDispatched
	operation.DispatchToken = fmt.Sprintf("%s-%d", operation.DispatchToken, operation.Attempt.AttemptNumber+1)
	operation.LeaseUntil = request.LeaseUntil
	operation.ExpiresAt = request.ExpiresAt
	operation.UpdatedAt = store.clock()
	store.operations[operation.ID] = operation
	return admission.ContinueResult{Operation: operation.Clone()}, nil
}

func (store *AdmissionStore) Get(ctx context.Context, id string) (admission.Operation, error) {
	if err := ctx.Err(); err != nil {
		return admission.Operation{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	operation, ok := store.operations[id]
	if !ok {
		return admission.Operation{}, admission.ErrOperationNotFound
	}
	return operation.Clone(), nil
}

func (store *AdmissionStore) loadToken(id, token string) (admission.Operation, error) {
	operation, ok := store.operations[id]
	if !ok {
		return admission.Operation{}, admission.ErrOperationNotFound
	}
	if operation.DispatchToken != token {
		return admission.Operation{}, admission.ErrInvalidToken
	}
	return operation, nil
}

func (store *AdmissionStore) checkReservations(reservations []admission.WindowReservation, at time.Time) *admission.Denial {
	for _, reservation := range reservations {
		amount := reservation.Amount
		if reservation.Limit <= 0 || reservation.BucketNanos <= 0 || reservation.DurationNanos <= 0 {
			return &admission.Denial{PolicyID: reservation.PolicyID, WindowID: reservation.WindowID, Limit: reservation.Limit, Requested: amount}
		}
		key := budgetKey{policy: reservation.PolicyID, window: reservation.WindowID}
		buckets := store.buckets[key]
		active := pricing.MicroUSD(0)
		first := floorDiv(at.UnixNano()-reservation.DurationNanos, reservation.BucketNanos)
		last := floorDiv(at.UnixNano(), reservation.BucketNanos)
		for index := first; index <= last; index++ {
			value := buckets[index]
			var err error
			active, err = active.Add(value)
			if err != nil {
				return &admission.Denial{PolicyID: reservation.PolicyID, WindowID: reservation.WindowID, Limit: reservation.Limit, Active: active, Requested: amount}
			}
		}
		if active > reservation.Limit || amount > reservation.Limit-active {
			return &admission.Denial{PolicyID: reservation.PolicyID, WindowID: reservation.WindowID, Limit: reservation.Limit, Active: active, Requested: amount}
		}
	}
	return nil
}

func (store *AdmissionStore) addReservation(reservation admission.WindowReservation, amount pricing.MicroUSD) error {
	if amount < 0 || !amount.Valid() {
		return fmt.Errorf("invalid reservation amount")
	}
	key := budgetKey{policy: reservation.PolicyID, window: reservation.WindowID}
	if store.buckets[key] == nil {
		store.buckets[key] = make(map[int64]pricing.MicroUSD)
	}
	current := store.buckets[key][reservation.Bucket]
	next, err := current.Add(amount)
	if err != nil {
		return err
	}
	store.buckets[key][reservation.Bucket] = next
	return nil
}

func (store *AdmissionStore) reconcile(reservations []admission.WindowReservation, reserved, actual pricing.MicroUSD) error {
	if actual < 0 || !actual.Valid() {
		return fmt.Errorf("invalid reconciliation cost")
	}
	for _, reservation := range reservations {
		if err := store.addReservation(reservation, pricing.MicroUSD(0)); err != nil {
			return err
		}
		current := store.buckets[budgetKey{policy: reservation.PolicyID, window: reservation.WindowID}][reservation.Bucket]
		if current < reservation.Amount {
			return admission.ErrStateUnavailable
		}
		current -= reservation.Amount
		if actual > 0 {
			var err error
			current, err = current.Add(actual)
			if err != nil {
				return err
			}
		}
		store.buckets[budgetKey{policy: reservation.PolicyID, window: reservation.WindowID}][reservation.Bucket] = current
	}
	_ = reserved
	return nil
}

func floorDiv(value, divisor int64) int64 {
	quotient, remainder := value/divisor, value%divisor
	if remainder != 0 && ((remainder < 0) != (divisor < 0)) {
		quotient--
	}
	return quotient
}

func cloneReservations(values []admission.WindowReservation) []admission.WindowReservation {
	return append([]admission.WindowReservation(nil), values...)
}

func cloneBlobRef(value *state.BlobRef) *state.BlobRef {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}
