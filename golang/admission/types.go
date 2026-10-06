package admission

import (
	"crypto/sha256"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

type OperationState string

const (
	StateReserved        OperationState = "reserved"
	StateDispatching     OperationState = "dispatching"
	StateProviderPending OperationState = "provider_pending"
	StateCompleted       OperationState = "completed"
	StateDefiniteFailed  OperationState = "definite_failed"
	StateAmbiguous       OperationState = "ambiguous"
	StateCanceled        OperationState = "canceled"
)

func (state OperationState) Terminal() bool {
	return state == StateCompleted || state == StateDefiniteFailed || state == StateAmbiguous || state == StateCanceled
}

type DispatchCertainty string

const (
	NotDispatched DispatchCertainty = "not_dispatched"
	Rejected      DispatchCertainty = "rejected"
	Accepted      DispatchCertainty = "accepted"
	Ambiguous     DispatchCertainty = "ambiguous"
)

type WindowReservation struct {
	PolicyID      string
	WindowID      string
	Bucket        int64
	Amount        pricing.MicroUSD
	Limit         pricing.MicroUSD
	AmountUSD     pricing.USD
	LimitUSD      pricing.USD
	BucketNanos   int64
	DurationNanos int64
}

type AttemptFacts struct {
	RouteID           string
	EndpointID        string
	Provider          string
	ResolvedModel     string
	ProviderRequestID string
	ServiceClass      string
	Dispatch          DispatchCertainty
	AttemptNumber     int
}

type Operation struct {
	ID               string
	ScopeKey         string
	RequestDigest    [32]byte
	State            OperationState
	ReservedMicroUSD pricing.MicroUSD
	IncurredMicroUSD pricing.MicroUSD
	FinalMicroUSD    pricing.MicroUSD
	ReservedCostUSD  *pricing.USD
	IncurredCostUSD  *pricing.USD
	ActualCostUSD    *pricing.USD
	Reservations     []WindowReservation
	ConfigVersion    string
	PriceVersion     string
	Attempt          AttemptFacts
	ResultRef        *state.BlobRef
	DispatchToken    string
	LeaseUntil       time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	ExpiresAt        time.Time
	// Retryable is set on a definite failure recorded with
	// FailRequest.Retryable.
	Retryable bool
}

func (operation Operation) Clone() Operation {
	operation.Reservations = append([]WindowReservation(nil), operation.Reservations...)
	if operation.ResultRef != nil {
		copyRef := *operation.ResultRef
		operation.ResultRef = &copyRef
	}
	return operation
}

type BeginRequest struct {
	ID             string
	ScopeKey       string
	RequestDigest  [32]byte
	Reservation    pricing.MicroUSD
	ReservationUSD pricing.USD
	Reservations   []WindowReservation
	ConfigVersion  string
	PriceVersion   string
	LeaseUntil     time.Time
	ExpiresAt      time.Time
	// Durable operation metadata. Legacy stores may ignore these optional
	// fields; durable stores retain them as the normalized request
	// envelope required for replay.
	OperationKind        string
	APIVersion           string
	RequestSchemaVersion int
	RequestManifest      []byte
	ConfigDigest         [32]byte
}

type BeginResult struct {
	Operation Operation
	Existing  bool
	Denied    *Denial
}

type Denial struct {
	RetryAfter   time.Duration
	PolicyID     string
	WindowID     string
	Limit        pricing.MicroUSD
	Active       pricing.MicroUSD
	Requested    pricing.MicroUSD
	LimitUSD     pricing.USD
	ActiveUSD    pricing.USD
	RequestedUSD pricing.USD
}

type DispatchRequest struct {
	OperationID   string
	DispatchToken string
	Attempt       AttemptFacts
	LeaseUntil    time.Time
}

// ProviderPendingRequest records a provider-assigned operation before the
// worker returns from an activity. It is intentionally separate from the
// dispatch request so adapters cannot accidentally mark a local submission
// as durable provider state.
type ProviderPendingRequest struct {
	OperationID         string
	DispatchToken       string
	ProviderOperationID string
	EndpointID          string
	Provider            string
	PollAfter           time.Time
}

type AttemptOutcome struct {
	Certainty         DispatchCertainty
	Incurred          pricing.MicroUSD
	IncurredCostUSD   pricing.USD
	ProviderRequestID string
	Attempt           AttemptFacts
}

type ContinueRequest struct {
	OperationID   string
	DispatchToken string
	Outcome       AttemptOutcome
	Remaining     pricing.MicroUSD
	RemainingUSD  pricing.USD
	Reservations  []WindowReservation
	LeaseUntil    time.Time
	ExpiresAt     time.Time
}

type ContinueResult struct {
	Operation Operation
	Denied    *Denial
}

type CompleteRequest struct {
	OperationID   string
	DispatchToken string
	Actual        pricing.MicroUSD
	ActualCostUSD pricing.USD
	ResultRef     *state.BlobRef
	Attempt       AttemptFacts
	CostStatus    string
	CostMethod    string
	UnknownReason string
}

type FailRequest struct {
	OperationID     string
	DispatchToken   string
	Certainty       DispatchCertainty
	Incurred        pricing.MicroUSD
	IncurredCostUSD pricing.USD
	Attempt         AttemptFacts
	Reason          string
	// Retryable marks a definite failure that never reached the provider
	// and may be retried under the same operation. A store that supports it
	// lets the next identical Begin reserve the operation again; others
	// keep it terminal.
	Retryable bool
}

func Digest(value []byte) [32]byte { return sha256.Sum256(value) }
