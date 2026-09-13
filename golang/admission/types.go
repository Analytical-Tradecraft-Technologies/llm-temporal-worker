package admission

import (
	"crypto/sha256"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	blobstore "github.com/mfow/llm-temporal-worker/golang/storage/blob"
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
	ID                string
	ScopeKey          string
	RequestDigest     [32]byte
	State             OperationState
	ReservedMicroUSD  pricing.MicroUSD
	IncurredMicroUSD  pricing.MicroUSD
	FinalMicroUSD     pricing.MicroUSD
	ReservedCostUSD   *pricing.USD
	IncurredCostUSD   *pricing.USD
	ActualCostUSD     *pricing.USD
	CostStatus        string
	CostMethod        string
	CostUnknownReason string
	FailureReason     string
	Reservations      []WindowReservation
	ConfigVersion     string
	PriceVersion      string
	Attempt           AttemptFacts
	ResultRef         *state.BlobRef
	// ImmutableFacts is an opaque, canonical JSON object persisted once after
	// reservation. Durable runtimes use it to recover the original route,
	// pricing, generation/incarnation, expiry, and budget event facts.
	ImmutableFacts []byte
	DispatchToken  string
	LeaseUntil     time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CompletedAt    time.Time
	ExpiresAt      time.Time
}

func (operation Operation) Clone() Operation {
	operation.Reservations = append([]WindowReservation(nil), operation.Reservations...)
	operation.ImmutableFacts = append([]byte(nil), operation.ImmutableFacts...)
	if operation.ResultRef != nil {
		copyRef := *operation.ResultRef
		operation.ResultRef = &copyRef
	}
	return operation
}

type BeginRequest struct {
	ID string
	// ReleasedID is the deterministic released-v1 identity for the same
	// logical request. PostgreSQL may use it once to bind an existing
	// released row to the current immutable identity; it is never used when
	// creating a row.
	ReleasedID string
	// OperationKey and Actor are caller-owned immutable identity components.
	// ScopeKey supplies tenant/project; OperationKind, APIVersion, and
	// RequestDigest complete the durable operation identity.
	OperationKey   string
	Actor          string
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
	// fields. PostgreSQL stores RequestManifest as content-free JSON metadata
	// and envelope-encrypts RequestPayload as the canonical request used for
	// exact replay.
	OperationKind        string
	APIVersion           string
	RequestSchemaVersion int
	RequestManifest      []byte
	RequestPayload       []byte
	ConfigDigest         [32]byte
	// ImmutableFacts may be supplied by a second idempotent Begin after
	// admission. PostgreSQL persists it only when absent and rejects changes.
	ImmutableFacts []byte
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
	OperationID        string
	DispatchToken      string
	Actual             pricing.MicroUSD
	ActualCostUSD      pricing.USD
	ResultRef          *state.BlobRef
	Attempt            AttemptFacts
	CostStatus         string
	CostMethod         string
	CostCatalogVersion string
	UnknownReason      string
}

type AtomicFinalization struct {
	ScopeID           string
	Checkpoint        state.DurableCheckpoint
	CheckpointObjects []blobstore.Ref
	Complete          CompleteRequest
}

type FailRequest struct {
	OperationID     string
	DispatchToken   string
	Certainty       DispatchCertainty
	Incurred        pricing.MicroUSD
	IncurredCostUSD pricing.USD
	// PostResponse identifies a terminal worker validation failure after the
	// provider returned a chargeable response. The provider outcome is known,
	// so the operation is definite_failed even though dispatch was accepted.
	PostResponse       bool
	CostStatus         string
	CostMethod         string
	CostCatalogVersion string
	UnknownReason      string
	Attempt            AttemptFacts
	Reason             string
}

func Digest(value []byte) [32]byte { return sha256.Sum256(value) }
