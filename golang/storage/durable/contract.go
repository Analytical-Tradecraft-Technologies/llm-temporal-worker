// Package durable defines the storage-neutral contract for the production
// durable-state composition. It intentionally contains ports and invariant
// checks only; client construction and schema provisioning remain runtime and
// deployment concerns.
package durable

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

var (
	ErrInvalidIdentity     = errors.New("invalid durable state identity")
	ErrInvalidPhase        = errors.New("invalid durable lifecycle phase")
	ErrReservationNotFound = errors.New("durable budget reservation is not known")
	ErrJournalRequired     = errors.New("postgres journal is required before dispatch")
	ErrReconcilePending    = errors.New("redis reconciliation is pending")
)

var redisPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// PostgresIdentity identifies the worker-owned PostgreSQL namespace. The
// namespace is validated by postgresstore.Namespace; credentials and DSNs are
// deliberately not part of the identity.
type PostgresIdentity struct {
	Database    string
	Schema      string
	TablePrefix string
}

func (identity PostgresIdentity) Namespace() (postgresstore.Namespace, error) {
	return postgresstore.NewNamespace(identity.Database, identity.Schema, identity.TablePrefix)
}

// RedisIdentity identifies the worker-owned Redis keyspace. KeyPrefix is
// intentionally the only clear-text key component; operation and tenant IDs
// remain HMAC-derived by the Redis adapter.
type RedisIdentity struct {
	KeyPrefix string
	HashTag   string
}

// StateIdentity binds the two durable stores to one immutable configuration
// snapshot. A worker must not combine stores with different identities.
type StateIdentity struct {
	Postgres     PostgresIdentity
	Redis        RedisIdentity
	ConfigDigest [32]byte
}

func (identity StateIdentity) Validate() error {
	if _, err := identity.Postgres.Namespace(); err != nil {
		return fmt.Errorf("%w: postgres namespace: %v", ErrInvalidIdentity, err)
	}
	if !redisPrefixPattern.MatchString(identity.Redis.KeyPrefix) {
		return fmt.Errorf("%w: Redis key prefix is invalid", ErrInvalidIdentity)
	}
	if identity.Redis.HashTag == "" || len(identity.Redis.HashTag) > 64 || strings.ContainsAny(identity.Redis.HashTag, "{} \t\r\n") {
		return fmt.Errorf("%w: Redis hash tag is invalid", ErrInvalidIdentity)
	}
	if identity.ConfigDigest == [32]byte{} {
		return fmt.Errorf("%w: configuration digest is required", ErrInvalidIdentity)
	}
	return nil
}

// OperationID, GenerationID, and IncarnationID prevent accidental mixing of
// operation and materialization identities at the composition boundary.
type OperationID string
type GenerationID string
type IncarnationID string

func validateID(value string, name string) error {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("%s is empty or unsafe", name)
	}
	return nil
}

func (id OperationID) Validate() error   { return validateID(string(id), "operation id") }
func (id GenerationID) Validate() error  { return validateID(string(id), "generation id") }
func (id IncarnationID) Validate() error { return validateID(string(id), "incarnation id") }

// BudgetMaterializer is the Redis-only active budget port. It has no
// PostgreSQL read capability by design: PostgreSQL receives the write-ahead
// journal after acceptance and is consulted only by an explicit cold rebuild.
type BudgetMaterializer interface {
	Accept(context.Context, ReserveRequest) (ReserveResult, error)
	// Confirm proves that an already-accepted immutable reservation is still
	// present in the expected Redis incarnation. It must never create or renew
	// a reservation.
	Confirm(context.Context, ReserveRequest) (ReserveResult, error)
	// FenceDispatch atomically proves the exact accepted reservation is still
	// live and extends every Redis accounting key through terminal retention.
	// Provider bytes must not leave before this call succeeds.
	FenceDispatch(context.Context, DispatchFenceRequest) error
	Reconcile(context.Context, ReconcileRequest) error
}

// BatchBudgetMaterializer atomically admits an ordered, content-bound set of
// operation reservations. Implementations must validate every request before
// mutating budget state: a denial creates no child reservation, while an
// accepted replay with the same digest returns the original ordered results.
// The optional port keeps existing single-operation materializers source
// compatible while reserve_batch composition fails closed when it is absent.
type BatchBudgetMaterializer interface {
	AcceptBatch(context.Context, [32]byte, []ReserveRequest) ([]ReserveResult, error)
}

// ErrBatchMaterializationConflict identifies an immutable materialization identity conflict.
var ErrBatchMaterializationConflict = errors.New("batch materialization identity conflict")

// BatchMaterialization is the immutable, ordered receipt of an atomic batch,
// including denials. RemainingEscrowCostUSD is the post-allocation logical balance.
type BatchMaterialization struct {
	Requests               []ReserveRequest
	Results                []ReserveResult
	RemainingEscrowCostUSD pricing.USD
}

// BatchMaterializationReader reads original receipts, never live child records.
// Absence is distinct from corrupt or identity-mismatched retained records.
type BatchMaterializationReader interface {
	LoadBatchMaterialization(context.Context, [32]byte) (BatchMaterialization, bool, error)
}

// BatchGrantMaterializer retrieves and confirms one precreated child
// reservation by its content-bound batch identity. It never creates or renews
// capacity and lets Generate consume the exact immutable timestamps and route
// facts produced by the batch transaction.
type BatchGrantMaterializer interface {
	ConfirmBatchGrant(context.Context, [32]byte, OperationID) (ReserveRequest, ReserveResult, error)
}

// BatchEscrowMaterializer reserves a group ceiling before operation identities
// exist, atomically allocates exact child grants from it, and closes the
// remainder without creating new capacity.
type BatchEscrowMaterializer interface {
	AcceptBatchEscrow(context.Context, [32]byte, ReserveRequest) (ReserveResult, error)
	LoadBatchEscrow(context.Context, [32]byte, OperationID) (ReserveRequest, ReserveResult, error)
	AllocateBatchGrants(context.Context, [32]byte, ReserveRequest, int64, []ReserveRequest) ([]ReserveResult, error)
	CloseBatchEscrow(context.Context, [32]byte, ReserveRequest, [32]byte) (pricing.USD, bool, error)
}

// ValidateBatchReserveRequest checks the storage-neutral invariants required
// before an atomic materializer may inspect or mutate shared budget state.
func ValidateBatchReserveRequest(contentDigest [32]byte, requests []ReserveRequest) error {
	if contentDigest == ([32]byte{}) {
		return errors.New("batch reservation content digest is required")
	}
	if len(requests) == 0 || len(requests) > 1024 {
		return errors.New("batch reservation must contain 1 to 1024 operations")
	}
	operations := make(map[OperationID]struct{}, len(requests))
	generation := requests[0].GenerationID
	incarnation := requests[0].IncarnationID
	for index, request := range requests {
		if _, err := PlannedReserveResult(request); err != nil {
			return fmt.Errorf("batch reservation operation %d: %w", index, err)
		}
		if len(request.Reservations) == 0 {
			return fmt.Errorf("batch reservation operation %d has no budget windows", index)
		}
		if request.GenerationID != generation || request.IncarnationID != incarnation {
			return errors.New("batch reservation operations span budget generations or incarnations")
		}
		if _, exists := operations[request.OperationID]; exists {
			return errors.New("batch reservation operation ids must be unique")
		}
		operations[request.OperationID] = struct{}{}
	}
	return nil
}

// ReservationBounds are the signed per-component ceilings attached to a
// precreated grant. They are persisted with the Redis operation so Generate
// can prove the actual routed estimate is component-wise within admission.
type ReservationBounds struct {
	OperationSHA256     string `json:"operation_sha256"`
	Model               string `json:"model"`
	MaxInputTokens      int64  `json:"max_input_tokens"`
	MaxOutputTokens     int64  `json:"max_output_tokens"`
	MaxReasoningTokens  int64  `json:"max_reasoning_tokens"`
	MaxCacheReadTokens  int64  `json:"max_cache_read_tokens"`
	MaxCacheWriteTokens int64  `json:"max_cache_write_tokens"`
	GrantKeyID          string `json:"grant_key_id,omitempty"`
	GrantKeySHA256      string `json:"grant_key_sha256,omitempty"`
}

// ValidateBatchReserveResult prevents a materializer from returning a partial,
// reordered, or fabricated acceptance vector.
func ValidateBatchReserveResult(requests []ReserveRequest, results []ReserveResult) error {
	if len(results) != len(requests) {
		return errors.New("batch reservation result count does not match request")
	}
	accepted := results[0].Accepted
	for index := range requests {
		if err := results[index].Validate(requests[index]); err != nil {
			return fmt.Errorf("batch reservation result %d: %w", index, err)
		}
		if results[index].Accepted != accepted {
			return errors.New("batch reservation returned a partial acceptance")
		}
		if accepted {
			if err := ValidatePlannedReserveResult(requests[index], results[index]); err != nil {
				return fmt.Errorf("batch reservation result %d: %w", index, err)
			}
		} else if results[index].Denial == nil {
			return errors.New("denied batch reservation result is missing denial facts")
		}
	}
	return nil
}

// DispatchRouteFacts are the immutable provider-route identity bound into the
// Redis reservation fingerprint before admission.
type DispatchRouteFacts struct {
	RouteID       string `json:"route_id"`
	EndpointID    string `json:"endpoint_id"`
	Provider      string `json:"provider"`
	ResolvedModel string `json:"resolved_model"`
	ServiceClass  string `json:"service_class"`
	PriceVersion  string `json:"price_version"`
}

func (facts DispatchRouteFacts) Validate() error {
	for name, value := range map[string]string{
		"route id": facts.RouteID, "endpoint id": facts.EndpointID,
		"provider": facts.Provider, "resolved model": facts.ResolvedModel,
		"service class": facts.ServiceClass, "price version": facts.PriceVersion,
	} {
		if err := validateID(value, name); err != nil {
			return err
		}
	}
	return nil
}

// DispatchFenceRequest binds one immutable reservation to its configured
// terminal-retention deadline. RetainUntil may extend an existing identical
// fence, but can never change the reservation or route facts.
type DispatchFenceRequest struct {
	Reservation ReserveRequest
	RetainUntil time.Time
}

func (request DispatchFenceRequest) Validate() error {
	if err := request.Reservation.OperationID.Validate(); err != nil {
		return err
	}
	if err := request.Reservation.GenerationID.Validate(); err != nil {
		return err
	}
	if err := request.Reservation.IncarnationID.Validate(); err != nil {
		return err
	}
	if err := request.Reservation.Route.Validate(); err != nil {
		return fmt.Errorf("dispatch route: %w", err)
	}
	if request.Reservation.ExpiresAt.IsZero() || request.Reservation.OccurredAt.IsZero() ||
		request.RetainUntil.IsZero() || !request.RetainUntil.After(request.Reservation.ExpiresAt) {
		return errors.New("dispatch fence retention must extend beyond the immutable reservation lease")
	}
	if len(request.Reservation.Reservations) == 0 {
		return errors.New("dispatch fence reservation list must not be empty")
	}
	return nil
}

type ReserveRequest struct {
	OperationID   OperationID
	GenerationID  GenerationID
	IncarnationID IncarnationID
	Reservations  []admission.WindowReservation
	ExpiresAt     time.Time
	OccurredAt    time.Time
	Route         DispatchRouteFacts
	Bounds        ReservationBounds
	// LogicalCostUSD is the original operation ceiling, counted once regardless
	// of how many enforcement windows match. It participates in the fingerprint.
	LogicalCostUSD pricing.USD
	// RemainingEscrowCostUSD is a mutable escrow observation, not identity.
	RemainingEscrowCostUSD pricing.USD
}

// PlannedReserveResult creates the immutable accepted result that Redis must
// echo if it accepts request. Persisting this value before acceptance makes
// route, incarnation, event identity, and event timestamp replay-stable.
func PlannedReserveResult(request ReserveRequest) (ReserveResult, error) {
	if err := request.OperationID.Validate(); err != nil {
		return ReserveResult{}, err
	}
	if err := request.GenerationID.Validate(); err != nil {
		return ReserveResult{}, err
	}
	if err := request.IncarnationID.Validate(); err != nil {
		return ReserveResult{}, err
	}
	if err := request.LogicalCostUSD.Validate(); err != nil {
		return ReserveResult{}, fmt.Errorf("logical operation cost: %w", err)
	}
	if request.ExpiresAt.IsZero() || request.OccurredAt.IsZero() || !request.ExpiresAt.After(request.OccurredAt) {
		return ReserveResult{}, errors.New("immutable reservation lease and occurrence timestamps are invalid")
	}
	reservations := append([]admission.WindowReservation(nil), request.Reservations...)
	sort.Slice(reservations, func(i, j int) bool {
		if reservations[i].PolicyID != reservations[j].PolicyID {
			return reservations[i].PolicyID < reservations[j].PolicyID
		}
		if reservations[i].WindowID != reservations[j].WindowID {
			return reservations[i].WindowID < reservations[j].WindowID
		}
		return reservations[i].Bucket < reservations[j].Bucket
	})
	result := ReserveResult{
		OperationID: request.OperationID, Accepted: true,
		GenerationID: request.GenerationID, IncarnationID: request.IncarnationID,
		Events: make([]budget.ReservationEvent, 0, len(reservations)),
	}
	for _, reservation := range reservations {
		amount := reservation.AmountUSD
		if amount.IsZero() {
			var err error
			amount, err = pricing.USDFromMicro(reservation.Amount)
			if err != nil {
				return ReserveResult{}, err
			}
		}
		if reservation.Bucket < 0 || reservation.BucketNanos <= 0 || reservation.Bucket > (1<<62)/reservation.BucketNanos {
			return ReserveResult{}, errors.New("immutable reservation bucket bounds are invalid")
		}
		bucketStart := reservation.Bucket * reservation.BucketNanos
		identity := fmt.Sprintf("llmtw/budget-reserve/v1\x00%s\x00%s\x00%s\x00%s\x00%d", request.OperationID, request.GenerationID, reservation.PolicyID, reservation.WindowID, bucketStart)
		result.Events = append(result.Events, budget.ReservationEvent{
			EventID:      uuid.NewSHA1(uuid.NameSpaceOID, []byte(identity)).String(),
			GenerationID: string(request.GenerationID), OperationID: string(request.OperationID),
			WindowID: reservation.WindowID, BucketStart: time.Unix(0, bucketStart).UTC(),
			ReservationRevision: 1, AmountUSD: amount, OccurredAt: request.OccurredAt.UTC(),
		})
	}
	if err := result.Validate(request); err != nil {
		return ReserveResult{}, err
	}
	return result, nil
}

func ValidatePlannedReserveResult(request ReserveRequest, result ReserveResult) error {
	if !result.Accepted {
		return nil
	}
	planned, err := PlannedReserveResult(request)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(planned, result) {
		return errors.New("Redis accepted reservation facts differ from immutable plan")
	}
	return nil
}

type ReserveResult struct {
	// OperationID is echoed by Redis so the journal cannot be bound to a
	// different caller operation during a retry or snapshot swap.
	OperationID   OperationID
	Accepted      bool
	GenerationID  GenerationID
	IncarnationID IncarnationID
	RetryAfter    time.Duration
	Denial        *admission.Denial
	Events        []budget.ReservationEvent
}

func (result ReserveResult) Validate(request ReserveRequest) error {
	if err := request.OperationID.Validate(); err != nil {
		return err
	}
	if err := result.OperationID.Validate(); err != nil {
		return err
	}
	if result.OperationID != request.OperationID {
		return fmt.Errorf("operation id changed during Redis acceptance")
	}
	if err := request.GenerationID.Validate(); err != nil {
		return err
	}
	if err := result.GenerationID.Validate(); err != nil {
		return err
	}
	if result.GenerationID != request.GenerationID {
		return fmt.Errorf("generation id changed during Redis acceptance")
	}
	if request.IncarnationID != "" && result.IncarnationID != request.IncarnationID {
		return fmt.Errorf("incarnation id changed during Redis acceptance")
	}
	if result.Accepted {
		if err := result.IncarnationID.Validate(); err != nil {
			return err
		}
		if len(result.Events) == 0 {
			return errors.New("Redis accepted a reservation without journal events")
		}
	} else if len(result.Events) != 0 {
		return errors.New("denied reservation must not produce journal events")
	}
	for _, event := range result.Events {
		if err := event.Validate(); err != nil {
			return fmt.Errorf("reservation event: %w", err)
		}
		if event.OperationID != string(request.OperationID) || event.GenerationID != string(request.GenerationID) {
			return errors.New("reservation event identity does not match request")
		}
	}
	return nil
}

type ReconcileRequest struct {
	OperationID   OperationID
	GenerationID  GenerationID
	IncarnationID IncarnationID
	Events        []budget.CompletionEvent
}

func (request ReconcileRequest) Validate() error {
	if err := request.OperationID.Validate(); err != nil {
		return err
	}
	if err := request.GenerationID.Validate(); err != nil {
		return err
	}
	if err := request.IncarnationID.Validate(); err != nil {
		return err
	}
	if len(request.Events) == 0 {
		return errors.New("Redis reconciliation requires at least one completion event")
	}
	for _, event := range request.Events {
		if err := event.Validate(); err != nil {
			return fmt.Errorf("completion event: %w", err)
		}
		if event.OperationID != string(request.OperationID) || event.GenerationID != string(request.GenerationID) {
			return errors.New("completion event identity does not match request")
		}
	}
	return nil
}

// Journal is the write-only PostgreSQL budget journal capability. Keeping this
// narrow prevents normal admission from accidentally reading PostgreSQL
// budget projections.
type Journal interface {
	AppendReservation(context.Context, budget.ReservationEvent) (postgresstore.JournalRecord, error)
	AppendCompletion(context.Context, budget.CompletionEvent) (postgresstore.JournalRecord, error)
}

// Composition is the snapshot-owned seam consumed by a runtime factory when
// the durable split is wired. Operation, checkpoint, and result state is
// authoritative in PostgreSQL/S3; active budget admission is provided by
// Redis; the journal is append-only PostgreSQL state between those operations.
type Composition struct {
	Identity      StateIdentity
	Operations    admission.AdmissionStore
	Continuations state.ContinuationStore
	Checkpoints   state.CheckpointHandleMaterializer
	Results       ResultStore
	Journal       Journal
	Materializer  BudgetMaterializer
	Finalizer     AtomicFinalizer
}

// ResultStore mirrors engine.ResultStore without importing the engine package,
// keeping this storage contract independent of runtime orchestration.
type ResultStore interface {
	Get(context.Context, string) (llm.Response, error)
	Put(context.Context, string, llm.Response) (state.BlobRef, error)
}

// AtomicFinalizer is the single PostgreSQL commit boundary for an uploaded
// checkpoint and result. Object-store uploads may happen first, but checkpoint
// locator rows, the checkpoint graph, the result reference, and the terminal
// operation transition become visible together.
type AtomicFinalizer interface {
	Finalize(context.Context, admission.AtomicFinalization) (state.DurableCheckpoint, error)
}

func (composition Composition) Validate() error {
	if err := composition.Identity.Validate(); err != nil {
		return err
	}
	if isNilPort(composition.Operations) {
		return errors.New("durable operation store is required")
	}
	if isNilPort(composition.Continuations) {
		return errors.New("durable continuation store is required")
	}
	if isNilPort(composition.Checkpoints) {
		return errors.New("durable checkpoint store is required")
	}
	if isNilPort(composition.Results) {
		return errors.New("durable result store is required")
	}
	if isNilPort(composition.Journal) {
		return errors.New("durable PostgreSQL journal is required")
	}
	if isNilPort(composition.Materializer) {
		return errors.New("durable Redis budget materializer is required")
	}
	// Validate the cross-store handoff as part of the composition so callers
	// cannot validate the operation ports while silently carrying a different
	// snapshot identity into the Redis/PostgreSQL budget path.
	if err := composition.BudgetBoundary().Validate(); err != nil {
		return err
	}
	return nil
}

// isNilPort treats an interface containing a typed nil pointer as nil. Ports
// are validated before any side effects, so an invalid composition fails
// closed instead of panicking when a method is invoked later.
func isNilPort(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// Phase is the only legal new-operation order. A journal failure must stop
// before Dispatch; after Dispatch, PostgreSQL finalization is authoritative and
// Redis reconciliation is retried independently (it never rolls back a result).
type Phase uint8

const (
	PhaseOperationReplay Phase = iota
	PhaseRedisAccepted
	PhasePostgresJournaled
	PhaseDispatched
	PhasePostgresFinalized
	PhaseRedisReconciled
)

func (phase Phase) String() string {
	switch phase {
	case PhaseOperationReplay:
		return "operation_replay"
	case PhaseRedisAccepted:
		return "redis_accepted"
	case PhasePostgresJournaled:
		return "postgres_journaled"
	case PhaseDispatched:
		return "dispatched"
	case PhasePostgresFinalized:
		return "postgres_finalized"
	case PhaseRedisReconciled:
		return "redis_reconciled"
	default:
		return fmt.Sprintf("phase(%d)", phase)
	}
}

// Lifecycle records the durable side-effect order. It is intentionally small
// enough to use in runtime tests and metrics without persisting another ledger.
type Lifecycle struct {
	phases []Phase
	// Reservation identity is bound when Redis first accepts. It prevents a
	// reload/retry from journaling a result from another incarnation.
	reservationOperation     OperationID
	reservationGeneration    GenerationID
	reservationIncarnation   IncarnationID
	reservationIdentityBound bool
	completionDigest         [32]byte
	completionDigestBound    bool
	reservationAborted       bool
}

func (l *Lifecycle) Advance(next Phase) error {
	if l == nil {
		return ErrInvalidPhase
	}
	if next < PhaseOperationReplay || next > PhaseRedisReconciled {
		return fmt.Errorf("%w: %d", ErrInvalidPhase, next)
	}
	want := PhaseOperationReplay
	if len(l.phases) > 0 {
		want = l.phases[len(l.phases)-1] + 1
	}
	if next != want {
		if next == PhaseDispatched && want <= PhasePostgresJournaled {
			return ErrJournalRequired
		}
		return fmt.Errorf("%w: got %s, want %s", ErrInvalidPhase, next, want)
	}
	l.phases = append(l.phases, next)
	return nil
}

func (l Lifecycle) Phases() []Phase { return append([]Phase(nil), l.phases...) }
func (l Lifecycle) Current() (Phase, bool) {
	if len(l.phases) == 0 {
		return 0, false
	}
	return l.phases[len(l.phases)-1], true
}

// ReconcileFailure makes the post-finalization failure policy explicit. The
// caller should persist/retry this error; it must not undo the PostgreSQL
// result or create new budget capacity while Redis is unavailable.
func (l Lifecycle) ReconcileFailure(err error) error {
	if err == nil {
		return nil
	}
	current, ok := l.Current()
	if !ok || current != PhasePostgresFinalized {
		return fmt.Errorf("%w: reconciliation is only valid after PostgreSQL finalization", ErrInvalidPhase)
	}
	return fmt.Errorf("%w: %v", ErrReconcilePending, err)
}

func (l *Lifecycle) bindReservationIdentity(result ReserveResult) error {
	if l == nil {
		return ErrInvalidPhase
	}
	if l.reservationIdentityBound {
		if l.reservationOperation != result.OperationID || l.reservationGeneration != result.GenerationID || l.reservationIncarnation != result.IncarnationID {
			return fmt.Errorf("%w: Redis reservation identity changed during retry", ErrInvalidIdentity)
		}
		return nil
	}
	l.reservationOperation = result.OperationID
	l.reservationGeneration = result.GenerationID
	l.reservationIncarnation = result.IncarnationID
	l.reservationIdentityBound = true
	return nil
}

func (l *Lifecycle) markReservationAborted() {
	if l != nil {
		l.reservationAborted = true
	}
}

func (l *Lifecycle) bindCompletionDigest(digest [32]byte) error {
	if l == nil {
		return ErrInvalidPhase
	}
	if l.completionDigestBound && l.completionDigest != digest {
		return fmt.Errorf("%w: completion batch changed during reconciliation retry", ErrInvalidIdentity)
	}
	l.completionDigest = digest
	l.completionDigestBound = true
	return nil
}
