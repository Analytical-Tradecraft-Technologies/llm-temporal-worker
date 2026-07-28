// Package durable defines the storage-neutral contract for the production
// durable-state composition. It intentionally contains ports and invariant
// checks only; client construction and schema provisioning remain runtime and
// deployment concerns.
package durable

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

var (
	ErrInvalidIdentity  = errors.New("invalid durable state identity")
	ErrInvalidPhase     = errors.New("invalid durable lifecycle phase")
	ErrJournalRequired  = errors.New("postgres journal is required before dispatch")
	ErrReconcilePending = errors.New("redis reconciliation is pending")
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
	Reconcile(context.Context, ReconcileRequest) error
}

type ReserveRequest struct {
	OperationID  OperationID
	GenerationID GenerationID
	Reservations []admission.WindowReservation
	ExpiresAt    time.Time
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

// Composition is the seam consumed by the runtime factory when the durable
// split is wired. Operation/continuation/result state is authoritative in
// PostgreSQL; active budget admission is provided by Redis; the journal is
// append-only PostgreSQL state between those two operations.
type Composition struct {
	Identity      StateIdentity
	Operations    admission.AdmissionStore
	Continuations state.ContinuationStore
	Results       ResultStore
	Journal       Journal
	Materializer  BudgetMaterializer
}

// ResultStore mirrors engine.ResultStore without importing the engine package,
// keeping this storage contract independent of runtime orchestration.
type ResultStore interface {
	Get(context.Context, string) (llm.Response, error)
	Put(context.Context, string, llm.Response) (state.BlobRef, error)
}

func (composition Composition) Validate() error {
	if err := composition.Identity.Validate(); err != nil {
		return err
	}
	if composition.Operations == nil {
		return errors.New("durable operation store is required")
	}
	if composition.Continuations == nil {
		return errors.New("durable continuation store is required")
	}
	if composition.Results == nil {
		return errors.New("durable result store is required")
	}
	if composition.Journal == nil {
		return errors.New("durable PostgreSQL journal is required")
	}
	if composition.Materializer == nil {
		return errors.New("durable Redis budget materializer is required")
	}
	return nil
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
