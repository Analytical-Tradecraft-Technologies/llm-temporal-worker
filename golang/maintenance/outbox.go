package maintenance

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

const (
	// MaxSafePayloadBytes bounds the JSON that can be persisted in an outbox
	// row. Maintenance payloads carry identifiers or encrypted locators, never
	// prompts, responses, or provider credentials.
	MaxSafePayloadBytes = 64 << 10
	maxSafePayloadDepth = 32
)

var (
	ErrObjectNotFound   = errors.New("external maintenance object not found")
	ErrOutboxConflict   = errors.New("maintenance outbox dedupe conflict")
	ErrOutboxNotClaimed = errors.New("maintenance outbox item is not owned by this worker")
)

// LeaseToken is an opaque fencing token assigned for one outbox claim.  A
// reclaimed row receives a new token, so a worker that outlives its lease can
// never complete or retry the work claimed by its successor.
type LeaseToken [16]byte

// NewLeaseToken returns a cryptographically random fencing token. Tokens are
// not identifiers for workers and must never be logged or included in a
// payload sent to an external object store.
func NewLeaseToken() (LeaseToken, error) {
	var token LeaseToken
	if _, err := rand.Read(token[:]); err != nil {
		return LeaseToken{}, fmt.Errorf("generate maintenance lease token: %w", err)
	}
	return token, nil
}

func (token LeaseToken) IsZero() bool { return token == (LeaseToken{}) }

type EventKind string

const (
	EventDeleteBlob          EventKind = "delete_blob"
	EventDeleteProviderState EventKind = "delete_provider_state"
	EventRefreshInventory    EventKind = "refresh_inventory"
)

type EventState string

const (
	EventPending    EventState = "pending"
	EventProcessing EventState = "processing"
	EventCompleted  EventState = "completed"
	EventFailed     EventState = "failed"
)

// Event is safe, bounded maintenance intent. Payloads contain identifiers or
// encrypted locators only; raw prompt, response, credentials, and provider
// payloads are intentionally outside this contract.
type Event struct {
	ID             string
	Kind           EventKind
	AggregateType  string
	AggregateID    string
	DedupeKey      [32]byte
	SafePayload    json.RawMessage
	State          EventState
	AttemptCount   int
	AvailableAt    time.Time
	LeaseExpiresAt time.Time
	LeaseToken     LeaseToken
	CreatedAt      time.Time
	CompletedAt    time.Time
}

// NormalizeSafePayload validates and canonicalizes one outbox payload. JSONB
// would otherwise silently discard duplicate keys and make equivalent retry
// payloads compare differently in storage-neutral adapters.
func NormalizeSafePayload(raw json.RawMessage) (json.RawMessage, error) {
	canonical, err := llm.CanonicalJSONWithLimits(raw, MaxSafePayloadBytes, maxSafePayloadDepth)
	if err != nil {
		return nil, fmt.Errorf("maintenance outbox payload: %w", err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(canonical, &object); err != nil || object == nil {
		return nil, errors.New("maintenance outbox payload must be a JSON object")
	}
	return canonical, nil
}

func (event Event) Validate() error {
	if event.ID == "" || event.AggregateType == "" || event.AggregateID == "" {
		return errors.New("maintenance outbox ID and aggregate identity are required")
	}
	switch event.Kind {
	case EventDeleteBlob, EventDeleteProviderState, EventRefreshInventory:
	default:
		return fmt.Errorf("unsupported maintenance event kind %q", event.Kind)
	}
	switch event.State {
	case EventPending, EventProcessing, EventCompleted, EventFailed:
	default:
		return fmt.Errorf("unsupported maintenance event state %q", event.State)
	}
	if event.DedupeKey == [32]byte{} {
		return errors.New("maintenance outbox dedupe key is required")
	}
	if _, err := NormalizeSafePayload(event.SafePayload); err != nil {
		return err
	}
	if err := validateEventPayload(event); err != nil {
		return err
	}
	if event.AttemptCount < 0 {
		return errors.New("maintenance outbox attempt count must not be negative")
	}
	if event.State == EventProcessing {
		if event.LeaseToken.IsZero() {
			return errors.New("processing maintenance outbox item has no lease token")
		}
		if event.LeaseExpiresAt.IsZero() || !event.LeaseExpiresAt.After(event.AvailableAt) {
			return errors.New("processing maintenance outbox item has no active lease")
		}
	}
	if event.AvailableAt.IsZero() {
		return errors.New("maintenance outbox available time is required")
	}
	if !event.LeaseExpiresAt.IsZero() && !event.LeaseExpiresAt.After(event.AvailableAt) {
		return errors.New("maintenance outbox lease must be after available time")
	}
	return nil
}

// validateEventPayload applies the event-kind contract after canonical JSON
// validation.  Outbox payloads are an input boundary for an external worker,
// so accepting an arbitrary object would make it possible to accidentally
// persist prompts, provider responses, or credentials under a maintenance
// event.  Delete-blob is currently the only event emitted by the production
// retention paths; keep its payload deliberately narrow and tied to the
// aggregate identity.  Other event kinds remain reserved until their typed
// constructors are introduced.
func validateEventPayload(event Event) error {
	if event.Kind == EventDeleteProviderState || event.Kind == EventRefreshInventory {
		return fmt.Errorf("%s maintenance event typed payload contract is not implemented", event.Kind)
	}
	if event.Kind != EventDeleteBlob {
		return nil
	}
	if event.AggregateType != "blob" {
		return errors.New("delete-blob maintenance event aggregate type must be blob")
	}
	var payload struct {
		BlobID string `json:"blob_id"`
	}
	decoder := json.NewDecoder(bytes.NewReader(event.SafePayload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return fmt.Errorf("delete-blob maintenance payload: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("delete-blob maintenance payload has trailing data")
		}
		return fmt.Errorf("delete-blob maintenance payload trailing data: %w", err)
	}
	if payload.BlobID == "" {
		return errors.New("delete-blob maintenance payload blob_id is required")
	}
	if payload.BlobID != event.AggregateID {
		return errors.New("delete-blob maintenance payload blob_id must match aggregate ID")
	}
	return nil
}

// NewDeleteBlobEvent creates a safe, deterministic event for a blob locator.
// The external worker can resolve the opaque aggregate ID from PostgreSQL;
// no locator or ciphertext is put in the event payload.
func NewDeleteBlobEvent(id, blobID string, availableAt, createdAt time.Time) (Event, error) {
	if id == "" || blobID == "" {
		return Event{}, errors.New("maintenance blob event IDs are required")
	}
	dedupe := sha256.Sum256([]byte("llm-temporal-worker/delete-blob/v1\x00" + blobID))
	payload, err := json.Marshal(struct {
		BlobID string `json:"blob_id"`
	}{BlobID: blobID})
	if err != nil {
		return Event{}, err
	}
	event := Event{ID: id, Kind: EventDeleteBlob, AggregateType: "blob", AggregateID: blobID, DedupeKey: dedupe, SafePayload: payload, State: EventPending, AvailableAt: availableAt, CreatedAt: createdAt}
	return event, event.Validate()
}

type ClaimOptions struct {
	Now   time.Time
	Limit int
	Lease time.Duration
}

func (options ClaimOptions) Validate() error {
	if options.Now.IsZero() {
		return errors.New("maintenance outbox claim time is required")
	}
	if options.Limit <= 0 || options.Limit > 10_000 {
		return errors.New("maintenance outbox claim limit must be between 1 and 10000")
	}
	if options.Lease <= 0 {
		return errors.New("maintenance outbox claim lease must be positive")
	}
	return nil
}

// OutboxStore is the transaction boundary used by the dispatcher. Claim must
// atomically lease at most Limit rows using SKIP LOCKED; Complete and Retry
// must only mutate rows currently owned by the caller.
type OutboxStore interface {
	Publish(context.Context, Event) error
	Claim(context.Context, ClaimOptions) ([]Event, error)
	Complete(context.Context, string, LeaseToken, time.Time) error
	Retry(context.Context, string, LeaseToken, time.Time, time.Time) error
}

type InMemoryOutbox struct {
	mu     sync.Mutex
	events map[string]Event
}

func NewInMemoryOutbox(events []Event) (*InMemoryOutbox, error) {
	store := &InMemoryOutbox{events: make(map[string]Event, len(events))}
	for _, event := range events {
		canonical, err := NormalizeSafePayload(event.SafePayload)
		if err != nil {
			return nil, err
		}
		event.SafePayload = canonical
		if err := event.Validate(); err != nil {
			return nil, err
		}
		if _, exists := store.events[event.ID]; exists {
			return nil, fmt.Errorf("maintenance outbox ID %q is duplicated", event.ID)
		}
		store.events[event.ID] = cloneEvent(event)
	}
	return store, nil
}

func (store *InMemoryOutbox) Publish(ctx context.Context, event Event) error {
	if store == nil {
		return errors.New("maintenance outbox store is nil")
	}
	canonical, err := NormalizeSafePayload(event.SafePayload)
	if err != nil {
		return err
	}
	event.SafePayload = canonical
	if err := event.Validate(); err != nil {
		return err
	}
	if event.State != EventPending {
		return errors.New("maintenance outbox publish state must be pending")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, existing := range store.events {
		if existing.Kind != event.Kind || existing.DedupeKey != event.DedupeKey {
			continue
		}
		if existing.AggregateType != event.AggregateType || existing.AggregateID != event.AggregateID || !bytes.Equal(existing.SafePayload, event.SafePayload) {
			return ErrOutboxConflict
		}
		return nil
	}
	if existing, ok := store.events[event.ID]; ok && (existing.Kind != event.Kind || existing.DedupeKey != event.DedupeKey) {
		return ErrOutboxConflict
	}
	store.events[event.ID] = cloneEvent(event)
	return nil
}

func (store *InMemoryOutbox) Claim(ctx context.Context, options ClaimOptions) ([]Event, error) {
	if store == nil {
		return nil, errors.New("maintenance outbox store is nil")
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	candidates := make([]Event, 0, len(store.events))
	for _, event := range store.events {
		ready := (event.State == EventPending || event.State == EventFailed) && !event.AvailableAt.After(options.Now)
		leaseExpired := event.State == EventProcessing && !event.LeaseExpiresAt.After(options.Now)
		if ready || leaseExpired {
			candidates = append(candidates, event)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].AvailableAt.Equal(candidates[j].AvailableAt) {
			return candidates[i].ID < candidates[j].ID
		}
		return candidates[i].AvailableAt.Before(candidates[j].AvailableAt)
	})
	if len(candidates) > options.Limit {
		candidates = candidates[:options.Limit]
	}
	claimed := make([]Event, 0, len(candidates))
	for _, candidate := range candidates {
		current := store.events[candidate.ID]
		current.State = EventProcessing
		current.AttemptCount++
		current.LeaseExpiresAt = options.Now.Add(options.Lease)
		token, err := NewLeaseToken()
		if err != nil {
			return nil, err
		}
		current.LeaseToken = token
		store.events[current.ID] = current
		claimed = append(claimed, cloneEvent(current))
	}
	return claimed, nil
}

func (store *InMemoryOutbox) Complete(ctx context.Context, id string, token LeaseToken, completedAt time.Time) error {
	return store.finish(ctx, id, token, completedAt, completedAt, EventCompleted)
}

func (store *InMemoryOutbox) Retry(ctx context.Context, id string, token LeaseToken, retriedAt, availableAt time.Time) error {
	if retriedAt.IsZero() {
		return errors.New("maintenance outbox retry time is required")
	}
	if availableAt.IsZero() {
		return errors.New("maintenance outbox retry time is required")
	}
	if !availableAt.After(retriedAt) {
		return errors.New("maintenance outbox retry must be scheduled after retry time")
	}
	if err := store.finish(ctx, id, token, retriedAt, availableAt, EventFailed); err != nil {
		return err
	}
	return nil
}

func (store *InMemoryOutbox) finish(ctx context.Context, id string, token LeaseToken, at, availableAt time.Time, state EventState) error {
	if store == nil {
		return errors.New("maintenance outbox store is nil")
	}
	if id == "" || token.IsZero() || at.IsZero() {
		return errors.New("maintenance outbox completion identity and time are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	event, ok := store.events[id]
	if !ok {
		return ErrOutboxNotClaimed
	}
	// Repeated completion/failure with the same token is an idempotent
	// success. A subsequent claim always replaces the token first, fencing
	// the old worker.
	if event.LeaseToken != token {
		return ErrOutboxNotClaimed
	}
	if event.State == state {
		return nil
	}
	if event.State != EventProcessing || !event.LeaseExpiresAt.After(at) {
		return ErrOutboxNotClaimed
	}
	event.State = state
	event.LeaseExpiresAt = time.Time{}
	if state == EventCompleted {
		event.CompletedAt = at
	} else {
		event.AvailableAt = availableAt
		event.CompletedAt = time.Time{}
	}
	store.events[id] = event
	return nil
}

func (store *InMemoryOutbox) Snapshot() []Event {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]Event, 0, len(store.events))
	for _, event := range store.events {
		result = append(result, cloneEvent(event))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func cloneEvent(event Event) Event {
	event.SafePayload = append(json.RawMessage(nil), event.SafePayload...)
	return event
}

type DeleteHandler func(context.Context, Event) error

type DispatchOptions struct {
	Now        time.Time
	Limit      int
	Lease      time.Duration
	RetryDelay time.Duration
}

func (options DispatchOptions) Validate() error {
	if options.Now.IsZero() {
		return errors.New("maintenance dispatch time is required")
	}
	if options.Limit <= 0 || options.Limit > 10_000 {
		return errors.New("maintenance dispatch limit must be between 1 and 10000")
	}
	if options.Lease <= 0 || options.RetryDelay <= 0 {
		return errors.New("maintenance dispatch lease and retry delay must be positive")
	}
	return nil
}

type DispatchResult struct {
	Claimed       int
	Completed     int
	MissingObject int
	Retried       int
}

// Dispatcher executes only one bounded batch. Handler failures are recorded
// for retry and returned as progress, while ownership/transaction failures
// stop the pass so operators can distinguish storage trouble from an object
// that was already deleted.
type Dispatcher struct {
	Store  OutboxStore
	Delete DeleteHandler
}

func (dispatcher Dispatcher) RunOnce(ctx context.Context, options DispatchOptions) (DispatchResult, error) {
	var result DispatchResult
	if dispatcher.Store == nil || dispatcher.Delete == nil {
		return result, errors.New("maintenance dispatcher store and delete handler are required")
	}
	if err := options.Validate(); err != nil {
		return result, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	events, err := dispatcher.Store.Claim(ctx, ClaimOptions{Now: options.Now, Limit: options.Limit, Lease: options.Lease})
	if err != nil {
		return result, err
	}
	result.Claimed = len(events)
	for _, event := range events {
		err := dispatcher.Delete(ctx, event)
		if err == nil || errors.Is(err, ErrObjectNotFound) {
			if err := dispatcher.Store.Complete(ctx, event.ID, event.LeaseToken, options.Now); err != nil {
				return result, err
			}
			result.Completed++
			if errors.Is(err, ErrObjectNotFound) {
				result.MissingObject++
			}
			continue
		}
		if err := dispatcher.Store.Retry(ctx, event.ID, event.LeaseToken, options.Now, options.Now.Add(options.RetryDelay)); err != nil {
			return result, err
		}
		result.Retried++
	}
	return result, nil
}

// DedupeHex is useful for logging and metric labels without exposing payload
// contents.
func DedupeHex(event Event) string { return hex.EncodeToString(event.DedupeKey[:]) }
