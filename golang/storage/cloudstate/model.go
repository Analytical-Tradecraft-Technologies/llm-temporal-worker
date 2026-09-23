// Package cloudstate persists worker requests using the portable cloud-storage
// KV, blob, and event-stream contracts. It does not submit provider requests or
// acquire, refund, or retry budget leases.
package cloudstate

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/llm"
)

const (
	RequestIDPrefix = "llmtw_req_"
	PendingShards   = 8
	maxPayloadBytes = 8 << 20
	maxRevisions    = 10000
)

var (
	ErrInvalid = errors.New("invalid durable request")
	ErrCorrupt = errors.New("corrupt durable request")
	// ErrIndexPending means the event committed but its recovery-index update
	// did not. Retry the SAME update, including token, revision and timestamp.
	ErrIndexPending = errors.New("durable request committed; recovery index update pending")
)

type RequestID string

func NewRequestID() (RequestID, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return RequestID(RequestIDPrefix + id.String()), nil
}

func (id RequestID) valid() bool {
	text := strings.TrimPrefix(string(id), RequestIDPrefix)
	parsed, err := uuid.Parse(text)
	return strings.HasPrefix(string(id), RequestIDPrefix) && err == nil && parsed != uuid.Nil && parsed.String() == text
}

// Scope is supplied by an authenticated caller. IDs and pagination tokens are
// locators, never authorization. Raw tenant/project names are encrypted at rest.
type Scope struct {
	Tenant  string `json:"tenant"`
	Project string `json:"project"`
}

func (s Scope) valid() bool {
	return safeText(s.Tenant, 256) && safeText(s.Project, 256)
}

type Status string

const (
	StatusPending         Status = "pending"
	StatusRunning         Status = "running"
	StatusProviderPending Status = "provider_pending"
	StatusCompleted       Status = "completed"
	StatusFailed          Status = "failed"
	StatusOutcomeUnknown  Status = "outcome_unknown"
)

func (s Status) terminal() bool { return s == StatusCompleted || s == StatusFailed }
func (s Status) valid() bool {
	switch s {
	case StatusPending, StatusRunning, StatusProviderPending, StatusCompleted, StatusFailed, StatusOutcomeUnknown:
		return true
	}
	return false
}

// CreateRequest must be retained unchanged across retries. Manifest contains
// the full normalized input needed for recovery, including configuration/policy
// versions and parent context references. CreatedAt is caller-supplied so a lost
// acknowledgement can be reconciled without generating a different record.
type CreateRequest struct {
	ID           RequestID       `json:"id"`
	Scope        Scope           `json:"scope"`
	Kind         string          `json:"kind"`
	RequestIndex int64           `json:"request_index"`
	Manifest     json.RawMessage `json:"manifest"`
	CreatedAt    time.Time       `json:"created_at"`
}

// Record exposes the materialized state, not event history. Progress is a
// versioned application JSON object for route, provider job, budget receipt and
// other recovery context. A provider ID alone is not sufficient recovery data.
// This repository deliberately does not interpret that workflow-owned schema.
type Record struct {
	Request     CreateRequest   `json:"request"`
	Fingerprint string          `json:"fingerprint"`
	Revision    uint64          `json:"revision"`
	Status      Status          `json:"status"`
	Progress    json.RawMessage `json:"progress"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// Update is one compare-and-set of the materialized state. Every logical update
// needs a distinct Token; retry an uncertain write with identical arguments.
// Completed/failed records are immutable. OutcomeUnknown remains discoverable;
// a higher layer must acquire fresh budget before retrying paid work.
type Update struct {
	ExpectedRevision uint64
	Token            string
	Status           Status
	Progress         json.RawMessage
	UpdatedAt        time.Time
}

type PendingRequest struct {
	ID        RequestID
	Status    Status
	Revision  uint64
	UpdatedAt time.Time
	// Initializing means the discovery entry exists but Create may not have
	// published the first event. Read must succeed before attempting any work.
	Initializing bool
}

type PendingPage struct {
	Requests      []PendingRequest
	NextPageToken string
}

func safeText(value string, max int) bool {
	return utf8.ValidString(value) && len(value) <= max && strings.TrimSpace(value) != "" && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func objectJSON(value json.RawMessage) (json.RawMessage, error) {
	if !utf8.Valid(value) || len(value) == 0 || len(value) > maxPayloadBytes {
		return nil, ErrInvalid
	}
	canonical, err := llm.CanonicalJSONWithLimits(value, maxPayloadBytes, llm.DefaultCanonicalMaxDepth)
	if err != nil || len(canonical) == 0 || canonical[0] != '{' {
		return nil, ErrInvalid
	}
	return canonical, nil
}

func normalizeRequest(r CreateRequest) (CreateRequest, error) {
	if !r.ID.valid() || !r.Scope.valid() || (r.Kind != "generate" && r.Kind != "compact") || r.RequestIndex < 0 || !validTime(r.CreatedAt) {
		return CreateRequest{}, ErrInvalid
	}
	var err error
	r.Manifest, err = objectJSON(r.Manifest)
	r.CreatedAt = r.CreatedAt.UTC()
	return r, err
}

func validTime(t time.Time) bool { return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 }

func validTransition(from, to Status) bool {
	if !to.valid() || from.terminal() {
		return false
	}
	switch from {
	case StatusPending:
		return to == StatusPending || to == StatusRunning || to == StatusCompleted || to == StatusFailed
	case StatusRunning:
		return to == StatusRunning || to == StatusProviderPending || to == StatusCompleted || to == StatusFailed || to == StatusOutcomeUnknown
	case StatusProviderPending:
		return to == StatusProviderPending || to == StatusCompleted || to == StatusFailed || to == StatusOutcomeUnknown
	case StatusOutcomeUnknown:
		// Retry orchestration must use a new paid attempt/budget receipt. Keeping
		// that decision above storage avoids implicitly authorizing provider calls.
		return to == StatusRunning || to == StatusFailed || to == StatusOutcomeUnknown
	}
	return false
}
