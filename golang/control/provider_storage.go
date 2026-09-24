package control

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"strings"
	"time"
)

// ProviderStatusReader exposes current operational state, without a storage dependency.
type ProviderStatusReader interface {
	ListRouteStatuses(context.Context, ProviderStatusListOptions) (ProviderStatusPage, error)
	ListCreditStatuses(context.Context, CreditStatusListOptions) (CreditStatusPage, error)
}

// InventoryReader exposes cached provider model listings.
type InventoryReader interface {
	ListInventoryModels(context.Context, InventoryModelListOptions) (InventoryModelPage, error)
}

// InventoryStore caches the latest observation per configured provider endpoint/account.
// PersistInventorySnapshot returns false for duplicate or older observations.
type InventoryStore interface {
	InventoryReader
	PersistInventorySnapshot(context.Context, InventorySnapshot) (bool, error)
	GetInventorySnapshot(context.Context, [32]byte, string, string, [32]byte) (InventorySnapshot, error)
}

var ErrProviderStatusNotFound = errors.New("provider route status not found")
var ErrInventorySnapshotNotFound = errors.New("provider inventory snapshot not found")
var ErrProviderViewExpired = errors.New("provider query view expired; restart pagination")

const (
	DefaultProviderStatusPageSize = 100
	MaxProviderStatusPageSize     = 1000
)

// ProviderStatusListOptions describes the storage portion of a
// provider-status query.  AfterRouteID is an unsigned keyset position.  The
// control layer must authenticate it before passing it here.
type ProviderStatusListOptions struct {
	ConfigDigest   [32]byte
	Provider       string
	EndpointID     string
	Availability   Availability
	IncludeHealthy bool
	// SnapshotHorizon pins a paginated read to the observation horizon chosen
	// by the control/query layer. A zero value preserves the historical
	// current-projection read for internal callers that do not paginate.
	SnapshotHorizon time.Time
	AfterRouteID    string
	Limit           int
}

// ProviderStatusPage is a bounded projection page.  NextRouteID is empty
// when the page is complete; otherwise it is the last route key needed for a
// subsequent keyset read.  It is not a signed public cursor.
type ProviderStatusPage struct {
	Routes      []RouteStatus
	NextRouteID string
}

func (options *ProviderStatusListOptions) Normalize() error {
	if options == nil {
		return errors.New("provider status list options are nil")
	}
	if options.ConfigDigest == ([32]byte{}) {
		return errors.New("provider status list config digest is required")
	}
	for name, value := range map[string]string{
		"provider":    options.Provider,
		"endpoint_id": options.EndpointID,
		"after_route": options.AfterRouteID,
	} {
		if value == "" {
			continue
		}
		if err := validateProviderStatusQueryIdentifier(name, value); err != nil {
			return err
		}
	}
	if options.Availability != "" && !validProviderStatusAvailability(options.Availability) {
		return fmt.Errorf("provider status availability %q is invalid", options.Availability)
	}
	if !options.SnapshotHorizon.IsZero() {
		options.SnapshotHorizon = options.SnapshotHorizon.UTC()
	}
	if options.Limit == 0 {
		options.Limit = DefaultProviderStatusPageSize
	}
	if options.Limit < 1 || options.Limit > MaxProviderStatusPageSize {
		return fmt.Errorf("provider status page size must be between 1 and %d", MaxProviderStatusPageSize)
	}
	return nil
}

const (
	DefaultInventoryPageSize = 100
	MaxInventoryPageSize     = 1000
)

// InventoryModelPosition is the unsigned storage keyset position.  Public
// query cursors must authenticate all four fields before passing them to the
// repository.
type InventoryModelPosition struct {
	Provider        string
	EndpointID      string
	SnapshotID      uuid.UUID
	ProviderModelID string
}

// InventoryModelListOptions describes filters and the pinned snapshot
// horizon for a persisted inventory read.  A zero SnapshotHorizon starts
// a current read and pins its snapshot horizon; callers
// must pass the returned horizon on the next page.
type InventoryModelListOptions struct {
	ConfigDigest    [32]byte
	Provider        string
	EndpointID      string
	ModelPrefix     string
	Lifecycle       Lifecycle
	SnapshotHorizon time.Time
	After           InventoryModelPosition
	Limit           int
}

// InventorySnapshotInfo is the provenance needed by the query layer to
// report support, completeness, and current/stale state without exposing
// provider credentials or raw responses.
type InventorySnapshotInfo struct {
	ID              uuid.UUID
	ConfigDigest    [32]byte
	InventoryDigest [32]byte
	Provider        string
	EndpointID      string
	Source          InventorySource
	ObservedAt      time.Time
	ExpiresAt       time.Time
	Complete        bool
}

func (snapshot InventorySnapshotInfo) ProvenanceAt(now time.Time) Provenance {
	if snapshot.Source == InventoryUnsupported {
		return ProvenanceUnsupported
	}
	if now.IsZero() || !now.Before(snapshot.ExpiresAt) {
		return ProvenanceStale
	}
	return ProvenanceCurrent
}

// InventoryModelRecord combines one normalized model row with its immutable
// snapshot provenance.  The Model value retains the capability digest
// and safe metadata exactly as persisted; it does not invent wire-level
// capability names.
type InventoryModelRecord struct {
	Snapshot InventorySnapshotInfo
	Model    Model
}

// InventoryModelPage is a bounded, stable storage page.  Next is nil when no
// further model row exists.  SnapshotHorizon is a required cursor-binding
// value for callers that request another page.
type InventoryModelPage struct {
	Models          []InventoryModelRecord
	Next            *InventoryModelPosition
	SnapshotHorizon time.Time
}

func (options *InventoryModelListOptions) Normalize() error {
	if options == nil {
		return errors.New("inventory model list options are nil")
	}
	if options.ConfigDigest == ([32]byte{}) {
		return errors.New("inventory model list config digest is required")
	}
	for name, value := range map[string]string{
		"provider":       options.Provider,
		"endpoint_id":    options.EndpointID,
		"model_prefix":   options.ModelPrefix,
		"after_provider": options.After.Provider,
		"after_endpoint": options.After.EndpointID,
		"after_model":    options.After.ProviderModelID,
	} {
		if value == "" {
			continue
		}
		if len(value) > 256 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("inventory model %s is empty or unsafe", name)
		}
	}
	if options.Lifecycle != "" && !validInventoryLifecycle(options.Lifecycle) {
		return fmt.Errorf("inventory model lifecycle %q is invalid", options.Lifecycle)
	}
	if options.Limit == 0 {
		options.Limit = DefaultInventoryPageSize
	}
	if options.Limit < 1 || options.Limit > MaxInventoryPageSize {
		return fmt.Errorf("inventory model page size must be between 1 and %d", MaxInventoryPageSize)
	}
	if options.SnapshotHorizon.IsZero() && (options.After != InventoryModelPosition{}) {
		return errors.New("inventory model continuation requires a snapshot horizon")
	}
	if (options.After.Provider == "") != (options.After.EndpointID == "") ||
		(options.After.EndpointID == "") != (options.After.ProviderModelID == "") ||
		(options.After.ProviderModelID == "") != (options.After.SnapshotID == uuid.Nil) {
		return errors.New("inventory model continuation position is incomplete")
	}
	if options.Provider != "" && options.After.Provider != "" && options.Provider != options.After.Provider {
		return errors.New("inventory model continuation provider does not match filter")
	}
	if options.EndpointID != "" && options.After.EndpointID != "" && options.EndpointID != options.After.EndpointID {
		return errors.New("inventory model continuation endpoint does not match filter")
	}
	if !options.SnapshotHorizon.IsZero() {
		options.SnapshotHorizon = options.SnapshotHorizon.UTC()
	}
	return nil
}

const (
	DefaultCreditStatusPageSize = 100
	MaxCreditStatusPageSize     = 1000
)

// CreditStatusListOptions describes the unsigned storage portion of a
// credit-status query. Endpoint IDs are the stable keyset position because a
// configured endpoint may have more than one route projection; the query
// chooses the latest projection for each endpoint deterministically.
type CreditStatusListOptions struct {
	ConfigDigest     [32]byte
	Provider         string
	EndpointID       string
	IncludeOK        bool
	SnapshotHorizon  time.Time
	AfterEndpointKey string
	Limit            int
}

func (options *CreditStatusListOptions) Normalize() error {
	if options == nil {
		return errors.New("credit status list options are nil")
	}
	if options.ConfigDigest == ([32]byte{}) {
		return errors.New("credit status list config digest is required")
	}
	for name, value := range map[string]string{
		"provider":    options.Provider,
		"endpoint_id": options.EndpointID,
	} {
		if value == "" {
			continue
		}
		if err := validateProviderStatusQueryIdentifier(name, value); err != nil {
			return err
		}
	}
	if options.AfterEndpointKey != "" {
		if _, _, err := splitCreditStatusKey(options.AfterEndpointKey); err != nil {
			return err
		}
	}
	if !options.SnapshotHorizon.IsZero() {
		options.SnapshotHorizon = options.SnapshotHorizon.UTC()
	}
	if options.Limit == 0 {
		options.Limit = DefaultCreditStatusPageSize
	}
	if options.Limit < 1 || options.Limit > MaxCreditStatusPageSize {
		return fmt.Errorf("credit status page size must be between 1 and %d", MaxCreditStatusPageSize)
	}
	return nil
}

func validateProviderStatusQueryIdentifier(name, value string) error {
	if len(value) > 256 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("provider status %s is empty or unsafe", name)
	}
	return nil
}

func validProviderStatusAvailability(value Availability) bool {
	switch value {
	case AvailabilityAvailable, AvailabilityDegraded,
		AvailabilityUnavailable, AvailabilityUnknown:
		return true
	default:
		return false
	}
}

func validProviderStatusCredit(value CreditState) bool {
	switch value {
	case CreditOK, CreditLow, CreditExhausted, CreditUnknown:
		return true
	default:
		return false
	}
}

func validProviderStatusBilling(value BillingState) bool {
	switch value {
	case BillingOK, BillingIssue, BillingUnknown:
		return true
	default:
		return false
	}
}

func validProviderStatusCircuit(value CircuitState) bool {
	switch value {
	case CircuitClosed, CircuitOpen, CircuitHalfOpen:
		return true
	default:
		return false
	}
}

func validInventoryLifecycle(value Lifecycle) bool {
	switch value {
	case LifecycleAvailable, LifecycleDeprecated,
		LifecycleUnavailable, LifecycleUnknown:
		return true
	default:
		return false
	}
}

func splitCreditStatusKey(key string) (provider, endpoint string, err error) {
	if key == "" {
		return "", "", nil
	}
	provider, endpoint, ok := strings.Cut(key, "\x00")
	if !ok || provider == "" || endpoint == "" {
		return "", "", errors.New("credit status continuation key is invalid")
	}
	if err := validateProviderStatusQueryIdentifier("after_provider", provider); err != nil {
		return "", "", err
	}
	if err := validateProviderStatusQueryIdentifier("after_endpoint", endpoint); err != nil {
		return "", "", err
	}
	return provider, endpoint, nil
}
