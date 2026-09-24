package budget

import (
	"fmt"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

// JournalEventKind is the budget accounting event vocabulary. The historical
// name remains for callers, but these events now settle directly in Redis;
// they do not require a SQL journal.
type JournalEventKind string

const (
	JournalReserve             JournalEventKind = "reserve"
	JournalFinalizeExact       JournalEventKind = "finalize_exact"
	JournalFinalizeUnknown     JournalEventKind = "finalize_unknown"
	JournalRetainAmbiguous     JournalEventKind = "retain_ambiguous"
	JournalResolveUnknownExact JournalEventKind = "resolve_unknown_exact"
	JournalRelease             JournalEventKind = "release"
)

// ActualCostStatus describes whether a journal event has a known exact cost.
// Unknown costs retain the conservative reservation and must carry a safe
// reason code; they never use a made-up zero amount.
type ActualCostStatus string

const (
	CostPending ActualCostStatus = "pending"
	CostExact   ActualCostStatus = "exact"
	CostUnknown ActualCostStatus = "unknown"
)

// ReservationEvent is the write-ahead record created after Redis accepts a
// reservation and before a provider side effect. IDs are opaque at the domain
// boundary; the PostgreSQL adapter validates them as UUIDs before binding.
type ReservationEvent struct {
	EventID             string
	GenerationID        string
	OperationID         string
	WindowID            string
	BucketStart         time.Time
	ReservationRevision int
	AmountUSD           pricing.USD
	OccurredAt          time.Time
}

// CompletionEvent represents every post-dispatch budget transition. The
// event kind determines which deltas are legal and how the reservation
// projection is updated.
type CompletionEvent struct {
	EventID              string
	GenerationID         string
	OperationID          string
	WindowID             string
	BucketStart          time.Time
	ReservationRevision  int
	Kind                 JournalEventKind
	ReservedDecreaseUSD  pricing.USD
	AccountedIncreaseUSD pricing.USD
	AccountedDecreaseUSD pricing.USD
	ActualCostUSD        *pricing.USD
	CostStatus           ActualCostStatus
	UnknownReasonCode    string
	OccurredAt           time.Time
}

func (event ReservationEvent) Validate() error {
	if err := validateIdentity(event.EventID, "event_id"); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"generation_id": event.GenerationID,
		"operation_id":  event.OperationID,
		"window_id":     event.WindowID,
	} {
		if err := validateIdentity(value, name); err != nil {
			return err
		}
	}
	if event.BucketStart.IsZero() || event.OccurredAt.IsZero() {
		return fmt.Errorf("budget reservation timestamps are required")
	}
	if event.ReservationRevision < 0 {
		return fmt.Errorf("reservation revision must be non-negative")
	}
	if err := event.AmountUSD.Validate(); err != nil {
		return fmt.Errorf("reservation amount: %w", err)
	}
	if event.AmountUSD.IsZero() {
		return fmt.Errorf("reservation amount must be positive")
	}
	return nil
}

func (event CompletionEvent) Validate() error {
	if err := validateIdentity(event.EventID, "event_id"); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"generation_id": event.GenerationID,
		"operation_id":  event.OperationID,
		"window_id":     event.WindowID,
	} {
		if err := validateIdentity(value, name); err != nil {
			return err
		}
	}
	if event.BucketStart.IsZero() || event.OccurredAt.IsZero() {
		return fmt.Errorf("budget completion timestamps are required")
	}
	if event.ReservationRevision < 0 {
		return fmt.Errorf("reservation revision must be non-negative")
	}
	if err := validateUSDFields(event.ReservedDecreaseUSD, event.AccountedIncreaseUSD, event.AccountedDecreaseUSD); err != nil {
		return err
	}
	if event.ActualCostUSD != nil {
		if err := event.ActualCostUSD.Validate(); err != nil {
			return fmt.Errorf("actual cost: %w", err)
		}
	}
	if event.CostStatus != CostPending && event.CostStatus != CostExact && event.CostStatus != CostUnknown {
		return fmt.Errorf("invalid actual cost status %q", event.CostStatus)
	}
	if event.UnknownReasonCode != "" && !safeReasonCode(event.UnknownReasonCode) {
		return fmt.Errorf("unknown cost reason code is unsafe")
	}
	if event.CostStatus != CostUnknown && event.UnknownReasonCode != "" {
		return fmt.Errorf("unknown cost reason is only valid for unknown cost")
	}
	if err := validateKindDeltas(event); err != nil {
		return err
	}
	return nil
}

func validateIdentity(value, name string) error {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("%s is empty or unsafe", name)
	}
	return nil
}

func validateUSDFields(values ...pricing.USD) error {
	for _, value := range values {
		if err := value.Validate(); err != nil {
			return fmt.Errorf("budget delta: %w", err)
		}
	}
	return nil
}

func safeReasonCode(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func validateKindDeltas(event CompletionEvent) error {
	zero := func(value pricing.USD) bool { return value.IsZero() }
	switch event.Kind {
	case JournalFinalizeExact:
		if event.CostStatus != CostExact || event.ActualCostUSD == nil || zero(*event.ActualCostUSD) && !zero(event.AccountedIncreaseUSD) {
			return fmt.Errorf("finalize_exact requires an exact actual cost")
		}
		if zero(event.ReservedDecreaseUSD) || event.AccountedIncreaseUSD.Cmp(*event.ActualCostUSD) != 0 || !zero(event.AccountedDecreaseUSD) {
			return fmt.Errorf("finalize_exact deltas are inconsistent")
		}
	case JournalFinalizeUnknown:
		if event.CostStatus != CostUnknown || event.ActualCostUSD != nil || event.UnknownReasonCode == "" {
			return fmt.Errorf("finalize_unknown requires a safe unknown cost")
		}
		if zero(event.ReservedDecreaseUSD) || event.AccountedIncreaseUSD.Cmp(event.ReservedDecreaseUSD) != 0 || !zero(event.AccountedDecreaseUSD) {
			return fmt.Errorf("finalize_unknown deltas are inconsistent")
		}
	case JournalRetainAmbiguous:
		if event.CostStatus != CostUnknown || event.ActualCostUSD != nil || event.UnknownReasonCode == "" {
			return fmt.Errorf("retain_ambiguous requires a safe unknown cost")
		}
		if !zero(event.ReservedDecreaseUSD) || !zero(event.AccountedIncreaseUSD) || !zero(event.AccountedDecreaseUSD) {
			return fmt.Errorf("retain_ambiguous must not change accounting")
		}
	case JournalResolveUnknownExact:
		if event.CostStatus != CostExact || event.ActualCostUSD == nil {
			return fmt.Errorf("resolve_unknown_exact requires an exact cost")
		}
		if zero(event.ReservedDecreaseUSD) == zero(event.AccountedDecreaseUSD) || event.AccountedIncreaseUSD.Cmp(*event.ActualCostUSD) != 0 {
			return fmt.Errorf("resolve_unknown_exact deltas are inconsistent")
		}
	case JournalRelease:
		if event.CostStatus != CostExact || event.ActualCostUSD == nil || !event.ActualCostUSD.IsZero() || zero(event.ReservedDecreaseUSD) || !zero(event.AccountedIncreaseUSD) || !zero(event.AccountedDecreaseUSD) {
			return fmt.Errorf("release must return a known zero cost")
		}
	default:
		return fmt.Errorf("invalid completion event kind %q", event.Kind)
	}
	return nil
}
