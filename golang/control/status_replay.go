package control

import (
	"errors"
	"fmt"
	"time"
)

// PersistedStatusEvent carries the database identity needed to replay the
// domain projection without importing a storage package into control. EventID
// is the append-order tie breaker and must be supplied in ascending ledger
// order by the caller.
type PersistedStatusEvent struct {
	EventID int64
	Event   StatusEvent
}

// StatusReplayInput is one route's bounded event stream. Complete is supplied
// by the storage reader: false means that its bounded read was truncated or
// otherwise cannot prove that every retained event was returned. Replay does
// not sort the stream because projection application follows ledger order,
// not observed timestamp order.
type StatusReplayInput struct {
	ConfigDigest [32]byte
	RouteID      string
	Horizon      time.Time
	Events       []PersistedStatusEvent
	Complete     bool
}

// StatusReplayCoverage describes the input actually considered by replay.
// Complete is deliberately caller-provided and does not assert that the event
// ledger has an unbroken retention history; a false value must not be exposed
// as a historical result.
type StatusReplayCoverage struct {
	ConfigDigest         [32]byte
	RouteID              string
	Horizon              time.Time
	EarliestObservedAt   time.Time
	LatestObservedAt     time.Time
	EventsSeen           int
	EventsApplied        int
	EventsIgnored        int
	EventsSkippedHorizon int
	Complete             bool
}

// StatusReplayResult contains only the RouteStatus domain projection. It does
// not reconstruct storage-specific counters, success/failure timestamps, projection
// versions, or query evidence selected from the event ledger.
type StatusReplayResult struct {
	Status   RouteStatus
	Present  bool
	Coverage StatusReplayCoverage
}

var (
	ErrStatusReplayInput = errors.New("status replay input is invalid")
	ErrStatusReplayOrder = errors.New("status replay events are not in ledger order")
	ErrStatusReplayEvent = errors.New("status replay event is invalid")
	ErrStatusReplayKey   = errors.New("status replay event key does not match input")
)

// ReplayRouteStatus replays one route's validated event stream through the
// same RouteStatus.Apply transition used by live persistence. The caller must
// supply events in strictly ascending EventID order. Events observed after
// Horizon are ignored, while stale events at or before Horizon are passed to
// Apply so its canonical stale/epoch/sticky rules remain authoritative.
func ReplayRouteStatus(input StatusReplayInput) (StatusReplayResult, error) {
	result := StatusReplayResult{Coverage: StatusReplayCoverage{
		ConfigDigest: input.ConfigDigest,
		RouteID:      input.RouteID,
		Horizon:      input.Horizon.UTC(),
		Complete:     input.Complete,
	}}
	if input.ConfigDigest == ([32]byte{}) || input.RouteID == "" || input.Horizon.IsZero() {
		return result, fmt.Errorf("%w: config digest, route id, and horizon are required", ErrStatusReplayInput)
	}
	if err := validateIdentifier("route_id", input.RouteID); err != nil {
		return result, fmt.Errorf("%w: %v", ErrStatusReplayInput, err)
	}
	if input.Horizon.Location() == nil {
		return result, fmt.Errorf("%w: horizon has no location", ErrStatusReplayInput)
	}

	previousID := int64(0)
	for index, persisted := range input.Events {
		result.Coverage.EventsSeen++
		if persisted.EventID <= 0 || (index > 0 && persisted.EventID <= previousID) {
			return result, fmt.Errorf("%w: event %d has id %d after %d", ErrStatusReplayOrder, index, persisted.EventID, previousID)
		}
		previousID = persisted.EventID
		if persisted.Event.ConfigDigest != input.ConfigDigest || persisted.Event.RouteID != input.RouteID {
			return result, fmt.Errorf("%w: event %d has config digest or route %q", ErrStatusReplayKey, index, persisted.Event.RouteID)
		}
		if err := validatePersistedStatusEvent(persisted.Event); err != nil {
			return result, fmt.Errorf("%w %d: %v", ErrStatusReplayEvent, index, err)
		}
		if persisted.Event.ObservedAt.After(result.Coverage.Horizon) {
			result.Coverage.EventsSkippedHorizon++
			continue
		}
		observed := persisted.Event.ObservedAt.UTC()
		if result.Coverage.EarliestObservedAt.IsZero() || observed.Before(result.Coverage.EarliestObservedAt) {
			result.Coverage.EarliestObservedAt = observed
		}
		if result.Coverage.LatestObservedAt.IsZero() || observed.After(result.Coverage.LatestObservedAt) {
			result.Coverage.LatestObservedAt = observed
		}
		if result.Status.Apply(persisted.Event) {
			result.Coverage.EventsApplied++
			result.Present = true
		} else {
			result.Coverage.EventsIgnored++
		}
	}
	return result, nil
}

func validatePersistedStatusEvent(event StatusEvent) error {
	if event.EventDigest == ([32]byte{}) {
		return errors.New("event digest is required")
	}
	return validateStatusObservation(event.StatusObservation)
}
