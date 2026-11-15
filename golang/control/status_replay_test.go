package control

import (
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"testing"
	"time"
)

func persistedEvent(t *testing.T, id int64, value StatusObservation) PersistedStatusEvent {
	t.Helper()
	event, err := NewStatusEvent(value)
	testassert.NoError(t, err, "NewStatusEvent")
	return PersistedStatusEvent{EventID: id, Event: event}
}

// replayInput returns a replay of events for the test route under digest(1).
func replayInput(horizon time.Time, complete bool, events ...PersistedStatusEvent) StatusReplayInput {
	return StatusReplayInput{ConfigDigest: digest(1), RouteID: "route", Horizon: horizon, Complete: complete, Events: events}
}

func TestReplayRouteStatusUsesLedgerOrderAndHorizonCoverage(t *testing.T) {
	base := time.Unix(100, 0).UTC()
	incident := creditIncident(base.Add(20 * time.Second))
	stale := observation(base.Add(10 * time.Second))
	later := observation(base.Add(40 * time.Second))

	result, err := ReplayRouteStatus(replayInput(base.Add(30*time.Second), false,
		persistedEvent(t, 10, incident),
		persistedEvent(t, 11, stale),
		persistedEvent(t, 12, later),
	))
	testassert.NoError(t, err, "ReplayRouteStatus")
	if !result.Present || result.Status.ObservedAt != incident.ObservedAt || result.Status.Credit != CreditExhausted {
		t.Fatalf("replay result = %#v, want ledger-first incident projection", result)
	}
	coverage := result.Coverage
	if coverage.EventsSeen != 3 || coverage.EventsApplied != 1 || coverage.EventsIgnored != 1 || coverage.EventsSkippedHorizon != 1 {
		t.Fatalf("coverage counts = %#v", coverage)
	}
	if !coverage.EarliestObservedAt.Equal(stale.ObservedAt) || !coverage.LatestObservedAt.Equal(incident.ObservedAt) {
		t.Fatalf("coverage interval = %v..%v", coverage.EarliestObservedAt, coverage.LatestObservedAt)
	}
	if coverage.Complete {
		t.Fatal("incomplete storage read was reported as complete")
	}
}

func TestReplayRouteStatusAllowsEpochTransition(t *testing.T) {
	base := time.Unix(200, 0).UTC()
	incident := creditIncident(base)
	nextEpoch := observation(base.Add(time.Second))
	nextEpoch.ConfigEpoch = "epoch-2"

	result, err := ReplayRouteStatus(replayInput(base.Add(time.Minute), true, persistedEvent(t, 1, incident), persistedEvent(t, 2, nextEpoch)))
	testassert.NoError(t, err, "ReplayRouteStatus")
	if result.Status.ConfigEpoch != "epoch-2" || result.Status.Credit != CreditOK || result.Status.Billing != BillingOK {
		t.Fatalf("epoch transition retained old incident: %#v", result.Status)
	}
	if result.Coverage.EventsApplied != 2 || !result.Coverage.Complete {
		t.Fatalf("epoch transition coverage = %#v", result.Coverage)
	}
}

func TestReplayRouteStatusIncludesEventAtHorizon(t *testing.T) {
	horizon := time.Unix(250, 0).UTC()
	value := observation(horizon)
	result, err := ReplayRouteStatus(replayInput(horizon, true, persistedEvent(t, 1, value)))
	testassert.NoError(t, err, "ReplayRouteStatus")
	if result.Coverage.EventsApplied != 1 || result.Coverage.EventsSkippedHorizon != 0 || !result.Status.ObservedAt.Equal(horizon) {
		t.Fatalf("event at horizon was not included: %#v", result)
	}
}

func TestReplayRouteStatusRejectsMixedKeysWithoutApplying(t *testing.T) {
	base := time.Unix(300, 0).UTC()
	tests := []struct {
		name  string
		value StatusObservation
	}{
		{name: "route", value: func() StatusObservation {
			value := observation(base)
			value.RouteID = "other-route"
			return value
		}()},
		{name: "config", value: func() StatusObservation {
			value := observation(base)
			value.ConfigDigest = digest(9)
			return value
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := persistedEvent(t, 1, test.value)
			result, err := ReplayRouteStatus(replayInput(base.Add(time.Minute), true, event))
			testassert.ErrorIs(t, err, ErrStatusReplayKey)
			if result.Present || !result.Status.ObservedAt.IsZero() || result.Coverage.EventsApplied != 0 {
				t.Fatalf("mixed key mutated replay result: %#v", result)
			}
		})
	}
}

func TestReplayRouteStatusRejectsNonascendingEventIDs(t *testing.T) {
	base := time.Unix(400, 0).UTC()
	events := []PersistedStatusEvent{
		persistedEvent(t, 2, observation(base)),
		persistedEvent(t, 1, observation(base.Add(time.Second))),
	}
	_, err := ReplayRouteStatus(replayInput(base.Add(time.Minute), false, events...))
	testassert.ErrorIs(t, err, ErrStatusReplayOrder)
}

func TestReplayRouteStatusRejectsMissingDigest(t *testing.T) {
	base := time.Unix(500, 0).UTC()
	event := persistedEvent(t, 1, observation(base))
	event.Event.EventDigest = [32]byte{}
	_, err := ReplayRouteStatus(replayInput(base.Add(time.Minute), false, event))
	if !errors.Is(err, ErrStatusReplayEvent) {
		t.Fatalf("error = %v, want ErrStatusReplayEvent for missing digest", err)
	}
}

func TestReplayRouteStatusAcceptsMicrosecondRoundedTimestamps(t *testing.T) {
	base := time.Unix(600, 123456789).UTC()
	original := observation(base)
	event := persistedEvent(t, 1, original)
	event.Event.ObservedAt = event.Event.ObservedAt.Round(time.Microsecond)
	event.Event.ExpiresAt = event.Event.ExpiresAt.Round(time.Microsecond)
	result, err := ReplayRouteStatus(replayInput(event.Event.ObservedAt, true, event))
	testassert.NoError(t, err, "rounded persisted event rejected")
	if !result.Present || !result.Status.ObservedAt.Equal(event.Event.ObservedAt) {
		t.Fatalf("rounded replay result = %#v", result)
	}
}
