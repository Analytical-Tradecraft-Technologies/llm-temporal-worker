package modelsync

import (
	"errors"
	"testing"
	"time"
)

func TestPositiveOrFallsBackForUnsetDurations(t *testing.T) {
	for _, test := range []struct{ value, want time.Duration }{{0, time.Minute}, {-time.Second, time.Minute}, {time.Second, time.Second}} {
		if got := positiveOr(test.value, time.Minute); got != test.want {
			t.Fatalf("positiveOr(%v) = %v, want %v", test.value, got, test.want)
		}
	}
}

func TestRefreshFailedObservesTheCause(t *testing.T) {
	var events []Event
	refresher := &Refresher{Observe: func(event Event) { events = append(events, event) }}
	cause := errors.New("boom")
	if err := refresher.refreshFailed("fetch", cause); err != cause {
		t.Fatalf("refreshFailed = %v", err)
	}
	if len(events) != 1 || events[0] != (Event{Step: "refresh", Outcome: OutcomeFailed, Cause: "fetch"}) {
		t.Fatalf("events = %+v", events)
	}
}
