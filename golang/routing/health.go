package routing

import (
	"sync"
	"time"
)

type FailureKind string

const (
	FailureDefiniteTransient FailureKind = "definite_transient"
	FailureAuthentication    FailureKind = "authentication"
	FailureConfiguration     FailureKind = "configuration"
	FailureAmbiguous         FailureKind = "ambiguous"
	FailureSuccess           FailureKind = "success"
)

type HealthPolicy struct {
	Threshold int
	Cooldown  time.Duration
}

type healthEntry struct {
	RouteID        string
	Failures       int
	OpenUntil      time.Time
	Authentication bool
	SnapshotID     string
}

type PassiveHealth struct {
	mu      sync.RWMutex
	policy  HealthPolicy
	clock   func() time.Time
	entries map[string]healthEntry
}

func NewPassiveHealth(policy HealthPolicy, clock func() time.Time) *PassiveHealth {
	if policy.Threshold <= 0 {
		policy.Threshold = 3
	}
	if policy.Cooldown <= 0 {
		policy.Cooldown = 30 * time.Second
	}
	if clock == nil {
		clock = time.Now
	}
	return &PassiveHealth{policy: policy, clock: clock, entries: make(map[string]healthEntry)}
}

func (health *PassiveHealth) Record(routeID, snapshotID string, kind FailureKind) {
	if health == nil || routeID == "" {
		return
	}
	health.mu.Lock()
	defer health.mu.Unlock()
	entry := health.entries[routeID]
	entry.RouteID = routeID
	if entry.SnapshotID != snapshotID && snapshotID != "" {
		entry = healthEntry{RouteID: routeID, SnapshotID: snapshotID}
	}
	switch kind {
	case FailureSuccess:
		entry.Failures = 0
		entry.OpenUntil = time.Time{}
		entry.Authentication = false
	case FailureDefiniteTransient:
		entry.Failures++
		if entry.Failures >= health.policy.Threshold {
			entry.OpenUntil = health.clock().Add(health.policy.Cooldown)
		}
	case FailureAuthentication, FailureConfiguration:
		entry.Authentication = true
		entry.OpenUntil = time.Time{}
	case FailureAmbiguous:
		// An ambiguous request never counts as safe endpoint failure.
	}
	health.entries[routeID] = entry
}

func (health *PassiveHealth) View(snapshotID string) HealthView {
	view := HealthView{Routes: make(map[string]RouteHealth)}
	if health == nil {
		return view
	}
	health.mu.RLock()
	defer health.mu.RUnlock()
	now := health.clock()
	for routeID, entry := range health.entries {
		// Authentication/configuration failures are scoped to the immutable
		// snapshot that produced them. A replacement snapshot is the explicit
		// operator signal that the route may be retried; do not carry the old
		// open state into planning for that snapshot. Keep the entry when the
		// caller has no snapshot identity so diagnostic consumers can still
		// inspect process-local health.
		if snapshotID != "" && entry.SnapshotID != "" && entry.SnapshotID != snapshotID {
			continue
		}
		open := entry.Authentication || (!entry.OpenUntil.IsZero() && now.Before(entry.OpenUntil))
		if !entry.OpenUntil.IsZero() && !now.Before(entry.OpenUntil) && !entry.Authentication {
			open = false
		}
		view.Routes[routeID] = RouteHealth{Enabled: true, Open: open, AuthOpen: entry.Authentication, Reason: healthReason(entry), SnapshotID: snapshotID}
	}
	return view
}

func healthReason(entry healthEntry) string {
	if entry.Authentication {
		return "authentication or configuration failure"
	}
	if entry.Failures > 0 {
		return "definite transient failures"
	}
	return ""
}
