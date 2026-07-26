package activity

import (
	"context"
	"sync"
	"time"
	"unicode"

	"github.com/mfow/llm-temporal-worker/golang/engine"
	sdkactivity "go.temporal.io/sdk/activity"
)

type HeartbeatDetails struct {
	OperationID string    `json:"operation_id,omitempty"`
	Phase       string    `json:"phase"`
	RouteIndex  int       `json:"route_index"`
	ClassIndex  int       `json:"class_index"`
	StartedAt   time.Time `json:"started_at"`
	LastEventAt time.Time `json:"last_event_at"`
	OutputItems int       `json:"output_items"`
}

const (
	// Temporal heartbeats are a liveness signal, not a second payload channel.
	// Keep every field bounded even if an adapter or embedding supplies malformed
	// progress to the public Heartbeater seam.
	maxHeartbeatIdentifierBytes = 128
	maxHeartbeatIndex           = 1 << 16
	maxHeartbeatOutputItems     = 1 << 20
	heartbeatUnknownPhase       = "other"
)

var heartbeatPhases = map[string]struct{}{
	"planning": {}, "admission": {}, "pre_write": {}, "provider_wait": {},
	"response_received": {}, "lift": {}, "finalization": {},
	"continuation_write": {},
}

type Heartbeater interface {
	Beat(context.Context, engine.Progress) error
}

// HeartbeatMetrics is the deliberately narrow metrics dependency needed by a
// per-Activity heartbeater. It keeps the Activity package independent of a
// particular Prometheus implementation.
type HeartbeatMetrics interface {
	SetHeartbeatAge(time.Duration)
}

type TemporalHeartbeaterOptions struct {
	Clock   func() time.Time
	Metrics HeartbeatMetrics
}

type TemporalHeartbeater struct {
	mu      sync.Mutex
	started time.Time
	clock   func() time.Time
	metrics HeartbeatMetrics
}

func NewTemporalHeartbeater(options TemporalHeartbeaterOptions) *TemporalHeartbeater {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	return &TemporalHeartbeater{clock: options.Clock, metrics: options.Metrics}
}

func (heartbeater *TemporalHeartbeater) Beat(ctx context.Context, progress engine.Progress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if heartbeater == nil {
		return nil
	}
	// Use one observation timestamp for the lifecycle update and the age
	// metric. The metric is the age of the most recent event, not the elapsed
	// duration of the Activity since its first heartbeat.
	now := heartbeater.now()
	heartbeater.mu.Lock()
	if heartbeater.started.IsZero() {
		heartbeater.started = progress.At
		if heartbeater.started.IsZero() {
			heartbeater.started = now
		}
	}
	started := heartbeater.started
	heartbeater.mu.Unlock()
	last := progress.At
	if last.IsZero() {
		last = now
	}
	details := normalizeHeartbeatProgress(progress, started, last)
	if heartbeater.metrics != nil {
		heartbeater.metrics.SetHeartbeatAge(nonNegativeHeartbeatAge(now, details.LastEventAt))
	}
	sdkactivity.RecordHeartbeat(ctx, details)
	return ctx.Err()
}

func nonNegativeHeartbeatAge(now, eventAt time.Time) time.Duration {
	age := now.Sub(eventAt)
	if age < 0 {
		return 0
	}
	return age
}

func normalizeHeartbeatProgress(progress engine.Progress, started, last time.Time) HeartbeatDetails {
	started = started.UTC()
	last = last.UTC()
	if last.Before(started) {
		last = started
	}
	return HeartbeatDetails{
		OperationID: safeHeartbeatIdentifier(progress.OperationID),
		Phase:       safeHeartbeatPhase(progress.Phase),
		RouteIndex:  boundedHeartbeatInt(progress.RouteIndex, maxHeartbeatIndex),
		ClassIndex:  boundedHeartbeatInt(progress.ClassIndex, maxHeartbeatIndex),
		StartedAt:   started.UTC(),
		LastEventAt: last.UTC(),
		OutputItems: boundedHeartbeatInt(progress.OutputItems, maxHeartbeatOutputItems),
	}
}

func safeHeartbeatIdentifier(value string) string {
	if value == "" || len(value) > maxHeartbeatIdentifierBytes {
		return ""
	}
	for _, runeValue := range value {
		if unicode.IsControl(runeValue) {
			return ""
		}
	}
	return value
}

func safeHeartbeatPhase(value string) string {
	if _, ok := heartbeatPhases[value]; ok {
		return value
	}
	// Do not copy provider-controlled phase text into Temporal history. The
	// fixed fallback preserves a useful liveness fact without exposing an
	// unbounded or undocumented value.
	return heartbeatUnknownPhase
}

func boundedHeartbeatInt(value, maximum int) int {
	if value < 0 {
		return 0
	}
	if value > maximum {
		return maximum
	}
	return value
}

func (heartbeater *TemporalHeartbeater) now() time.Time {
	if heartbeater != nil && heartbeater.clock != nil {
		return heartbeater.clock().UTC()
	}
	return time.Now().UTC()
}
