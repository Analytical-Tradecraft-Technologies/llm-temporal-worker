package activity

import (
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/engine"
)

func TestNormalizeHeartbeatProgressBoundsProviderControlledFacts(t *testing.T) {
	started := time.Date(2026, 7, 27, 0, 0, 0, 0, time.FixedZone("test", 3600))
	last := started.Add(time.Second)
	progress := engine.Progress{
		OperationID: strings.Repeat("x", maxHeartbeatIdentifierBytes+1),
		Phase:       "provider-controlled-raw-phase\nwith-payload",
		RouteIndex:  -1,
		ClassIndex:  maxHeartbeatIndex + 1,
		OutputItems: maxHeartbeatOutputItems + 1,
	}

	got := normalizeHeartbeatProgress(progress, started, last)
	if got.OperationID != "" {
		t.Fatalf("oversized operation ID = %q, want omitted", got.OperationID)
	}
	if got.Phase != heartbeatUnknownPhase {
		t.Fatalf("unknown phase = %q, want %q", got.Phase, heartbeatUnknownPhase)
	}
	if got.RouteIndex != 0 || got.ClassIndex != maxHeartbeatIndex || got.OutputItems != maxHeartbeatOutputItems {
		t.Fatalf("bounded numeric details = %#v", got)
	}
	if got.StartedAt.Location() != time.UTC || got.LastEventAt.Location() != time.UTC {
		t.Fatalf("heartbeat timestamps were not normalized to UTC: %#v", got)
	}
}

func TestNormalizeHeartbeatProgressRejectsUnsafeIdentifier(t *testing.T) {
	progress := engine.Progress{OperationID: "safe\nsecret", Phase: "planning"}
	got := normalizeHeartbeatProgress(progress, time.Unix(1, 0), time.Unix(2, 0))
	if got.OperationID != "" {
		t.Fatalf("control-character operation ID = %q, want omitted", got.OperationID)
	}
	if got.Phase != "planning" {
		t.Fatalf("known phase = %q, want planning", got.Phase)
	}
}

func TestNormalizeHeartbeatProgressClampsEventOrder(t *testing.T) {
	started := time.Unix(100, 0)
	got := normalizeHeartbeatProgress(engine.Progress{Phase: "finalization"}, started, started.Add(-time.Second))
	if !got.LastEventAt.Equal(started) {
		t.Fatalf("out-of-order timestamp = %s, want %s", got.LastEventAt, started)
	}
	got = normalizeHeartbeatProgress(engine.Progress{Phase: "finalization"}, started, started.Add(48*time.Hour))
	if !got.LastEventAt.Equal(started.Add(48 * time.Hour)) {
		t.Fatalf("valid long-running timestamp = %s, want %s", got.LastEventAt, started.Add(48*time.Hour))
	}
}
