package activity

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/engine"
	"go.temporal.io/sdk/testsuite"
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

func TestTemporalHeartbeaterRecordsAgeSinceMostRecentEvent(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	metrics := &heartbeatAgeRecorder{}
	heartbeater := NewTemporalHeartbeater(TemporalHeartbeaterOptions{
		Clock:   func() time.Time { return now },
		Metrics: metrics,
	})

	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	beat := func(ctx context.Context, progress engine.Progress) error {
		return heartbeater.Beat(ctx, progress)
	}
	environment.RegisterActivity(beat)
	_, err := environment.ExecuteActivity(beat, engine.Progress{Phase: "planning", At: now})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.age != 0 {
		t.Fatalf("initial heartbeat age = %s, want zero", metrics.age)
	}

	now = now.Add(4 * time.Second)
	_, err = environment.ExecuteActivity(beat, engine.Progress{Phase: "provider_wait", At: now.Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.age != time.Second {
		t.Fatalf("most recent heartbeat age = %s, want 1s", metrics.age)
	}
}

type heartbeatAgeRecorder struct{ age time.Duration }

func (recorder *heartbeatAgeRecorder) SetHeartbeatAge(age time.Duration) {
	recorder.age = age
}
