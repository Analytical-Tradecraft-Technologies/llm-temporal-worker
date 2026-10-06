package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/storage/memory"
)

// A completed operation replays its stored result even after its route was
// removed from the catalog (#753).
func TestGenerateReplaysACompletedOperationAfterItsRouteIsRemoved(t *testing.T) {
	adapter := &fakeAdapter{name: "replay", response: successfulResponse()}
	harness := newHarness(t, adapter)
	first, err := harness.engine.Generate(context.Background(), baseRequest("replay-after-route-removal"))
	if err != nil {
		t.Fatal(err)
	}
	static := harness.engine.dependencies.Snapshots.(StaticSnapshot)
	static.Value.Routes = routing.Catalog{Version: "routes-2", Models: map[string]routing.Model{}}
	harness.engine.dependencies.Snapshots = static
	replay, err := harness.engine.Generate(context.Background(), baseRequest("replay-after-route-removal"))
	if err != nil {
		t.Fatalf("replay error = %v, want the stored result", err)
	}
	if replay.OperationID != first.OperationID || adapter.invokes != 1 {
		t.Fatalf("replay operation = %q (first %q), provider invokes = %d", replay.OperationID, first.OperationID, adapter.invokes)
	}
	// A new request still needs a route.
	if _, err := harness.engine.Generate(context.Background(), baseRequest("new-after-route-removal")); err == nil {
		t.Fatal("new request without a route succeeded")
	}
}

type unavailableSnapshot struct{}

func (unavailableSnapshot) Current(context.Context) (Snapshot, error) {
	return Snapshot{}, errors.New("configuration source unavailable")
}

// A completed operation replays before the configuration snapshot is read, so
// an unavailable configuration source does not hide its stored result.
func TestGenerateReplaysACompletedOperationWhileConfigurationIsUnavailable(t *testing.T) {
	adapter := &fakeAdapter{name: "replay", response: successfulResponse()}
	harness := newHarness(t, adapter)
	first, err := harness.engine.Generate(context.Background(), baseRequest("replay-without-snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	harness.engine.dependencies.Snapshots = unavailableSnapshot{}
	replay, err := harness.engine.Generate(context.Background(), baseRequest("replay-without-snapshot"))
	if err != nil || replay.OperationID != first.OperationID || adapter.invokes != 1 {
		t.Fatalf("replay = %q, %v (first %q), provider invokes = %d", replay.OperationID, err, first.OperationID, adapter.invokes)
	}
}

// An operation past its retention is not replayed by the fast path; the
// request goes through admission, which starts a fresh dispatch.
func TestGenerateDoesNotReplayAnExpiredCompletedOperation(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	clock := func() time.Time { return now }
	adapter := &fakeAdapter{name: "replay", response: successfulResponse()}
	harness := newHarnessWithAdmission(t, adapter, memory.NewAdmissionStore(memory.AdmissionOptions{Clock: clock}), clock)
	if _, err := harness.engine.Generate(context.Background(), baseRequest("replay-after-retention")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	if _, err := harness.engine.Generate(context.Background(), baseRequest("replay-after-retention")); err != nil {
		t.Fatalf("generate after retention: %v", err)
	}
	if adapter.invokes != 2 {
		t.Fatalf("provider invokes = %d, want a fresh dispatch after retention", adapter.invokes)
	}
}
