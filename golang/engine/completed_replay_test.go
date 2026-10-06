package engine

import (
	"context"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/routing"
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
