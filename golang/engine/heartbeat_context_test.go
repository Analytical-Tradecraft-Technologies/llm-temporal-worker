package engine

import (
	"context"
	"sync"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

type recordedHeartbeat struct {
	mu       sync.Mutex
	progress []Progress
}

func (heartbeat *recordedHeartbeat) Beat(_ context.Context, progress Progress) error {
	heartbeat.mu.Lock()
	defer heartbeat.mu.Unlock()
	heartbeat.progress = append(heartbeat.progress, progress)
	return nil
}

func (heartbeat *recordedHeartbeat) phases() []string {
	heartbeat.mu.Lock()
	defer heartbeat.mu.Unlock()
	result := make([]string, 0, len(heartbeat.progress))
	for _, progress := range heartbeat.progress {
		result = append(result, progress.Phase)
	}
	return result
}

func TestContextHeartbeatOverridesStaticEngineHeartbeat(t *testing.T) {
	harness := newHarness(t, &fakeAdapter{name: "heartbeat", response: successfulResponse()})
	static := &recordedHeartbeat{}
	bound := &recordedHeartbeat{}
	harness.engine.dependencies.Heartbeat = static

	if _, err := harness.engine.Generate(WithHeartbeat(context.Background(), bound), baseRequest("context-heartbeat")); err != nil {
		t.Fatal(err)
	}
	if got := static.phases(); len(got) != 0 {
		t.Fatalf("static heartbeat received %v, want no progress when an Activity-scoped heartbeat is bound", got)
	}
	got := bound.phases()
	for _, phase := range []string{"planning", "admission", "pre_write", "response_received", "lift", "finalization"} {
		if !containsPhase(got, phase) {
			t.Fatalf("bound heartbeat phases = %v, missing %q", got, phase)
		}
	}
	if containsPhase(got, "streaming") {
		t.Fatalf("one-shot Generate heartbeat phases = %v, must not claim streaming", got)
	}
}

func TestGenerateDoesNotExposeProviderStreamingPhase(t *testing.T) {
	adapter := &fakeAdapter{
		name:          "one-shot-stream-progress",
		response:      successfulResponse(),
		progressPhase: provider.PhaseStream,
	}
	harness := newHarness(t, adapter)
	bound := &recordedHeartbeat{}

	if _, err := harness.engine.Generate(WithHeartbeat(context.Background(), bound), baseRequest("one-shot-stream-progress")); err != nil {
		t.Fatal(err)
	}
	got := bound.phases()
	if !containsPhase(got, "response_received") {
		t.Fatalf("one-shot Generate heartbeat phases = %v, missing response_received", got)
	}
	if containsPhase(got, "streaming") {
		t.Fatalf("one-shot Generate heartbeat phases = %v, must not expose provider stream progress", got)
	}
}

func TestStreamRetainsStreamingHeartbeat(t *testing.T) {
	adapter := &streamingAdapter{
		fakeAdapter: &fakeAdapter{name: "stream-heartbeat", response: successfulResponse()},
		events: []provider.Event{
			provider.OutputStarted{Index: 0},
			provider.TextDelta{Index: 0, Text: "ok"},
			provider.OutputFinished{Index: 0},
			provider.StreamCompleted{},
		},
	}
	harness := newHarness(t, adapter)
	bound := &recordedHeartbeat{}

	stream, err := harness.engine.Stream(WithHeartbeat(context.Background(), bound), baseRequest("stream-heartbeat"))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	readTerminalStream(t, stream)
	if got := bound.phases(); !containsPhase(got, "streaming") {
		t.Fatalf("stream heartbeat phases = %v, missing streaming", got)
	}
}

func containsPhase(phases []string, want string) bool {
	for _, phase := range phases {
		if phase == want {
			return true
		}
	}
	return false
}

var _ Heartbeat = (*recordedHeartbeat)(nil)
