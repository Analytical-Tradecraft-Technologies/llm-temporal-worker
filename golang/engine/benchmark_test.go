package engine

import (
	"context"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// BenchmarkGenerateMemoryAdmissionAndCompile measures the deterministic,
// in-process successful Generate path with the memory admission store. It is
// a local proxy for admission and compilation work: it deliberately has no
// Redis server or provider network request, so it does not prove either SLO.
func BenchmarkGenerateMemoryAdmissionAndCompile(b *testing.B) {
	adapter := &benchmarkAdapter{fakeAdapter: fakeAdapter{name: "benchmark", response: successfulResponse()}}
	harness := newHarness(b, adapter)
	requests := make([]requestWithDuration, b.N)
	for index := range requests {
		requests[index].request = baseRequest("benchmark-memory-" + strconv.Itoa(index))
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := range requests {
		started := time.Now()
		if _, err := harness.engine.Generate(ctx, requests[index].request); err != nil {
			b.Fatal(err)
		}
		requests[index].duration = time.Since(started)
	}
	b.StopTimer()
	adapter.mu.Lock()
	recordedCalls := len(adapter.calls)
	invokes := adapter.invokes
	adapter.mu.Unlock()
	if recordedCalls != 0 {
		b.Fatalf("benchmark adapter retained %d calls", recordedCalls)
	}
	if invokes != b.N {
		b.Fatalf("benchmark adapter invoked %d times, want %d", invokes, b.N)
	}
	b.ReportMetric(float64(p99Duration(requests))/float64(time.Millisecond), "p99_ms/op")
}

// benchmarkAdapter retains aggregate counts but never a per-call history, so
// its test instrumentation does not affect the benchmark's allocation or p99
// measurements.
type benchmarkAdapter struct{ fakeAdapter }

func (adapter *benchmarkAdapter) Invoke(ctx context.Context, _ provider.Call, observer provider.Observer) (provider.Result, error) {
	adapter.mu.Lock()
	adapter.invokes++
	response := adapter.response
	adapter.mu.Unlock()
	if err := observer.BeforePossibleWrite(ctx); err != nil {
		return provider.Result{}, err
	}
	if err := observer.AfterResponseHeaders(ctx, provider.ResponseMetadata{}); err != nil {
		return provider.Result{}, err
	}
	observer.OnProgress(ctx, provider.Progress{Phase: string(provider.PhaseLift), OutputItems: len(response.Output)})
	return provider.Result{Response: response}, nil
}

type requestWithDuration struct {
	request  llm.Request
	duration time.Duration
}

func p99Duration(samples []requestWithDuration) time.Duration {
	durations := make([]time.Duration, len(samples))
	for index, sample := range samples {
		durations[index] = sample.duration
	}
	sort.Slice(durations, func(left, right int) bool { return durations[left] < durations[right] })
	if len(durations) == 0 {
		return 0
	}
	rank := (99*len(durations) + 99) / 100
	return durations[rank-1]
}
