package runtime

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// BenchmarkV1AdmissionAndCompile measures the v1 durable path production
// workers run for a new root Generate: the prepare, acquire-budget and
// generate steps (#1497). Request state uses the in-memory key-value and blob
// stores, the budget the in-memory reference materializer, and the provider a
// content-free synchronous adapter, so it measures admission and compilation
// work without network latency; it does not prove the Redis objective. The
// untimed complete step closes each turn.
func BenchmarkV1AdmissionAndCompile(b *testing.B) {
	f := boundedCloud(b, false)
	ctx := context.Background()
	durations := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		request := f.request
		request.OperationKey = fmt.Sprintf("benchmark-v1-%d", i)
		started := time.Now()
		v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &request})
		boundedState(b, v, err, llm.ExecutionBudgetRequired)
		ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: request.Context, Capability: v.Capability}
		v, err = f.runtime.AcquireBudgetV1(ctx, ref)
		boundedState(b, v, err, llm.ExecutionAcquired)
		v, err = f.runtime.GenerateStepV1(ctx, request)
		boundedState(b, v, err, llm.ExecutionProviderCompleted)
		durations = append(durations, time.Since(started))
		b.StopTimer()
		v, err = f.runtime.CompleteExecutionV1(ctx, ref)
		boundedState(b, v, err, llm.ExecutionCompleted)
		b.StartTimer()
	}
	b.StopTimer()
	if submits := f.submits.Load(); int(submits) != b.N {
		b.Fatalf("adapter submitted %d times, want %d", submits, b.N)
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	p99 := durations[(len(durations)*99+99)/100-1]
	b.ReportMetric(float64(p99)/float64(time.Millisecond), "p99_ms/op")
}
