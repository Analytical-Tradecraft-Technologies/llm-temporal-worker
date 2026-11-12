package runtime

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// BenchmarkV1ActivitiesAdmissionMemory measures admission and compilation on
// the production v1 durable path: the PrepareExecutionV1, AcquireBudgetV1 and
// GenerateStepV1 Activities of one fresh Generate turn, called through
// activity.Activities over the CloudExecutionRuntime that production composes.
// State is the in-memory request table and payload store, budgets use the
// in-memory reference materializer, and the adapter is content-free, so the
// p99 covers request compilation, checkpoint and request writes, budget
// planning and reservation, provider planning and adapter compilation, but no
// network round trip. The untimed CompleteExecutionV1 settles each turn.
func BenchmarkV1ActivitiesAdmissionMemory(b *testing.B) {
	benchmarkV1ActivitiesAdmission(b, boundedCloud(b, false))
}

// benchmarkV1ActivitiesAdmission times prepare, acquire and generate of one
// turn per iteration and reports the sampled p99 as p99_ms/op. Every turn has
// a fresh operation key, so none is a replay of a stored request.
func benchmarkV1ActivitiesAdmission(b *testing.B, f *boundedCloudFixture) {
	b.Helper()
	metrics, err := observability.NewMetrics(observability.AllowedValues{})
	if err != nil {
		b.Fatal(err)
	}
	activities := &activity.Activities{V1Runtime: &cloudV1Runtime{CloudExecutionRuntime: f.runtime}, Metrics: metrics}
	ctx := context.Background()
	request := f.request
	durations := make([]time.Duration, b.N)
	retained := f.retainedStorage()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range durations {
		b.StopTimer()
		// The fixture clock follows wall time so a shared Redis budget
		// store and the runtime agree on reservation and bucket times.
		f.now = time.Now().UTC()
		request.OperationKey = "admission-benchmark-" + strconv.Itoa(i)
		b.StartTimer()
		started := time.Now()
		prepared, err := activities.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &request})
		admissionBenchmarkState(b, "prepare", prepared, err, llm.ExecutionBudgetRequired)
		reference := llm.ExecutionReferenceV1{RequestID: prepared.RequestID, Context: request.Context, Capability: prepared.Capability}
		acquired, err := activities.AcquireBudgetV1(ctx, reference)
		admissionBenchmarkState(b, "acquire", acquired, err, llm.ExecutionAcquired)
		generated, err := activities.GenerateStepV1(ctx, request)
		durations[i] = time.Since(started)
		b.StopTimer()
		admissionBenchmarkState(b, "generate", generated, err, llm.ExecutionProviderCompleted)
		completed, err := activities.CompleteExecutionV1(ctx, reference)
		admissionBenchmarkState(b, "complete", completed, err, llm.ExecutionCompleted)
		// A completed turn is never read again. Drop its rows, payloads and
		// recorded compile inputs so the heap stays at the size of a worker
		// whose state lives in remote stores and GC cost does not grow with
		// the sample count.
		retained.restore(f)
		b.StartTimer()
	}
	b.StopTimer()
	if got := int(f.submits.Load()); got != b.N {
		b.Fatalf("adapter submitted %d calls, want %d", got, b.N)
	}
	b.ReportMetric(float64(p99Duration(durations))/float64(time.Millisecond), "p99_ms/op")
}

// retainedStorage is the fixture state present before the first turn.
type retainedStorage struct {
	rows  map[kv.KeyValueKey]struct{}
	blobs map[blob.BlobKey]struct{}
}

func (f *boundedCloudFixture) retainedStorage() retainedStorage {
	f.table.mu.Lock()
	defer f.table.mu.Unlock()
	f.blobs.mu.Lock()
	defer f.blobs.mu.Unlock()
	retained := retainedStorage{rows: map[kv.KeyValueKey]struct{}{}, blobs: map[blob.BlobKey]struct{}{}}
	for key := range f.table.rows {
		retained.rows[key] = struct{}{}
	}
	for key := range f.blobs.values {
		retained.blobs[key] = struct{}{}
	}
	return retained
}

// restore deletes every row and payload written since retainedStorage.
func (retained retainedStorage) restore(f *boundedCloudFixture) {
	f.table.mu.Lock()
	for key := range f.table.rows {
		if _, ok := retained.rows[key]; !ok {
			delete(f.table.rows, key)
			delete(f.table.partitions[key.PartitionKey], key)
		}
	}
	f.table.mu.Unlock()
	f.blobs.mu.Lock()
	for key := range f.blobs.values {
		if _, ok := retained.blobs[key]; !ok {
			delete(f.blobs.values, key)
		}
	}
	f.blobs.mu.Unlock()
	f.adapter.mu.Lock()
	f.adapter.inputs = nil
	f.adapter.mu.Unlock()
}

func admissionBenchmarkState(b *testing.B, step string, result *llm.ExecutionResultV1, err error, want llm.ExecutionStateV1) {
	if err != nil {
		b.Fatalf("%s: %v", step, err)
	}
	if result == nil || result.State != want {
		b.Fatalf("%s: state = %+v, want %s", step, result, want)
	}
}

// p99Duration is the nearest-rank 99th percentile of samples.
func p99Duration(samples []time.Duration) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	return sorted[(99*len(sorted)+99)/100-1]
}

func TestP99DurationUsesNearestRank(t *testing.T) {
	samples := make([]time.Duration, 200)
	for i := range samples {
		samples[len(samples)-1-i] = time.Duration(i+1) * time.Millisecond
	}
	if got := p99Duration(samples); got != 198*time.Millisecond {
		t.Fatalf("p99 of 1..200 ms = %v, want 198ms", got)
	}
	if got := p99Duration([]time.Duration{7 * time.Millisecond}); got != 7*time.Millisecond {
		t.Fatalf("p99 of one sample = %v", got)
	}
	if got := p99Duration(nil); got != 0 {
		t.Fatalf("p99 of no samples = %v", got)
	}
}
