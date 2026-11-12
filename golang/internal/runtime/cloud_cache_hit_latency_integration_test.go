//go:build cloudworkflowintegration

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
	"github.com/google/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// cacheHitGenerateStorageBound bounds the storage calls of one warm hit
// through the public Generate workflow: planning, resolution and the
// Langfuse export. It was 150 before #1782.
const cacheHitGenerateStorageBound = 82

// TestCloudWorkflowCacheHitLatency measures exact cache hits through the
// public Generate workflow on a real Temporal server and Redis. It primes the
// cache once, then submits equivalent requests under new operation keys and
// workflow IDs, so every warm run takes the cache lookup rather than reusing a
// workflow or operation result. Warm runs go one at a time and then as a
// concurrent burst; cold runs use a fresh cache variant and so call the
// provider. Each run is broken down from its Temporal history.
//
// The default sizes keep the gate quick. LLMTW_CACHE_BENCH_SEQUENTIAL,
// _BURST, _COLD and _STORAGE_LATENCY (a Go duration applied to every KV and
// blob operation) enlarge it, and LLMTW_CACHE_BENCH_OUT writes the report as
// JSON.
func TestCloudWorkflowCacheHitLatency(t *testing.T) {
	sequential := benchEnvInt(t, "LLMTW_CACHE_BENCH_SEQUENTIAL", 5)
	burst := benchEnvInt(t, "LLMTW_CACHE_BENCH_BURST", 8)
	cold := benchEnvInt(t, "LLMTW_CACHE_BENCH_COLD", 3)
	latency := time.Duration(0)
	if raw := os.Getenv("LLMTW_CACHE_BENCH_STORAGE_LATENCY"); raw != "" {
		var err error
		if latency, err = time.ParseDuration(raw); err != nil {
			t.Fatal(err)
		}
	}
	h := newLiveCloudWorkflow(t, false, false)
	f := h.fixture
	f.table.latency, f.blobs.latency = latency, latency
	var hitsMu sync.Mutex
	hits := map[string]time.Time{}
	h.cacheHitObserver = func(requestID string, at time.Time) {
		hitsMu.Lock()
		defer hitsMu.Unlock()
		if _, seen := hits[requestID]; !seen {
			hits[requestID] = at
		}
	}
	// The production worker's Activity limits (config.example.yaml); the
	// workflow task limits are the SDK defaults, as in production.
	h.startWorkerWithOptions(t, h.queue, worker.Options{WorkerStopTimeout: time.Second, MaxConcurrentActivityExecutionSize: 16, MaxConcurrentActivityTaskPollers: 8})

	request := f.request
	request.Cache = &llm.CachePolicyV1{}
	request.OperationKey = "cache-bench-prime"
	prime := h.generate(t, request)
	if f.submits.Load() != 1 || prime.Cache.Disposition == "hit" {
		t.Fatalf("priming made %d provider calls, disposition %q", f.submits.Load(), prime.Cache.Disposition)
	}

	report := cacheBenchReport{StorageLatency: latency.String()}
	measure := func(name string, count, parallel int, configure func(int, *llm.GenerateRequestV1)) cacheBenchPhase {
		runs := make([]cacheBenchRun, count)
		storageBefore := readExecutionStorageCounts(f.table, f.blobs)
		var wg sync.WaitGroup
		gate := make(chan struct{}, parallel)
		for i := range count {
			wg.Add(1)
			gate <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-gate }()
				input := request
				input.OperationKey = fmt.Sprintf("cache-bench-%s-%d", name, i)
				configure(i, &input)
				runs[i] = h.benchRun(t, input)
			}()
		}
		wg.Wait()
		used := readExecutionStorageCounts(f.table, f.blobs).minus(storageBefore)
		for i := range runs {
			hitsMu.Lock()
			runs[i].lookup = hits[runs[i].RequestID]
			hitsMu.Unlock()
			h.breakDown(t, &runs[i])
		}
		phase := summarizeCacheBench(name, runs)
		n := float64(count)
		phase.StoragePerRun = map[string]float64{"kv_get": float64(used.gets) / n, "kv_query": float64(used.queries) / n, "kv_write": float64(used.writes) / n, "blob_get": float64(used.opens) / n, "blob_put": float64(used.creates) / n}
		phase.StorageCallsPerRun = float64(storageCalls(used)) / n
		return phase
	}
	warm := func(int, *llm.GenerateRequestV1) {}
	before := f.submits.Load() + f.polls.Load()
	report.Phases = append(report.Phases, measure("warm-sequential", sequential, 1, warm))
	report.Phases = append(report.Phases, measure("warm-burst", burst, burst, warm))
	if calls := f.submits.Load() + f.polls.Load() - before; calls != 0 {
		t.Fatalf("warm runs made %d provider calls", calls)
	}
	report.WarmProviderCalls = 0
	for _, phase := range report.Phases {
		if phase.Hits != phase.Runs {
			t.Fatalf("%s: %d of %d runs were cache hits", phase.Name, phase.Hits, phase.Runs)
		}
		if phase.StorageCallsPerRun > cacheHitGenerateStorageBound {
			t.Fatalf("%s: %.1f storage calls per hit, budget %d", phase.Name, phase.StorageCallsPerRun, cacheHitGenerateStorageBound)
		}
	}
	coldBefore := f.submits.Load()
	report.Phases = append(report.Phases, measure("cold-sequential", cold, 1, func(i int, input *llm.GenerateRequestV1) {
		input.Cache = &llm.CachePolicyV1{Variant: int32(1000 + i)}
	}))
	if calls := f.submits.Load() - coldBefore; calls != int32(cold) {
		t.Fatalf("cold runs made %d provider calls, want %d", calls, cold)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cache hit latency report:\n%s", data)
	if out := os.Getenv("LLMTW_CACHE_BENCH_OUT"); out != "" {
		if err := os.WriteFile(out, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func benchEnvInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		t.Fatalf("%s must be a positive integer", name)
	}
	return value
}

type cacheBenchRun struct {
	RequestID   string
	Disposition string
	workflowID  string
	started     time.Time
	finished    time.Time
	// lookup is when the Activity step that resolved the hit returned.
	lookup time.Time
	// Server-side breakdown of the parent and the request child, which are
	// the caller's critical path. The Langfuse export child is abandoned and
	// reported only as a task count.
	ServerTotal time.Duration
	QueueWait   time.Duration
	// QueueWait split into workflow task and Activity task waits.
	WorkflowTaskWait, ActivityWait time.Duration
	WorkflowTaskExec               time.Duration
	ActivityExec                   time.Duration
	LookupToResult                 time.Duration
	ClientTotal                    time.Duration
	// CriticalTasks counts workflow and Activity tasks on the parent and
	// request child; AllTasks adds every other child, including the export.
	CriticalWorkflowTasks, CriticalActivityTasks int
	AllWorkflowTasks, AllActivityTasks           int
	ChildWorkflows                               int
}

func (h *liveCloudWorkflow) benchRun(t *testing.T, request llm.GenerateRequestV1) cacheBenchRun {
	run := cacheBenchRun{workflowID: h.queue + "-" + uuid.NewString(), started: time.Now()}
	execution, err := h.client.ExecuteWorkflow(h.ctx, client.StartWorkflowOptions{ID: run.workflowID, TaskQueue: h.queue, WorkflowExecutionTimeout: 2 * time.Minute}, workflows.GenerateWorkflowName, request)
	if err != nil {
		t.Error(err)
		return run
	}
	var response llm.GenerateResponseV1
	if err := execution.Get(h.ctx, &response); err != nil {
		t.Error(err)
		return run
	}
	run.finished = time.Now()
	if response.OperationKey != request.OperationKey || response.Checkpoint.Handle == "" {
		t.Error("lost response identity or checkpoint")
	}
	run.RequestID, run.Disposition, run.ClientTotal = response.OperationID, response.Cache.Disposition, run.finished.Sub(run.started)
	return run
}

// breakDown reads the run's histories. Waits and execution times are summed
// over the parent's and request child's tasks; the two overlap only while the
// parent records that its child started.
func (h *liveCloudWorkflow) breakDown(t *testing.T, run *cacheBenchRun) {
	t.Helper()
	if run.RequestID == "" {
		return
	}
	if !run.lookup.IsZero() {
		run.LookupToResult = run.finished.Sub(run.lookup)
	}
	var visit func(id, runID string, critical bool) []*history.HistoryEvent
	visit = func(id, runID string, critical bool) []*history.HistoryEvent {
		var events []*history.HistoryEvent
		iterator := h.client.GetWorkflowHistory(h.ctx, id, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iterator.HasNext() {
			event, err := iterator.Next()
			if err != nil {
				t.Fatal(err)
			}
			events = append(events, event)
		}
		scheduled := map[int64]time.Time{}
		started := map[int64]time.Time{}
		for _, event := range events {
			at := event.GetEventTime().AsTime()
			switch event.GetEventType() {
			case enums.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
				scheduled[event.GetEventId()] = at
			case enums.EVENT_TYPE_WORKFLOW_TASK_STARTED:
				attributes := event.GetWorkflowTaskStartedEventAttributes()
				started[event.GetEventId()] = at
				if critical {
					wait := at.Sub(scheduled[attributes.GetScheduledEventId()])
					run.QueueWait += wait
					run.WorkflowTaskWait += wait
				}
			case enums.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
				attributes := event.GetWorkflowTaskCompletedEventAttributes()
				run.AllWorkflowTasks++
				if critical {
					run.CriticalWorkflowTasks++
					run.WorkflowTaskExec += at.Sub(started[attributes.GetStartedEventId()])
				}
			case enums.EVENT_TYPE_ACTIVITY_TASK_STARTED:
				attributes := event.GetActivityTaskStartedEventAttributes()
				started[event.GetEventId()] = at
				if critical {
					wait := at.Sub(scheduled[attributes.GetScheduledEventId()])
					run.QueueWait += wait
					run.ActivityWait += wait
				}
			case enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
				attributes := event.GetActivityTaskCompletedEventAttributes()
				run.AllActivityTasks++
				if critical {
					run.CriticalActivityTasks++
					run.ActivityExec += at.Sub(started[attributes.GetStartedEventId()])
				}
			case enums.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
				attributes := event.GetChildWorkflowExecutionStartedEventAttributes()
				run.ChildWorkflows++
				child := attributes.GetWorkflowExecution()
				visit(child.GetWorkflowId(), child.GetRunId(), critical && attributes.GetWorkflowType().GetName() == workflows.RequestWorkflowName)
			}
		}
		return events
	}
	events := visit(run.workflowID, "", true)
	if len(events) > 0 {
		run.ServerTotal = events[len(events)-1].GetEventTime().AsTime().Sub(events[0].GetEventTime().AsTime())
	}
}

type cacheBenchReport struct {
	StorageLatency    string
	WarmProviderCalls int
	Phases            []cacheBenchPhase
}

type cacheBenchPhase struct {
	Name       string
	Runs, Hits int
	// Milliseconds, p50 and p95 by nearest rank.
	ClientP50, ClientP95                     float64
	ServerP50, ServerP95                     float64
	QueueWaitP50, QueueWaitP95               float64
	WorkflowTaskWaitP50, WorkflowTaskWaitP95 float64
	ActivityWaitP50, ActivityWaitP95         float64
	WorkflowTaskExecP50, WorkflowTaskExecP95 float64
	ActivityExecP50, ActivityExecP95         float64
	LookupToResultP50, LookupToResultP95     float64
	// Tasks per run (the median run; every run of a phase takes one path).
	CriticalWorkflowTasks, CriticalActivityTasks int
	AllWorkflowTasks, AllActivityTasks           int
	ChildWorkflows                               int
	// StoragePerRun is the mean KV and blob operations per run, every
	// Activity included.
	StoragePerRun map[string]float64
	// StorageCallsPerRun is their sum.
	StorageCallsPerRun float64
}

func summarizeCacheBench(name string, runs []cacheBenchRun) cacheBenchPhase {
	phase := cacheBenchPhase{Name: name, Runs: len(runs)}
	pick := func(value func(cacheBenchRun) time.Duration) (float64, float64) {
		var values []time.Duration
		for _, run := range runs {
			values = append(values, value(run))
		}
		return percentileMillis(values, 50), percentileMillis(values, 95)
	}
	phase.ClientP50, phase.ClientP95 = pick(func(r cacheBenchRun) time.Duration { return r.ClientTotal })
	phase.ServerP50, phase.ServerP95 = pick(func(r cacheBenchRun) time.Duration { return r.ServerTotal })
	phase.QueueWaitP50, phase.QueueWaitP95 = pick(func(r cacheBenchRun) time.Duration { return r.QueueWait })
	phase.WorkflowTaskWaitP50, phase.WorkflowTaskWaitP95 = pick(func(r cacheBenchRun) time.Duration { return r.WorkflowTaskWait })
	phase.ActivityWaitP50, phase.ActivityWaitP95 = pick(func(r cacheBenchRun) time.Duration { return r.ActivityWait })
	phase.WorkflowTaskExecP50, phase.WorkflowTaskExecP95 = pick(func(r cacheBenchRun) time.Duration { return r.WorkflowTaskExec })
	phase.ActivityExecP50, phase.ActivityExecP95 = pick(func(r cacheBenchRun) time.Duration { return r.ActivityExec })
	phase.LookupToResultP50, phase.LookupToResultP95 = pick(func(r cacheBenchRun) time.Duration { return r.LookupToResult })
	median := func(value func(cacheBenchRun) int) int {
		var values []int
		for _, run := range runs {
			values = append(values, value(run))
		}
		slices.Sort(values)
		return values[len(values)/2]
	}
	phase.CriticalWorkflowTasks = median(func(r cacheBenchRun) int { return r.CriticalWorkflowTasks })
	phase.CriticalActivityTasks = median(func(r cacheBenchRun) int { return r.CriticalActivityTasks })
	phase.AllWorkflowTasks = median(func(r cacheBenchRun) int { return r.AllWorkflowTasks })
	phase.AllActivityTasks = median(func(r cacheBenchRun) int { return r.AllActivityTasks })
	phase.ChildWorkflows = median(func(r cacheBenchRun) int { return r.ChildWorkflows })
	for _, run := range runs {
		if run.Disposition == "hit" {
			phase.Hits++
		}
	}
	return phase
}

func percentileMillis(values []time.Duration, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	slices.Sort(values)
	rank := int(math.Ceil(percentile/100*float64(len(values)))) - 1
	return math.Round(float64(values[max(rank, 0)].Microseconds())/10) / 100
}

// flakyResponses fails cache lookups on demand, as a cache outage would.
type flakyResponses struct {
	cache.ResponseRepository
	failures atomic.Int32
	corrupt  atomic.Bool
}

func (r *flakyResponses) Lookup(ctx context.Context, lookup cache.ResponseLookup) (*cache.ResponseEntry, error) {
	if r.corrupt.Load() {
		return nil, durable.ErrResponseCacheCorrupt
	}
	if r.failures.Add(-1) >= 0 {
		return nil, contracts.ErrUnavailable
	}
	return r.ResponseRepository.Lookup(ctx, lookup)
}

// childTypes lists the workflow types a run started as children.
func (h *liveCloudWorkflow) childTypes(t *testing.T, id string) []string {
	t.Helper()
	var types []string
	iterator := h.client.GetWorkflowHistory(h.ctx, id, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if attributes := event.GetChildWorkflowExecutionStartedEventAttributes(); attributes != nil {
			types = append(types, attributes.GetWorkflowType().GetName())
		}
	}
	return types
}

// TestCloudWorkflowResolvedCacheSemantics checks the warm hit path against
// replays, expiry, misses and cache errors on a real Temporal server. A hit
// answers without the request workflow; everything else takes the request
// workflow as before, and none of it reaches the provider except a miss.
func TestCloudWorkflowResolvedCacheSemantics(t *testing.T) {
	h := newLiveCloudWorkflow(t, false, false)
	f := h.fixture
	responses := &flakyResponses{ResponseRepository: f.cap.Responses}
	f.cap.Responses = responses
	h.startWorker(t)
	exportOnly := []string{workflows.LangfuseExportWorkflowName}
	requestPath := []string{workflows.RequestWorkflowName}
	run := func(request llm.GenerateRequestV1) (llm.GenerateResponseV1, []string, error) {
		t.Helper()
		id := h.queue + "-" + uuid.NewString()
		execution, err := h.client.ExecuteWorkflow(h.ctx, client.StartWorkflowOptions{ID: id, TaskQueue: h.queue, WorkflowExecutionTimeout: 2 * time.Minute}, workflows.GenerateWorkflowName, request)
		if err != nil {
			t.Fatal(err)
		}
		var response llm.GenerateResponseV1
		err = execution.Get(h.ctx, &response)
		return response, h.childTypes(t, id), err
	}
	request := f.request
	request.Cache = &llm.CachePolicyV1{MaxAgeSeconds: 3}
	request.OperationKey = "semantics-prime"
	prime, children, err := run(request)
	if err != nil || prime.Cache.Disposition == "hit" || f.submits.Load() != 1 || !slices.Equal(children, requestPath) {
		t.Fatalf("prime: %v, disposition %q, %d calls, children %v", err, prime.Cache.Disposition, f.submits.Load(), children)
	}

	request.OperationKey = "semantics-hit"
	hit, children, err := run(request)
	if err != nil || hit.Cache.Disposition != "hit" || hit.OperationID == prime.OperationID || hit.Checkpoint.Handle == prime.Checkpoint.Handle || f.submits.Load() != 1 {
		t.Fatalf("hit: %v, %+v, %d calls", err, hit.Cache, f.submits.Load())
	}
	if !slices.Equal(children, exportOnly) {
		t.Fatalf("hit started %v, want only the export", children)
	}
	if *hit.Cost.ActualCostUSD != "0" {
		t.Fatal("cache hit charged")
	}
	assertOriginCost(t, hit.Diagnostics, prime.OperationID, *prime.Cost.ActualCostUSD)

	// The same operation key under a new workflow ID replays the saved hit.
	replay, children, err := run(request)
	if err != nil || !reflect.DeepEqual(replay, hit) || !slices.Equal(children, exportOnly) || f.submits.Load() != 1 {
		t.Fatalf("replay: %v, children %v, %d calls", err, children, f.submits.Load())
	}

	// A transient lookup failure is never a miss: the request workflow
	// retries preparation until the lookup succeeds and finds the entry.
	responses.failures.Store(2)
	request.OperationKey = "semantics-outage"
	recovered, children, err := run(request)
	if err != nil || recovered.Cache.Disposition != "hit" || !slices.Equal(children, requestPath) || f.submits.Load() != 1 || responses.failures.Load() >= 0 {
		t.Fatalf("transient cache error: %v, %q, children %v, %d calls", err, recovered.Cache.Disposition, children, f.submits.Load())
	}

	// A corrupt cache fails the request without calling the provider.
	responses.corrupt.Store(true)
	request.OperationKey = "semantics-corrupt"
	if _, _, err := run(request); err == nil || f.submits.Load() != 1 {
		t.Fatalf("corrupt cache: %v, %d calls", err, f.submits.Load())
	}
	responses.corrupt.Store(false)

	// Another variant is a miss and pays once.
	miss := request
	miss.OperationKey, miss.Cache = "semantics-miss", &llm.CachePolicyV1{MaxAgeSeconds: 3, Variant: 7}
	missed, children, err := run(miss)
	if err != nil || missed.Cache.Disposition == "hit" || !slices.Equal(children, requestPath) || f.submits.Load() != 2 {
		t.Fatalf("miss: %v, %q, children %v, %d calls", err, missed.Cache.Disposition, children, f.submits.Load())
	}

	// Once the entry is older than max_age_seconds it is not reused.
	time.Sleep(4 * time.Second)
	request.OperationKey = "semantics-expired"
	expired, children, err := run(request)
	if err != nil || expired.Cache.Disposition == "hit" || !slices.Equal(children, requestPath) || f.submits.Load() != 3 {
		t.Fatalf("expired: %v, %q, children %v, %d calls", err, expired.Cache.Disposition, children, f.submits.Load())
	}
}
