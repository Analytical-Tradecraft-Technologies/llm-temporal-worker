//go:build cloudworkflowintegration

package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
	"github.com/mfow/llm-temporal-worker/golang/workflows"
	redisclient "github.com/redis/go-redis/v9"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// This gate uses a real Temporal server and Redis. By default only the generic
// KV/blob substrate is in memory; the cloud repository, encryption, activities,
// workflows and Redis accounting are the production implementations. The AWS
// gate below opens explicitly configured disposable resources instead.
type liveCloudWorkflow struct {
	fixture       *boundedCloudFixture
	ctx           context.Context
	client        client.Client
	redis         *redisclient.Client
	queue, prefix string
	pending       chan struct{}
	budgetWait    chan struct{}
	waitObserved  sync.Once
	observed      sync.Once
	complete      atomic.Bool
}

type observedCloudRuntime struct {
	*cloudV1Runtime
	harness *liveCloudWorkflow
}

func (r *observedCloudRuntime) GenerateStepV1(ctx context.Context, request llm.GenerateRequestV1) (llm.ExecutionResultV1, error) {
	result, err := r.CloudExecutionRuntime.GenerateStepV1(ctx, request)
	if err == nil && result.State == llm.ExecutionPending {
		r.harness.observed.Do(func() { close(r.harness.pending) })
	}
	return result, err
}

func (r *observedCloudRuntime) AcquireBudgetV1(ctx context.Context, ref llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	result, err := r.CloudExecutionRuntime.AcquireBudgetV1(ctx, ref)
	if err == nil && result.State == llm.ExecutionBudgetWait {
		r.harness.waitObserved.Do(func() { close(r.harness.budgetWait) })
	}
	return result, err
}

func newLiveCloudWorkflow(t *testing.T, async, aws bool) *liveCloudWorkflow {
	t.Helper()
	address, redisAddress := os.Getenv("LLMTW_TEMPORAL_ADDRESS"), os.Getenv("LLMTW_REDIS_ADDR")
	if address == "" || redisAddress == "" {
		t.Fatal("cloud workflow gate requires LLMTW_TEMPORAL_ADDRESS and LLMTW_REDIS_ADDR")
	}
	h := &liveCloudWorkflow{fixture: boundedCloud(t, async), queue: "llmtw-cloud-" + uuid.NewString(), pending: make(chan struct{}), budgetWait: make(chan struct{})}
	var cancel context.CancelFunc
	h.ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	var err error
	h.client, err = client.DialContext(h.ctx, client.Options{HostPort: address, Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.client.Close)
	h.redis = redisclient.NewClient(&redisclient.Options{Addr: redisAddress, Username: os.Getenv("LLMTW_REDIS_USERNAME"), Password: os.Getenv("LLMTW_REDIS_PASSWORD")})
	t.Cleanup(func() { _ = h.redis.Close() })
	if err := h.redis.Ping(h.ctx).Err(); err != nil {
		t.Fatal(err)
	}
	// Only the isolated Makefile harness provisions shared code. An operator's
	// externally provisioned Redis must already have the exact library.
	if os.Getenv("LLMTW_CLOUD_TEST_PROVISION") == "1" {
		if err := h.redis.FunctionLoad(h.ctx, redisstore.AdmissionFunctionSource()).Err(); err != nil && !strings.Contains(err.Error(), "already exists") {
			t.Fatal(err)
		}
	}
	base := os.Getenv("LLMTW_REDIS_KEY_PREFIX")
	if base == "" {
		base = "llmtw"
	}
	h.prefix = base + ":{" + h.queue + "}:"
	// Delete only this test's random namespace, including non-expiring lease
	// tombstones. Never flush a shared Redis instance or shared Functions.
	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		it := h.redis.Scan(ctx, 0, h.prefix+"*", 100).Iterator()
		for it.Next(ctx) {
			if err := h.redis.Del(ctx, it.Val()).Err(); err != nil {
				t.Error(err)
			}
		}
		if err := it.Err(); err != nil {
			t.Error(err)
		}
	})
	f := h.fixture
	f.cap.Clock = time.Now
	if aws {
		raw := os.Getenv("LLMTW_CLOUD_TEST_CONFIG")
		var cfg cloudstate.Config
		if raw == "" || json.Unmarshal([]byte(raw), &cfg) != nil {
			t.Fatal("AWS gate requires LLMTW_CLOUD_TEST_CONFIG with disposable table and bucket aliases")
		}
		cfg.Namespace = h.queue
		f.repository, err = cloudstate.Open(h.ctx, cfg, bytes.Repeat([]byte{7}, 32))
		if err != nil {
			t.Fatal(err)
		}
		// The synthetic encrypted records are intentionally retained for inspection;
		// an operator deletes the disposable resources after the gate. No general
		// table/bucket deletion permission is needed by this test.
		t.Logf("AWS test namespace: %s", cfg.Namespace)
	}
	checkpoints := f.repository.Checkpoints()
	f.cap.Requests = f.repository
	f.cap.Responses, f.cap.ResponseFills = f.repository.Responses(), f.repository.ResponseFills()
	f.cap.Checkpoints = CheckpointCapabilities{Repository: checkpoints, Blobs: checkpoints, BlobWriter: checkpoints, Materializer: &state.DurableCheckpointMaterializer{Repository: checkpoints, Blobs: checkpoints, HandleVerifier: f.options.Keyring, Now: time.Now}}
	f.cap.Budgets, err = redisstore.NewRedisBudgetMaterializer(redisstore.RedisBudgetMaterializerOptions{Client: h.redis, Mode: redisstore.AdmissionModeFunction, Keys: redisstore.KeyOptions{Prefix: base, HashTag: h.queue, KeySecret: bytes.Repeat([]byte{9}, 32)}, GenerationID: "generation-1", IncarnationID: "incarnation-1", Clock: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	f.cap.Finalizer, err = newCloudFinalizer(f.repository, f.cap.Responses, f.cap.ResponseFills, f.cap.Budgets, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	original := f.adapter.poll
	f.adapter.poll = func(ctx context.Context, call provider.Call, id string, observer provider.Observer) (provider.ResumableResult, error) {
		if !h.complete.Load() {
			return provider.ResumableResult{State: provider.ResumablePending, Dispatch: provider.DispatchAccepted, ProviderOperationID: id, NextPollAfter: time.Second}, nil
		}
		return original(ctx, call, id, observer)
	}
	return h
}

func (h *liveCloudWorkflow) startWorker(t *testing.T) func() {
	t.Helper()
	// Reconstruct all invocation-local execution state from the retained stores.
	runtime, err := h.fixture.cap.NewCloudExecutionRuntime(h.ctx, h.fixture.options)
	if err != nil {
		t.Fatal(err)
	}
	w := worker.New(h.client, h.queue, worker.Options{WorkerStopTimeout: time.Second, MaxConcurrentWorkflowTaskExecutionSize: 4})
	workflows.Register(w, activity.PayloadLimits{})
	a := &activity.Activities{V1Runtime: &observedCloudRuntime{cloudV1Runtime: &cloudV1Runtime{CloudExecutionRuntime: runtime}, harness: h}}
	if err := a.RegisterForTaskQueue(w, h.queue); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	stop := sync.OnceFunc(w.Stop)
	t.Cleanup(stop)
	return stop
}
func (h *liveCloudWorkflow) start(t *testing.T, name string, input any) client.WorkflowRun {
	t.Helper()
	run, err := h.client.ExecuteWorkflow(h.ctx, client.StartWorkflowOptions{ID: h.queue + "-" + uuid.NewString(), TaskQueue: h.queue, WorkflowExecutionTimeout: 2 * time.Minute}, name, input)
	if err != nil {
		t.Fatal(err)
	}
	return run
}
func (h *liveCloudWorkflow) generate(t *testing.T, request llm.GenerateRequestV1) llm.GenerateResponseV1 {
	t.Helper()
	var result llm.GenerateResponseV1
	if err := h.start(t, workflows.GenerateWorkflowName, request).Get(h.ctx, &result); err != nil {
		t.Fatal(err)
	}
	if result.OperationKey != request.OperationKey || result.Checkpoint.Handle == "" {
		t.Fatal("lost response identity or checkpoint")
	}
	return result
}
func (h *liveCloudWorkflow) compact(t *testing.T, request llm.CompactRequestV1) llm.CompactResponseV1 {
	t.Helper()
	var result llm.CompactResponseV1
	if err := h.start(t, workflows.CompactWorkflowName, request).Get(h.ctx, &result); err != nil {
		t.Fatal(err)
	}
	if result.OperationKey != request.OperationKey || result.Checkpoint.Handle == "" {
		t.Fatal("lost compaction identity or checkpoint")
	}
	return result
}

func TestCloudWorkflowLiveLifecycle(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async-%t", async), func(t *testing.T) { exerciseCloudWorkflowLifecycle(t, async, false) })
	}
}
func TestCloudWorkflowAWSLifecycle(t *testing.T) {
	if os.Getenv("LLMTW_CLOUD_TEST_AWS") != "1" {
		t.Skip("AWS resources are explicitly opt-in")
	}
	exerciseCloudWorkflowLifecycle(t, true, true)
}
func exerciseCloudWorkflowLifecycle(t *testing.T, async, aws bool) {
	h := newLiveCloudWorkflow(t, async, aws)
	request := h.fixture.request
	request.Cache = &llm.CachePolicyV1{}
	policy := json.RawMessage(`{"recent_turns":0}`)
	request.SettingsPatch.CompactionPolicy.Set = &policy
	stop := h.startWorker(t)
	run := h.start(t, workflows.GenerateWorkflowName, request)
	if async {
		select {
		case <-h.pending:
		case <-h.ctx.Done():
			t.Fatal("submission did not reach durable pending state")
		}
		stop()
		if h.fixture.submits.Load() != 1 {
			t.Fatal("wrong submission count before restart")
		}
		h.complete.Store(true)
		h.startWorker(t)
	}
	var original llm.GenerateResponseV1
	if err := run.Get(h.ctx, &original); err != nil {
		t.Fatal(err)
	}
	if original.OperationKey != request.OperationKey || original.Checkpoint.Handle == "" {
		t.Fatal("invalid initial result")
	}
	replay := h.generate(t, request)
	if replay.Checkpoint.Handle != original.Checkpoint.Handle {
		t.Fatal("replay republished checkpoint")
	}
	request.OperationKey = "cached-generation"
	hit := h.generate(t, request)
	if hit.Cache.Disposition != "hit" || hit.OperationID == original.OperationID || hit.Checkpoint.Handle == original.Checkpoint.Handle || h.fixture.submits.Load() != 1 {
		t.Fatal("cache hit submitted or reused caller identity")
	}
	request.OperationKey = "independent-generation"
	request.Cache = &llm.CachePolicyV1{Variant: 1}
	h.generate(t, request)
	if h.fixture.submits.Load() != 2 {
		t.Fatal("independent sample reused cached answer")
	}
	compact := llm.CompactRequestV1{OperationKey: "summary", Context: request.Context, Parent: original.Checkpoint.Handle, Cache: &llm.CachePolicyV1{}}
	summary := h.compact(t, compact)
	if h.fixture.submits.Load() != 3 {
		t.Fatal("compaction did not invoke summarizer once")
	}
	compact.OperationKey = "cached-summary"
	cached := h.compact(t, compact)
	if cached.Cache.Disposition != "hit" || cached.OperationID == summary.OperationID || h.fixture.submits.Load() != 3 {
		t.Fatal("compaction cache did not preserve caller identity")
	}
	compact.OperationKey = "independent-summary"
	compact.Cache = &llm.CachePolicyV1{Variant: 1}
	h.compact(t, compact)
	if h.fixture.submits.Load() != 4 {
		t.Fatal("independent compaction reused cached summary")
	}
	denied := request
	denied.OperationKey, denied.Context.Tenant = "unauthorized", "other"
	var response llm.GenerateResponseV1
	if err := h.start(t, workflows.GenerateWorkflowName, denied).Get(h.ctx, &response); err == nil {
		t.Fatal("unauthorized caller was accepted")
	}
	if h.fixture.submits.Load() != 4 {
		t.Fatal("authorization failure dispatched provider work")
	}
	h.assertSettled(t, 4)
	for shard := range cloudstate.PendingShards {
		page, err := h.fixture.repository.ListPending(h.ctx, shard, 100, "")
		if err != nil || len(page.Requests) != 0 || page.NextPageToken != "" {
			t.Fatalf("successful lifecycle left pending work in shard %d: %v", shard, err)
		}
	}
}

// Inspect the actual Redis lease tombstones, not an in-memory accounting fake.
func (h *liveCloudWorkflow) assertSettled(t *testing.T, expected int) {
	t.Helper()
	count := 0
	it := h.redis.Scan(h.ctx, 0, h.prefix+"*", 100).Iterator()
	for it.Next(h.ctx) {
		if h.redis.Type(h.ctx, it.Val()).Val() != "string" {
			continue
		}
		raw, err := h.redis.Get(h.ctx, it.Val()).Result()
		if err != nil {
			t.Fatal(err)
		}
		var record struct {
			OperationID  string `json:"operation_id"`
			Status       string `json:"status"`
			Claimed      bool   `json:"claimed"`
			Reservations []struct {
				Status    string `json:"status"`
				Reserved  string `json:"reserved_nano"`
				Accounted string `json:"accounted_nano"`
			} `json:"reservations"`
		}
		if json.Unmarshal([]byte(raw), &record) != nil || record.OperationID == "" {
			continue
		}
		if record.Status != "accepted" || !record.Claimed || len(record.Reservations) != 2 {
			t.Fatalf("unsettled Redis lease: status=%s claimed=%t", record.Status, record.Claimed)
		}
		for _, reservation := range record.Reservations {
			if reservation.Status != "finalized" || reservation.Reserved != "0" || reservation.Accounted == "0" {
				t.Fatal("Redis reservation did not settle exactly")
			}
		}
		count++
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("Redis has %d paid leases, want %d", count, expected)
	}
}

// Capacity is exhausted in real Redis, then released while the public workflow
// is alive. Admission must return wait and yield to its workflow timer without
// invoking the provider, then acquire and dispatch once after capacity returns.
func TestCloudWorkflowLiveBudgetWait(t *testing.T) {
	h := newLiveCloudWorkflow(t, false, false)
	f := h.fixture
	f.restart(t)
	prepared, err := f.runtime.PrepareExecutionV1(h.ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, prepared, err, llm.ExecutionBudgetRequired)
	scope := cloudstate.Scope{Tenant: f.request.Context.Tenant, Project: f.request.Context.Project}
	attempt, err := f.repository.LoadRequestAttempt(h.ctx, scope, cloudstate.RequestID(prepared.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := f.repository.LoadBudgetPlan(h.ctx, scope, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	blocker := plan.Reservation
	blocker.OperationID = "budget-blocker"
	for i := range blocker.Reservations {
		blocker.Reservations[i].AmountUSD = blocker.Reservations[i].LimitUSD
		blocker.Reservations[i].Amount = blocker.Reservations[i].Limit
	}
	reserved, err := f.cap.Budgets.Accept(h.ctx, blocker)
	if err != nil || !reserved.Accepted {
		t.Fatal("could not occupy budget", err)
	}
	h.startWorker(t)
	run := h.start(t, workflows.GenerateWorkflowName, f.request)
	select {
	case <-h.budgetWait:
	case <-h.ctx.Done():
		t.Fatal("budget activity did not return wait")
	}
	if f.submits.Load() != 0 {
		t.Fatal("provider called before budget became available")
	}
	leaser := f.cap.Budgets.(durable.BudgetLeaser)
	if _, err := leaser.Claim(h.ctx, durable.ClaimRequest{OperationID: blocker.OperationID, GenerationID: blocker.GenerationID, IncarnationID: reserved.IncarnationID}); err != nil {
		t.Fatal(err)
	}
	zero := pricing.MustUSD("0")
	release := durable.ReconcileRequest{OperationID: blocker.OperationID, GenerationID: blocker.GenerationID, IncarnationID: reserved.IncarnationID}
	for i, event := range reserved.Events {
		release.Events = append(release.Events, budget.CompletionEvent{EventID: fmt.Sprintf("blocker-release-%d", i), OperationID: string(blocker.OperationID), GenerationID: string(blocker.GenerationID), WindowID: event.WindowID, BucketStart: event.BucketStart, ReservationRevision: event.ReservationRevision + 1, Kind: budget.JournalFinalizeExact, ReservedDecreaseUSD: event.AmountUSD, AccountedIncreaseUSD: zero, ActualCostUSD: &zero, CostStatus: budget.CostExact, OccurredAt: time.Now()})
	}
	if err := f.cap.Budgets.Reconcile(h.ctx, release); err != nil {
		t.Fatal(err)
	}
	var result llm.GenerateResponseV1
	if err := run.Get(h.ctx, &result); err != nil {
		t.Fatal(err)
	}
	if f.submits.Load() != 1 || result.Checkpoint.Handle == "" {
		t.Fatal("workflow did not resume exactly once")
	}
}
