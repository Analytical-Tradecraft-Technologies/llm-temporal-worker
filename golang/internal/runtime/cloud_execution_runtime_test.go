package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

type boundedCloudFixture struct {
	runtime        *CloudExecutionRuntime
	cap            V1RuntimeCapabilities
	options        CloudExecutionOptions
	repository     *cloudstate.Repository
	table          *executionMemoryTable
	blobs          *executionMemoryBlobs
	now            time.Time
	request        llm.GenerateRequestV1
	adapter        *executionAsyncAdapter
	submits, polls atomic.Int32
}

func boundedCloud(t testing.TB, async bool, configure ...func(*budgetPlanningFixture)) *boundedCloudFixture {
	t.Helper()
	f := &boundedCloudFixture{now: time.Date(2026, 10, 4, 1, 0, 0, 0, time.UTC)}
	b := newBudgetPlanningFixture(t)
	for i := range b.source.value.BudgetPolicies[0].Windows {
		b.source.value.BudgetPolicies[0].Windows[i].LimitUSD = pricing.MustUSD("100")
	}
	for _, change := range configure {
		change(b)
	}
	f.request = b.gen
	table := &executionMemoryTable{rows: map[kv.KeyValueKey]kv.KeyValueRecord{}}
	blobs := &executionMemoryBlobs{values: map[blob.BlobKey][]byte{}}
	f.table, f.blobs = table, blobs
	var err error
	f.repository, err = cloudstate.NewRepository(cloudstate.Options{Table: table, Blobs: blobs, Namespace: "runtime-test", Secret: bytes.Repeat([]byte{7}, 32), ParentSnapshotBlob: b.parentSnapshotBlob})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := state.NewKeyring([]state.Key{{ID: "test", Primary: true, Secret: bytes.Repeat([]byte{8}, 32)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkpoints := f.repository.Checkpoints()
	f.cap = b.cap
	f.cap.Requests = f.repository
	f.cap.Clock = func() time.Time { return f.now }
	f.cap.Checkpoints = CheckpointCapabilities{Repository: checkpoints, Blobs: checkpoints, BlobWriter: checkpoints, Materializer: &state.DurableCheckpointMaterializer{Repository: checkpoints, Blobs: checkpoints, HandleVerifier: keys, Now: f.cap.Clock}}
	f.cap.Responses, f.cap.ResponseFills = f.repository.Responses(), f.repository.ResponseFills()
	f.cap.BudgetEstimator, f.cap.MaxBudgetBucketsPerWindow = b.estimator, 100
	f.cap.CloudIdentity = durable.CloudIdentity{Provider: "aws", Namespace: "runtime-test", RequestTable: "requests", PayloadStore: "payloads", ProviderDigest: [32]byte{2}}
	f.cap.RedisIdentity = validCapabilityComposition().Identity.Redis
	f.cap.Budgets, err = durable.NewReferenceBudgetMaterializer("generation-1", "incarnation-1", f.cap.Clock)
	if err != nil {
		t.Fatal(err)
	}
	f.cap.Finalizer, err = newCloudFinalizer(f.repository, f.cap.Responses, f.cap.ResponseFills, f.cap.Budgets, f.cap.Clock)
	if err != nil {
		t.Fatal(err)
	}
	f.adapter = &executionAsyncAdapter{executionSyncAdapter: &executionSyncAdapter{planningAdapter: &planningAdapter{version: "profile/v1"}}}
	answer := func(call provider.Call) provider.ResumableResult {
		v := executionResponse(call)
		v.ProviderOperationID = "job"
		v.Result.Response.Output = []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "answer"}}}}
		return v
	}
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return answer(call).Result, nil
	}
	f.adapter.submit = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.ResumableResult, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.ResumableResult{}, err
		}
		return provider.ResumableResult{State: provider.ResumablePending, Dispatch: provider.DispatchAccepted, ProviderOperationID: "job", NextPollAfter: time.Second}, nil
	}
	f.adapter.poll = func(ctx context.Context, call provider.Call, id string, o provider.Observer) (provider.ResumableResult, error) {
		f.polls.Add(1)
		if id != "job" {
			t.Fatal("changed provider id")
		}
		return answer(call), nil
	}
	var adapter provider.Adapter = f.adapter
	if !async {
		adapter = f.adapter.executionSyncAdapter
	}
	f.cap.Adapters = engine.AdapterMap{"endpoint": adapter}
	f.options = CloudExecutionOptions{Keyring: keys, CheckpointTTL: 24 * time.Hour, BudgetGeneration: "generation-1", ResolveScope: func(_ context.Context, caller llm.RequestContext) (string, error) {
		if caller.Tenant != "tenant" || caller.Project != "project" {
			return "", errors.New("denied")
		}
		return "trusted-scope", nil
	}}
	f.restart(t)
	return f
}
func (f *boundedCloudFixture) restart(t testing.TB) {
	t.Helper()
	var err error
	f.runtime, err = f.cap.NewCloudExecutionRuntime(context.Background(), f.options)
	if err != nil {
		t.Fatal(err)
	}
}
func boundedState(t testing.TB, v llm.ExecutionResultV1, err error, want llm.ExecutionStateV1) llm.ExecutionResultV1 {
	t.Helper()
	if err != nil || v.State != want || v.Validate() != nil {
		t.Fatalf("state=%+v wanted=%s error=%v", v, want, err)
	}
	return v
}
func (f *boundedCloudFixture) finish(t testing.TB) llm.ExecutionResultV1 {
	t.Helper()
	ctx := context.Background()
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	if err == nil && v.State == llm.ExecutionCompleted {
		return v
	}
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	f.restart(t)
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	if err == nil && v.State == llm.ExecutionPending {
		f.now = f.now.Add(2 * time.Second)
		v, err = f.runtime.PollExecutionV1(ctx, ref)
	}
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	f.restart(t)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	return boundedState(t, v, err, llm.ExecutionCompleted)
}
func TestCloudExecutionRuntimeGeneration(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, cached := range []bool{false, true} {
			t.Run(map[bool]string{false: "sync", true: "async"}[async]+map[bool]string{false: "/uncached", true: "/cached"}[cached], func(t *testing.T) {
				f := boundedCloud(t, async)
				if cached {
					f.request.Cache = &llm.CachePolicyV1{}
				}
				result := f.finish(t)
				if result.Generate == nil || f.submits.Load() != 1 {
					t.Fatal("missing result or duplicate submission")
				}
				again := f.finish(t)
				if again.Generate.Checkpoint.Handle != result.Generate.Checkpoint.Handle || f.submits.Load() != 1 {
					t.Fatal("retry changed publication")
				}
				if cached {
					f.now = f.now.Add(time.Minute)
					f.request.OperationKey = "independent"
					hit := f.finish(t)
					if hit.Generate.Cache.Disposition != "hit" || hit.RequestID == result.RequestID || hit.Generate.Checkpoint.Handle == result.Generate.Checkpoint.Handle || f.submits.Load() != 1 {
						t.Fatal("cache hit reused caller identity or dispatched")
					}
					assertOriginCost(t, hit.Generate.Diagnostics, result.Generate.OperationID, *result.Generate.Cost.ActualCostUSD)
					if *hit.Generate.Cost.ActualCostUSD != "0" {
						t.Fatal("cache replay charged")
					}
					retried := f.finish(t)
					assertOriginCost(t, retried.Generate.Diagnostics, result.Generate.OperationID, *result.Generate.Cost.ActualCostUSD)
					if f.submits.Load() != 1 {
						t.Fatal("cache retry dispatched")
					}
					for shard := range cloudstate.PendingShards {
						page, err := f.repository.ListPending(context.Background(), shard, 100, "")
						if err != nil || len(page.Requests) != 0 {
							t.Fatalf("cache hit left unused pending attempts: %+v, err=%v", page.Requests, err)
						}
					}
				}
			})
		}
	}
}

func TestCloudExecutionRuntimeCompaction(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, work := range []bool{false, true} {
			for _, index := range []int32{0, 1, 2147483647} {
				t.Run(map[bool]string{false: "sync", true: "async"}[async]+map[bool]string{false: "/no-work", true: "/summary"}[work]+fmt.Sprintf("/sample-%d", index), func(t *testing.T) {
					f := boundedCloud(t, async)
					if work {
						policy := json.RawMessage(`{"recent_turns":0}`)
						f.request.SettingsPatch.CompactionPolicy.Set = &policy
					}
					parent := f.finish(t)
					f.now = f.now.Add(time.Minute)
					request := llm.CompactRequestV1{OperationKey: "compact", Context: f.request.Context, Parent: parent.Generate.Checkpoint.Handle, Cache: &llm.CachePolicyV1{Variant: index}}
					run := func() llm.ExecutionResultV1 {
						t.Helper()
						ctx := context.Background()
						v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Compact: &request})
						if err == nil && v.State == llm.ExecutionCompleted {
							return v
						}
						boundedState(t, v, err, llm.ExecutionBudgetRequired)
						ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: request.Context}
						v, err = f.runtime.AcquireBudgetV1(ctx, ref)
						boundedState(t, v, err, llm.ExecutionAcquired)
						f.restart(t)
						v, err = f.runtime.CompactStepV1(ctx, request)
						if err == nil && v.State == llm.ExecutionPending {
							f.now = f.now.Add(2 * time.Second)
							v, err = f.runtime.PollExecutionV1(ctx, ref)
						}
						boundedState(t, v, err, llm.ExecutionProviderCompleted)
						f.restart(t)
						v, err = f.runtime.CompleteExecutionV1(ctx, ref)
						return boundedState(t, v, err, llm.ExecutionCompleted)
					}
					first := run()
					want := int32(1)
					if work {
						want++
					}
					if first.Compact == nil || first.Compact.Cache.Variant != index || f.submits.Load() != want {
						t.Fatal("incorrect compaction dispatch")
					}
					replay := run()
					if replay.Compact.Checkpoint.Handle != first.Compact.Checkpoint.Handle || f.submits.Load() != want {
						t.Fatal("compaction retry changed result")
					}
					f.now = f.now.Add(time.Minute)
					request.OperationKey = "another-compaction"
					hit := run()
					if hit.Compact.Checkpoint.Handle == first.Compact.Checkpoint.Handle || hit.Compact.Cache.Variant != index || f.submits.Load() != want {
						t.Fatal("compaction cache/no-work repeated provider call")
					}
					if work && hit.Compact.Cache.Disposition != "hit" {
						t.Fatal("summary artifact not reused")
					}
					if work {
						assertOriginCost(t, hit.Compact.Diagnostics, first.Compact.OperationID, *first.Compact.Cost.ActualCostUSD)
						if *hit.Compact.Cost.ActualCostUSD != "0" {
							t.Fatal("cached compaction charged")
						}
					}
					request.OperationKey = "different-sample"
					request.Cache.Variant = (index + 1) % 2
					if index == 2147483647 {
						request.Cache.Variant = 0
					}
					distinct := run()
					if work {
						want++
					}
					if distinct.Compact.Cache.Variant != request.Cache.Variant || f.submits.Load() != want {
						t.Fatal("different sample reused compaction cache")
					}
				})
			}
		}
	}
}

func TestCloudExecutionRuntimeUnknownRetryGetsNewBudget(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	original := f.adapter.invoke
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, errors.New("lost paid response")
	}
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
	first, err := f.repository.LoadRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	saved, err := f.repository.LoadProviderExecution(ctx, scope, first.ID)
	if err != nil || saved.Execution.Claim == nil || saved.Execution.Settled {
		t.Fatal("unknown paid claim was lost", err)
	}
	f.now = f.now.Add(16 * time.Minute)
	f.restart(t)
	v, err = f.runtime.PollExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionOutcomeUnknown)
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	next, err := f.repository.LoadRequestAttempt(ctx, scope, first.RootID)
	if err != nil || next.ID == first.ID || next.PreviousID != first.ID {
		t.Fatal("reused paid identity", err)
	}
	f.adapter.invoke = original
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	paid, err := f.repository.LoadProviderExecution(ctx, scope, next.ID)
	if err != nil || paid.Execution.Claim == nil || paid.Execution.Claim.OperationID == saved.Execution.Claim.OperationID || !paid.Execution.Settled {
		t.Fatal("new attempt did not pay independently", err)
	}
	old, err := f.repository.Read(ctx, scope, first.ID)
	if err != nil || old.Status != cloudstate.StatusOutcomeUnknown {
		t.Fatal("lost original unknown work", err)
	}
	shard, _ := cloudstate.PendingShard(first.ID)
	page, err := f.repository.ListPending(ctx, shard, 100, "")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, request := range page.Requests {
		if request.ID == first.ID {
			found = true
		}
	}
	if !found || f.submits.Load() != 2 {
		t.Fatal("unknown work undiscoverable or duplicate submission")
	}
}

func TestCloudExecutionRuntimeExpiresUnusedAttempt(t *testing.T) {
	for _, quote := range []bool{false, true} {
		t.Run(fmt.Sprint(quote), func(t *testing.T) {
			f := boundedCloud(t, false)
			ctx := context.Background()
			p, err := f.runtime.preparation.Prepare(ctx, llm.PrepareExecutionV1{Generate: &f.request})
			if err != nil {
				t.Fatal(err)
			}
			first, err := f.repository.BeginRequestAttempt(ctx, p.Record.Request.Scope, p.Record.Request.ID, "", f.now)
			if err != nil {
				t.Fatal(err)
			}
			if quote {
				v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
				boundedState(t, v, err, llm.ExecutionBudgetRequired)
			}
			f.now = f.now.Add(16 * time.Minute)
			result := f.finish(t)
			retired, err := f.repository.Read(ctx, p.Record.Request.Scope, first.ID)
			if err != nil || retired.Status != cloudstate.StatusFailed {
				t.Fatal("expired attempt not retired", err)
			}
			if result.State != llm.ExecutionCompleted || f.submits.Load() != 1 {
				t.Fatal("expired unused attempt stuck")
			}
		})
	}
}

func TestCloudExecutionRuntimeAuthorizationPrecedesStorage(t *testing.T) {
	f := boundedCloud(t, false)
	completed := f.finish(t)
	f.options.ResolveScope = func(context.Context, llm.RequestContext) (string, error) { return "", errors.New("denied") }
	f.restart(t)
	// A panic repository proves even a terminal replay is authorized before access.
	f.runtime.preparation.store = (*preparationTestStore)(nil)
	ctx := context.Background()
	ref := llm.ExecutionReferenceV1{RequestID: completed.RequestID, Context: f.request.Context}
	for _, call := range []func() (llm.ExecutionResultV1, error){
		func() (llm.ExecutionResultV1, error) { return f.runtime.GenerateStepV1(ctx, f.request) },
		func() (llm.ExecutionResultV1, error) {
			return f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
		},
		func() (llm.ExecutionResultV1, error) { return f.runtime.AcquireBudgetV1(ctx, ref) },
		func() (llm.ExecutionResultV1, error) { return f.runtime.PollExecutionV1(ctx, ref) },
		func() (llm.ExecutionResultV1, error) { return f.runtime.CompleteExecutionV1(ctx, ref) },
	} {
		if _, err := call(); err == nil {
			t.Fatal("unauthorized access")
		}
	}
}

func TestCloudExecutionRuntimeSingleSubmissionUnderConcurrency(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	ctx := context.Background()
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = f.runtime.GenerateStepV1(ctx, f.request) }()
	}
	wg.Wait()
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	if f.submits.Load() != 1 {
		t.Fatalf("%d submissions", f.submits.Load())
	}
}

func TestCloudExecutionRuntimeStartAcknowledgementLost(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	underlying := f.cap.ResponseFills
	fills := &boundedFaultFills{FillRepository: underlying}
	f.cap.ResponseFills = fills
	f.restart(t)
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	if f.submits.Load() != 0 || fills.starts != 1 {
		t.Fatal("HTTP after uncertain cache fence")
	}
	f.restart(t)
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	if f.submits.Load() != 0 || fills.starts != 1 {
		t.Fatal("retry reused dispatch authorization")
	}
	f.now = f.now.Add(16 * time.Minute)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)

	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	if f.submits.Load() != 1 {
		t.Fatal("replacement not submitted once")
	}
}

type boundedFaultFills struct {
	cache.FillRepository
	starts int
}

func (s *boundedFaultFills) Start(ctx context.Context, lease cache.FillLease, now time.Time) (bool, error) {
	s.starts++
	won, err := s.FillRepository.Start(ctx, lease, now)
	if err == nil && s.starts == 1 {
		return false, errors.New("lost start reply")
	}
	return won, err
}

func TestCloudExecutionRuntimeCacheWaitAndBudgetWait(t *testing.T) {
	f := boundedCloud(t, true)
	f.request.Cache = &llm.CachePolicyV1{}
	ctx := context.Background()
	first, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, first, err, llm.ExecutionPending)
	second := f.request
	second.OperationKey = "second"
	v, err := f.runtime.GenerateStepV1(ctx, second)
	boundedState(t, v, err, llm.ExecutionCacheWait)
	if f.submits.Load() != 1 {
		t.Fatal("cache waiter submitted")
	}
	f.now = f.now.Add(2 * time.Second)
	ref := llm.ExecutionReferenceV1{RequestID: first.RequestID, Context: f.request.Context}
	v, err = f.runtime.PollExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	v, err = f.runtime.GenerateStepV1(ctx, second)
	boundedState(t, v, err, llm.ExecutionCompleted)
	if v.Generate.Cache.Disposition != "hit" || f.submits.Load() != 1 {
		t.Fatal("waiter did not consume cache")
	}
	// A fresh uncached request tries once and returns a timer instruction.
	f.cap.Budgets = &admissionLeaser{BudgetLeaser: f.cap.Budgets, accept: func(_ context.Context, request durable.ReserveRequest) (durable.ReserveResult, error) {
		return durable.ReserveResult{OperationID: request.OperationID, GenerationID: request.GenerationID, RetryAfter: 2 * time.Second}, nil
	}}
	f.restart(t)
	f.request.OperationKey = "budget-wait"
	f.request.Cache = nil
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionBudgetWait)
	if v.RetryAfterSeconds != 2 || f.submits.Load() != 1 {
		t.Fatal("budget waiter submitted")
	}
}

func TestCloudExecutionRuntimeUnreserved(t *testing.T) {
	for _, mode := range []string{"free", "unmatched"} {
		t.Run(mode, func(t *testing.T) {
			f := boundedCloud(t, false, unreservedPlanning(t, mode))
			f.cap.Budgets = &admissionLeaser{BudgetLeaser: f.cap.Budgets, accept: func(context.Context, durable.ReserveRequest) (durable.ReserveResult, error) {
				t.Fatal("unreserved work touched budgets")
				return durable.ReserveResult{}, nil
			}}
			f.restart(t)
			result := f.finish(t)
			if result.Generate == nil || f.submits.Load() != 1 {
				t.Fatal("unreserved request did not complete")
			}
		})
	}
}

func TestCloudExecutionRuntimeFailurePreservesOlderSuccess(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	first := f.finish(t)
	original := f.adapter.invoke
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		return provider.Result{}, provider.NewError(provider.CodePermissionDenied, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNever, "rejected")
	}
	f.now = f.now.Add(2 * time.Second)
	f.request.OperationKey = "failing"
	f.request.Cache = &llm.CachePolicyV1{MaxAgeSeconds: 1}
	v, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	f.now = f.now.Add(time.Second)
	f.request.OperationKey = "older-eligible"
	f.request.Cache = &llm.CachePolicyV1{}
	hit := f.finish(t)
	if hit.Generate.Cache.Disposition != "hit" || hit.Generate.Output[0].(llm.Message).Content[0].(llm.TextPart).Text != first.Generate.Output[0].(llm.Message).Content[0].(llm.TextPart).Text || f.submits.Load() != 2 {
		t.Fatal("failure erased older success")
	}
	// A new freshness-limited fill must proceed after the failed owner.
	f.adapter.invoke = original
	f.request.OperationKey = "fresh-after-failure"
	f.request.Cache = &llm.CachePolicyV1{MaxAgeSeconds: 1}
	f.finish(t)
	if f.submits.Load() != 3 {
		t.Fatal("failed fill blocked fresh work")
	}
}

func TestCloudExecutionRuntimeIncompleteCompaction(t *testing.T) {
	f := boundedCloud(t, false)
	policy := json.RawMessage(`{"recent_turns":0}`)
	f.request.SettingsPatch.CompactionPolicy.Set = &policy
	parent := f.finish(t)
	f.now = f.now.Add(time.Minute)
	request := llm.CompactRequestV1{OperationKey: "compact", Context: f.request.Context, Parent: parent.Generate.Checkpoint.Handle, Cache: &llm.CachePolicyV1{}}
	original := f.adapter.invoke
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		v, err := original(ctx, call, o)
		v.Response.Status = llm.ResponseStatusLength
		return v, err
	}
	ctx := context.Background()
	v, err := f.runtime.CompactStepV1(ctx, request)
	boundedState(t, v, err, llm.ExecutionFailed)
	if v.FailureCode != "incomplete_response" {
		t.Fatal("bad summary accepted")
	}
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: request.Context}
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionFailed)
	f.adapter.invoke = original
	f.now = f.now.Add(time.Second)
	request.OperationKey = "new-summary"
	v, err = f.runtime.CompactStepV1(ctx, request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	if f.submits.Load() != 3 {
		t.Fatal("incomplete summary cached or fill not closed")
	}
}

func TestCloudExecutionRuntimeCacheSemanticIdentity(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	temperature, _ := llm.NewDecimalV1("0.5")
	f.request.SettingsPatch.Temperature.Set = &temperature
	f.request.Append = []llm.Item{preparationMessage(strings.Repeat("a", 300<<10))}
	first := f.finish(t)
	if first.Generate.Cache.Disposition != "miss_populated" {
		t.Fatal("large semantic input not cached")
	}
	f.now = f.now.Add(time.Second)
	f.request.OperationKey = "different-actor"
	f.request.Context.Actor = "actor-two"
	hit := f.finish(t)
	if hit.Generate.Cache.Disposition != "hit" || f.submits.Load() != 1 {
		t.Fatal("actor changed semantic identity")
	}
	f.now = f.now.Add(time.Second)
	f.request.OperationKey = "different-sample"
	f.request.Cache = &llm.CachePolicyV1{Variant: 1}
	next := f.finish(t)
	if next.Generate.Cache.Disposition != "miss_populated" || f.submits.Load() != 2 {
		t.Fatal("sample index did not break cache")
	}
}

func TestCloudExecutionRuntimeRequiresBoundCapabilities(t *testing.T) {
	f := boundedCloud(t, false)
	for _, test := range []struct {
		name   string
		change func(*V1RuntimeCapabilities, *CloudExecutionOptions)
	}{
		{"store", func(c *V1RuntimeCapabilities, _ *CloudExecutionOptions) { c.Requests = nil }},
		{"budgets", func(c *V1RuntimeCapabilities, _ *CloudExecutionOptions) { c.Budgets = nil }},
		{"redis", func(c *V1RuntimeCapabilities, _ *CloudExecutionOptions) { c.RedisIdentity = durable.RedisIdentity{} }},
		{"cloud", func(c *V1RuntimeCapabilities, _ *CloudExecutionOptions) { c.CloudIdentity = durable.CloudIdentity{} }},
		{"digest", func(c *V1RuntimeCapabilities, _ *CloudExecutionOptions) { c.ConfigDigest = [32]byte{} }},
		{"cache", func(c *V1RuntimeCapabilities, _ *CloudExecutionOptions) { c.Responses = nil }},
		{"fills", func(c *V1RuntimeCapabilities, _ *CloudExecutionOptions) { c.ResponseFills = nil }},
		{"finalizer", func(c *V1RuntimeCapabilities, _ *CloudExecutionOptions) { c.Finalizer = nil }},
		{"clock", func(c *V1RuntimeCapabilities, _ *CloudExecutionOptions) { c.Clock = nil }},
		{"keys", func(_ *V1RuntimeCapabilities, o *CloudExecutionOptions) { o.Keyring = nil }},
		{"scope", func(_ *V1RuntimeCapabilities, o *CloudExecutionOptions) { o.ResolveScope = nil }},
		{"ttl", func(_ *V1RuntimeCapabilities, o *CloudExecutionOptions) { o.CheckpointTTL = 0 }},
		{"generation", func(_ *V1RuntimeCapabilities, o *CloudExecutionOptions) { o.BudgetGeneration = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cap, options := f.cap, f.options
			test.change(&cap, &options)
			if _, err := cap.NewCloudExecutionRuntime(context.Background(), options); err == nil {
				t.Fatal("incomplete capabilities accepted")
			}
		})
	}
	// The cloud path must never ask a legacy composition factory for SQL ports.
	f.cap.CompositionFactory = func(context.Context, V1RuntimeCapabilities) (durable.Composition, error) {
		t.Fatal("legacy composition used")
		return durable.Composition{}, nil
	}
	f.restart(t)
}

func TestCloudExecutionRuntimeCacheKeepsExactTemperature(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	for i, value := range []string{"0.10000000000000001", "0.100000000000000009"} {
		decimal, err := llm.NewDecimalV1(value)
		if err != nil {
			t.Fatal(err)
		}
		f.request.SettingsPatch.Temperature.Set = &decimal
		f.request.OperationKey = fmt.Sprintf("exact-temperature-%d", i)
		result := f.finish(t)
		if result.Generate.Cache.Disposition != "miss_populated" {
			t.Fatal("distinct exact temperature reused a response")
		}
		f.now = f.now.Add(time.Second)
	}
	if f.submits.Load() != 2 {
		t.Fatal("exact temperatures collided")
	}
}

func TestCloudExecutionRuntimeIndependentSamples(t *testing.T) {
	for _, temperature := range []string{"", "0", "0.5"} {
		t.Run("temperature-"+temperature, func(t *testing.T) {
			f := boundedCloud(t, false)
			if temperature != "" {
				value, err := llm.NewDecimalV1(temperature)
				if err != nil {
					t.Fatal(err)
				}
				f.request.SettingsPatch.Temperature.Set = &value
			} else {
				f.request.SettingsPatch.Temperature.Set = nil
			}
			for attempt, index := range []int32{0, 1, 2147483647} {
				f.request.Cache = &llm.CachePolicyV1{Variant: index}
				f.request.OperationKey = fmt.Sprintf("sample-%d", index)
				result := f.finish(t)
				if result.Generate.Cache.Variant != index || result.Generate.Cache.Disposition != "miss_populated" || f.submits.Load() != int32(attempt+1) {
					t.Fatal("sample did not get independent provider result")
				}
				replay := f.finish(t)
				if replay.Generate.Checkpoint.Handle != result.Generate.Checkpoint.Handle {
					t.Fatal("retry changed checkpoint")
				}
				f.now = f.now.Add(time.Second)
				f.request.OperationKey += "-cached"
				hit := f.finish(t)
				if hit.Generate.Cache.Variant != index || hit.Generate.Cache.Disposition != "hit" || f.submits.Load() != int32(attempt+1) {
					t.Fatal("sample cache miss on identical request")
				}
			}
		})
	}
}

func TestCloudExecutionRuntimeTerminalFillStorageFailureIsRetryable(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	fills := &boundedCompleteFaultFills{FillRepository: f.cap.ResponseFills, failures: 1}
	f.cap.ResponseFills = fills
	f.restart(t)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		return provider.Result{}, provider.NewError(provider.CodePermissionDenied, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNever, "rejected")
	}
	_, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeStateUnavailable || mapped.Retry != provider.RetrySameOperation {
		t.Fatalf("transient fill completion error = %#v, want retryable state_unavailable", err)
	}
	v, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	if fills.completes != 2 || f.submits.Load() != 1 {
		t.Fatalf("fill completes = %d, submits = %d", fills.completes, f.submits.Load())
	}
}

type boundedCompleteFaultFills struct {
	cache.FillRepository
	failures  int
	completes int
}

func (s *boundedCompleteFaultFills) Complete(ctx context.Context, lease cache.FillLease, completion cache.FillCompletion) error {
	s.completes++
	if s.completes <= s.failures {
		return errors.New("throttled: transient storage failure")
	}
	return s.FillRepository.Complete(ctx, lease, completion)
}

func TestCloudExecutionRuntimePreDispatchContextEndIsRetryable(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			f := boundedCloud(t, false)
			original := f.adapter.invoke
			f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
				f.submits.Add(1)
				return provider.Result{}, provider.NewPreDispatchContextError(cause)
			}
			v, err := f.runtime.GenerateStepV1(context.Background(), f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			if !v.Retryable {
				t.Fatalf("pre-dispatch %v became a permanent failure: %#v", cause, v)
			}
			f.adapter.invoke = original
			ctx := context.Background()
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
			f.now = f.now.Add(2 * time.Second)
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionAcquired)
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			v, err = f.runtime.CompleteExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionCompleted)
		})
	}
}

func TestCloudExecutionRuntimeRejectedDispatchStaysPermanent(t *testing.T) {
	f := boundedCloud(t, false)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		return provider.Result{}, provider.NewError(provider.CodeCanceled, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "canceled without pre-dispatch evidence")
	}
	v, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	if v.Retryable {
		t.Fatal("cancellation without pre-dispatch evidence became retryable")
	}
}

// pinnedCloudFixture is a boundedCloud with two OpenAI Responses routes for
// one model on different endpoints (accounts). Every response carries an
// encrypted reasoning item ahead of its answer, except while plain is set
// (a compaction summary must be plain text).
type pinnedCloudFixture struct {
	*boundedCloudFixture
	source *planningSource
	served []string
	plain  bool
}

func pinnedCloud(t *testing.T) *pinnedCloudFixture {
	t.Helper()
	p := &pinnedCloudFixture{}
	p.boundedCloudFixture = boundedCloud(t, false, func(b *budgetPlanningFixture) {
		p.source = b.source
		model := b.source.value.Routes.Models["alias"]
		model.Routes = append([]routing.Route(nil), model.Routes...)
		model.Routes[0].EndpointAccountDigest = pinnedAccount("endpoint")
		other := model.Routes[0]
		other.ID, other.EndpointID, other.EndpointAccountHMAC, other.EndpointAccountDigest = "route-b", "endpoint-b", [32]byte{9}, pinnedAccount("endpoint-b")
		model.Routes = append(model.Routes, other)
		b.source.value.Routes.Models["alias"] = model
		b.source.value.BudgetPolicies[0].Match.EndpointID = ""
		second := b.entry
		second.EndpointID = "endpoint-b"
		b.prices(t, []pricing.Entry{b.entry, second})
	})
	adapter := p.adapter.executionSyncAdapter
	adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		p.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		p.served = append(p.served, call.EndpointID)
		result := executionResponse(call).Result
		result.Response.Output = []llm.Item{
			llm.ProviderState{Provider: "openai", EndpointFamily: "responses", MediaType: "application/vnd.openai.reasoning+json",
				Opaque: []byte(`{"type":"reasoning","encrypted_content":"` + call.EndpointID + `"}`)},
			llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "answer"}}},
		}
		if p.plain {
			result.Response.Output = result.Response.Output[1:]
		}
		return result, nil
	}
	p.cap.Adapters = engine.AdapterMap{"endpoint": adapter, "endpoint-b": adapter}
	p.restart(t)
	return p
}

// reorder puts route-b ahead of the route that served the first turn.
func (p *pinnedCloudFixture) reorder(t *testing.T) {
	t.Helper()
	model := p.source.value.Routes.Models["alias"]
	model.Routes = []routing.Route{model.Routes[1], model.Routes[0]}
	p.source.value.Routes.Models["alias"] = model
	p.restart(t)
}

func (p *pinnedCloudFixture) child(parent llm.ExecutionResultV1, key string) {
	handle := parent.Generate.Checkpoint.Handle
	p.request = llm.GenerateRequestV1{OperationKey: key, Context: p.request.Context, Parent: &handle,
		Append: []llm.Item{preparationMessage("next question")}}
}

// lastCompiled returns the request most recently compiled for an endpoint.
func (p *pinnedCloudFixture) lastCompiled(t *testing.T, endpoint string) llm.Request {
	t.Helper()
	inputs := p.adapter.planningAdapter.inputs
	for index := len(inputs) - 1; index >= 0; index-- {
		if inputs[index].Query.EndpointID == endpoint {
			return inputs[index].Request
		}
	}
	t.Fatalf("nothing compiled for %s", endpoint)
	return llm.Request{}
}

func compiledProviderState(request llm.Request) int {
	count := 0
	for _, item := range request.Input {
		if itemHasProviderState(item) {
			count++
		}
	}
	return count
}

func (p *pinnedCloudFixture) checkpoint(t *testing.T, result llm.ExecutionResultV1) state.DurableCheckpoint {
	t.Helper()
	id, err := p.options.Keyring.VerifyCheckpointHandle(context.Background(), "trusted-scope", string(result.Generate.Checkpoint.Handle))
	if err != nil {
		t.Fatal(err)
	}
	row, err := p.repository.Checkpoints().Get(context.Background(), "trusted-scope", id)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

// pinnedAccount is the test account identity of an endpoint.
func pinnedAccount(endpoint string) [32]byte { return sha256.Sum256([]byte("account/" + endpoint)) }

func pinnedProvenance(ordinal int, endpoint string) []state.ProviderStateProvenance {
	account := pinnedAccount(endpoint)
	return []state.ProviderStateProvenance{{Ordinal: ordinal, Provider: "openai", EndpointID: endpoint,
		EndpointFamily: string(provider.FamilyOpenAIResponses), ModelLineage: "provider-model", Account: hex.EncodeToString(account[:])}}
}

func TestCloudExecutionRuntimePinnedContinuationRoutesToItsEndpoint(t *testing.T) {
	p := pinnedCloud(t)
	first := p.finish(t)
	if got := p.checkpoint(t, first).ProviderStateProvenance; fmt.Sprint(got) != fmt.Sprint(pinnedProvenance(1, "endpoint")) {
		t.Fatalf("root provenance = %+v", got)
	}
	// With route-b now ordered first, the pin still selects the endpoint
	// that produced the reasoning state, and that state is replayed there.
	p.reorder(t)
	p.child(first, "pinned-turn")
	second := p.finish(t)
	if len(p.served) != 2 || p.served[1] != "endpoint" {
		t.Fatalf("served %v, want the pinned endpoint", p.served)
	}
	if compiledProviderState(p.lastCompiled(t, "endpoint")) != 1 || len(second.Generate.Diagnostics) != 0 {
		t.Fatalf("pinned route lost its state or reported a drop: %+v", second.Generate.Diagnostics)
	}
	// The child records only its own response, at its transcript offset.
	if got := p.checkpoint(t, second).ProviderStateProvenance; fmt.Sprint(got) != fmt.Sprint(pinnedProvenance(4, "endpoint")) {
		t.Fatalf("child provenance = %+v", got)
	}
}

func TestCloudExecutionRuntimeContinuationPinnedWhenPinnedRouteIneligible(t *testing.T) {
	p := pinnedCloud(t)
	first := p.finish(t)
	p.source.value.Health.Routes = map[string]routing.RouteHealth{"route": {Enabled: false}}
	p.restart(t)
	p.child(first, "pinned-unavailable")
	ctx := context.Background()
	v, err := p.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &p.request})
	if err == nil && v.State == llm.ExecutionBudgetRequired {
		v, err = p.runtime.AcquireBudgetV1(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: p.request.Context})
	}
	var failure *provider.Error
	if !errors.As(err, &failure) {
		t.Fatalf("state=%+v error=%v, want a planning failure", v, err)
	}
	if failure.Code != provider.CodeNoRoute || failure.Dispatch != provider.DispatchNotDispatched || failure.SafeDetails["continuation"] != "continuation_pinned" {
		t.Fatalf("failure = %#v", failure)
	}
	pinned := false
	for key, value := range failure.SafeDetails {
		pinned = pinned || (strings.HasPrefix(key, "reason_") && value == "continuation_pinned")
	}
	if !pinned || len(p.served) != 1 {
		t.Fatalf("route-b was not rejected as pinned, or was dispatched: %#v served=%v", failure.SafeDetails, p.served)
	}
}

func TestCloudExecutionRuntimeBestEffortDropsPinnedState(t *testing.T) {
	p := pinnedCloud(t)
	first := p.finish(t)
	p.source.value.Health.Routes = map[string]routing.RouteHealth{"route": {Enabled: false}}
	p.restart(t)
	p.child(first, "best-effort-turn")
	mode := llm.PortabilityBestEffort
	p.request.SettingsPatch.Portability.Set = &mode
	second := p.finish(t)
	if len(p.served) != 2 || p.served[1] != "endpoint-b" {
		t.Fatalf("served %v, want the portable route", p.served)
	}
	compiled := p.lastCompiled(t, "endpoint-b")
	if compiledProviderState(compiled) != 0 || len(compiled.Input) != 3 {
		t.Fatalf("another account received pinned state: %+v", compiled.Input)
	}
	diagnostics := second.Generate.Diagnostics
	if len(diagnostics) != 1 || diagnostics[0].Code != "provider_state_dropped" || diagnostics[0].Severity != llm.DiagnosticWarning {
		t.Fatalf("diagnostics = %+v", diagnostics)
	}
	// The transcript stays complete; only the new state is pinned to route-b.
	if got := p.checkpoint(t, second).ProviderStateProvenance; fmt.Sprint(got) != fmt.Sprint(pinnedProvenance(4, "endpoint-b")) {
		t.Fatalf("child provenance = %+v", got)
	}
	// A retry returns the same publication, diagnostic included.
	again := p.finish(t)
	if again.Generate.Checkpoint.Handle != second.Generate.Checkpoint.Handle || len(again.Generate.Diagnostics) != 1 || p.submits.Load() != 2 {
		t.Fatal("retry changed the published diagnostic")
	}
	// The next turn is pinned to route-b. The first account's older state is
	// dropped there too, while route-b's own state is replayed.
	p.child(second, "after-move")
	third := p.finish(t)
	compiled = p.lastCompiled(t, "endpoint-b")
	if p.served[2] != "endpoint-b" || compiledProviderState(compiled) != 1 || len(third.Generate.Diagnostics) != 1 {
		t.Fatalf("served %v state=%d diagnostics=%+v", p.served, compiledProviderState(compiled), third.Generate.Diagnostics)
	}
}

// legacyProvenanceRepository reads checkpoints as if they were written before
// provider-state provenance existed.
type legacyProvenanceRepository struct {
	state.CheckpointRepository
}

func (repository legacyProvenanceRepository) Get(ctx context.Context, scope string, id state.CheckpointID) (state.DurableCheckpoint, error) {
	row, err := repository.CheckpointRepository.Get(ctx, scope, id)
	row.ProviderStateProvenance = nil
	return row, err
}

func TestCloudExecutionRuntimeCheckpointWithoutProvenanceKeepsFamilyPinning(t *testing.T) {
	p := pinnedCloud(t)
	first := p.finish(t)
	materializer := *p.cap.Checkpoints.Materializer.(*state.DurableCheckpointMaterializer)
	materializer.Repository = legacyProvenanceRepository{materializer.Repository}
	p.cap.Checkpoints.Materializer = &materializer
	p.reorder(t)
	p.child(first, "legacy-parent")
	second := p.finish(t)
	// Without provenance nothing is pinned: route order decides, as before,
	// and the state is replayed under the adapter's family check alone.
	if p.served[1] != "endpoint-b" || compiledProviderState(p.lastCompiled(t, "endpoint-b")) != 1 || len(second.Generate.Diagnostics) != 0 {
		t.Fatalf("served %v diagnostics %+v", p.served, second.Generate.Diagnostics)
	}
	if got := p.checkpoint(t, second).ProviderStateProvenance; fmt.Sprint(got) != fmt.Sprint(pinnedProvenance(4, "endpoint-b")) {
		t.Fatalf("child provenance = %+v", got)
	}
}

// planFailure runs preparation and budget acquisition for p.request and
// returns the planning failure they report.
func (p *pinnedCloudFixture) planFailure(t *testing.T) *provider.Error {
	t.Helper()
	ctx := context.Background()
	v, err := p.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &p.request})
	if err == nil && v.State == llm.ExecutionBudgetRequired {
		v, err = p.runtime.AcquireBudgetV1(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: p.request.Context})
	}
	var failure *provider.Error
	if !errors.As(err, &failure) || failure.Code != provider.CodeNoRoute || failure.SafeDetails["continuation"] != "continuation_pinned" {
		t.Fatalf("state=%+v error=%#v, want continuation_pinned", v, err)
	}
	return failure
}

func TestCloudExecutionRuntimeReusedEndpointIDOnAnotherAccountIsNotThePin(t *testing.T) {
	p := pinnedCloud(t)
	first := p.finish(t)
	// A reload points the pinned endpoint ID at another account. The ID
	// still matches, but the account does not, so the state is not replayed.
	model := p.source.value.Routes.Models["alias"]
	model.Routes = append([]routing.Route(nil), model.Routes...)
	model.Routes[0].EndpointAccountDigest = pinnedAccount("other-account")
	p.source.value.Routes.Models["alias"] = model
	p.restart(t)
	p.child(first, "reused-endpoint-id")
	p.planFailure(t)
	if len(p.served) != 1 {
		t.Fatalf("dispatched to a changed account: %v", p.served)
	}
	// Best effort serves the turn on the changed endpoint without the state.
	mode := llm.PortabilityBestEffort
	p.request.OperationKey = "reused-endpoint-id-best-effort"
	p.request.SettingsPatch.Portability.Set = &mode
	second := p.finish(t)
	if p.served[1] != "endpoint" || compiledProviderState(p.lastCompiled(t, "endpoint")) != 0 || len(second.Generate.Diagnostics) != 1 {
		t.Fatalf("served %v diagnostics %+v", p.served, second.Generate.Diagnostics)
	}
}

// unrecordedAccountRepository reads provenance as if it had been written
// without an account identity.
type unrecordedAccountRepository struct {
	state.CheckpointRepository
}

func (repository unrecordedAccountRepository) Get(ctx context.Context, scope string, id state.CheckpointID) (state.DurableCheckpoint, error) {
	row, err := repository.CheckpointRepository.Get(ctx, scope, id)
	row.ProviderStateProvenance = append([]state.ProviderStateProvenance(nil), row.ProviderStateProvenance...)
	for index := range row.ProviderStateProvenance {
		row.ProviderStateProvenance[index].Account = ""
	}
	return row, err
}

func TestCloudExecutionRuntimeProvenanceWithoutAccountFailsClosed(t *testing.T) {
	p := pinnedCloud(t)
	first := p.finish(t)
	materializer := *p.cap.Checkpoints.Materializer.(*state.DurableCheckpointMaterializer)
	materializer.Repository = unrecordedAccountRepository{materializer.Repository}
	p.cap.Checkpoints.Materializer = &materializer
	p.restart(t)
	// Even the endpoint that produced the state cannot prove its account.
	p.child(first, "unproven-account")
	p.planFailure(t)
	mode := llm.PortabilityBestEffort
	p.request.OperationKey = "unproven-account-best-effort"
	p.request.SettingsPatch.Portability.Set = &mode
	second := p.finish(t)
	if p.served[1] != "endpoint" || compiledProviderState(p.lastCompiled(t, "endpoint")) != 0 || len(second.Generate.Diagnostics) != 1 {
		t.Fatalf("served %v diagnostics %+v", p.served, second.Generate.Diagnostics)
	}
}

// row reads the checkpoint a handle names.
func (p *pinnedCloudFixture) row(t *testing.T, handle llm.CheckpointHandle) state.DurableCheckpoint {
	t.Helper()
	id, err := p.options.Keyring.VerifyCheckpointHandle(context.Background(), "trusted-scope", string(handle))
	if err != nil {
		t.Fatal(err)
	}
	row, err := p.repository.Checkpoints().Get(context.Background(), "trusted-scope", id)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func TestCloudExecutionRuntimeCacheReplayKeepsOriginProvenance(t *testing.T) {
	p := pinnedCloud(t)
	p.request.Cache = &llm.CachePolicyV1{}
	first := p.finish(t)
	if first.Generate.Cache.Disposition != "miss_populated" {
		t.Fatalf("origin disposition %q", first.Generate.Cache.Disposition)
	}
	p.now = p.now.Add(time.Second)
	p.request.OperationKey = "replayed"
	hit := p.finish(t)
	if hit.Generate.Cache.Disposition != "hit" || p.submits.Load() != 1 {
		t.Fatalf("disposition %q after %d submits, want a cache hit", hit.Generate.Cache.Disposition, p.submits.Load())
	}
	// The replay records the attempt that produced the reasoning state, not
	// family-only state.
	replay := p.checkpoint(t, hit)
	if replay.Kind != state.CheckpointCacheReplay || fmt.Sprint(replay.ProviderStateProvenance) != fmt.Sprint(pinnedProvenance(1, "endpoint")) {
		t.Fatalf("replay %s provenance = %+v", replay.Kind, replay.ProviderStateProvenance)
	}
	// So the next turn is pinned to that endpoint even with route-b first.
	p.reorder(t)
	p.child(hit, "after-replay")
	next := p.finish(t)
	if len(p.served) != 2 || p.served[1] != "endpoint" || compiledProviderState(p.lastCompiled(t, "endpoint")) != 1 || len(next.Generate.Diagnostics) != 0 {
		t.Fatalf("served %v diagnostics %+v", p.served, next.Generate.Diagnostics)
	}
}

func TestCloudExecutionRuntimeCacheReplayOfLegacyOriginKeepsFamilyPinning(t *testing.T) {
	p := pinnedCloud(t)
	p.request.Cache = &llm.CachePolicyV1{}
	p.finish(t)
	// The origin reads as if it had been published before provenance existed.
	p.cap.Checkpoints.Repository = legacyProvenanceRepository{p.cap.Checkpoints.Repository}
	p.restart(t)
	p.now = p.now.Add(time.Second)
	p.request.OperationKey = "legacy-replay"
	hit := p.finish(t)
	if hit.Generate.Cache.Disposition != "hit" || p.checkpoint(t, hit).ProviderStateProvenance != nil {
		t.Fatalf("disposition %q provenance %+v", hit.Generate.Cache.Disposition, p.checkpoint(t, hit).ProviderStateProvenance)
	}
	// Nothing is pinned: route order decides and the state is replayed under
	// the adapter's family check alone, as before.
	p.reorder(t)
	p.child(hit, "after-legacy-replay")
	next := p.finish(t)
	if p.served[len(p.served)-1] != "endpoint-b" || compiledProviderState(p.lastCompiled(t, "endpoint-b")) != 1 || len(next.Generate.Diagnostics) != 0 {
		t.Fatalf("served %v diagnostics %+v", p.served, next.Generate.Diagnostics)
	}
}

// The compaction summarizer receives the summarized prefix rendered as plain
// text, never provider state, so it is not pinned. The retained exchange
// keeps its pin at its new position and the next turn honours it.
func TestCloudExecutionRuntimeCompactionSummarizerIsNotPinned(t *testing.T) {
	p := pinnedCloud(t)
	policy := json.RawMessage(`{"recent_turns":1}`)
	p.request.SettingsPatch.CompactionPolicy.Set = &policy
	first := p.finish(t)
	p.child(first, "second-turn")
	second := p.finish(t)
	if got := p.checkpoint(t, second).ProviderStateProvenance; fmt.Sprint(got) != fmt.Sprint(pinnedProvenance(4, "endpoint")) {
		t.Fatalf("second provenance = %+v", got)
	}
	p.reorder(t)
	p.child(second, "after-compaction")
	p.plain = true
	compacted := p.compact(t, "compaction")
	p.plain = false
	if len(p.served) != 3 || p.served[2] != "endpoint-b" {
		t.Fatalf("served %v, want the summarizer on the first-ordered route", p.served)
	}
	summarizer := p.lastCompiled(t, "endpoint-b")
	if compiledProviderState(summarizer) != 0 || len(summarizer.Input) != 1 {
		t.Fatalf("summarizer input = %+v", summarizer.Input)
	}
	// One retained turn is the model's state and answer, so the compacted
	// transcript is [summary, state, answer] and the pin moved to ordinal 1.
	if got := p.row(t, compacted).ProviderStateProvenance; fmt.Sprint(got) != fmt.Sprint(pinnedProvenance(1, "endpoint")) {
		t.Fatalf("compacted provenance = %+v", got)
	}
	next := p.finish(t)
	if len(p.served) != 4 || p.served[3] != "endpoint" || compiledProviderState(p.lastCompiled(t, "endpoint")) != 1 || len(next.Generate.Diagnostics) != 0 {
		t.Fatalf("served %v diagnostics %+v", p.served, next.Generate.Diagnostics)
	}
}

func TestEndpointAccountDigestIgnoresNonAccountSettings(t *testing.T) {
	endpoint := config.EndpointConfig{Family: "openai_responses", BaseURL: "https://api.openai.com/v1", Region: "us", Auth: config.AuthConfig{Kind: "bearer_env", Name: "OPENAI_API_KEY"}}
	base := endpointAccountDigest("openai", endpoint)
	tuned := endpoint
	tuned.Timeout, tuned.CapabilityProfile, tuned.PriceCatalog = config.Duration(time.Minute), "other-profile", "other-prices"
	if endpointAccountDigest("openai", tuned) != base {
		t.Fatal("a non-account setting changed the account identity")
	}
	for _, change := range []func(*config.EndpointConfig){
		func(value *config.EndpointConfig) { value.BaseURL = "https://example.openai.azure.com/openai/v1" },
		func(value *config.EndpointConfig) { value.Auth.Name = "OTHER_OPENAI_API_KEY" },
		func(value *config.EndpointConfig) { value.AccountRegion = "eu" },
		func(value *config.EndpointConfig) { value.AWSWorkspaceID = "workspace" },
	} {
		changed := endpoint
		change(&changed)
		if endpointAccountDigest("openai", changed) == base {
			t.Fatalf("account change kept the identity: %+v", changed)
		}
	}
}

// TestCloudRequestPreparationReusesValidatedParent proves that restoring a
// preparation from the parent snapshot its load already validated (#1112)
// gives exactly what validating and decoding it again gives.
func TestCloudRequestPreparationReusesValidatedParent(t *testing.T) {
	f := boundedCloud(t, false, withParentSnapshotBlob)
	parent := f.finish(t)
	handle := parent.Generate.Checkpoint.Handle
	child := llm.GenerateRequestV1{OperationKey: "child", Context: f.request.Context, Parent: &handle, Append: []llm.Item{preparationMessage("next")}}
	ctx := context.Background()
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &child})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	scope, id := cloudstate.Scope{Tenant: child.Context.Tenant, Project: child.Context.Project}, cloudstate.RequestID(v.RequestID)
	record, err := f.repository.Read(ctx, scope, id)
	if err != nil {
		t.Fatal(err)
	}
	// The record references the parent snapshot blob instead of embedding it.
	if !bytes.Contains(record.Progress, []byte(`"parent_snapshot_ref"`)) || bytes.Contains(record.Progress, []byte(`"parent_snapshot"`)) {
		t.Fatal("request record embeds the parent snapshot")
	}
	preparation, snapshot, err := f.repository.LoadRequestPreparationParent(ctx, scope, id)
	if err != nil || snapshot == nil {
		t.Fatalf("snapshot=%v err=%v", snapshot, err)
	}
	if decoded, err := preparation.ValidateParent(); err != nil || !reflect.DeepEqual(*decoded, *snapshot) {
		t.Fatalf("loaded parent differs from a fresh decode: %v", err)
	}
	want, err := f.runtime.preparation.restore(ctx, record, preparation, "trusted-scope")
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.runtime.preparation.restoreLoaded(ctx, record, preparation, snapshot, true, "trusted-scope")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("validated restore differs: err=%v", err)
	}
	loaded, err := f.runtime.preparation.Load(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: child.Context})
	if err != nil || !reflect.DeepEqual(loaded.GenerateReplay, want.GenerateReplay) || !reflect.DeepEqual(loaded.Preparation, want.Preparation) {
		t.Fatalf("Load differs from a fully validated restore: err=%v", err)
	}
	// A preparation the store did not validate is still validated in restore.
	corrupt := preparation
	corrupt.ParentSnapshot = append(json.RawMessage(nil), preparation.ParentSnapshot[:len(preparation.ParentSnapshot)/2]...)
	if _, err := f.runtime.preparation.restoreLoaded(ctx, record, corrupt, nil, false, "trusted-scope"); err == nil {
		t.Fatal("restore accepted a corrupt parent snapshot")
	}
}

// TestCloudExecutionRuntimeTurnParentSnapshotStorage runs a whole child turn
// with each parent-snapshot storage setting (#1112): the default stores the
// parent inline, blob stores a reference, and both complete.
func TestCloudExecutionRuntimeTurnParentSnapshotStorage(t *testing.T) {
	for _, blobStorage := range []bool{false, true} {
		t.Run(fmt.Sprintf("blob=%t", blobStorage), func(t *testing.T) {
			var options []func(*budgetPlanningFixture)
			if blobStorage {
				options = append(options, withParentSnapshotBlob)
			}
			f := boundedCloud(t, false, options...)
			parent := f.finish(t)
			handle := parent.Generate.Checkpoint.Handle
			child := llm.GenerateRequestV1{OperationKey: "child", Context: f.request.Context, Parent: &handle, Append: []llm.Item{preparationMessage("next")}}
			ctx := context.Background()
			v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &child})
			boundedState(t, v, err, llm.ExecutionBudgetRequired)
			record, err := f.repository.Read(ctx, cloudstate.Scope{Tenant: child.Context.Tenant, Project: child.Context.Project}, cloudstate.RequestID(v.RequestID))
			if err != nil {
				t.Fatal(err)
			}
			inline, referenced := bytes.Contains(record.Progress, []byte(`"parent_snapshot"`)), bytes.Contains(record.Progress, []byte(`"parent_snapshot_ref"`))
			if inline == blobStorage || referenced != blobStorage {
				t.Fatalf("inline=%t referenced=%t with blob storage %t", inline, referenced, blobStorage)
			}
			f.now = f.now.Add(time.Second)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: child.Context}
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionAcquired)
			v, err = f.runtime.GenerateStepV1(ctx, child)
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			v, err = f.runtime.CompleteExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionCompleted)
		})
	}
}

// BenchmarkCloudExecutionRuntimeTurnLargeParent measures each Activity step of
// one Generate turn that appends a 64-byte message to a parent transcript of
// about 1 MB (issue #1112). Only the named step is timed; the other steps run
// untimed so every iteration is a complete, fresh turn on the same parent.
// Each step runs with the default inline parent snapshot and with the
// referenced blob form (state.requests.parent_snapshot_storage: blob).
func BenchmarkCloudExecutionRuntimeTurnLargeParent(b *testing.B) {
	for _, storage := range []string{"inline", "blob"} {
		for _, step := range []string{"prepare", "acquire", "generate", "complete"} {
			benchmarkCloudExecutionRuntimeTurnLargeParent(b, storage, step)
		}
	}
}

func benchmarkCloudExecutionRuntimeTurnLargeParent(b *testing.B, storage, step string) {
	b.Run(storage+"-"+step, func(b *testing.B) {
		var options []func(*budgetPlanningFixture)
		if storage == "blob" {
			options = append(options, withParentSnapshotBlob)
		}
		f := boundedCloud(b, false, options...)
		f.request.Append = nil
		for i := 0; i < 10; i++ {
			f.request.Append = append(f.request.Append, preparationMessage(strings.Repeat(fmt.Sprintf("parent %d ", i), 100<<10/9)))
		}
		parent := f.finish(b)
		if parent.Generate == nil {
			b.Fatal("missing parent")
		}
		handle := parent.Generate.Checkpoint.Handle
		child := llm.GenerateRequestV1{Context: f.request.Context, Parent: &handle, Append: []llm.Item{preparationMessage(strings.Repeat("x", 64))}}
		ctx := context.Background()
		var stepUsed executionStorageCounts
		timed := func(name string, run func() (llm.ExecutionResultV1, error)) (llm.ExecutionResultV1, error) {
			if name != step {
				return run()
			}
			before := readExecutionStorageCounts(f.table, f.blobs)
			defer func() { stepUsed = stepUsed.plus(readExecutionStorageCounts(f.table, f.blobs).minus(before)) }()
			b.StartTimer()
			defer b.StopTimer()
			return run()
		}
		b.ReportAllocs()
		b.StopTimer()
		b.ResetTimer()
		start := readExecutionStorageCounts(f.table, f.blobs)
		for i := 0; i < b.N; i++ {
			f.now = f.now.Add(time.Minute)
			child.OperationKey = fmt.Sprintf("bench-%d", i)
			v, err := timed("prepare", func() (llm.ExecutionResultV1, error) {
				return f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &child})
			})
			boundedState(b, v, err, llm.ExecutionBudgetRequired)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: child.Context}
			v, err = timed("acquire", func() (llm.ExecutionResultV1, error) { return f.runtime.AcquireBudgetV1(ctx, ref) })
			boundedState(b, v, err, llm.ExecutionAcquired)
			v, err = timed("generate", func() (llm.ExecutionResultV1, error) { return f.runtime.GenerateStepV1(ctx, child) })
			boundedState(b, v, err, llm.ExecutionProviderCompleted)
			v, err = timed("complete", func() (llm.ExecutionResultV1, error) { return f.runtime.CompleteExecutionV1(ctx, ref) })
			boundedState(b, v, err, llm.ExecutionCompleted)
		}
		// Storage traffic of a whole turn (all four steps), not only the
		// timed step: the same in every sub-benchmark.
		used, n := readExecutionStorageCounts(f.table, f.blobs).minus(start), float64(b.N)
		b.ReportMetric(float64(used.gets)/n, "kv-get/turn")
		b.ReportMetric(float64(used.queries)/n, "kv-query/turn")
		b.ReportMetric(float64(used.writes)/n, "kv-write/turn")
		b.ReportMetric(float64(used.opens)/n, "blob-get/turn")
		b.ReportMetric(float64(used.openBytes)/n, "blob-get-B/turn")
		b.ReportMetric(float64(used.creates)/n, "blob-put/turn")
		b.ReportMetric(float64(used.createBytes)/n, "blob-put-B/turn")
		// Storage traffic of the timed step alone.
		b.ReportMetric(float64(stepUsed.gets)/n, "kv-get/op")
		b.ReportMetric(float64(stepUsed.queries)/n, "kv-query/op")
		b.ReportMetric(float64(stepUsed.writes)/n, "kv-write/op")
		b.ReportMetric(float64(stepUsed.opens)/n, "blob-get/op")
		b.ReportMetric(float64(stepUsed.openBytes)/n, "blob-get-B/op")
		b.ReportMetric(float64(stepUsed.creates)/n, "blob-put/op")
		b.ReportMetric(float64(stepUsed.createBytes)/n, "blob-put-B/op")
	})
}
