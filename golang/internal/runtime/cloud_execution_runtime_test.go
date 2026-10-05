package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
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
	table          *executionMemoryTable
	blobs          *executionMemoryBlobs
}

func boundedCloud(t *testing.T, async bool, configure ...func(*budgetPlanningFixture)) *boundedCloudFixture {
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
	f.repository, err = cloudstate.NewRepository(cloudstate.Options{Table: table, Blobs: blobs, Namespace: "runtime-test", Secret: bytes.Repeat([]byte{7}, 32)})
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
func (f *boundedCloudFixture) restart(t *testing.T) {
	t.Helper()
	var err error
	f.runtime, err = f.cap.NewCloudExecutionRuntime(context.Background(), f.options)
	if err != nil {
		t.Fatal(err)
	}
}
func boundedState(t *testing.T, v llm.ExecutionResultV1, err error, want llm.ExecutionStateV1) llm.ExecutionResultV1 {
	t.Helper()
	if err != nil || v.State != want || v.Validate() != nil {
		t.Fatalf("state=%+v wanted=%s error=%v", v, want, err)
	}
	return v
}
func (f *boundedCloudFixture) finish(t *testing.T) llm.ExecutionResultV1 {
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
