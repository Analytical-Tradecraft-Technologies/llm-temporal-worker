package runtime

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func cloudBuilderFixture(t *testing.T, async bool) (*boundedCloudFixture, *config.Snapshot, *productionClientSet, V1RuntimeBuilder) {
	t.Helper()
	f := boundedCloud(t, async)
	snapshot := cloudSnapshot(t, "runtime-test")
	f.cap.ConfigDigest = snapshot.Digest()
	source, err := f.cap.Snapshot.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	source.ConfigDigest = snapshot.Digest()
	f.cap.Snapshot = engine.StaticSnapshot{Value: source}
	f.cap.CloudIdentity, err = cloudRequestIdentity(snapshot.Config().State.Requests)
	if err != nil {
		t.Fatal(err)
	}
	redis := snapshot.Config().State.Redis
	f.cap.RedisIdentity = durable.RedisIdentity{KeyPrefix: redis.KeyPrefix, HashTag: redis.AdmissionHashTag}
	f.cap.CheckpointKeyring = f.options.Keyring
	f.cap.Budgets, err = durable.NewReferenceBudgetMaterializer(redisBudgetGeneration, "incarnation-1", f.cap.Clock)
	if err != nil {
		t.Fatal(err)
	}
	f.cap.Finalizer, err = newCloudFinalizer(f.repository, f.cap.Responses, f.cap.ResponseFills, f.cap.Budgets, f.cap.Clock)
	if err != nil {
		t.Fatal(err)
	}
	clients := &productionClientSet{v1Capabilities: f.cap, checkpointVerifier: f.options.Keyring, queryService: &testQueryService{}, probes: cloudBaseTestProbes()}
	builder, err := NewCloudV1RuntimeBuilder(CloudV1RuntimeOptions{ResolveScope: f.options.ResolveScope, CheckpointTTL: f.options.CheckpointTTL})
	if err != nil {
		t.Fatal(err)
	}
	return f, snapshot, clients, builder
}

func TestCloudV1BuilderFactoryExecutionSurvivesReload(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "async"}[async], func(t *testing.T) {
			f, snapshot, clients, builder := cloudBuilderFixture(t, async)
			factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
				V1RuntimeBuilder: builder, Clock: f.cap.Clock,
				Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
					return []byte(base64.StdEncoding.EncodeToString(make([]byte, 32))), nil
				}),
				CloudRequestFactory: func(context.Context, cloudstate.Config, []byte) (CloudRequestRepository, error) {
					return f.repository, nil
				},
			}}
			var prior activity.V1Runtime
			closed := 0
			build := func() activity.ExecutionRuntime {
				t.Helper()
				// A new client bundle represents the next immutable reload, while
				// the durable stores retain the already-paid request.
				next := *clients
				next.close = func(context.Context) error { closed++; return nil }
				_, set, err := factory.attachV1Runtime(context.Background(), snapshot, nil, &next)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = set.Close(context.Background()) })
				v1 := set.(V1RuntimeSource).V1Runtime()
				if v1 == prior {
					t.Fatal("reused runtime across snapshots")
				}
				prior = v1
				planning, ok := v1.(activity.GenerationPlanningRuntime)
				if !ok {
					t.Fatal("factory hid planning interface")
				}
				if _, err := planning.PlanGenerationV1(context.Background(), f.request); err != nil {
					t.Fatal(err)
				}
				bounded, ok := v1.(activity.ExecutionRuntime)
				if !ok {
					t.Fatal("factory hid bounded execution interface")
				}
				return bounded
			}
			ctx := context.Background()
			runtime := build()
			v, err := runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
			boundedState(t, v, err, llm.ExecutionBudgetRequired)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
			v, err = runtime.AcquireBudgetV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionAcquired)
			runtime = build()
			v, err = runtime.GenerateStepV1(ctx, f.request)
			if async {
				boundedState(t, v, err, llm.ExecutionPending)
				f.now = f.now.Add(2 * time.Second)
				v, err = build().PollExecutionV1(ctx, ref)
			}
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			v, err = build().CompleteExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionCompleted)
			v, err = build().GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionCompleted)
			if f.submits.Load() != 1 || closed != 0 {
				t.Fatal("resubmitted paid request or closed an active snapshot")
			}
			if _, err := prior.GenerateV1(ctx, f.request); err == nil {
				t.Fatal("legacy generate is enabled")
			}
			if _, err := prior.CompactV1(ctx, llm.CompactRequestV1{}); err == nil {
				t.Fatal("legacy compact is enabled")
			}
			if _, err := prior.QueryV1(ctx, llm.QueryRequestV1{}); err != nil {
				t.Fatal(err)
			}
			if clients.queryService.(*testQueryService).called.Load() != 1 {
				t.Fatal("lost snapshot query service")
			}
		})
	}
}

func TestCloudV1BuilderRejectsInvalidOptions(t *testing.T) {
	valid := CloudV1RuntimeOptions{ResolveScope: func(context.Context, llm.RequestContext) (string, error) { return "scope", nil }, CheckpointTTL: time.Hour}
	for name, change := range map[string]func(*CloudV1RuntimeOptions){
		"authorization": func(o *CloudV1RuntimeOptions) { o.ResolveScope = nil },
		"retention":     func(o *CloudV1RuntimeOptions) { o.CheckpointTTL = 0 },
		"depth":         func(o *CloudV1RuntimeOptions) { o.Limits.MaxDepth = -1 },
		"rows":          func(o *CloudV1RuntimeOptions) { o.Limits.MaxRows = -1 },
		"items":         func(o *CloudV1RuntimeOptions) { o.Limits.MaxItems = -1 },
		"bytes":         func(o *CloudV1RuntimeOptions) { o.Limits.MaxBytes = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			options := valid
			change(&options)
			if _, err := NewCloudV1RuntimeBuilder(options); !errors.Is(err, ErrDurableV1Composition) {
				t.Fatal(err)
			}
		})
	}
}

func TestCloudV1BuilderRejectsIncompleteOrMixedSnapshots(t *testing.T) {
	for name, change := range map[string]func(*V1RuntimeCapabilities){
		"unbound config":          func(c *V1RuntimeCapabilities) { c.ConfigDigest = [32]byte{} },
		"different config":        func(c *V1RuntimeCapabilities) { c.ConfigDigest = [32]byte{1} },
		"cloud namespace":         func(c *V1RuntimeCapabilities) { c.CloudIdentity.Namespace = "other" },
		"Redis namespace":         func(c *V1RuntimeCapabilities) { c.RedisIdentity.KeyPrefix = "other" },
		"signing keys":            func(c *V1RuntimeCapabilities) { c.CheckpointKeyring = nil },
		"checkpoint materializer": func(c *V1RuntimeCapabilities) { c.Checkpoints.Materializer = nil },
		"cloud requests":          func(c *V1RuntimeCapabilities) { c.Requests = (*cloudstate.Repository)(nil) },
		"finalizer":               func(c *V1RuntimeCapabilities) { c.Finalizer = nil },
		"cache":                   func(c *V1RuntimeCapabilities) { c.Responses = nil },
		"cache fills":             func(c *V1RuntimeCapabilities) { c.ResponseFills = nil },
		"budgets":                 func(c *V1RuntimeCapabilities) { c.Budgets = nil },
		"clock":                   func(c *V1RuntimeCapabilities) { c.Clock = nil },
		"providers":               func(c *V1RuntimeCapabilities) { c.Adapters = nil },
	} {
		t.Run(name, func(t *testing.T) {
			f, snapshot, clients, builder := cloudBuilderFixture(t, false)
			change(&clients.v1Capabilities)
			if runtime, err := builder(context.Background(), snapshot, nil, clients); !errors.Is(err, ErrDurableV1Composition) || runtime != nil {
				t.Fatalf("runtime=%T err=%v", runtime, err)
			}
			if f.submits.Load() != 0 {
				t.Fatal("provider called during construction")
			}
		})
	}
}

func TestCloudV1BuilderContextAndClientGuards(t *testing.T) {
	_, snapshot, clients, builder := cloudBuilderFixture(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := builder(ctx, snapshot, nil, clients); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, test := range []struct {
		ctx      context.Context
		snapshot *config.Snapshot
		clients  app.ClientSet
	}{
		{nil, snapshot, clients}, {context.Background(), nil, clients}, {context.Background(), snapshot, nil},
		{context.Background(), snapshot, (*productionClientSet)(nil)}, {context.Background(), snapshot, &testQueryClientSet{}},
		{context.Background(), &config.Snapshot{}, clients},
	} {
		if _, err := builder(test.ctx, test.snapshot, nil, test.clients); !errors.Is(err, ErrDurableV1Composition) {
			t.Fatal(err)
		}
	}
	for _, query := range []activity.QueryService{nil, (*testQueryService)(nil)} {
		clients.queryService = query
		runtime, err := builder(context.Background(), snapshot, nil, clients)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = runtime.QueryV1(context.Background(), llm.QueryRequestV1{}); err == nil {
			t.Fatal("query authorization was inferred")
		}
	}
}

func TestCloudV1BuilderAuthorizationPrecedesStorage(t *testing.T) {
	f, snapshot, clients, _ := cloudBuilderFixture(t, false)
	denied := errors.New("private denied reason")
	builder, err := NewCloudV1RuntimeBuilder(CloudV1RuntimeOptions{ResolveScope: func(context.Context, llm.RequestContext) (string, error) { return "", denied }, CheckpointTTL: time.Hour, Limits: state.MaterializeLimits{MaxItems: 100}})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := builder(context.Background(), snapshot, nil, clients)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runtime.(activity.ExecutionRuntime).PrepareExecutionV1(context.Background(), llm.PrepareExecutionV1{Generate: &f.request}); err == nil {
		t.Fatal("unauthorized preparation accepted")
	}
	if f.submits.Load() != 0 {
		t.Fatal("unauthorized provider call")
	}
	for shard := 0; shard < cloudstate.PendingShards; shard++ {
		pending, err := f.repository.ListPending(context.Background(), shard, 100, "")
		if err != nil || len(pending.Requests) != 0 {
			t.Fatalf("unauthorized durable write: %+v %v", pending, err)
		}
	}
}

// Deliberately incomplete custom builders must be rejected before registration.
type executionOnlyCloudRuntime struct {
	activity.UnconfiguredV1Runtime
	activity.ExecutionRuntime
}
type planningOnlyCloudRuntime struct {
	activity.UnconfiguredV1Runtime
	activity.GenerationPlanningRuntime
}

func TestFactoryRejectsPartialBoundedRuntimeAndDrainsClients(t *testing.T) {
	for _, runtime := range []activity.V1Runtime{&executionOnlyCloudRuntime{}, &planningOnlyCloudRuntime{}} {
		closed := 0
		clients := &productionClientSet{close: func(context.Context) error { closed++; return nil }}
		factory := &ProductionEngineFactory{options: ProductionFactoryOptions{V1RuntimeBuilder: func(context.Context, *config.Snapshot, llm.Engine, app.ClientSet) (activity.V1Runtime, error) {
			return runtime, nil
		}}}
		_, set, err := factory.attachV1Runtime(context.Background(), &config.Snapshot{}, nil, clients)
		if !errors.Is(err, ErrProductionFactoryInvalid) || set != nil || closed != 1 {
			t.Fatalf("runtime=%T set=%T closed=%d err=%v", runtime, set, closed, err)
		}
	}
}
