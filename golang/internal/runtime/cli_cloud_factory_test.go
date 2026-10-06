package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/diagnostic"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/blob"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	redisclient "github.com/redis/go-redis/v9"
)

func trustedTemporalSnapshot(t *testing.T, value config.Config) *config.Snapshot {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.Compile(context.Background(), data, nil)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func trustedTemporalTestConfig(t *testing.T) config.Config {
	t.Helper()
	value := cloudSnapshot(t, "runtime-test").Config()
	value.Authorization = &config.AuthorizationConfig{Mode: config.AuthorizationTrustedTemporal, AllowedScopes: []config.AuthorizedScope{{Tenant: "tenant", Project: "project"}, {Tenant: "acme", Project: "invoice-processing"}}}
	return value
}

func TestTrustedTemporalScopeBinding(t *testing.T) {
	value := trustedTemporalTestConfig(t)
	value.Authorization.AllowedScopes = append(value.Authorization.AllowedScopes,
		config.AuthorizedScope{Tenant: "a:b", Project: "c"}, config.AuthorizedScope{Tenant: "a", Project: "b:c"},
		config.AuthorizedScope{Tenant: "tenant", Project: "second"})
	options, err := trustedTemporalCloudOptions(value)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	caller := llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "first"}
	scope, err := options.ResolveScope(ctx, caller)
	if err != nil || scope == "" {
		t.Fatal(scope, err)
	}
	caller.Actor = "second"
	caller.Tags = map[string]string{"purpose": "changed"}
	if got, err := options.ResolveScope(ctx, caller); err != nil || got != scope {
		t.Fatal("actor or tags changed checkpoint scope", err)
	}
	if options.CheckpointTTL != time.Duration(value.State.ContinuationRetention) || options.Limits.MaxDepth != int32(value.Limits.ContinuationDepth) || options.Limits.MaxRows != value.Limits.ContinuationDepth+1 {
		t.Fatal("lost configured retention or depth")
	}
	for _, denied := range []config.AuthorizedScope{{}, {Tenant: "tenant", Project: "missing"}, {Tenant: "other", Project: "project"}, {Tenant: "Tenant", Project: "project"}, {Tenant: " tenant", Project: "project"}} {
		if got, err := options.ResolveScope(ctx, llm.RequestContext{Tenant: denied.Tenant, Project: denied.Project}); err == nil || got != "" {
			t.Fatal("allowed unlisted pair")
		}
	}
	seen := map[string]bool{}
	for _, pair := range value.Authorization.AllowedScopes {
		got, err := options.ResolveScope(ctx, llm.RequestContext{Tenant: pair.Tenant, Project: pair.Project})
		if err != nil || seen[got] {
			t.Fatal("scope collision", err)
		}
		seen[got] = true
	}
	// A resolver owns a copy of its policy, even when its input is mutated.
	value.Authorization.AllowedScopes[0].Tenant = "replacement"
	if got, err := options.ResolveScope(ctx, caller); err != nil || got != scope {
		t.Fatal("resolver retained mutable policy")
	}
	next, err := trustedTemporalCloudOptions(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := next.ResolveScope(ctx, caller); err == nil {
		t.Fatal("new policy retained revoked pair")
	}
	value.Authorization.AllowedScopes[0].Tenant = "tenant"
	value.Authorization.AllowedScopes[0], value.Authorization.AllowedScopes[1] = value.Authorization.AllowedScopes[1], value.Authorization.AllowedScopes[0]
	value.Telemetry.Logs.Level = "debug"
	next, err = trustedTemporalCloudOptions(value)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := next.ResolveScope(ctx, caller); err != nil || got != scope {
		t.Fatal("unrelated reload invalidated scope")
	}
	for _, mutate := range []func(*config.Config){func(v *config.Config) { v.Environment += "-other" }, func(v *config.Config) { v.Temporal.Namespace += "-other" }} {
		copy := value.Clone()
		mutate(&copy)
		next, err := trustedTemporalCloudOptions(copy)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := next.ResolveScope(ctx, caller); err != nil || got == scope {
			t.Fatal("deployment scope collision")
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := options.ResolveScope(canceled, caller); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := options.ResolveScope(nil, caller); err == nil {
		t.Fatal("nil context accepted")
	}
}

func TestTrustedTemporalInvalidPolicy(t *testing.T) {
	for name, change := range map[string]func(*config.Config){
		"missing":        func(v *config.Config) { v.Authorization = nil },
		"mode":           func(v *config.Config) { v.Authorization.Mode = "" },
		"empty":          func(v *config.Config) { v.Authorization.AllowedScopes = nil },
		"state":          func(v *config.Config) { v.State.Kind = config.StateKindMemory },
		"namespace":      func(v *config.Config) { v.Temporal.Namespace = "" },
		"environment":    func(v *config.Config) { v.Environment = "" },
		"depth":          func(v *config.Config) { v.Limits.ContinuationDepth = 0 },
		"depth overflow": func(v *config.Config) { v.Limits.ContinuationDepth = math.MaxInt32 + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			value := trustedTemporalTestConfig(t)
			change(&value)
			if _, err := trustedTemporalCloudOptions(value); !errors.Is(err, ErrDurableV1Composition) {
				t.Fatal(err)
			}
		})
	}
}

func TestCLICloudPolicyPrecedesDependencies(t *testing.T) {
	factory, err := newCLIEngineFactory(ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
			t.Fatal("resolved secret before policy")
			return nil, nil
		}),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) {
			t.Fatal("loaded dependencies before policy")
			return engine.Snapshot{}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	// An empty snapshot also fails the policy gate, without client construction.
	value := trustedTemporalTestConfig(t)
	value.Authorization = nil
	for _, snapshot := range []*config.Snapshot{nil, {}, trustedTemporalSnapshot(t, value)} {
		if _, clients, err := factory.Build(context.Background(), snapshot); !errors.Is(err, ErrDurableV1Composition) || clients != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := factory.Build(nil, cloudSnapshot(t, "runtime-test")); err == nil {
		t.Fatal("nil context accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := factory.Build(ctx, cloudSnapshot(t, "runtime-test")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestProductionRuntimeRejectsMissingPolicyBeforeSecretResolution(t *testing.T) {
	value := trustedTemporalTestConfig(t)
	value.Authorization = nil
	value.State.Redis.Username = config.SecretRef{Kind: "file", Path: t.TempDir() + "/missing-secret"}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if runtime, err := newProductionRuntime(context.Background(), data); !errors.Is(err, ErrDurableV1Composition) || runtime != nil {
		t.Fatalf("CLI did not reject missing policy before reading the missing secret: %v", err)
	}
}

func TestCLIPolicyPreflightOnInitialConfigAndReload(t *testing.T) {
	var resolved atomic.Int32
	options := testRuntimeOptions(t, &testWorker{}, &atomic.Bool{})
	options.LogOutput = io.Discard
	options.Resolver = newCLIReferenceResolver(secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
		resolved.Add(1)
		return []byte("test-secret"), nil
	}))
	value := trustedTemporalTestConfig(t)
	encode := func(value config.Config) []byte {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	invalid := []config.Config{value.Clone(), value.Clone()}
	invalid[0].Authorization = nil
	invalid[1].Authorization.AllowedScopes[0].Project = "*"
	for _, candidate := range invalid {
		if runtime, err := New(context.Background(), encode(candidate), options); err == nil || runtime != nil || resolved.Load() != 0 {
			t.Fatalf("initial policy failure reached secret resolution: %v", err)
		}
	}
	runtime, err := New(context.Background(), encode(value), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	before := resolved.Load()
	if before == 0 {
		t.Fatal("valid configuration skipped secret resolution")
	}
	previous := runtime.App.Current()
	for _, candidate := range invalid {
		if err := runtime.App.Reload(context.Background(), encode(candidate)); err == nil {
			t.Fatal("accepted invalid CLI policy on reload")
		}
		if resolved.Load() != before || runtime.App.Current() != previous {
			t.Fatal("rejected policy resolved secrets or replaced the active snapshot")
		}
	}
	value.Authorization.AllowedScopes[0].Project = "updated-project"
	if err := runtime.App.Reload(context.Background(), encode(value)); err != nil {
		t.Fatal(err)
	}
	if resolved.Load() <= before || runtime.App.Current() == previous {
		t.Fatal("valid policy reload did not resolve and publish a new snapshot")
	}
}

func TestCLIExamplePolicies(t *testing.T) {
	for _, path := range []string{"../../deploy/kubernetes/base/config.yaml", "../../deploy/local/config.yaml"} {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var resolved int
			snapshot, err := config.Compile(context.Background(), data, newCLIReferenceResolver(secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
				resolved++
				return []byte("test-secret"), nil
			})))
			if err != nil || resolved == 0 {
				t.Fatalf("example did not pass CLI policy before resolving references: %v", err)
			}
			value := snapshot.Config()
			if strings.Contains(path, "kubernetes") {
				policy, err := trustedTemporalCloudOptions(value)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := policy.ResolveScope(context.Background(), llm.RequestContext{Tenant: "replace-with-tenant", Project: "replace-with-project"}); err != nil {
					t.Fatal("Kubernetes example has no replaceable allowed scope", err)
				}
			} else if !isCLIReadinessFixture(value) {
				t.Fatal("local example no longer represents the development readiness fixture")
			}
		})
	}
}

func TestCLICloudFactoryBuildsAndReloadsBoundedRuntime(t *testing.T) {
	f := boundedCloud(t, false)
	value := trustedTemporalTestConfig(t)
	preparation, err := f.repository.PrepareBudgetInitialization(context.Background(), testBudgetIdentity(t, value), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := f.repository.CompleteBudgetInitialization(context.Background(), preparation.Receipt); err != nil {
		t.Fatal(err)
	}
	value.Endpoints = map[string]config.EndpointConfig{"endpoint": value.Endpoints["openai-prod"]}
	value.Models = map[string]config.ModelConfig{"alias": {AllowedTenants: []string{"tenant"}, Routes: []config.RouteConfig{{ID: "route", Endpoint: "endpoint", Model: "provider-model", Classes: []llm.ServiceClass{llm.ServiceClassStandard}}}}}
	source, err := f.cap.Snapshot.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	redis := redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = redis.Close() })
	builds, closed := 0, 0
	factory, err := newCLIEngineFactory(ProductionFactoryOptions{
		SnapshotLoader: SnapshotLoaderFunc(func(_ context.Context, snapshot *config.Snapshot) (engine.Snapshot, error) {
			builds++
			next := source
			next.ConfigDigest, next.ConfigEpoch = snapshot.Digest(), snapshot.ConfigVersion()
			return next, nil
		}),
		Resolver: secrets.ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
			if ref.Name == "REQUEST_KEY" {
				return []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{4}, 32))), nil
			}
			return bytes.Repeat([]byte{7}, 32), nil
		}),
		RedisClient: redis, RedisKeySecret: bytes.Repeat([]byte{8}, 32),
		BlobFactory: func(context.Context, config.Config) (blob.Store, io.Closer, error) {
			return &cloudBootstrapBlobStore{newTestBlobStore()}, cloudBootstrapCloser{func() { closed++ }}, nil
		},
		CloudRequestFactory: func(context.Context, cloudstate.Config, []byte) (CloudRequestRepository, error) {
			return f.repository, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	build := func() *cloudV1Runtime {
		t.Helper()
		_, set, err := factory.Build(context.Background(), trustedTemporalSnapshot(t, value))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := set.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		v1 := set.(V1RuntimeSource).V1Runtime()
		if _, ok := v1.(activity.ExecutionRuntime); !ok {
			t.Fatal("CLI did not install execution")
		}
		if _, ok := v1.(activity.GenerationPlanningRuntime); !ok {
			t.Fatal("CLI did not install planning")
		}
		if _, err := v1.QueryV1(context.Background(), llm.QueryRequestV1{}); err == nil {
			t.Fatal("CLI accepted an invalid query")
		}
		// Control queries are composed under the trusted-Temporal allowlist
		// (#817): an allowed scope gets past authorization to storage, while
		// another scope is denied before any read.
		query := func(project string) error {
			caller := f.request.Context
			caller.Project = project
			_, err := v1.QueryV1(context.Background(), llm.QueryRequestV1{APIVersion: llm.QueryAPIVersion, OperationKey: "query-1", Context: caller, Kind: llm.QueryProviderStatus, Query: json.RawMessage(`{"page_size":10}`)})
			return err
		}
		allowed := value.Authorization.AllowedScopes[0]
		if allowed.Tenant == f.request.Context.Tenant {
			if err := query(allowed.Project); err == nil || errors.Is(err, control.ErrQueryAuthorization) {
				t.Fatalf("allowed scope query = %v, want it past authorization", err)
			}
			// budget_status is composed too: with Redis unreachable the
			// Function check is a retryable outage, not "not configured".
			caller := f.request.Context
			caller.Project = allowed.Project
			_, err := v1.QueryV1(context.Background(), llm.QueryRequestV1{APIVersion: llm.QueryAPIVersion, OperationKey: "query-2", Context: caller, Kind: llm.QueryBudgetStatus, Query: json.RawMessage(`{}`)})
			var classified *provider.Error
			if !errors.As(err, &classified) || classified.Code != provider.CodeStateUnavailable {
				t.Fatalf("budget_status with unreachable Redis = %v, want state unavailable", err)
			}
		}
		if err := query("not-allowed"); !errors.Is(err, control.ErrQueryAuthorization) {
			t.Fatalf("disallowed scope query = %v, want authorization denial", err)
		}
		return v1.(*cloudV1Runtime)
	}
	first := build()
	scope, err := first.options.ResolveScope(context.Background(), f.request.Context)
	if err != nil {
		t.Fatal(err)
	}
	value.Authorization.AllowedScopes[0].Project = "other"
	value.State.ContinuationRetention = config.Duration(15 * 24 * time.Hour)
	second := build()
	if second == first || second.options.CheckpointTTL != 15*24*time.Hour {
		t.Fatal("reload reused stale runtime policy")
	}
	if _, err := second.options.ResolveScope(context.Background(), f.request.Context); err == nil {
		t.Fatal("reload retained revoked caller")
	}
	if got, err := first.options.ResolveScope(context.Background(), f.request.Context); err != nil || got != scope {
		t.Fatal("reload mutated active snapshot")
	}
	if closed != 0 || builds != 2 {
		t.Fatal("reload closed active clients or skipped composition")
	}
	value.Authorization = nil
	if _, set, err := factory.Build(context.Background(), trustedTemporalSnapshot(t, value)); !errors.Is(err, ErrDurableV1Composition) || set != nil || builds != 2 {
		t.Fatal("missing policy passed reload preflight", err)
	}
	if f.submits.Load() != 0 {
		t.Fatal("construction dispatched paid work")
	}
}

func TestTrustedTemporalRevocationPrecedesTerminalReplay(t *testing.T) {
	f := boundedCloud(t, false)
	value := trustedTemporalTestConfig(t)
	options, err := trustedTemporalCloudOptions(value)
	if err != nil {
		t.Fatal(err)
	}
	f.options.ResolveScope = options.ResolveScope
	f.restart(t)
	completed := f.finish(t)
	value.Authorization.AllowedScopes[0].Project = "other"
	next, err := trustedTemporalCloudOptions(value)
	if err != nil {
		t.Fatal(err)
	}
	f.options.ResolveScope = next.ResolveScope
	f.restart(t)
	// Every bounded activity must deny before even reading storage, including
	// access to an already completed paid operation.
	store := &preparationTestStore{}
	f.runtime.preparation.store = store
	ctx := context.Background()
	ref := llm.ExecutionReferenceV1{RequestID: completed.RequestID, Context: f.request.Context}
	compact := llm.CompactRequestV1{APIVersion: llm.CompactAPIVersion, OperationKey: "compact", Parent: completed.Generate.Checkpoint.Handle, Context: f.request.Context}
	for name, call := range map[string]func() error{
		"plan": func() error { _, err := f.runtime.PlanGenerationV1(ctx, f.request); return err },
		"prepare": func() error {
			_, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
			return err
		},
		"generate": func() error { _, err := f.runtime.GenerateStepV1(ctx, f.request); return err },
		"compact":  func() error { _, err := f.runtime.CompactStepV1(ctx, compact); return err },
		"acquire":  func() error { _, err := f.runtime.AcquireBudgetV1(ctx, ref); return err },
		"poll":     func() error { _, err := f.runtime.PollExecutionV1(ctx, ref); return err },
		"complete": func() error { _, err := f.runtime.CompleteExecutionV1(ctx, ref); return err },
	} {
		t.Run(name, func(t *testing.T) {
			var classified *provider.Error
			if err := call(); !errors.As(err, &classified) || classified.Code != provider.CodePermissionDenied {
				t.Fatalf("want permission denial, got %v", err)
			}
		})
	}
	if f.submits.Load() != 1 {
		t.Fatal("revoked caller dispatched work")
	}
	if store.accesses != 0 {
		t.Fatal("revoked caller accessed storage")
	}
}

func TestProductionRuntimeReportsUnresolvedSecretAsOperatorDiagnostic(t *testing.T) {
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("REDIS_USERNAME", "")
	_, err = newProductionRuntime(context.Background(), data)
	message, ok := diagnostic.Message(err)
	if !ok || !strings.Contains(message, `environment secret "REDIS_USERNAME" is not set`) {
		t.Fatalf("production runtime error = %v, diagnostic = %q", err, message)
	}
}

func TestKubernetesBaseCatalogsLoadThroughSnapshotLoader(t *testing.T) {
	base, err := filepath.Abs("../../deploy/kubernetes/base")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(base, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.ReplaceAll(string(data), "/etc/llmtw/", base+"/"))
	snapshot, err := config.Compile(context.Background(), data, newCLIReferenceResolver(secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
		return []byte("test-secret"), nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := (CatalogSnapshotLoader{}).Load(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("Kubernetes base catalogs do not load: %v", err)
	}
	model, ok := loaded.Routes.Models["default"]
	if !ok || len(model.Routes) != 1 || model.Routes[0].Model != "replace-with-model" || model.Routes[0].Provider != "openai" || !model.Routes[0].PriceAvailable {
		t.Fatalf("Kubernetes base routes = %#v", loaded.Routes)
	}
}
