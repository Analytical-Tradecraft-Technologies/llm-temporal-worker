//go:build readinessintegration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appactivity "github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	runtimepkg "github.com/mfow/llm-temporal-worker/golang/internal/runtime"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
	redisclient "github.com/redis/go-redis/v9"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
)

// TestReadinessIntegrationRedisRecovery exercises the runtime exactly as an
// operator would: the Make target creates one isolated, pinned Redis daemon,
// this test explicitly provisions its Function before creating the worker,
// then stops and restores Redis while the worker is live. No provider adapter
// is constructed or called by the dependency monitor.
func TestReadinessIntegrationRedisRecovery(t *testing.T) {
	address := os.Getenv("LLMTW_READINESS_REDIS_ADDR")
	container := os.Getenv("LLMTW_READINESS_REDIS_CONTAINER")
	if address == "" || container == "" {
		t.Skip("make readiness-integration supplies an isolated Redis address and container")
	}
	configuration, value := readinessIntegrationConfig(t, address)
	redisClient := redisclient.NewClient(&redisclient.Options{Addr: address})
	t.Cleanup(func() { _ = redisClient.Close() })
	initialContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := redisClient.Ping(initialContext).Err(); err != nil {
		t.Fatalf("ping isolated Redis: %v", err)
	}
	// The test container is dedicated to this target. Provisioning here is
	// explicit and happens before the runtime starts; the runtime itself only
	// verifies the immutable identity and never loads or replaces it.
	if err := redisClient.FunctionLoad(initialContext, redisstore.AdmissionFunctionSource()).Err(); err != nil {
		t.Fatalf("provision isolated admission Function: %v", err)
	}
	redisProbe, err := runtimepkg.NewRedisDependencyProbe(redisClient, value.State.Redis)
	if err != nil {
		t.Fatal(err)
	}
	if result := redisProbe.Probe(initialContext); result.Status != runtimepkg.ProbeStatusReady {
		t.Fatalf("isolated Redis preflight = %#v", result)
	}
	bucket := &readinessBucket{}
	bucketProbe, err := runtimepkg.NewBlobDependencyProbe(bucket)
	if err != nil {
		t.Fatal(err)
	}
	provider := &readinessNoProviderEngine{}
	controllers := &readinessControllerFactory{}
	runtime, err := runtimepkg.New(context.Background(), configuration, runtimepkg.Options{
		Resolver: config.ReferenceResolverFunc(func(context.Context, *config.Config) error { return nil }),
		TemporalFactory: runtimepkg.TemporalClientFactoryFunc(func(_ context.Context, configured config.Config) (client.Client, error) {
			return client.NewLazyClient(client.Options{HostPort: "127.0.0.1:1", Namespace: configured.Temporal.Namespace})
		}),
		EngineFactory: runtimepkg.EngineFactoryFunc(func(context.Context, *config.Snapshot) (llm.Engine, app.ClientSet, error) {
			return provider, app.ClientSetFunc(func(context.Context) error { return nil }), nil
		}),
		V1Runtime:        readinessV1Runtime{},
		WorkerFactory:    controllers.Build,
		DependencyProbes: []runtimepkg.DependencyProbe{redisProbe, bucketProbe},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	if err := runtime.Start(); err != nil {
		t.Fatal(err)
	}
	assertReadinessStatus(t, runtime.HealthServer.Addr(), "/health/live", http.StatusOK)
	assertReadinessStatus(t, runtime.HealthServer.Addr(), "/health/ready", http.StatusOK)
	if starts := controllers.starts.Load(); starts != 1 {
		t.Fatalf("initial worker starts = %d, want 1", starts)
	}

	runReadinessDocker(t, "stop", container)
	waitForReadinessStatus(t, runtime.HealthServer.Addr(), "/health/ready", http.StatusServiceUnavailable)
	assertReadinessStatus(t, runtime.HealthServer.Addr(), "/health/live", http.StatusOK)
	if stops := controllers.stops.Load(); stops == 0 {
		t.Fatal("Redis loss did not stop Temporal polling")
	}

	runReadinessDocker(t, "start", container)
	waitForReadinessStatus(t, runtime.HealthServer.Addr(), "/health/ready", http.StatusOK)
	assertReadinessStatus(t, runtime.HealthServer.Addr(), "/health/live", http.StatusOK)
	if starts := controllers.starts.Load(); starts < 2 {
		t.Fatalf("worker did not resume after Redis recovery; starts=%d", starts)
	}
	if provider.calls.Load() != 0 {
		t.Fatalf("dependency readiness called a provider %d times", provider.calls.Load())
	}
	if bucket.calls.Load() == 0 {
		t.Fatal("bucket-only dependency probe was never called")
	}
}

func readinessIntegrationConfig(t *testing.T, redisAddress string) ([]byte, config.Config) {
	t.Helper()
	data, err := os.ReadFile("../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	value := string(data)
	for _, replacement := range []struct{ old, new string }{
		{"health_address: 0.0.0.0:8080", "health_address: 127.0.0.1:0"},
		{"metrics_address: 0.0.0.0:9090", "metrics_address: 127.0.0.1:0"},
		{"readiness_probe_interval: 5s", "readiness_probe_interval: 50ms"},
		{"readiness_probe_timeout: 2s", "readiness_probe_timeout: 25ms"},
		{"addresses: [redis.example.internal:6379]", fmt.Sprintf("addresses: [%q]", redisAddress)},
		// This recovery fixture does not provision the namespaced coordination
		// stream; stream readiness has dedicated unit coverage below the runtime
		// boundary, so keep this integration test focused on Redis recovery.
		{"coordination_stream_enabled: true", "coordination_stream_enabled: false"},
	} {
		if !strings.Contains(value, replacement.old) {
			t.Fatalf("test configuration is missing %q", replacement.old)
		}
		value = strings.Replace(value, replacement.old, replacement.new, 1)
	}
	configured, err := config.Load([]byte(value))
	if err != nil {
		t.Fatal(err)
	}
	return []byte(value), configured
}

func runReadinessDocker(t *testing.T, arguments ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v (%s)", strings.Join(arguments, " "), err, strings.TrimSpace(string(output)))
	}
}

func waitForReadinessStatus(t *testing.T, address, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		status, err := readinessStatus(address, path)
		if err == nil && status == want {
			return
		}
		if err != nil {
			last = err
		} else {
			last = fmt.Errorf("status %d", status)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s did not reach HTTP %d: %v", path, want, last)
}

func assertReadinessStatus(t *testing.T, address, path string, want int) {
	t.Helper()
	status, err := readinessStatus(address, path)
	if err != nil {
		t.Fatal(err)
	}
	if status != want {
		t.Fatalf("%s status = %d, want %d", path, status, want)
	}
}

func readinessStatus(address, path string) (int, error) {
	response, err := (&http.Client{Timeout: time.Second}).Get("http://" + address + path)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	return response.StatusCode, nil
}

type readinessNoProviderEngine struct{ calls atomic.Int32 }

// readinessV1Runtime is intentionally inert: this integration test exercises
// dependency readiness and worker lifecycle, not provider-backed v1 work. The
// production environment still requires an explicitly composed V1Runtime;
// injecting this fixture keeps that fail-closed guard enabled while making the
// test's startup contract explicit.
type readinessV1Runtime struct{}

func (readinessV1Runtime) GenerateV1(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	return llm.GenerateResponseV1{}, nil
}

func (readinessV1Runtime) CompactV1(context.Context, llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	return llm.CompactResponseV1{}, nil
}

func (readinessV1Runtime) QueryV1(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return llm.QueryResponseV1{}, nil
}

var _ appactivity.V1Runtime = readinessV1Runtime{}

func TestReadinessNoProviderEngineRejectsGeneration(t *testing.T) {
	engine := &readinessNoProviderEngine{}
	if _, err := engine.Generate(context.Background(), llm.Request{}); err == nil {
		t.Fatal("Generate unexpectedly succeeded")
	}
	if calls := engine.calls.Load(); calls != 1 {
		t.Fatalf("forbidden provider calls = %d, want 1", calls)
	}
}

func (engine *readinessNoProviderEngine) Generate(context.Context, llm.Request) (llm.Response, error) {
	engine.calls.Add(1)
	return llm.Response{}, errors.New("provider calls are forbidden in readiness integration")
}

type readinessBucket struct{ calls atomic.Int32 }

func (bucket *readinessBucket) ProbeBucket(ctx context.Context) error {
	bucket.calls.Add(1)
	return ctx.Err()
}

type readinessControllerFactory struct {
	starts atomic.Int32
	stops  atomic.Int32
}

func (factory *readinessControllerFactory) Build(client.Client, string, sdkworker.Options) (app.WorkerController, sdkworker.ActivityRegistry, error) {
	return &readinessController{factory: factory}, readinessRegistry{}, nil
}

type readinessController struct{ factory *readinessControllerFactory }

func (controller *readinessController) Start() error {
	if controller == nil || controller.factory == nil {
		return errors.New("readiness controller is nil")
	}
	controller.factory.starts.Add(1)
	return nil
}

func (controller *readinessController) Stop() {
	if controller != nil && controller.factory != nil {
		controller.factory.stops.Add(1)
	}
}

type readinessRegistry struct{}

func (readinessRegistry) RegisterActivity(any) {}

func (readinessRegistry) RegisterActivityWithOptions(any, activity.RegisterOptions) {}

func (readinessRegistry) RegisterDynamicActivity(any, activity.DynamicRegisterOptions) {}
