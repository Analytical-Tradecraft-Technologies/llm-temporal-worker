//go:build redisbenchmark

package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
	"github.com/google/uuid"
	redisclient "github.com/redis/go-redis/v9"
)

const (
	redisBenchmarkGate          = "LLMTW_REDIS_BENCHMARK"
	redisBenchmarkMutationGate  = "LLMTW_REDIS_BENCHMARK_ALLOW_MUTATION"
	redisBenchmarkAddress       = "LLMTW_REDIS_BENCHMARK_ADDR"
	redisBenchmarkPrefix        = "llmtw-bench-"
	redisBenchmarkFunctionLimit = 128
)

type redisBenchmarkConfig struct {
	address string
}

func redisBenchmarkConfigFromEnv(getenv func(string) string) (redisBenchmarkConfig, error) {
	if getenv(redisBenchmarkGate) != "1" {
		return redisBenchmarkConfig{}, fmt.Errorf("%s=1 is required", redisBenchmarkGate)
	}
	if getenv(redisBenchmarkMutationGate) != "1" {
		return redisBenchmarkConfig{}, fmt.Errorf("%s=1 is required", redisBenchmarkMutationGate)
	}
	if getenv("CI") != "" {
		return redisBenchmarkConfig{}, fmt.Errorf("Redis benchmark refuses CI")
	}
	if address := getenv(redisBenchmarkAddress); address != "" {
		return redisBenchmarkConfig{address: address}, nil
	}
	return redisBenchmarkConfig{}, fmt.Errorf("%s is required", redisBenchmarkAddress)
}

func redisBenchmarkAdmissionFunctionAvailable(libraries []redisclient.Library) error {
	for _, library := range libraries {
		if library.Name != redisstore.AdmissionFunctionLibrary || library.Code != redisstore.AdmissionFunctionSource() {
			continue
		}
		for _, function := range library.Functions {
			if function.Name == redisstore.AdmissionFunctionVersion {
				return nil
			}
		}
	}
	return fmt.Errorf("exact preloaded Redis Function %s/%s is required", redisstore.AdmissionFunctionLibrary, redisstore.AdmissionFunctionVersion)
}

func newRedisBenchmarkKeyOptions() (redisstore.KeyOptions, error) {
	id := make([]byte, 16)
	secret := make([]byte, 32)
	if _, err := rand.Read(id); err != nil {
		return redisstore.KeyOptions{}, fmt.Errorf("generate benchmark key prefix: %w", err)
	}
	if _, err := rand.Read(secret); err != nil {
		return redisstore.KeyOptions{}, fmt.Errorf("generate benchmark key secret: %w", err)
	}
	return redisstore.KeyOptions{Prefix: redisBenchmarkPrefix + hex.EncodeToString(id), HashTag: "admission", KeySecret: secret}, nil
}

// BenchmarkV1ActivitiesAdmissionRedis measures the same production v1
// Activities turn as BenchmarkV1ActivitiesAdmissionMemory, with budgets held
// by the production Redis materializer against an explicitly addressed,
// operator-provisioned Redis 7+ deployment. It initializes a fresh budget
// namespace beneath a random key prefix, as a durable production worker
// requires, and removes that prefix afterwards. Requests and payloads stay in
// memory. It is build-tagged and refuses CI; it neither starts Redis nor
// loads Functions.
func BenchmarkV1ActivitiesAdmissionRedis(b *testing.B) {
	config, err := redisBenchmarkConfigFromEnv(os.Getenv)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	client := newRedisBenchmarkClient(config.address)
	defer func() { _ = client.Close() }()
	libraries, err := client.FunctionList(ctx, redisclient.FunctionListQuery{LibraryNamePattern: redisstore.AdmissionFunctionLibrary, WithCode: true}).Result()
	if err != nil {
		b.Fatal(err)
	}
	if err := redisBenchmarkAdmissionFunctionAvailable(libraries); err != nil {
		b.Fatal(err)
	}
	keys, err := newRedisBenchmarkKeyOptions()
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := cleanupRedisBenchmarkPrefix(ctx, client, keys.Prefix); err != nil {
			b.Error(err)
		}
	}()
	f := boundedCloud(b, false)
	f.now = time.Now().UTC()
	initializer, err := redisstore.NewRedisBudgetInitializer(client, keys, redisstore.AdmissionModeFunction, redisstore.AdmissionFunctionVersion)
	if err != nil {
		b.Fatal(err)
	}
	receipt := budget.Initialization{Schema: budget.InitializationSchema, Identity: initializer.Identity(), Epoch: uuid.NewString(), CreatedAt: f.now}
	if err := initializer.Initialize(ctx, budget.InitializationPreparation{Receipt: receipt, Created: true}); err != nil {
		b.Fatal(err)
	}
	receipt.Ready = true
	budgets, err := redisstore.NewRedisBudgetMaterializer(redisstore.RedisBudgetMaterializerOptions{
		Client: client, Keys: keys, Mode: redisstore.AdmissionModeFunction, FunctionVersion: redisstore.AdmissionFunctionVersion,
		GenerationID: redisBudgetGeneration, IncarnationID: durable.IncarnationID(redisBudgetGeneration), Clock: f.cap.Clock,
		Initialization: &receipt,
	})
	if err != nil {
		b.Fatal(err)
	}
	f.cap.Budgets = budgets
	if f.cap.Finalizer, err = newCloudFinalizer(f.repository, f.cap.Responses, f.cap.ResponseFills, budgets, f.cap.Clock); err != nil {
		b.Fatal(err)
	}
	f.options.BudgetGeneration = redisBudgetGeneration
	f.restart(b)
	benchmarkV1ActivitiesAdmission(b, f)
}

func newRedisBenchmarkClient(address string) *redisclient.Client {
	// go-redis uses -1 to disable retries. An FCALL transport failure can leave
	// the commit ambiguous, so keep the benchmark on the production mutation path.
	return redisclient.NewClient(&redisclient.Options{Addr: address, MaxRetries: -1})
}

func cleanupRedisBenchmarkPrefix(ctx context.Context, client *redisclient.Client, prefix string) error {
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+":*", redisBenchmarkFunctionLimit).Result()
		if err != nil {
			return fmt.Errorf("scan benchmark Redis keys: %w", err)
		}
		if len(keys) > 0 {
			if err := client.Unlink(ctx, keys...).Err(); err != nil {
				return fmt.Errorf("unlink benchmark Redis keys: %w", err)
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

func TestRedisBenchmarkConfigRequiresExplicitOperatorGates(t *testing.T) {
	base := map[string]string{
		"LLMTW_REDIS_BENCHMARK":                "1",
		"LLMTW_REDIS_BENCHMARK_ALLOW_MUTATION": "1",
		"LLMTW_REDIS_BENCHMARK_ADDR":           "127.0.0.1:6379",
	}

	for _, test := range []struct {
		name   string
		env    map[string]string
		wantOK bool
	}{
		{name: "all explicit gates", env: base, wantOK: true},
		{name: "benchmark gate missing", env: withoutBenchmarkEnv(base, "LLMTW_REDIS_BENCHMARK"), wantOK: false},
		{name: "mutation gate missing", env: withoutBenchmarkEnv(base, "LLMTW_REDIS_BENCHMARK_ALLOW_MUTATION"), wantOK: false},
		{name: "address missing", env: withoutBenchmarkEnv(base, "LLMTW_REDIS_BENCHMARK_ADDR"), wantOK: false},
		{name: "CI is refused", env: withBenchmarkEnv(base, "CI", "true"), wantOK: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := redisBenchmarkConfigFromEnv(func(name string) string { return test.env[name] })
			if (err == nil) != test.wantOK {
				t.Fatalf("redis benchmark config error = %v, want success %t", err, test.wantOK)
			}
		})
	}
}

func TestRedisBenchmarkRequiresExactPreloadedAdmissionFunction(t *testing.T) {
	exact := redisclient.Library{
		Name: redisstore.AdmissionFunctionLibrary,
		Code: redisstore.AdmissionFunctionSource(),
		Functions: []redisclient.Function{{
			Name: redisstore.AdmissionFunctionVersion,
		}},
	}

	for _, test := range []struct {
		name      string
		libraries []redisclient.Library
		wantOK    bool
	}{
		{name: "exact immutable function", libraries: []redisclient.Library{exact}, wantOK: true},
		{name: "library missing", libraries: nil, wantOK: false},
		{name: "function missing", libraries: []redisclient.Library{{Name: exact.Name, Code: exact.Code}}, wantOK: false},
		{name: "source differs", libraries: []redisclient.Library{{Name: exact.Name, Code: exact.Code + "-- changed", Functions: exact.Functions}}, wantOK: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := redisBenchmarkAdmissionFunctionAvailable(test.libraries)
			if (err == nil) != test.wantOK {
				t.Fatalf("admission Function check error = %v, want success %t", err, test.wantOK)
			}
		})
	}
}

func TestRedisBenchmarkKeyOptionsAreUniqueAndBounded(t *testing.T) {
	first, err := newRedisBenchmarkKeyOptions()
	if err != nil {
		t.Fatalf("first key options: %v", err)
	}
	second, err := newRedisBenchmarkKeyOptions()
	if err != nil {
		t.Fatalf("second key options: %v", err)
	}

	if first.Prefix == second.Prefix {
		t.Fatal("benchmark key prefixes must be unique")
	}
	if len(first.Prefix) > 64 {
		t.Fatalf("benchmark prefix length = %d, want at most 64", len(first.Prefix))
	}
	if got, want := len(first.Prefix), len(redisBenchmarkPrefix)+32; got != want {
		t.Fatalf("benchmark prefix length = %d, want %d for a 128-bit nonce", got, want)
	}
	if len(first.KeySecret) != 32 {
		t.Fatalf("key secret length = %d, want 32", len(first.KeySecret))
	}
	if first.HashTag != "admission" {
		t.Fatalf("hash tag = %q, want admission", first.HashTag)
	}
}

func TestRedisBenchmarkClientDisablesRetries(t *testing.T) {
	client := newRedisBenchmarkClient("127.0.0.1:6379")
	t.Cleanup(func() { _ = client.Close() })
	if got := client.Options().MaxRetries; got != 0 {
		t.Fatalf("MaxRetries = %d, want 0 to match production mutation semantics", got)
	}
}

func withoutBenchmarkEnv(source map[string]string, key string) map[string]string {
	copy := withBenchmarkEnv(source, "", "")
	delete(copy, key)
	return copy
}

func withBenchmarkEnv(source map[string]string, key, value string) map[string]string {
	copy := make(map[string]string, len(source)+1)
	for name, value := range source {
		copy[name] = value
	}
	if key != "" {
		copy[key] = value
	}
	return copy
}
