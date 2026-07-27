package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
	redisclient "github.com/redis/go-redis/v9"
)

func TestRedisDependencyProbeChecksEveryConfiguredPolicyWithoutMutation(t *testing.T) {
	client := healthyRedisProbeClient()
	probe, err := NewRedisDependencyProbe(client, testRedisProbeConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := probe.Probe(context.Background())
	if result.Dependency != DependencyRedis || result.Status != ProbeStatusReady || result.Reason != ProbeReasonReady {
		t.Fatalf("healthy Redis result = %#v", result)
	}
	for _, setting := range []string{"maxmemory-policy", "appendonly", "save"} {
		if client.configGetCalls[setting] != 1 {
			t.Fatalf("CONFIG GET %q calls = %d, want 1", setting, client.configGetCalls[setting])
		}
	}
	if client.pingCalls != 1 || client.timeCalls != 1 || client.functionListCalls != 1 {
		t.Fatalf("Redis probe call counts ping=%d time=%d function-list=%d", client.pingCalls, client.timeCalls, client.functionListCalls)
	}
	if client.mutations != 0 {
		t.Fatalf("readiness probe mutated Redis %d times", client.mutations)
	}
}

func TestRedisDependencyProbeVerifiesActiveBudgetGenerationWhenConfigured(t *testing.T) {
	client := healthyRedisProbeClient()
	generation := &fakeBudgetGenerationProbe{}
	probe, err := NewRedisDependencyProbeWithBudgetGeneration(client, testRedisProbeConfig(), generation)
	if err != nil {
		t.Fatal(err)
	}
	if result := probe.Probe(context.Background()); result.Status != ProbeStatusReady || result.Reason != ProbeReasonReady {
		t.Fatalf("durable Redis result = %#v", result)
	}
	if generation.activeCalls != 1 || generation.manifestCalls != 1 {
		t.Fatalf("budget generation calls active=%d manifest=%d, want one each", generation.activeCalls, generation.manifestCalls)
	}
}

func TestRedisDependencyProbeValidatesEnabledCoordinationStream(t *testing.T) {
	value := testRedisProbeConfig()
	enabled := true
	value.CoordinationStreamEnabled = &enabled
	value.StreamTrimSafety = config.Duration(time.Minute)
	client := healthyRedisProbeClient()
	client.streamType = "stream"
	client.streamInfo = &redisclient.XInfoStream{
		Length:            1,
		EntriesAdded:      1,
		LastGeneratedID:   "1-0",
		MaxDeletedEntryID: "0-0",
		FirstEntry:        redisclient.XMessage{ID: "1-0"},
		LastEntry:         redisclient.XMessage{ID: "1-0"},
	}
	probe, err := NewRedisDependencyProbeWithStream(client, value, "llm:events")
	if err != nil {
		t.Fatal(err)
	}
	if result := probe.Probe(context.Background()); result.Status != ProbeStatusReady || result.Reason != ProbeReasonReady {
		t.Fatalf("coordination stream result = %#v", result)
	}
	if client.typeCalls != 1 || client.streamInfoCalls != 1 {
		t.Fatalf("coordination stream calls type=%d xinfo=%d, want one each", client.typeCalls, client.streamInfoCalls)
	}
}

func TestRedisDependencyProbeRejectsInvalidCoordinationStream(t *testing.T) {
	for _, test := range []struct {
		name   string
		client func(*fakeRedisProbeClient)
	}{
		{name: "wrong type", client: func(client *fakeRedisProbeClient) { client.streamType = "hash" }},
		{name: "missing stream", client: func(client *fakeRedisProbeClient) { client.streamInfoErr = redisclient.Nil }},
		{name: "consumer groups", client: func(client *fakeRedisProbeClient) {
			client.streamInfo.Groups = 1
		}},
		{name: "malformed id", client: func(client *fakeRedisProbeClient) {
			client.streamInfo.LastEntry.ID = "invalid"
		}},
		{name: "recent trim", client: func(client *fakeRedisProbeClient) {
			client.streamInfo.MaxDeletedEntryID = "9999999999999-0"
			client.streamInfo.LastGeneratedID = "9999999999999-0"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := testRedisProbeConfig()
			enabled := true
			value.CoordinationStreamEnabled = &enabled
			value.StreamTrimSafety = config.Duration(time.Minute)
			client := healthyRedisProbeClient()
			client.streamType = "stream"
			client.streamInfo = &redisclient.XInfoStream{
				Length:            1,
				EntriesAdded:      1,
				LastGeneratedID:   "1-0",
				MaxDeletedEntryID: "0-0",
				FirstEntry:        redisclient.XMessage{ID: "1-0"},
				LastEntry:         redisclient.XMessage{ID: "1-0"},
			}
			test.client(client)
			probe, err := NewRedisDependencyProbeWithStream(client, value, "llm:events")
			if err != nil {
				t.Fatal(err)
			}
			result := probe.Probe(context.Background())
			if result.Status != ProbeStatusPolicy || result.Reason != ProbeReasonPolicyMismatch {
				t.Fatalf("invalid coordination stream result = %#v", result)
			}
		})
	}
}

func TestRedisDependencyProbeRequiresCoordinationStreamKey(t *testing.T) {
	value := testRedisProbeConfig()
	enabled := true
	value.CoordinationStreamEnabled = &enabled
	value.StreamTrimSafety = config.Duration(time.Minute)
	if _, err := NewRedisDependencyProbeWithStream(healthyRedisProbeClient(), value, ""); err == nil {
		t.Fatal("enabled coordination stream unexpectedly accepted an empty key")
	}
}

func TestRedisDependencyProbeFailsClosedForInvalidActiveBudgetGeneration(t *testing.T) {
	for _, test := range []struct {
		name string
		set  func(*fakeBudgetGenerationProbe)
	}{
		{name: "missing pointer", set: func(probe *fakeBudgetGenerationProbe) {
			probe.activeErr = redisstore.ErrBudgetManifestInvalid
		}},
		{name: "invalid manifest", set: func(probe *fakeBudgetGenerationProbe) {
			probe.manifestErr = redisstore.ErrBudgetManifestInvalid
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := healthyRedisProbeClient()
			generation := &fakeBudgetGenerationProbe{}
			test.set(generation)
			probe, err := NewRedisDependencyProbeWithBudgetGeneration(client, testRedisProbeConfig(), generation)
			if err != nil {
				t.Fatal(err)
			}
			result := probe.Probe(context.Background())
			if result.Dependency != DependencyRedis || result.Status != ProbeStatusPolicy || result.Reason != ProbeReasonPolicyMismatch {
				t.Fatalf("invalid generation result = %#v", result)
			}
		})
	}
}

func TestRedisDependencyProbeRequiresBudgetGenerationPort(t *testing.T) {
	if _, err := NewRedisDependencyProbeWithBudgetGeneration(healthyRedisProbeClient(), testRedisProbeConfig(), nil); err == nil {
		t.Fatal("nil budget generation port unexpectedly accepted")
	}
}

func TestPostgresDependencyProbeVerifiesHealthAndSchemaWithoutMutation(t *testing.T) {
	namespace, err := postgresstore.NewNamespace("worker_db", "worker_state", "tenant_")
	if err != nil {
		t.Fatal(err)
	}
	client := &fakePostgresProbeClient{}
	probe, err := NewPostgresDependencyProbe(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	result := probe.Probe(context.Background())
	if result != (ProbeResult{Dependency: DependencyPostgres, Status: ProbeStatusReady, Reason: ProbeReasonReady}) {
		t.Fatalf("PostgreSQL probe result = %#v", result)
	}
	if client.healthCalls != 1 || client.verifyCalls != 1 {
		t.Fatalf("PostgreSQL probe calls health=%d verify=%d, want one each", client.healthCalls, client.verifyCalls)
	}
}

func TestPostgresDependencyProbeFailsClosedOnSchemaMismatch(t *testing.T) {
	namespace, err := postgresstore.NewNamespace("worker_db", "worker_state", "")
	if err != nil {
		t.Fatal(err)
	}
	client := &fakePostgresProbeClient{verifyErr: errors.New("contract mismatch")}
	probe, err := NewPostgresDependencyProbe(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	result := probe.Probe(context.Background())
	if result.Dependency != DependencyPostgres || result.Status != ProbeStatusUnavailable || result.Reason != ProbeReasonUnavailable {
		t.Fatalf("schema mismatch result = %#v", result)
	}
}

func TestNormalizeProbeResultAcceptsPostgres(t *testing.T) {
	result := normalizeProbeResult(ProbeResult{Dependency: DependencyPostgres, Status: ProbeStatusReady, Reason: ProbeReasonReady})
	if result.Dependency != DependencyPostgres || result.Status != ProbeStatusReady {
		t.Fatalf("normalized PostgreSQL result = %#v", result)
	}
}

func TestRedisDependencyProbeRejectsEveryConfiguredPolicyMismatch(t *testing.T) {
	for _, test := range []struct {
		name   string
		config func(*config.RedisConfig)
		client func(*fakeRedisProbeClient)
	}{
		{
			name:   "eviction policy",
			client: func(client *fakeRedisProbeClient) { client.settings["maxmemory-policy"] = "allkeys-lru" },
		},
		{
			name:   "aof required",
			config: func(value *config.RedisConfig) { value.RequiredPersistence = "aof" },
			client: func(client *fakeRedisProbeClient) { client.settings["appendonly"] = "no" },
		},
		{
			name:   "rdb required",
			config: func(value *config.RedisConfig) { value.RequiredPersistence = "rdb" },
			client: func(client *fakeRedisProbeClient) { client.settings["save"] = "" },
		},
		{
			name: "function version",
			client: func(client *fakeRedisProbeClient) {
				client.libraries[0].Functions = []redisclient.Function{{Name: "admission/v0"}}
			},
		},
		{
			name:   "function digest",
			client: func(client *fakeRedisProbeClient) { client.libraries[0].Code = "different" },
		},
		{
			name: "lua digest",
			config: func(value *config.RedisConfig) {
				value.AdmissionMode = "lua"
				value.AdmissionDigest = redisstore.AdmissionLuaDigest()
			},
			client: func(client *fakeRedisProbeClient) { client.scriptExists = []bool{false} },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := testRedisProbeConfig()
			if test.config != nil {
				test.config(&value)
			}
			client := healthyRedisProbeClient()
			if test.client != nil {
				test.client(client)
			}
			probe, err := NewRedisDependencyProbe(client, value)
			if err != nil {
				t.Fatal(err)
			}
			result := probe.Probe(context.Background())
			if result.Status != ProbeStatusPolicy || result.Reason != ProbeReasonPolicyMismatch {
				t.Fatalf("policy result = %#v", result)
			}
			if client.mutations != 0 {
				t.Fatalf("policy rejection mutated Redis %d times", client.mutations)
			}
		})
	}
}

func TestRedisDependencyProbeAcceptsEachConfiguredPersistencePolicy(t *testing.T) {
	for _, test := range []struct {
		name        string
		persistence string
		appendOnly  string
		save        string
	}{
		{name: "AOF only", persistence: "aof", appendOnly: "yes", save: ""},
		{name: "RDB only", persistence: "rdb", appendOnly: "no", save: "60 1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := testRedisProbeConfig()
			value.RequiredPersistence = test.persistence
			client := healthyRedisProbeClient()
			client.settings["appendonly"] = test.appendOnly
			client.settings["save"] = test.save
			probe, err := NewRedisDependencyProbe(client, value)
			if err != nil {
				t.Fatal(err)
			}
			if result := probe.Probe(context.Background()); result.Status != ProbeStatusReady || result.Reason != ProbeReasonReady {
				t.Fatalf("%s probe result = %#v", test.persistence, result)
			}
		})
	}
}

func TestRedisDependencyProbeLuaFallbackUsesPreloadedScript(t *testing.T) {
	value := testRedisProbeConfig()
	value.AdmissionMode = "lua"
	value.AdmissionDigest = redisstore.AdmissionLuaDigest()
	client := healthyRedisProbeClient()
	client.scriptExists = []bool{true}
	probe, err := NewRedisDependencyProbe(client, value)
	if err != nil {
		t.Fatal(err)
	}
	result := probe.Probe(context.Background())
	if result.Status != ProbeStatusReady || client.scriptExistsCalls != 1 || client.functionListCalls != 0 {
		t.Fatalf("Lua fallback result/calls = %#v script-exists=%d function-list=%d", result, client.scriptExistsCalls, client.functionListCalls)
	}
}

func TestDependencyProbeReportsOnlySafeFailureReasons(t *testing.T) {
	probe := DependencyProbeFunc(func(context.Context) ProbeResult {
		return ProbeResult{Dependency: DependencyRedis, Status: ProbeStatusUnavailable, Reason: ProbeReasonUnavailable}
	})
	err := CheckDependencyProbes(context.Background(), []DependencyProbe{probe}, time.Second)
	if err == nil {
		t.Fatal("unavailable dependency unexpectedly passed")
	}
	if err.Error() != "required runtime dependency is unavailable" {
		t.Fatalf("unsafe dependency error %q", err)
	}
}

func TestDependencyProbesAcceptNilContext(t *testing.T) {
	redisProbe, err := NewRedisDependencyProbe(healthyRedisProbeClient(), testRedisProbeConfig())
	if err != nil {
		t.Fatal(err)
	}
	if result := redisProbe.Probe(nil); result.Status != ProbeStatusReady {
		t.Fatalf("Redis probe with nil context = %#v", result)
	}
	blobProbe, err := NewBlobDependencyProbe(&fakeBucketProbe{})
	if err != nil {
		t.Fatal(err)
	}
	if result := blobProbe.Probe(nil); result.Status != ProbeStatusReady {
		t.Fatalf("blob probe with nil context = %#v", result)
	}
}

func TestDependencyProbesBoundEachProbeWithContextDeadline(t *testing.T) {
	probe := DependencyProbeFunc(func(ctx context.Context) ProbeResult {
		<-ctx.Done()
		return ProbeResult{Dependency: DependencyRedis, Status: ProbeStatusTimeout, Reason: ProbeReasonTimeout}
	})
	started := time.Now()
	err := CheckDependencyProbes(context.Background(), []DependencyProbe{probe}, 10*time.Millisecond)
	if !errors.Is(err, errRequiredDependencyUnavailable) {
		t.Fatalf("bounded probe error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("probe timeout took %s", elapsed)
	}
}

func TestDependencyProbesRejectCanceledParentBeforeInvokingProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := CheckDependencyProbes(ctx, []DependencyProbe{DependencyProbeFunc(func(context.Context) ProbeResult {
		calls++
		return ProbeResult{Dependency: DependencyRedis, Status: ProbeStatusReady, Reason: ProbeReasonReady}
	})}, time.Second)
	if !errors.Is(err, errRequiredDependencyUnavailable) {
		t.Fatalf("canceled probe gate error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("canceled probe gate invoked %d probes", calls)
	}
}

func TestDependencyProbesRejectCancellationAfterProbeReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := CheckDependencyProbes(ctx, []DependencyProbe{DependencyProbeFunc(func(context.Context) ProbeResult {
		calls++
		// Model an injected implementation that completes successfully without
		// observing its context. The gate must still fail closed.
		cancel()
		return ProbeResult{Dependency: DependencyRedis, Status: ProbeStatusReady, Reason: ProbeReasonReady}
	})}, time.Second)
	if !errors.Is(err, errRequiredDependencyUnavailable) {
		t.Fatalf("canceled-after-probe gate error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("canceled-after-probe invoked %d probes, want 1", calls)
	}
}

func TestBlobDependencyProbeUsesOnlyBucketCapabilityAndMasksErrors(t *testing.T) {
	bucket := &fakeBucketProbe{err: errors.New("bucket secret marker")}
	probe, err := NewBlobDependencyProbe(bucket)
	if err != nil {
		t.Fatal(err)
	}
	result := probe.Probe(context.Background())
	if bucket.calls != 1 || result.Dependency != DependencyBlobStore || result.Status != ProbeStatusUnavailable || result.Reason != ProbeReasonUnavailable {
		t.Fatalf("bucket probe result = %#v calls=%d", result, bucket.calls)
	}
}

type fakeBudgetGenerationProbe struct {
	activeErr     error
	manifestErr   error
	activeCalls   int
	manifestCalls int
}

func (probe *fakeBudgetGenerationProbe) ActiveGeneration(context.Context) (redisstore.ActiveBudgetGeneration, error) {
	probe.activeCalls++
	if probe.activeErr != nil {
		return redisstore.ActiveBudgetGeneration{}, probe.activeErr
	}
	return redisstore.ActiveBudgetGeneration{GenerationID: "generation-1", IncarnationID: "incarnation-1", ManifestDigest: strings.Repeat("a", 64)}, nil
}

func (probe *fakeBudgetGenerationProbe) LoadManifest(context.Context, redisstore.ActiveBudgetGeneration) (redisstore.BudgetManifest, error) {
	probe.manifestCalls++
	if probe.manifestErr != nil {
		return redisstore.BudgetManifest{}, probe.manifestErr
	}
	return redisstore.BudgetManifest{}, nil
}

func (probe *fakeBudgetGenerationProbe) PublishGeneration(context.Context, redisstore.BudgetManifest) (redisstore.ActiveBudgetGeneration, error) {
	return redisstore.ActiveBudgetGeneration{}, errors.New("readiness must not publish budget state")
}

type fakeBucketProbe struct {
	calls int
	err   error
}

type fakePostgresProbeClient struct {
	healthErr   error
	verifyErr   error
	healthCalls int
	verifyCalls int
}

func (client *fakePostgresProbeClient) Health(context.Context, postgresstore.Namespace) error {
	client.healthCalls++
	return client.healthErr
}

func (client *fakePostgresProbeClient) Verify(context.Context, postgresstore.Namespace) error {
	client.verifyCalls++
	return client.verifyErr
}

func (probe *fakeBucketProbe) ProbeBucket(context.Context) error {
	probe.calls++
	return probe.err
}

type fakeRedisProbeClient struct {
	pingErr           error
	timeErr           error
	typeErr           error
	streamInfoErr     error
	functionListErr   error
	scriptExistsErr   error
	settings          map[string]string
	streamType        string
	streamInfo        *redisclient.XInfoStream
	libraries         []redisclient.Library
	scriptExists      []bool
	pingCalls         int
	timeCalls         int
	typeCalls         int
	streamInfoCalls   int
	functionListCalls int
	scriptExistsCalls int
	configGetCalls    map[string]int
	mutations         int
}

func healthyRedisProbeClient() *fakeRedisProbeClient {
	return &fakeRedisProbeClient{
		settings: map[string]string{
			"maxmemory-policy": "noeviction",
			"appendonly":       "yes",
			"save":             "60 1",
		},
		libraries: []redisclient.Library{{
			Name:      redisstore.AdmissionFunctionLibrary,
			Engine:    "LUA",
			Code:      redisstore.AdmissionFunctionSource(),
			Functions: []redisclient.Function{{Name: redisstore.AdmissionFunctionVersion}},
		}},
		scriptExists:   []bool{true},
		configGetCalls: make(map[string]int),
	}
}

func testRedisProbeConfig() config.RedisConfig {
	return config.RedisConfig{
		AdmissionMode:       "function",
		FunctionLibrary:     redisstore.AdmissionFunctionLibrary,
		AdmissionVersion:    redisstore.AdmissionFunctionVersion,
		AdmissionDigest:     redisstore.AdmissionFunctionDigest(),
		RequiredPersistence: "aof_and_rdb",
	}
}

func (client *fakeRedisProbeClient) Ping(ctx context.Context) *redisclient.StatusCmd {
	client.pingCalls++
	command := redisclient.NewStatusCmd(ctx)
	command.SetVal("PONG")
	command.SetErr(client.pingErr)
	return command
}

func (client *fakeRedisProbeClient) Time(ctx context.Context) *redisclient.TimeCmd {
	client.timeCalls++
	command := redisclient.NewTimeCmd(ctx)
	command.SetVal(time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC))
	command.SetErr(client.timeErr)
	return command
}

func (client *fakeRedisProbeClient) Type(ctx context.Context, _ string) *redisclient.StatusCmd {
	client.typeCalls++
	command := redisclient.NewStatusCmd(ctx)
	command.SetVal(client.streamType)
	command.SetErr(client.typeErr)
	return command
}

func (client *fakeRedisProbeClient) XInfoStream(ctx context.Context, _ string) *redisclient.XInfoStreamCmd {
	client.streamInfoCalls++
	command := redisclient.NewXInfoStreamCmd(ctx, "stream")
	command.SetVal(client.streamInfo)
	command.SetErr(client.streamInfoErr)
	return command
}

func (client *fakeRedisProbeClient) ConfigGet(ctx context.Context, setting string) *redisclient.MapStringStringCmd {
	client.configGetCalls[setting]++
	command := redisclient.NewMapStringStringCmd(ctx)
	command.SetVal(map[string]string{setting: client.settings[setting]})
	return command
}

func (client *fakeRedisProbeClient) FunctionList(ctx context.Context, _ redisclient.FunctionListQuery) *redisclient.FunctionListCmd {
	client.functionListCalls++
	command := redisclient.NewFunctionListCmd(ctx)
	command.SetVal(client.libraries)
	command.SetErr(client.functionListErr)
	return command
}

func (client *fakeRedisProbeClient) ScriptExists(ctx context.Context, _ ...string) *redisclient.BoolSliceCmd {
	client.scriptExistsCalls++
	command := redisclient.NewBoolSliceCmd(ctx)
	command.SetVal(client.scriptExists)
	command.SetErr(client.scriptExistsErr)
	return command
}

// The mutation methods deliberately exist on the fake so a future readiness
// implementation cannot add a silent load/replace/config-set path unnoticed.
func (client *fakeRedisProbeClient) FunctionLoad(context.Context, string) *redisclient.StringCmd {
	client.mutations++
	return redisclient.NewStringCmd(context.Background())
}

func (client *fakeRedisProbeClient) FunctionLoadReplace(context.Context, string) *redisclient.StringCmd {
	client.mutations++
	return redisclient.NewStringCmd(context.Background())
}

func (client *fakeRedisProbeClient) ScriptLoad(context.Context, string) *redisclient.StringCmd {
	client.mutations++
	return redisclient.NewStringCmd(context.Background())
}

func (client *fakeRedisProbeClient) ConfigSet(context.Context, string, string) *redisclient.StatusCmd {
	client.mutations++
	return redisclient.NewStatusCmd(context.Background())
}
