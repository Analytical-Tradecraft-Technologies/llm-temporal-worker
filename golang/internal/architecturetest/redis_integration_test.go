package architecturetest

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func TestRedisIntegrationForwardsConfiguredContainerPrefixToRestartGate(t *testing.T) {
	makefile := readRepositoryFile(t, moduleRoot(t), "Makefile")
	assertContains(t, makefile, `LLMTW_REDIS_CONTAINER_PREFIX="$(REDIS_INTEGRATION_CONTAINER_PREFIX)"`, "redis-integration does not pass its configured container prefix to the persistence gate")

	testSource := readRepositoryFile(t, moduleRoot(t), "storage", "redis", "shared_state_conformance_integration_test.go")
	expectContainsEach(t, testSource, "Redis persistence gate does not honor configured container prefixes: missing %q",
		`os.Getenv("LLMTW_REDIS_CONTAINER_PREFIX")`,
		`isLiveRedisPersistenceContainer(container, configuredPrefix)`,
	)
}

func TestRedisIntegrationFailureRedactorRunsThroughBash(t *testing.T) {
	makefile := readRepositoryFile(t, moduleRoot(t), "Makefile")
	target := redisIntegrationTarget(t, makefile)
	assertContains(t, target, "bash ./scripts/redact-compose-logs.sh", "redis-integration must invoke its failure redactor through bash")
}

func TestReadinessIntegrationProvisionsRedisFunctionsForStorageGate(t *testing.T) {
	makefile := readRepositoryFile(t, moduleRoot(t), "Makefile")
	target := makeTarget(t, makefile, "readiness-integration:\n", "\n\n# Runs the black-box StoreFactory contract")
	assertOmits(t, makefile, "READINESS_REDIS_PORT", "Makefile must not retain a fixed readiness Redis port setting")
	if regexp.MustCompile(`--publish 127\.0\.0\.1:[^:]`).MatchString(target) {
		t.Fatal("readiness-integration must not publish Redis on a fixed host port")
	}
	expectContainsEach(t, target, "readiness-integration is missing Redis provisioning wiring %q",
		`--publish 127.0.0.1::6379`,
		`port="$$(docker inspect --format '{{(index (index .NetworkSettings.Ports "6379/tcp") 0).HostPort}}' "$$container")"`,
		`''|*[!0-9]*) echo "readiness-integration could not discover its loopback Redis port" >&2; exit 1 ;;`,
		`LLMTW_READINESS_REDIS_ADDR="127.0.0.1:$$port"`,
		`LLMTW_REDIS_ADDR="127.0.0.1:$$port"`,
		`LLMTW_REDIS_CONTAINER="$$container"`,
		`LLMTW_REDIS_CONTAINER_PREFIX="$(READINESS_REDIS_CONTAINER_PREFIX)"`,
		"LLMTW_REDIS_TEST_PROVISION=1",
		"redis-server --appendonly yes --appendfsync always --save 60 1 --maxmemory-policy noeviction",
		`-tags=integration ./storage/redis -run '^TestLiveRedis'`,
	)
}

func TestRedisBenchmarkIsOperatorGatedAndExcludedFromCI(t *testing.T) {
	makefile := readRepositoryFile(t, moduleRoot(t), "Makefile")
	target := redisBenchmarkTarget(t, makefile)
	expectContainsEach(t, target, "redis-benchmark is missing required operator guard %q",
		"LLMTW_REDIS_BENCHMARK=1",
		"LLMTW_REDIS_BENCHMARK_ALLOW_MUTATION=1",
		"LLMTW_REDIS_BENCHMARK_ADDR",
		"CI",
		"-tags=redisbenchmark",
		"BenchmarkV1ActivitiesAdmissionRedis",
	)
	expectOmitsEach(t, target, "redis-benchmark must not contain %q", "docker", "FunctionLoad", "LLMTW_REDIS_TEST_PROVISION")
	compileTarget := redisBenchmarkCompileTarget(t, makefile)
	expectContainsEach(t, compileTarget, "redis-benchmark-compile is missing %q",
		// The safety tests run; the benchmark itself never does (#1534).
		"$(GO) test -tags=redisbenchmark ./internal/runtime -run '^TestRedisBenchmark' -bench '^$$'",
	)
	expectOmitsEach(t, compileTarget, "redis-benchmark-compile must not contain %q",
		"LLMTW_REDIS_BENCHMARK",
		"docker",
		"Function",
		"redis-benchmark:",
	)
	for _, workflow := range []string{"master.yml", "pull-request.yml"} {
		data := readRepositoryFile(t, repositoryRoot(t), ".github", "workflows", workflow)
		if !strings.Contains(data, "make redis-benchmark-compile") {
			t.Fatalf("%s must compile the build-tagged Redis benchmark", workflow)
		}
		if strings.Contains(data, "make redis-benchmark\n") {
			t.Fatalf("%s must not execute the operator-only redis-benchmark target", workflow)
		}
	}
}

func TestRedisPersistenceReopensAtCurrentContainerAddressAfterRestart(t *testing.T) {
	testSource := readRepositoryFile(t, moduleRoot(t), "storage", "redis", "shared_state_conformance_integration_test.go")
	expectContainsEach(t, testSource, "Redis restart persistence test is missing %q",
		`runLiveRedisDocker(t, "restart", container)`,
		`reopenLiveRedisAfterRestart(t, container)`,
		`liveRedisAddressForContainer(container)`,
	)
}

func TestRedisPersistenceCleanupUsesRestartedClient(t *testing.T) {
	testSource := readRepositoryFile(t, moduleRoot(t), "storage", "redis", "shared_state_conformance_integration_test.go")
	const start = "func TestLiveRedisConfiguredPersistenceSurvivesRestart(t *testing.T) {"
	const end = "\nfunc TestLiveRedisConfiguredPersistenceGateAllowsOverriddenPrefix"
	persistenceTest := textSection(t, testSource, start, end, "Redis restart persistence test")
	assertOmits(t, persistenceTest, "cleanupLivePrefix(t, client, keys.Prefix)", "Redis restart persistence cleanup must not retain the pre-restart client")
	assertContains(t, persistenceTest, "cleanupLivePrefix(t, restartedClient, keys.Prefix)", "Redis restart persistence cleanup must use the restarted client")
}

func TestRedisLiveHarnessRefreshesEphemeralPortBeforeEachTest(t *testing.T) {
	testSource := readRepositoryFile(t, moduleRoot(t), "storage", "redis", "shared_state_conformance_integration_test.go")
	open := textSection(t, testSource, "func openLiveRedis(t *testing.T) *redisclient.Client {", "\nfunc openLiveRedisAt(", "live Redis harness openLiveRedis")
	expectContainsEach(t, open, "openLiveRedis must refresh an ephemeral container mapping: missing %q",
		`os.Getenv("LLMTW_REDIS_CONTAINER")`,
		`liveRedisAddressForContainer(container)`,
		`address = current`,
	)
}

func TestRedisContinuationLiveTestUsesRefreshedHarness(t *testing.T) {
	source := readRepositoryFile(t, moduleRoot(t), "storage", "redis", "continuation_integration_test.go")
	assertContains(t, source, "client := openLiveRedis(t)", "continuation live test must use the shared Redis harness")
	assertOmits(t, source, `os.Getenv("LLMTW_REDIS_ADDR")`, "continuation live test must not retain a stale Redis address")
}

func redisIntegrationTarget(t *testing.T, makefile string) string {
	return makeTarget(t, makefile, "redis-integration:\n", "\n\n# Builds a fresh local image")
}

func makeTarget(t *testing.T, makefile, start, end string) string {
	t.Helper()
	return textSection(t, makefile, start, end, fmt.Sprintf("Makefile target %q", strings.TrimSpace(start)))
}

// textSection returns text from the first start up to, not including, the
// next end; name labels the section in failures.
func textSection(t *testing.T, text, start, end, name string) string {
	t.Helper()
	startOffset := strings.Index(text, start)
	if startOffset < 0 {
		t.Fatalf("%s is missing", name)
	}
	endOffset := strings.Index(text[startOffset:], end)
	if endOffset < 0 {
		t.Fatalf("%s is missing its boundary", name)
	}
	return text[startOffset : startOffset+endOffset]
}

func redisBenchmarkTarget(t *testing.T, makefile string) string {
	t.Helper()
	return makeTarget(t, makefile, "redis-benchmark:\n", "\n\n# Runs the readiness recovery gate")
}

func redisBenchmarkCompileTarget(t *testing.T, makefile string) string {
	t.Helper()
	return makeTarget(t, makefile, "redis-benchmark-compile:\n", "\n\n# Times the production v1 Activities admission path")
}
