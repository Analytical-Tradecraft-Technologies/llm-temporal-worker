package compose_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"
)

type composeDocument struct {
	Name     string                    `yaml:"name"`
	Services map[string]composeService `yaml:"services"`
}

type composeService struct {
	Image       string               `yaml:"image"`
	Build       composeBuild         `yaml:"build"`
	Profiles    []string             `yaml:"profiles"`
	User        string               `yaml:"user"`
	WorkingDir  string               `yaml:"working_dir"`
	Entrypoint  yaml.Node            `yaml:"entrypoint"`
	DependsOn   map[string]yaml.Node `yaml:"depends_on"`
	Command     yaml.Node            `yaml:"command"`
	ReadOnly    bool                 `yaml:"read_only"`
	CapDrop     []string             `yaml:"cap_drop"`
	Healthcheck map[string]yaml.Node `yaml:"healthcheck"`
}

type composeBuild struct {
	Context string `yaml:"context"`
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	directory := filepath.Dir(source)
	for {
		if _, err := os.Stat(filepath.Join(directory, ".git")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("repository checkout root not found")
		}
		directory = parent
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(repositoryRoot(t), "golang")
}

func readCompose(t *testing.T) (composeDocument, []byte) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(moduleRoot(t), "compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var document composeDocument
	if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&document); err != nil {
		t.Fatalf("decode compose.yaml: %v", err)
	}
	return document, data
}

func TestComposeFixtureIsOfflineSafe(t *testing.T) {
	document, raw := readCompose(t)
	if document.Name != "llmtw" {
		t.Fatalf("Compose project name = %q, want llmtw", document.Name)
	}
	for _, name := range []string{"temporal", "redis", "provider-mock", "worker"} {
		if _, ok := document.Services[name]; !ok {
			t.Fatalf("Compose fixture is missing service %q", name)
		}
	}

	for _, name := range []string{"temporal", "redis"} {
		service := document.Services[name]
		if !strings.Contains(service.Image, "@sha256:") {
			t.Errorf("%s image must be digest pinned, got %q", name, service.Image)
		}
		if service.Healthcheck == nil {
			t.Errorf("%s must expose a healthcheck", name)
		}
	}

	mock := document.Services["provider-mock"]
	if mock.Build.Context != "./deploy/local/provider-mock" {
		t.Errorf("provider-mock build context = %q", mock.Build.Context)
	}
	if mock.Healthcheck == nil {
		t.Error("provider-mock must expose a healthcheck")
	}

	worker := document.Services["worker"]
	if len(worker.Profiles) != 1 || worker.Profiles[0] != "worker" {
		t.Fatalf("worker profiles = %#v, want [worker]", worker.Profiles)
	}
	if !worker.ReadOnly {
		t.Error("worker must use a read-only root filesystem")
	}
	if len(worker.CapDrop) != 1 || worker.CapDrop[0] != "ALL" {
		t.Fatalf("worker cap_drop = %#v, want [ALL]", worker.CapDrop)
	}
	for _, dependency := range []string{"temporal", "redis", "provider-mock"} {
		if _, ok := worker.DependsOn[dependency]; !ok {
			t.Errorf("worker does not wait for %s health", dependency)
		}
	}

	if !strings.Contains(string(raw), "continuation_hmac") ||
		!strings.Contains(string(raw), "${LLMTW_CONTINUATION_KEY_FILE:-./.local/continuation-hmac}") {
		t.Error("continuation key must come from an explicit local secret-file override")
	}
}

func TestComposeTemporalUsesPinnedPostgresStorage(t *testing.T) {
	document, raw := readCompose(t)
	postgres, ok := document.Services["postgres"]
	if !ok {
		t.Fatal("Compose fixture is missing the Temporal PostgreSQL service")
	}
	if !strings.Contains(postgres.Image, "@sha256:") {
		t.Errorf("postgres image must be digest pinned, got %q", postgres.Image)
	}
	if postgres.Healthcheck == nil {
		t.Error("postgres must expose a healthcheck before Temporal starts")
	}

	temporal := document.Services["temporal"]
	dependency, ok := temporal.DependsOn["postgres"]
	if !ok {
		t.Fatal("temporal does not wait for PostgreSQL health")
	}
	var condition struct {
		Condition string `yaml:"condition"`
	}
	if err := dependency.Decode(&condition); err != nil {
		t.Fatalf("decode PostgreSQL dependency: %v", err)
	}
	if condition.Condition != "service_healthy" {
		t.Fatalf("temporal PostgreSQL condition = %q, want service_healthy", condition.Condition)
	}

	for _, required := range []string{
		"DB: postgres12",
		"DB_PORT: \"5432\"",
		"POSTGRES_USER: temporal",
		"POSTGRES_PWD: ${LLMTW_POSTGRES_PASSWORD:-local-only}",
		"POSTGRES_SEEDS: postgres",
		"DYNAMIC_CONFIG_FILE_PATH: /etc/temporal/config/dynamicconfig/docker.yaml",
		"${LLMTW_POSTGRES_PASSWORD:-local-only}",
		"temporal-postgres-data:/var/lib/postgresql/data",
	} {
		if !strings.Contains(string(raw), required) {
			t.Errorf("Compose fixture is missing PostgreSQL Temporal input %q", required)
		}
	}
	if strings.Contains(string(raw), "DB: sqlite") {
		t.Error("Temporal auto-setup must not use unsupported sqlite storage")
	}
	if strings.Contains(string(raw), "development-sql.yaml") {
		t.Error("Temporal auto-setup image does not ship development-sql.yaml; use its docker.yaml dynamic config")
	}
	if got := strings.Count(string(raw), "${LLMTW_POSTGRES_PASSWORD:-local-only}"); got != 2 {
		t.Fatalf("PostgreSQL password variable occurrences = %d, want the same variable on Postgres and Temporal", got)
	}
}

func TestComposeTemporalHealthcheckUsesEntrypointBoundSemanticReadiness(t *testing.T) {
	document, _ := readCompose(t)
	temporal, ok := document.Services["temporal"]
	if !ok {
		t.Fatal("Compose fixture is missing the Temporal service")
	}
	healthcheckTest, ok := temporal.Healthcheck["test"]
	if !ok {
		t.Fatal("Temporal service is missing a healthcheck test")
	}
	if healthcheckTest.Kind != yaml.SequenceNode || len(healthcheckTest.Content) != 2 {
		t.Fatalf("Temporal healthcheck test = kind %d with %d arguments, want CMD-SHELL command", healthcheckTest.Kind, len(healthcheckTest.Content))
	}
	if got, want := healthcheckTest.Content[0].Value, "CMD-SHELL"; got != want {
		t.Fatalf("Temporal healthcheck mode = %q, want %q", got, want)
	}
	if got, want := healthcheckTest.Content[1].Value, `BIND_ON_IP="$$(getent hosts "$$(hostname)" | awk '{print $$1;}')"; case "$$BIND_ON_IP" in *:*) TEMPORAL_ADDRESS="[$$BIND_ON_IP]:7233" ;; *) TEMPORAL_ADDRESS="$$BIND_ON_IP:7233" ;; esac; TEMPORAL_ADDRESS="$$TEMPORAL_ADDRESS" temporal operator cluster health | grep -q SERVING`; got != want {
		t.Fatalf("Temporal healthcheck command = %q, want %q", got, want)
	}
	for name, required := range map[string]string{
		"bracketed IPv6": `*:*) TEMPORAL_ADDRESS="[$$BIND_ON_IP]:7233" ;;`,
		"IPv4":           `*) TEMPORAL_ADDRESS="$$BIND_ON_IP:7233" ;;`,
	} {
		if !strings.Contains(healthcheckTest.Content[1].Value, required) {
			t.Errorf("Temporal healthcheck is missing %s address handling %q", name, required)
		}
	}
}

func TestLocalFixtureDocumentationPreservesFailClosedProviderEgress(t *testing.T) {
	root := repositoryRoot(t)
	module := moduleRoot(t)
	for path, required := range map[string][]string{
		"deploy/local/README.md": {
			"parser/configuration/readiness fixture",
			"provider egress is not available",
			"content-free adapter",
			"weaken the policy",
		},
		"docs/architecture/deployment-and-operations.md": {
			"LLMTW_COMPOSE_LIVE=1 make compose-live-integration",
			"content-free injected adapter",
			"Docker-private-address bypass",
		},
	} {
		base := root
		if strings.HasPrefix(path, "deploy/") {
			base = module
		}
		data, err := os.ReadFile(filepath.Join(base, path))
		if err != nil {
			t.Fatal(err)
		}
		for _, phrase := range required {
			if !strings.Contains(string(data), phrase) {
				t.Errorf("%s must state %q", path, phrase)
			}
		}
	}
}

func TestWorkerComposeProvisionsAdmissionFunctionBeforeStart(t *testing.T) {
	document, raw := readCompose(t)
	provisioner, ok := document.Services["redis-function-provisioner"]
	if !ok {
		t.Fatal("Compose fixture is missing redis Function provisioner")
	}
	if len(provisioner.Profiles) != 1 || provisioner.Profiles[0] != "worker" {
		t.Fatalf("provisioner profiles = %#v, want [worker]", provisioner.Profiles)
	}
	if _, ok := provisioner.DependsOn["redis"]; !ok {
		t.Fatal("redis Function provisioner does not wait for Redis")
	}
	worker := document.Services["worker"]
	dependency, ok := worker.DependsOn["redis-function-provisioner"]
	if !ok {
		t.Fatal("worker does not wait for Redis Function provisioning")
	}
	var condition struct {
		Condition string `yaml:"condition"`
	}
	if err := dependency.Decode(&condition); err != nil {
		t.Fatalf("decode provisioner dependency: %v", err)
	}
	if condition.Condition != "service_completed_successfully" {
		t.Fatalf("worker provisioner condition = %q, want service_completed_successfully", condition.Condition)
	}
	for _, required := range []string{
		"FUNCTION LOAD",
		"./storage/redis/functions/admission.lua",
		"llmtw_admission_v1",
		"admission_v1",
	} {
		if !strings.Contains(string(raw), required) {
			t.Errorf("Compose fixture is missing explicit Redis Function provisioning input %q", required)
		}
	}
}

func TestRedisFunctionProvisionerEscapesShellVariablesFromCompose(t *testing.T) {
	_, raw := readCompose(t)
	for _, variable := range []string{"library", "version", "source"} {
		if !strings.Contains(string(raw), "$${"+variable+"}") {
			t.Errorf("provisioner does not escape shell variable %q from Compose interpolation", variable)
		}
	}
}

func TestComposeRedisRequiresTheWorkerNamedACL(t *testing.T) {
	document, raw := readCompose(t)
	redis := document.Services["redis"]
	if redis.User != "" {
		t.Fatalf("Redis ACL wrapper must invoke the image entrypoint as root so it can initialize /data, got user %q", redis.User)
	}
	if redis.WorkingDir != "/data" {
		t.Fatalf("Redis ACL wrapper working directory = %q, want /data for entrypoint volume ownership initialization", redis.WorkingDir)
	}
	if redis.Entrypoint.Kind != yaml.SequenceNode || len(redis.Entrypoint.Content) != 3 {
		t.Fatalf("Redis ACL wrapper entrypoint must be a three-argument shell invocation, got kind=%d len=%d", redis.Entrypoint.Kind, len(redis.Entrypoint.Content))
	}
	if redis.Command.Kind != yaml.SequenceNode || len(redis.Command.Content) != 0 {
		t.Fatalf("Redis ACL wrapper must clear the image command, got kind=%d len=%d", redis.Command.Kind, len(redis.Command.Content))
	}
	aclScript := redis.Entrypoint.Content[2].Value
	for _, required := range []string{
		"acl_file=\"$$(mktemp /tmp/llmtw-users.XXXXXX)\"",
		"chmod 0600 \"$${acl_file}\"",
		"chown redis \"$${acl_file}\"",
		"exec /usr/local/bin/docker-entrypoint.sh redis-server",
	} {
		if !strings.Contains(aclScript, required) {
			t.Errorf("Redis ACL wrapper is missing %q", required)
		}
	}
	aclFileCreation := strings.Index(aclScript, "acl_file=\"$$(mktemp /tmp/llmtw-users.XXXXXX)\"")
	aclMode := strings.Index(aclScript, "chmod 0600 \"$${acl_file}\"")
	aclOwnership := strings.Index(aclScript, "chown redis \"$${acl_file}\"")
	entrypointHandoff := strings.Index(aclScript, "exec /usr/local/bin/docker-entrypoint.sh redis-server")
	if aclFileCreation < 0 || aclMode < aclFileCreation || aclOwnership < aclMode || entrypointHandoff < aclOwnership {
		t.Error("Redis ACL wrapper must create a private fresh ACL, prepare ownership, then hand off as root to the image entrypoint")
	}
	if strings.Contains(aclScript, "exec /docker-entrypoint.sh") {
		t.Error("Redis ACL wrapper must use the image's resolved /usr/local/bin/docker-entrypoint.sh path")
	}
	for _, forbidden := range []string{
		"/tmp/llmtw-users.acl",
		"echo \"$${acl_file}\"",
		"printf '%s\\n' \"$${acl_file}\"",
		"set -x",
	} {
		if strings.Contains(aclScript, forbidden) {
			t.Errorf("Redis ACL wrapper must not retain or expose %q", forbidden)
		}
	}
	for _, required := range []string{
		"REDIS_USERNAME: ${LLMTW_REDIS_USERNAME:-local}",
		"REDIS_PASSWORD: ${LLMTW_REDIS_PASSWORD:-local-only}",
		"REDIS_KEY_PREFIX: ${LLMTW_REDIS_KEY_PREFIX:-llmtw}",
		"umask 077",
		"user default off",
		"user %s on >%s ~%s:* +@all",
		"acl_file=\"$$(mktemp /tmp/llmtw-users.XXXXXX)\"",
		"chmod 0600 \"$${acl_file}\"",
		"--aclfile \"$${acl_file}\"",
		"--save 60 1",
		"redis-cli --user \"$${REDIS_USERNAME}\" --pass \"$${REDIS_PASSWORD}\" ping",
		"redis-cli -h redis --user \"$${REDIS_USERNAME}\" --pass \"$${REDIS_PASSWORD}\"",
	} {
		if !strings.Contains(string(raw), required) {
			t.Errorf("Compose Redis ACL fixture is missing %q", required)
		}
	}
	if strings.Contains(string(raw), "--save \"60 1\"") {
		t.Error("Redis save interval and change count must be separate command-line arguments")
	}
	for _, inlineRule := range []string{
		"--user \"default off\"",
		"--user \"$${REDIS_USERNAME} on >$${REDIS_PASSWORD} ~* +@all\"",
	} {
		if !strings.Contains(string(raw), inlineRule) {
			continue
		}
		t.Error("Redis ACLs must use the generated aclfile rather than quoted command-line rules")
	}
}

func TestComposeRedisDeclaresNoEvictionAndPersistencePolicy(t *testing.T) {
	document, raw := readCompose(t)
	redis, ok := document.Services["redis"]
	if !ok {
		t.Fatal("Compose fixture is missing Redis")
	}
	if redis.Healthcheck == nil {
		t.Fatal("Redis must expose a healthcheck")
	}
	test, ok := redis.Healthcheck["test"]
	if !ok || test.Kind != yaml.SequenceNode || len(test.Content) != 2 {
		t.Fatalf("Redis healthcheck must be a CMD-SHELL sequence, got %#v", test)
	}
	if got, want := test.Content[0].Value, "CMD-SHELL"; got != want {
		t.Fatalf("Redis healthcheck mode = %q, want %q", got, want)
	}
	command := test.Content[1].Value
	for _, required := range []string{
		"CONFIG GET maxmemory-policy",
		"= noeviction",
		"CONFIG GET appendonly",
		"= yes",
		"CONFIG GET save",
	} {
		if !strings.Contains(command, required) {
			t.Errorf("Redis healthcheck is missing %q", required)
		}
	}
	for _, required := range []string{
		"--appendonly yes",
		"--save 60 1",
		"--maxmemory-policy noeviction",
	} {
		if !strings.Contains(string(raw), required) {
			t.Errorf("Compose Redis command is missing explicit policy %q", required)
		}
	}
}

func TestComposeProvisionerShellScriptsAreSingleArguments(t *testing.T) {
	document, _ := readCompose(t)
	for _, name := range []string{"redis-function-provisioner", "blob-volume-provisioner"} {
		service := document.Services[name]
		if service.Command.Kind != yaml.SequenceNode || len(service.Command.Content) != 1 {
			t.Errorf("%s command must be a one-element sequence so /bin/sh -c receives the full script, got kind=%d len=%d", name, service.Command.Kind, len(service.Command.Content))
		}
	}
}

func TestComposeWorkerUsesDurableFileBlobAndExactHealthEndpoints(t *testing.T) {
	document, raw := readCompose(t)
	worker := document.Services["worker"]
	if worker.Healthcheck == nil {
		t.Error("worker must have a healthcheck that verifies its live and ready endpoints")
	}
	if _, ok := document.Services["blob-volume-provisioner"]; !ok {
		t.Error("Compose fixture is missing a file-blob volume provisioner")
	}
	if _, ok := worker.DependsOn["blob-volume-provisioner"]; !ok {
		t.Error("worker does not wait for file-blob volume provisioning")
	}
	for _, required := range []string{
		"blob-data:/var/lib/llmtw/blobs",
		"/health/live",
		"/health/ready",
		"healthcheck",
	} {
		if !strings.Contains(string(raw), required) {
			t.Errorf("Compose fixture is missing %q", required)
		}
	}
	localConfig, err := os.ReadFile(filepath.Join(moduleRoot(t), "deploy", "local", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"kind: file", "root: /var/lib/llmtw/blobs"} {
		if !strings.Contains(string(localConfig), required) {
			t.Errorf("local configuration is missing %q", required)
		}
	}
	deployment, err := os.ReadFile(filepath.Join(moduleRoot(t), "deploy", "kubernetes", "base", "deployment.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"/health/live", "/health/ready"} {
		if !strings.Contains(string(raw), endpoint) || !strings.Contains(string(deployment), endpoint) {
			t.Errorf("Compose and Kubernetes must both use %q", endpoint)
		}
	}
}

func TestComposeLiveIntegrationTargetIsExplicitAndFailsClosed(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"compose-live-integration:",
		"LLMTW_COMPOSE_LIVE:-0",
		"docker info",
		"set -e;",
		"--profile worker",
		"build --no-cache --quiet",
		"up --wait --wait-timeout 300",
		"temporal_port=0",
		"temporal_ui_port=0",
		"redis_port=0",
		"health_port=0",
		"metrics_port=0",
		"postgres_password=\"$${LLMTW_POSTGRES_PASSWORD:-local-only}\"",
		"mock_api_key=local-only",
		"continuation_hmac=",
		"od -An -N32 -tx1 /dev/urandom",
		"LLMTW_COMPOSE_TEMPORAL_UI_PORT",
		"LLMTW_POSTGRES_PASSWORD=\"$$postgres_password\"",
		"$(COMPOSE) port temporal 7233",
		"$(COMPOSE) port temporal 8233",
		"$(COMPOSE) port redis 6379",
		"$(COMPOSE) port worker 8080",
		"$(COMPOSE) port worker 9090",
		"compose-live-integration service logs (redacted; service output only; no environment inspection):",
		"$(COMPOSE) logs --no-color temporal postgres redis redis-function-provisioner blob-volume-provisioner provider-mock worker",
		"LLMTW_LOG_REDACT_REDIS_PASSWORD=\"$$redis_password\"",
		"LLMTW_LOG_REDACT_POSTGRES_PASSWORD=\"$$postgres_password\"",
		"LLMTW_LOG_REDACT_MOCK_API_KEY=\"$$mock_api_key\"",
		"LLMTW_LOG_REDACT_CONTINUATION_HMAC=\"$$continuation_hmac\"",
		"sh ./scripts/redact-compose-logs.sh",
		"LLMTW_TEMPORAL_ADDRESS=\"$$temporal_address\"",
		"LLMTW_REDIS_ADDR=\"$$redis_address\"",
		"LLMTW_REDIS_USERNAME",
		"LLMTW_REDIS_PASSWORD",
		"LLMTW_REDIS_KEY_PREFIX",
	} {
		if !strings.Contains(string(data), required) {
			t.Errorf("compose live integration target is missing %q", required)
		}
	}
	for _, fixedPort := range []string{
		"LLMTW_COMPOSE_TEMPORAL_PORT:-17233",
		"LLMTW_COMPOSE_REDIS_PORT:-16380",
		"LLMTW_COMPOSE_HEALTH_PORT:-18080",
		"LLMTW_COMPOSE_METRICS_PORT:-19090",
	} {
		if strings.Contains(string(data), fixedPort) {
			t.Errorf("compose live integration target retains fixed host port %q", fixedPort)
		}
	}
	if strings.Contains(string(data), "up --wait --wait-timeout 180") {
		t.Error("compose live integration retains a wait timeout shorter than the manifest healthcheck bound")
	}
}

func TestComposeLiveIntegrationMakesFileSecretReadableToNonrootWorker(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}

	makefile := string(data)
	for _, required := range []string{
		`tmpdir="$$(mktemp -d`,
		`umask 077;`,
		"printf '%s' \"$$continuation_hmac\" > \"$$key\"; \\\n\tchmod 0444 \"$$key\"; \\",
	} {
		if !strings.Contains(makefile, required) {
			t.Errorf("compose live integration target is missing %q", required)
		}
	}
}

func TestComposeFailureDiagnosticsIncludeRedactedTemporalHealthOutput(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	const marker = "compose-live-integration Temporal healthcheck output (redacted):"
	start := strings.Index(string(data), marker)
	if start < 0 {
		t.Fatalf("compose live integration target is missing %q", marker)
	}
	diagnostics := string(data)[start:]
	for _, required := range []string{
		"$$( $(COMPOSE) ps -q temporal 2>/dev/null || true )",
		"docker inspect --format '{{range .State.Health.Log}}{{.Output}}{{end}}'",
		"LLMTW_LOG_REDACT_REDIS_PASSWORD=\"$$redis_password\"",
		"LLMTW_LOG_REDACT_POSTGRES_PASSWORD=\"$$postgres_password\"",
		"LLMTW_LOG_REDACT_MOCK_API_KEY=\"$$mock_api_key\"",
		"LLMTW_LOG_REDACT_CONTINUATION_HMAC=\"$$continuation_hmac\"",
		"sh ./scripts/redact-compose-logs.sh",
	} {
		if !strings.Contains(diagnostics, required) {
			t.Errorf("Temporal healthcheck diagnostics are missing %q", required)
		}
	}
}

func TestComposeLifecycleFailureDiagnosticsUseRedactedServiceLogs(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}

	const lifecycleTest = `GOCACHE="$$go_cache" LLMTW_COMPOSE_WORKER_HEALTH_ADDR="$$health_address" LLMTW_COMPOSE_REDIS_CONTAINER="$$redis_container" $(GO) test -count=1 -tags=composeliveintegration ./integration/compose -run '^TestComposeWorkerReadinessTracksRedis$$'`
	const nextTest = `GOCACHE="$$go_cache" LLMTW_TEMPORAL_ADDRESS="$$temporal_address"`
	makefile := string(data)
	start := strings.Index(makefile, "if ! "+lifecycleTest)
	if start < 0 {
		t.Fatalf("compose live integration target is missing the Redis lifecycle failure wrapper")
	}
	end := strings.Index(makefile[start:], nextTest)
	if end < 0 {
		t.Fatalf("compose live integration target is missing the subsequent Temporal recovery test")
	}
	failurePath := makefile[start : start+end]

	for _, required := range []string{
		`if ! ` + lifecycleTest + `; then \`,
		"compose-live-integration Redis lifecycle test service logs (redacted; service output only; no environment inspection):",
		"$(COMPOSE) logs --no-color temporal postgres redis redis-function-provisioner blob-volume-provisioner provider-mock worker",
		"LLMTW_LOG_REDACT_REDIS_PASSWORD=\"$$redis_password\"",
		"LLMTW_LOG_REDACT_POSTGRES_PASSWORD=\"$$postgres_password\"",
		"LLMTW_LOG_REDACT_MOCK_API_KEY=\"$$mock_api_key\"",
		"LLMTW_LOG_REDACT_CONTINUATION_HMAC=\"$$continuation_hmac\"",
		`sh ./scripts/redact-compose-logs.sh >&2 || true; \`,
		`exit 1; \`,
	} {
		if !strings.Contains(failurePath, required) {
			t.Errorf("Redis lifecycle failure diagnostics are missing %q", required)
		}
	}
	if strings.Contains(failurePath, "docker inspect") {
		t.Error("Redis lifecycle failure diagnostics must emit only redacted Compose service logs")
	}
}

func TestComposeTemporalRecoveryFailureDiagnosticsUseRedactedServiceLogs(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}

	const temporalRecoveryTest = `GOCACHE="$$go_cache" LLMTW_TEMPORAL_ADDRESS="$$temporal_address" LLMTW_REDIS_ADDR="$$redis_address" LLMTW_REDIS_USERNAME="$$redis_username" LLMTW_REDIS_PASSWORD="$$redis_password" LLMTW_REDIS_KEY_PREFIX="$$redis_key_prefix" $(GO) test -count=1 -tags=composeliveintegration ./integration/temporal -run '^(TestTemporalRecoveryWithSharedRedis|TestTemporalKeepaliveCompletesLongOneShotProviderCall)$$'`
	makefile := string(data)
	start := strings.Index(makefile, "if ! "+temporalRecoveryTest)
	if start < 0 {
		t.Fatalf("compose live integration target is missing the Temporal recovery failure wrapper")
	}
	end := strings.Index(makefile[start:], "\n\n")
	if end < 0 {
		t.Fatal("compose live integration target is missing the end of the Temporal recovery failure wrapper")
	}
	failurePath := makefile[start : start+end]

	for _, required := range []string{
		`if ! ` + temporalRecoveryTest + `; then \`,
		"compose-live-integration Temporal recovery test service logs (redacted; service output only; no environment inspection):",
		"$(COMPOSE) logs --no-color temporal postgres redis redis-function-provisioner blob-volume-provisioner provider-mock worker",
		"LLMTW_LOG_REDACT_REDIS_PASSWORD=\"$$redis_password\"",
		"LLMTW_LOG_REDACT_POSTGRES_PASSWORD=\"$$postgres_password\"",
		"LLMTW_LOG_REDACT_MOCK_API_KEY=\"$$mock_api_key\"",
		"LLMTW_LOG_REDACT_CONTINUATION_HMAC=\"$$continuation_hmac\"",
		`sh ./scripts/redact-compose-logs.sh >&2 || true; \`,
		`exit 1; \`,
	} {
		if !strings.Contains(failurePath, required) {
			t.Errorf("Temporal recovery failure diagnostics are missing %q", required)
		}
	}
	if strings.Contains(failurePath, "docker inspect") {
		t.Error("Temporal recovery failure diagnostics must emit only redacted Compose service logs")
	}
}

func TestComposeTemporalRecoveryRefreshesRedisAddressAfterLifecycle(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}

	const lifecycleTest = `GOCACHE="$$go_cache" LLMTW_COMPOSE_WORKER_HEALTH_ADDR="$$health_address" LLMTW_COMPOSE_REDIS_CONTAINER="$$redis_container" $(GO) test -count=1 -tags=composeliveintegration ./integration/compose -run '^TestComposeWorkerReadinessTracksRedis$$'`
	const temporalRecoveryTest = `GOCACHE="$$go_cache" LLMTW_TEMPORAL_ADDRESS="$$temporal_address" LLMTW_REDIS_ADDR="$$redis_address" LLMTW_REDIS_USERNAME="$$redis_username" LLMTW_REDIS_PASSWORD="$$redis_password" LLMTW_REDIS_KEY_PREFIX="$$redis_key_prefix" $(GO) test -count=1 -tags=composeliveintegration ./integration/temporal -run '^(TestTemporalRecoveryWithSharedRedis|TestTemporalKeepaliveCompletesLongOneShotProviderCall)$$'`
	const redisAddressRefresh = `redis_address="$$( $(COMPOSE) port redis 6379 )";`
	makefile := string(data)
	lifecycleStart := strings.Index(makefile, "if ! "+lifecycleTest)
	if lifecycleStart < 0 {
		t.Fatal("compose live integration target is missing the Redis lifecycle wrapper")
	}
	lifecycleEndOffset := strings.Index(makefile[lifecycleStart:], "\tfi; \\\n\t")
	if lifecycleEndOffset < 0 {
		t.Fatal("compose live integration target is missing the end of the Redis lifecycle wrapper")
	}
	lifecycleEnd := lifecycleStart + lifecycleEndOffset + len("\tfi; \\\n")
	temporalStart := strings.Index(makefile, "if ! "+temporalRecoveryTest)
	if temporalStart < 0 {
		t.Fatal("compose live integration target is missing the Temporal recovery wrapper")
	}
	if temporalStart <= lifecycleEnd {
		t.Fatal("Temporal recovery wrapper must follow the Redis lifecycle wrapper")
	}
	refreshOffset := strings.Index(makefile[lifecycleEnd:temporalStart], redisAddressRefresh)
	if refreshOffset < 0 {
		t.Fatalf("compose live integration target must refresh Redis with %q after lifecycle recovery", redisAddressRefresh)
	}
	refreshPath := makefile[lifecycleEnd+refreshOffset : temporalStart]
	for _, required := range []string{
		redisAddressRefresh,
		`if [ -z "$$redis_address" ]; then \`,
		`echo "compose-live-integration could not rediscover the Redis host port after lifecycle recovery" >&2; \`,
		`exit 1; \`,
		`fi; \`,
	} {
		if !strings.Contains(refreshPath, required) {
			t.Errorf("post-lifecycle Redis address refresh is missing %q", required)
		}
	}
	if !strings.Contains(makefile[temporalStart:], `LLMTW_REDIS_ADDR="$$redis_address"`) {
		t.Error("Temporal recovery command must receive the post-lifecycle Redis address")
	}
}

func TestComposeFailureLogRedactorRedactsEveryReachableSecret(t *testing.T) {
	t.Parallel()
	secrets := map[string]string{
		"continuation HMAC":     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"custom Redis password": "redis-custom-\\password-$[]",
		"mock API key":          "mock-api-key-123+/:",
		"PostgreSQL password":   "postgres-custom:with=equals",
	}
	input := strings.Join([]string{
		"worker | Redis authentication failed: " + secrets["custom Redis password"],
		"postgres | startup password=" + secrets["PostgreSQL password"],
		"provider-mock | Authorization: Bearer " + secrets["mock API key"],
		"worker | continuation signer=" + secrets["continuation HMAC"],
		"worker | repeated Redis authentication failed: " + secrets["custom Redis password"],
	}, "\n")

	command := exec.Command("sh", filepath.Join(moduleRoot(t), "scripts", "redact-compose-logs.sh"))
	command.Dir = moduleRoot(t)
	command.Env = append(os.Environ(),
		"LLMTW_LOG_REDACT_REDIS_PASSWORD="+secrets["custom Redis password"],
		"LLMTW_LOG_REDACT_POSTGRES_PASSWORD="+secrets["PostgreSQL password"],
		"LLMTW_LOG_REDACT_MOCK_API_KEY="+secrets["mock API key"],
		"LLMTW_LOG_REDACT_CONTINUATION_HMAC="+secrets["continuation HMAC"],
	)
	command.Stdin = strings.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run compose failure-log redactor: %v: %s", err, output)
	}
	redacted := string(output)
	for name, secret := range secrets {
		if strings.Contains(redacted, secret) {
			t.Errorf("compose failure-log redactor leaked %s", name)
		}
	}
	if got, want := strings.Count(redacted, "[REDACTED]"), 5; got != want {
		t.Errorf("redaction count = %d, want %d; output = %q", got, want, redacted)
	}
}

func TestComposePublishedPortsDefaultToDockerAllocatedPorts(t *testing.T) {
	_, raw := readCompose(t)
	for _, required := range []string{
		"${LLMTW_COMPOSE_TEMPORAL_PORT:-0}:7233",
		"${LLMTW_COMPOSE_TEMPORAL_UI_PORT:-0}:8233",
		"${LLMTW_COMPOSE_REDIS_PORT:-0}:6379",
		"${LLMTW_COMPOSE_HEALTH_PORT:-0}:8080",
		"${LLMTW_COMPOSE_METRICS_PORT:-0}:9090",
	} {
		if !strings.Contains(string(raw), required) {
			t.Errorf("Compose fixture does not request a Docker-assigned host port for %q", required)
		}
	}
}

func TestCIExercisesAuthorizedComposeLiveGate(t *testing.T) {
	root := repositoryRoot(t)
	for _, path := range []string{
		".github/workflows/pull-request.yml",
		".github/workflows/master.yml",
	} {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), "LLMTW_COMPOSE_LIVE=1 make compose-live-integration") {
			t.Errorf("%s must exercise the authorized Compose lifecycle gate", path)
		}
	}
}
