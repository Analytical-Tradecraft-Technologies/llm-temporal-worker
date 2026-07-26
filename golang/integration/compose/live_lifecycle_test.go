//go:build composeliveintegration

package compose_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"go.yaml.in/yaml/v4"
)

const (
	composeStatusPollInterval            = 100 * time.Millisecond
	composeStatusRequestTimeout          = time.Second
	composeContainerHealthPollInterval   = 100 * time.Millisecond
	composeContainerHealthInspectTimeout = time.Second
	composeMetricsBodyLimit              = 256 * 1024
	// This idle Compose recovery test exercises the Temporal SDK v1.43.0
	// AggregatedWorker's two remote-poller stop phases: workflow then activity.
	// Each may consume WorkerStopTimeout, which this application maps from
	// GracefulStopTimeout. Active local work has a separate drain path and is
	// intentionally outside this readiness-recovery contract.
	composeTemporalRemotePollerStopPhaseCount = 2
	// The Compose worker factory installs Redis and blob-store probes. Runtime
	// checks required probes serially with an independent timeout for each.
	composeRequiredDependencyProbeCount = 2
)

type composeHealthcheckTiming struct {
	interval    time.Duration
	timeout     time.Duration
	startPeriod time.Duration
	retries     int
}

type composeContainerHealthSnapshot struct {
	status               string
	latestCheckStartedAt time.Time
}

type composeContainerHealthLogDiagnostic struct {
	startedAt string
	endedAt   string
	exitCode  string
}

type composeContainerRestartDiagnostic struct {
	running      string
	status       string
	startedAt    string
	finishedAt   string
	restartCount string
	healthStatus string
	healthLog    []composeContainerHealthLogDiagnostic
}

type composeContainerHealthVerdict string

const (
	composeContainerHealthPending   composeContainerHealthVerdict = "pending"
	composeContainerHealthHealthy   composeContainerHealthVerdict = "healthy"
	composeContainerHealthUnhealthy composeContainerHealthVerdict = "unhealthy"
)

var composeDockerHealthTimestampLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999 -0700 MST",
}

const composeContainerRestartDiagnosticFormat = `{{.State.Running}}
{{.State.Status}}
{{.State.StartedAt}}
{{.State.FinishedAt}}
{{.RestartCount}}
{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}
{{if .State.Health}}{{range .State.Health.Log}}{{.Start}}	{{.End}}	{{.ExitCode}}
{{end}}{{end}}`

// composeRedisAuthenticatedPingCommand intentionally resolves credentials only
// inside the already-running Redis container. Its raw Compose equivalent is
// covered by TestComposeRedisHealthcheckTimingComesFromManifest.
const composeRedisAuthenticatedPingCommand = `redis-cli --user "$REDIS_USERNAME" --pass "$REDIS_PASSWORD" ping`

func TestParseComposeContainerRestartDiagnostic(t *testing.T) {
	output := strings.Join([]string{
		"true",
		"running",
		"2026-07-14 20:10:00.000000000 +0000 UTC",
		"0001-01-01 00:00:00 +0000 UTC",
		"0",
		"unhealthy",
		"2026-07-14 20:09:55.000000000 +0000 UTC\t2026-07-14 20:09:58.000000000 +0000 UTC\t1",
	}, "\n")

	diagnostic, err := parseComposeContainerRestartDiagnostic(output)
	if err != nil {
		t.Fatalf("parse restart diagnostic: %v", err)
	}
	if got, want := diagnostic.running, "true"; got != want {
		t.Fatalf("running = %q, want %q", got, want)
	}
	if got, want := diagnostic.status, "running"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := diagnostic.startedAt, "2026-07-14 20:10:00.000000000 +0000 UTC"; got != want {
		t.Fatalf("started at = %q, want %q", got, want)
	}
	if got, want := diagnostic.healthStatus, "unhealthy"; got != want {
		t.Fatalf("health status = %q, want %q", got, want)
	}
	if got, want := diagnostic.healthLog, []composeContainerHealthLogDiagnostic{{
		startedAt: "2026-07-14 20:09:55.000000000 +0000 UTC",
		endedAt:   "2026-07-14 20:09:58.000000000 +0000 UTC",
		exitCode:  "1",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("health log = %#v, want %#v", got, want)
	}
}

func TestComposeRestartDiagnosticUsesOnlySafeDockerHealthFields(t *testing.T) {
	for _, required := range []string{
		".State.Running",
		".State.Status",
		".State.StartedAt",
		".State.Health.Status",
		".Start",
		".End",
		".ExitCode",
	} {
		if !strings.Contains(composeContainerRestartDiagnosticFormat, required) {
			t.Errorf("restart diagnostic is missing %q", required)
		}
	}
	if strings.Contains(composeContainerRestartDiagnosticFormat, ".Output") {
		t.Error("restart diagnostic must not inspect Docker health output")
	}
	if got, want := composeRedisAuthenticatedPingCommand, `redis-cli --user "$REDIS_USERNAME" --pass "$REDIS_PASSWORD" ping`; got != want {
		t.Fatalf("authenticated Redis diagnostic command = %q, want %q", got, want)
	}
}

func TestComposeContainerHealthVerdictRequiresPostRestartProbe(t *testing.T) {
	containerStartedAt := time.Date(2026, time.July, 14, 20, 10, 0, 0, time.UTC)
	for _, test := range []struct {
		name     string
		snapshot composeContainerHealthSnapshot
		want     composeContainerHealthVerdict
	}{
		{
			name: "stale unhealthy health status remains pending",
			snapshot: composeContainerHealthSnapshot{
				status:               "unhealthy",
				latestCheckStartedAt: containerStartedAt.Add(-time.Second),
			},
			want: composeContainerHealthPending,
		},
		{
			name: "stale healthy health status remains pending",
			snapshot: composeContainerHealthSnapshot{
				status:               "healthy",
				latestCheckStartedAt: containerStartedAt.Add(-time.Second),
			},
			want: composeContainerHealthPending,
		},
		{
			name: "health status without a recorded probe remains pending",
			snapshot: composeContainerHealthSnapshot{
				status: "unhealthy",
			},
			want: composeContainerHealthPending,
		},
		{
			name: "post-restart unhealthy health status fails promptly",
			snapshot: composeContainerHealthSnapshot{
				status:               "unhealthy",
				latestCheckStartedAt: containerStartedAt.Add(time.Second),
			},
			want: composeContainerHealthUnhealthy,
		},
		{
			name: "post-restart healthy health status succeeds",
			snapshot: composeContainerHealthSnapshot{
				status:               "healthy",
				latestCheckStartedAt: containerStartedAt.Add(time.Second),
			},
			want: composeContainerHealthHealthy,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := composeContainerHealthVerdictAfterRestart(test.snapshot, containerStartedAt); got != test.want {
				t.Fatalf("health verdict = %q, want %q", got, test.want)
			}
		})
	}
}

func TestParseComposeContainerHealthSnapshot(t *testing.T) {
	for _, test := range []struct {
		name   string
		output string
	}{
		{
			name:   "RFC3339Nano health timestamps",
			output: "unhealthy\n2026-07-14T20:09:55.000000000Z\n2026-07-14T20:10:01.000000000Z\n",
		},
		{
			name:   "Docker Go time-string health timestamps",
			output: "unhealthy\n2026-07-14 20:09:55.000000000 +0000 UTC\n2026-07-14 20:10:01.000000000 +0000 UTC\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot, err := parseComposeContainerHealthSnapshot(test.output)
			if err != nil {
				t.Fatalf("parse health snapshot: %v", err)
			}
			if got, want := snapshot.status, "unhealthy"; got != want {
				t.Fatalf("health status = %q, want %q", got, want)
			}
			if got, want := snapshot.latestCheckStartedAt, time.Date(2026, time.July, 14, 20, 10, 1, 0, time.UTC); !got.Equal(want) {
				t.Fatalf("latest health check started at = %s, want %s", got, want)
			}
		})
	}
}

func TestComposeContainerHealthTransitionTimeoutUsesHealthcheckContract(t *testing.T) {
	healthcheck := composeHealthcheckTiming{
		interval:    5 * time.Second,
		timeout:     3 * time.Second,
		startPeriod: 2 * time.Second,
		retries:     3,
	}

	if got, want := composeContainerHealthTransitionTimeout(healthcheck), 27*time.Second+100*time.Millisecond; got != want {
		t.Fatalf("Compose container health transition timeout = %s, want %s", got, want)
	}
}

func TestComposeRedisHealthcheckTimingComesFromManifest(t *testing.T) {
	healthcheck := composeRedisHealthcheckTiming(t)
	if got, want := healthcheck.interval, 5*time.Second; got != want {
		t.Fatalf("Redis healthcheck interval = %s, want %s", got, want)
	}
	if got, want := healthcheck.timeout, 3*time.Second; got != want {
		t.Fatalf("Redis healthcheck timeout = %s, want %s", got, want)
	}
	if got, want := healthcheck.retries, 24; got != want {
		t.Fatalf("Redis healthcheck retries = %d, want %d", got, want)
	}
	if got, want := composeContainerHealthTransitionTimeout(healthcheck), 193*time.Second+100*time.Millisecond; got != want {
		t.Fatalf("Redis health transition timeout = %s, want %s", got, want)
	}
	document, _ := readCompose(t)
	healthcheckTest := document.Services["redis"].Healthcheck["test"]
	if healthcheckTest.Kind != yaml.SequenceNode || len(healthcheckTest.Content) != 2 {
		t.Fatalf("Redis healthcheck test = kind %d with %d arguments, want CMD-SHELL command", healthcheckTest.Kind, len(healthcheckTest.Content))
	}
	if got, want := healthcheckTest.Content[0].Value, "CMD-SHELL"; got != want {
		t.Fatalf("Redis healthcheck mode = %q, want %q", got, want)
	}
	if got, want := healthcheckTest.Content[1].Value, `redis-cli --user "$${REDIS_USERNAME}" --pass "$${REDIS_PASSWORD}" ping`; got != want {
		t.Fatalf("Redis healthcheck command = %q, want %q", got, want)
	}
}

func TestComposeReadinessTransitionTimeoutUsesWorkerAndProbeConfiguration(t *testing.T) {
	configuration := config.Config{
		Server: config.ServerConfig{
			ReadinessProbeInterval: config.Duration(5 * time.Second),
			ReadinessProbeTimeout:  config.Duration(2 * time.Second),
		},
		Temporal: config.TemporalConfig{
			Worker: config.TemporalWorkerConfig{
				GracefulStopTimeout: config.Duration(30 * time.Second),
			},
		},
	}

	if got, want := composeReadinessTransitionTimeoutForConfig(configuration), 70*time.Second+100*time.Millisecond; got != want {
		t.Fatalf("compose readiness transition timeout = %s, want %s", got, want)
	}
}

// TestComposeWorkerReadinessTracksRedis exercises the already-running worker
// profile rather than a test double. The Make target owns the Docker project
// and passes its Redis container name, so this test can prove that a loss of
// the durable admission dependency makes only readiness unavailable and that
// polling becomes eligible again after the same Redis instance is restored.
func TestComposeWorkerReadinessTracksRedis(t *testing.T) {
	address := os.Getenv("LLMTW_COMPOSE_WORKER_HEALTH_ADDR")
	metricsAddress := os.Getenv("LLMTW_COMPOSE_WORKER_METRICS_ADDR")
	container := os.Getenv("LLMTW_COMPOSE_REDIS_CONTAINER")
	if address == "" || metricsAddress == "" || container == "" {
		t.Skip("make compose-live-integration supplies worker health/metrics addresses and the isolated Redis container")
	}
	readinessTransitionTimeout := composeReadinessTransitionTimeout(t)

	assertComposeStatus(t, address, "/health/live", http.StatusOK)
	assertComposeStatus(t, address, "/health/ready", http.StatusOK)
	assertComposeMetric(t, metricsAddress, "llmtw_worker_polling", 1)

	stopped := false
	t.Cleanup(func() {
		if stopped {
			runComposeDocker(t, "start", container)
		}
	})
	runComposeDocker(t, "stop", container)
	stopped = true
	waitForComposeStatus(t, address, "/health/ready", http.StatusServiceUnavailable, readinessTransitionTimeout)
	assertComposeStatus(t, address, "/health/live", http.StatusOK)
	waitForComposeMetric(t, metricsAddress, "llmtw_worker_polling", 0, readinessTransitionTimeout)

	runComposeDocker(t, "start", container)
	stopped = false
	restartedAt := composeContainerStartedAt(t, container)
	waitForComposeContainerHealthy(t, address, container, restartedAt, composeContainerHealthTransitionTimeout(composeRedisHealthcheckTiming(t)))
	waitForComposeStatus(t, address, "/health/ready", http.StatusOK, readinessTransitionTimeout)
	assertComposeStatus(t, address, "/health/live", http.StatusOK)
	waitForComposeMetric(t, metricsAddress, "llmtw_worker_polling", 1, readinessTransitionTimeout)
}

func TestComposeMetricScrapeIsBoundedAndReadsWorkerPolling(t *testing.T) {
	if got, want := composeMetricsBodyLimit, 256*1024; got != want {
		t.Fatalf("metrics body limit = %d, want %d", got, want)
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/metrics" {
			http.NotFound(response, request)
			return
		}
		response.Header().Set("Content-Type", string(expfmt.FmtText))
		_, _ = io.WriteString(response, "# HELP llmtw_worker_polling Whether the Temporal worker is polling.\n# TYPE llmtw_worker_polling gauge\nllmtw_worker_polling 1\n")
	}))
	defer server.Close()

	value, err := composeMetricValue(strings.TrimPrefix(server.URL, "http://"), "llmtw_worker_polling")
	if err != nil {
		t.Fatalf("scrape worker polling metric: %v", err)
	}
	if value != 1 {
		t.Fatalf("worker polling metric = %v, want 1", value)
	}
}

func TestComposeMetricScrapeRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(response, strings.Repeat("x", composeMetricsBodyLimit+1))
	}))
	defer server.Close()

	_, err := composeMetricValue(strings.TrimPrefix(server.URL, "http://"), "llmtw_worker_polling")
	if err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("oversized metrics response error = %v, want bounded-response error", err)
	}
}

func TestComposeMetricScrapeSanitizesParserErrors(t *testing.T) {
	secret := "metric-secret-token"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(response, "llmtw_worker_polling{token=\""+secret+"\" 1\n")
	}))
	defer server.Close()

	_, err := composeMetricValue(strings.TrimPrefix(server.URL, "http://"), "llmtw_worker_polling")
	if err == nil {
		t.Fatal("malformed metrics response unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("parser error leaked metric payload: %v", err)
	}
}

func runComposeDocker(t *testing.T, arguments ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v (%s)", strings.Join(arguments, " "), err, strings.TrimSpace(string(output)))
	}
}

func composeReadinessTransitionTimeout(t *testing.T) time.Duration {
	t.Helper()
	configurationPath := filepath.Join("..", "..", "deploy", "local", "config.yaml")
	data, err := os.ReadFile(configurationPath)
	if err != nil {
		t.Fatalf("read mounted Compose configuration %s: %v", configurationPath, err)
	}
	configuration, err := config.Load(data)
	if err != nil {
		t.Fatalf("load mounted Compose configuration %s: %v", configurationPath, err)
	}
	return composeReadinessTransitionTimeoutForConfig(configuration)
}

func composeRedisHealthcheckTiming(t *testing.T) composeHealthcheckTiming {
	t.Helper()
	document, _ := readCompose(t)
	redis, ok := document.Services["redis"]
	if !ok {
		t.Fatal("Compose fixture is missing Redis")
	}
	return composeHealthcheckTimingFromManifest(t, "Redis", redis.Healthcheck)
}

func composeHealthcheckTimingFromManifest(t *testing.T, service string, healthcheck map[string]yaml.Node) composeHealthcheckTiming {
	t.Helper()
	if healthcheck == nil {
		t.Fatalf("%s service is missing a healthcheck", service)
	}
	interval := composeHealthcheckDuration(t, service, "interval", healthcheck["interval"].Value, false)
	timeout := composeHealthcheckDuration(t, service, "timeout", healthcheck["timeout"].Value, false)
	startPeriod := composeHealthcheckDuration(t, service, "start_period", healthcheck["start_period"].Value, true)
	retries, err := strconv.Atoi(healthcheck["retries"].Value)
	if err != nil || retries < 1 {
		t.Fatalf("%s healthcheck retries = %q, want a positive integer", service, healthcheck["retries"].Value)
	}
	return composeHealthcheckTiming{
		interval:    interval,
		timeout:     timeout,
		startPeriod: startPeriod,
		retries:     retries,
	}
}

func composeHealthcheckDuration(t *testing.T, service, field, raw string, optional bool) time.Duration {
	t.Helper()
	if raw == "" && optional {
		return 0
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		t.Fatalf("%s healthcheck %s = %q, want a positive Go duration", service, field, raw)
	}
	return duration
}

// composeContainerHealthTransitionTimeout bounds the Docker health verdict
// after docker start returns. Docker runs the first check after interval and
// subsequent checks after each previous check completes. A passing check marks
// the container healthy; retries consecutive failed checks mark it unhealthy,
// which this test reports immediately rather than waiting for arbitrary later
// recovery. The final terms bound this test's own inspect call and polling
// interval rather than adding a sleep.
func composeContainerHealthTransitionTimeout(healthcheck composeHealthcheckTiming) time.Duration {
	return healthcheck.startPeriod + time.Duration(healthcheck.retries)*(healthcheck.interval+healthcheck.timeout) +
		composeContainerHealthInspectTimeout + composeContainerHealthPollInterval
}

// composeReadinessTransitionTimeoutForConfig covers the longest worker
// recovery path: each synchronous remote-poller graceful-stop phase exercised
// by this idle Compose test, one readiness monitor interval, and every
// required dependency probe. The phase and probe counts are explicit because
// the Temporal SDK and
// CheckDependencyProbes execute them serially. The final terms bound this
// test's own HTTP request and status polling rather than adding arbitrary
// headroom.
func composeReadinessTransitionTimeoutForConfig(configuration config.Config) time.Duration {
	return time.Duration(composeTemporalRemotePollerStopPhaseCount)*time.Duration(configuration.Temporal.Worker.GracefulStopTimeout) +
		time.Duration(configuration.Server.ReadinessProbeInterval) +
		time.Duration(composeRequiredDependencyProbeCount)*time.Duration(configuration.Server.ReadinessProbeTimeout) +
		composeStatusRequestTimeout + composeStatusPollInterval
}

func waitForComposeStatus(t *testing.T, address, path string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		status, err := composeStatus(address, path)
		if err == nil && status == want {
			return
		}
		if err != nil {
			last = err
		} else {
			last = fmt.Errorf("status %d", status)
		}
		time.Sleep(composeStatusPollInterval)
	}
	t.Fatalf("%s did not reach HTTP %d within %s: %v", path, want, timeout, last)
}

func waitForComposeContainerHealthy(t *testing.T, address, container string, restartedAt time.Time, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := "no Docker health status"
	for time.Now().Before(deadline) {
		snapshot, err := composeContainerHealthSnapshotForContainer(container)
		if err != nil {
			last = fmt.Sprintf("inspect error: %v", err)
		} else {
			switch composeContainerHealthVerdictAfterRestart(snapshot, restartedAt) {
			case composeContainerHealthHealthy:
				return
			case composeContainerHealthUnhealthy:
				t.Fatalf("Redis container reported post-restart Docker health status unhealthy before readiness recovered")
			default:
				if snapshot.status == "unhealthy" {
					last = "stale Docker health status \"unhealthy\" before a post-restart health check"
				} else {
					last = fmt.Sprintf("Docker health status %q", snapshot.status)
				}
			}
		}
		time.Sleep(composeContainerHealthPollInterval)
	}
	t.Fatalf("Redis container did not reach Docker health status healthy within %s: %s\n%s", timeout, last, composeRedisRestartTimeoutDiagnostic(address, container))
}

func composeContainerStartedAt(t *testing.T, container string) time.Time {
	t.Helper()
	output, err := composeDockerInspect(container, "{{.State.StartedAt}}")
	if err != nil {
		t.Fatalf("inspect Redis container start time: %v", err)
	}
	startedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(output))
	if err != nil {
		t.Fatalf("parse Redis container start time: %v", err)
	}
	return startedAt
}

func composeContainerHealthSnapshotForContainer(container string) (composeContainerHealthSnapshot, error) {
	output, err := composeDockerInspect(container, "{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}\n{{if .State.Health}}{{range .State.Health.Log}}{{.Start}}\n{{end}}{{end}}")
	if err != nil {
		return composeContainerHealthSnapshot{}, err
	}
	return parseComposeContainerHealthSnapshot(output)
}

func composeContainerRestartDiagnosticForContainer(container string) (composeContainerRestartDiagnostic, error) {
	output, err := composeDockerInspect(container, composeContainerRestartDiagnosticFormat)
	if err != nil {
		return composeContainerRestartDiagnostic{}, err
	}
	return parseComposeContainerRestartDiagnostic(output)
}

func composeRedisAuthenticatedPing(container string) error {
	ctx, cancel := context.WithTimeout(context.Background(), composeContainerHealthInspectTimeout)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "exec", container, "/bin/sh", "-ec", composeRedisAuthenticatedPingCommand).Run(); err != nil {
		return fmt.Errorf("docker exec authenticated Redis PING: %w", err)
	}
	return nil
}

func composeRedisRestartTimeoutDiagnostic(address, container string) string {
	parts := make([]string, 0, 3)
	snapshot, err := composeContainerRestartDiagnosticForContainer(container)
	if err != nil {
		parts = append(parts, fmt.Sprintf("post-start Redis Docker state inspection error: %v", err))
	} else {
		parts = append(parts, snapshot.String())
	}
	if err := composeRedisAuthenticatedPing(container); err != nil {
		parts = append(parts, fmt.Sprintf("authenticated Redis PING failed: %v", err))
	} else {
		parts = append(parts, "authenticated Redis PING succeeded")
	}
	status, err := composeStatus(address, "/health/ready")
	if err != nil {
		parts = append(parts, fmt.Sprintf("worker readiness request error: %v", err))
	} else {
		parts = append(parts, fmt.Sprintf("worker readiness HTTP status: %d", status))
	}
	return strings.Join(parts, "\n")
}

func composeDockerInspect(container, format string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), composeContainerHealthInspectTimeout)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "inspect", "--format", format, container).Output()
	if err != nil {
		return "", fmt.Errorf("docker inspect Redis container: %w", err)
	}
	return string(output), nil
}

func parseComposeContainerHealthSnapshot(output string) (composeContainerHealthSnapshot, error) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) == "" {
		return composeContainerHealthSnapshot{}, fmt.Errorf("Docker health status is empty")
	}
	snapshot := composeContainerHealthSnapshot{status: strings.TrimSpace(lines[0])}
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		startedAt, err := parseComposeDockerHealthTimestamp(line)
		if err != nil {
			return composeContainerHealthSnapshot{}, fmt.Errorf("parse Docker health check timestamp %q: %w", line, err)
		}
		if startedAt.After(snapshot.latestCheckStartedAt) {
			snapshot.latestCheckStartedAt = startedAt
		}
	}
	return snapshot, nil
}

func parseComposeContainerRestartDiagnostic(output string) (composeContainerRestartDiagnostic, error) {
	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
	if len(lines) < 6 {
		return composeContainerRestartDiagnostic{}, fmt.Errorf("Docker restart diagnostic has %d fields, want at least 6", len(lines))
	}
	diagnostic := composeContainerRestartDiagnostic{
		running:      strings.TrimSpace(lines[0]),
		status:       strings.TrimSpace(lines[1]),
		startedAt:    strings.TrimSpace(lines[2]),
		finishedAt:   strings.TrimSpace(lines[3]),
		restartCount: strings.TrimSpace(lines[4]),
		healthStatus: strings.TrimSpace(lines[5]),
	}
	for _, line := range lines[6:] {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			return composeContainerRestartDiagnostic{}, fmt.Errorf("Docker health log diagnostic has %d fields, want 3", len(fields))
		}
		diagnostic.healthLog = append(diagnostic.healthLog, composeContainerHealthLogDiagnostic{
			startedAt: strings.TrimSpace(fields[0]),
			endedAt:   strings.TrimSpace(fields[1]),
			exitCode:  strings.TrimSpace(fields[2]),
		})
	}
	return diagnostic, nil
}

func (diagnostic composeContainerRestartDiagnostic) String() string {
	healthLog := make([]string, 0, len(diagnostic.healthLog))
	for _, entry := range diagnostic.healthLog {
		healthLog = append(healthLog, fmt.Sprintf("{start=%q end=%q exit_code=%q}", entry.startedAt, entry.endedAt, entry.exitCode))
	}
	return fmt.Sprintf(
		"post-start Redis Docker state: running=%q status=%q started_at=%q finished_at=%q restart_count=%q health_status=%q health_log=[%s]",
		diagnostic.running,
		diagnostic.status,
		diagnostic.startedAt,
		diagnostic.finishedAt,
		diagnostic.restartCount,
		diagnostic.healthStatus,
		strings.Join(healthLog, ", "),
	)
}

func parseComposeDockerHealthTimestamp(value string) (time.Time, error) {
	var errors []string
	for _, layout := range composeDockerHealthTimestampLayouts {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed, nil
		}
		errors = append(errors, err.Error())
	}
	return time.Time{}, fmt.Errorf("does not match accepted Docker timestamp layouts: %s", strings.Join(errors, "; "))
}

func composeContainerHealthVerdictAfterRestart(snapshot composeContainerHealthSnapshot, restartedAt time.Time) composeContainerHealthVerdict {
	if snapshot.latestCheckStartedAt.Before(restartedAt) {
		return composeContainerHealthPending
	}
	switch snapshot.status {
	case "healthy":
		return composeContainerHealthHealthy
	case "unhealthy":
		return composeContainerHealthUnhealthy
	default:
		return composeContainerHealthPending
	}
}

func assertComposeStatus(t *testing.T, address, path string, want int) {
	t.Helper()
	status, err := composeStatus(address, path)
	if err != nil {
		t.Fatal(err)
	}
	if status != want {
		t.Fatalf("%s status = %d, want %d", path, status, want)
	}
}

func assertComposeMetric(t *testing.T, address, name string, want float64) {
	t.Helper()
	value, err := composeMetricValue(address, name)
	if err != nil {
		t.Fatal(err)
	}
	if value != want {
		t.Fatalf("%s = %v, want %v", name, value, want)
	}
}

func waitForComposeMetric(t *testing.T, address, name string, want float64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		value, err := composeMetricValue(address, name)
		if err == nil && value == want {
			return
		}
		if err != nil {
			last = err
		} else {
			last = fmt.Errorf("value %v", value)
		}
		time.Sleep(composeStatusPollInterval)
	}
	t.Fatalf("%s did not reach %v within %s: %v", name, want, timeout, last)
}

// composeMetricValue reads only the bounded, non-secret metric needed by the
// lifecycle proof. It deliberately never includes the exposition body in an
// error so a future label mistake cannot leak the worker's metrics payload.
func composeMetricValue(address, name string) (float64, error) {
	response, err := (&http.Client{Timeout: composeStatusRequestTimeout}).Get("http://" + address + "/metrics")
	if err != nil {
		return 0, fmt.Errorf("metrics request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("metrics request returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, composeMetricsBodyLimit+1))
	if err != nil {
		return 0, fmt.Errorf("read metrics response: %w", err)
	}
	if len(body) > composeMetricsBodyLimit {
		return 0, fmt.Errorf("metrics response exceeded %d-byte bound", composeMetricsBodyLimit)
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return 0, errors.New("parse metrics response")
	}
	family, ok := families[name]
	if !ok {
		return 0, fmt.Errorf("metrics response omitted %s", name)
	}
	if family.GetType() != dto.MetricType_GAUGE || len(family.GetMetric()) != 1 || family.GetMetric()[0].GetGauge() == nil {
		return 0, fmt.Errorf("metrics response %s was not one gauge", name)
	}
	return family.GetMetric()[0].GetGauge().GetValue(), nil
}

func composeStatus(address, path string) (int, error) {
	response, err := (&http.Client{Timeout: composeStatusRequestTimeout}).Get("http://" + address + path)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	return response.StatusCode, nil
}
