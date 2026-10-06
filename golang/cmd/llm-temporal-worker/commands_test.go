package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/buildinfo"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/diagnostic"
)

func TestVersionCommandEmitsBuildMetadata(t *testing.T) {
	var output, errorsOut bytes.Buffer
	if code := Execute(context.Background(), []string{"version"}, CommandOptions{Out: &output, ErrOut: &errorsOut}); code != 0 {
		t.Fatalf("version code=%d error=%s", code, errorsOut.String())
	}
	if errorsOut.Len() != 0 {
		t.Fatalf("version wrote stderr: %q", errorsOut.String())
	}
	var got buildinfo.Metadata
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatalf("version output is not JSON: %v (%q)", err, output.String())
	}
	if want := buildinfo.Current(); got != want {
		t.Fatalf("version metadata = %#v, want %#v", got, want)
	}
}

func TestHealthServerCommandServesLiveOnlyUntilCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, writer := io.Pipe()
	var errorsOut bytes.Buffer
	done := make(chan int, 1)
	go func() {
		code := Execute(ctx, []string{"health-server", "--address", "127.0.0.1:0"}, CommandOptions{Out: writer, ErrOut: &errorsOut})
		_ = writer.Close()
		done <- code
	}()

	address, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil {
		code := <-done
		if strings.Contains(errorsOut.String(), "operation not permitted") {
			t.Skip("sandbox does not permit binding a TCP listener")
		}
		t.Fatalf("health-server exited before reporting an address: code=%d error=%q read=%v", code, errorsOut.String(), err)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	for _, probe := range []struct {
		path string
		want int
	}{
		{path: "/health/live", want: http.StatusOK},
		{path: "/health/ready", want: http.StatusServiceUnavailable},
	} {
		response, err := client.Get("http://" + strings.TrimSpace(address) + probe.path)
		if err != nil {
			t.Fatalf("GET %s: %v", probe.path, err)
		}
		_ = response.Body.Close()
		if response.StatusCode != probe.want {
			t.Fatalf("%s status=%d, want %d", probe.path, response.StatusCode, probe.want)
		}
	}

	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("health-server code=%d error=%q", code, errorsOut.String())
	}
}

func exampleConfigPath(t *testing.T) string {
	t.Helper()
	return filepath.Join("..", "..", "config.example.yaml")
}

func TestConfigCommandsDoNotPrintSecretValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data, err := os.ReadFile(exampleConfigPath(t))
	if err != nil {
		t.Fatal(err)
	}
	secret := "adversarial-secret-value"
	data = append(data, []byte("\n# "+secret+"\n")...)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range [][]string{{"validate-config", "--config", path}, {"print-effective-config", "--config", path}} {
		var output, errorsOut bytes.Buffer
		code := Execute(context.Background(), command, CommandOptions{Out: &output, ErrOut: &errorsOut})
		if code != 0 {
			t.Fatalf("command %v code=%d error=%s", command, code, errorsOut.String())
		}
		if strings.Contains(output.String(), secret) || strings.Contains(errorsOut.String(), secret) {
			t.Fatalf("command %v leaked secret: out=%q err=%q", command, output.String(), errorsOut.String())
		}
	}
}

func TestWorkerCommandUsesPathAwareRuntime(t *testing.T) {
	path := exampleConfigPath(t)
	workerCalled := false
	var output, errorsOut bytes.Buffer
	code := Execute(context.Background(), []string{"worker", "--config", path}, CommandOptions{
		Out: &output, ErrOut: &errorsOut,
		RunWorkerFile: func(_ context.Context, gotPath string, data []byte, _ io.Writer) error {
			workerCalled = gotPath == path && len(data) > 0
			return nil
		},
	})
	if code != 0 || !workerCalled {
		t.Fatalf("worker code=%d called=%v errors=%s", code, workerCalled, errorsOut.String())
	}
}

func TestUnavailableReconcileCommandIsNotAdvertised(t *testing.T) {
	var output, errorsOut bytes.Buffer
	code := Execute(context.Background(), []string{"reconcile", "--operation-id", "op-safe"}, CommandOptions{Out: &output, ErrOut: &errorsOut})
	if code != 2 || !strings.Contains(errorsOut.String(), "unknown command") {
		t.Fatalf("reconcile command code=%d output=%q", code, errorsOut.String())
	}
	output.Reset()
	if code = Execute(context.Background(), []string{"help"}, CommandOptions{Out: &output, ErrOut: &errorsOut}); code != 0 || strings.Contains(output.String(), "reconcile") {
		t.Fatalf("usage code=%d output=%q", code, output.String())
	}
}

func TestCommandFailuresAreRedacted(t *testing.T) {
	var output, errorsOut bytes.Buffer
	code := Execute(context.Background(), []string{"worker", "--config", exampleConfigPath(t)}, CommandOptions{
		Out: &output, ErrOut: &errorsOut,
		RunWorker: func(context.Context, []byte, io.Writer) error { return errors.New("secret-token-value") },
	})
	if code != 1 || strings.Contains(errorsOut.String(), "secret-token-value") {
		t.Fatalf("failure code=%d output=%q", code, errorsOut.String())
	}
}

func TestHealthcheckCommandProbesEveryConfiguredURL(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.URL.Path)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	var output, errorsOut bytes.Buffer
	code := Execute(context.Background(), []string{
		"healthcheck",
		"--url", server.URL + "/health/live",
		"--url", server.URL + "/health/ready",
	}, CommandOptions{Out: &output, ErrOut: &errorsOut})
	if code != 0 {
		t.Fatalf("healthcheck code=%d error=%q", code, errorsOut.String())
	}
	if want := []string{"/health/live", "/health/ready"}; !reflect.DeepEqual(paths, want) {
		t.Fatalf("healthcheck paths=%#v, want %#v", paths, want)
	}
}

func TestHealthcheckCommandFailsWhenAProbeIsUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	var errorsOut bytes.Buffer
	code := Execute(context.Background(), []string{"healthcheck", "--url", server.URL + "/health/ready"}, CommandOptions{ErrOut: &errorsOut})
	if code != 1 {
		t.Fatalf("healthcheck code=%d error=%q", code, errorsOut.String())
	}
	if !strings.Contains(errorsOut.String(), "healthcheck failed") {
		t.Fatalf("healthcheck failure=%q", errorsOut.String())
	}
}

func TestConfigCommandsReportValidationAndReferenceFailures(t *testing.T) {
	example, err := os.ReadFile(exampleConfigPath(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		from, to string
		resolver config.ReferenceResolver
		want     string
		hidden   string
	}{
		{name: "output limit", from: "max_output_tokens: 32768", to: "max_output_tokens: -1", want: "limits.max_output_tokens"},
		{name: "token ratio", from: `token_estimate_safety_ratio: "1.35"`, to: `token_estimate_safety_ratio: "0"`, want: "token_estimate_safety_ratio"},
		{name: "secret reference", resolver: config.ReferenceResolverFunc(func(context.Context, *config.Config) error {
			return diagnostic.Safe(`secret reference 0 could not be resolved: environment secret "REDIS_USERNAME" is not set`, errors.New("raw"))
		}), want: `environment secret "REDIS_USERNAME" is not set`},
		{name: "unclassified reference", resolver: config.ReferenceResolverFunc(func(context.Context, *config.Config) error {
			return errors.New(`reference "sk-live-secret-value" rejected`)
		}), want: "reference <value> rejected", hidden: "sk-live-secret-value"},
		{name: "enum value", from: "level: info", to: "level: sk-live-secret-value", want: "telemetry.logs.level", hidden: "sk-live-secret-value"},
		{name: "yaml value", from: "max_output_tokens: 32768", to: "max_output_tokens: sk-live-secret-value", want: "configuration YAML", hidden: "sk-live-secret-value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := string(example)
			if test.from != "" {
				data = strings.Replace(data, test.from, test.to, 1)
				if data == string(example) {
					t.Fatalf("example does not contain %q", test.from)
				}
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			for _, command := range []string{"validate-config", "worker"} {
				var output, errorsOut bytes.Buffer
				code := Execute(context.Background(), []string{command, "--config", path}, CommandOptions{Out: &output, ErrOut: &errorsOut, Resolver: test.resolver,
					RunWorker: func(context.Context, []byte, io.Writer) error { return nil }})
				if code != 1 || !strings.Contains(errorsOut.String(), test.want) {
					t.Fatalf("%s code=%d error=%q, want %q", command, code, errorsOut.String(), test.want)
				}
				if test.hidden != "" && strings.Contains(errorsOut.String(), test.hidden) {
					t.Fatalf("%s echoed a YAML value: %q", command, errorsOut.String())
				}
			}
		})
	}
}

func TestWorkerCommandShowsOperatorSafeRuntimeDiagnostics(t *testing.T) {
	var output, errorsOut bytes.Buffer
	code := Execute(context.Background(), []string{"worker", "--config", exampleConfigPath(t)}, CommandOptions{
		Out: &output, ErrOut: &errorsOut,
		RunWorker: func(context.Context, []byte, io.Writer) error {
			return fmt.Errorf("build configuration snapshot: %w", diagnostic.Safe(`secret reference 0 could not be resolved: environment secret "REDIS_USERNAME" is not set`, errors.New("raw cause")))
		},
	})
	if code != 1 || strings.TrimSpace(errorsOut.String()) != `secret reference 0 could not be resolved: environment secret "REDIS_USERNAME" is not set` {
		t.Fatalf("worker code=%d error=%q", code, errorsOut.String())
	}
}

func TestWriteConfigErrorRedactsQuotedInputValues(t *testing.T) {
	var output bytes.Buffer
	writeConfigError(&output, fmt.Errorf(`state.kind %q is unsupported`, `sk-live "quoted" secret`))
	if got := strings.TrimSpace(output.String()); got != "state.kind <value> is unsupported" {
		t.Fatalf("writeConfigError() = %q", got)
	}
}

func TestConfigCommandsRejectWorkloadIdentityTheBinaryCannotResolve(t *testing.T) {
	example, err := os.ReadFile(exampleConfigPath(t))
	if err != nil {
		t.Fatal(err)
	}
	data := strings.Replace(string(example), "    password:\n      kind: file\n      path: /var/run/secrets/redis-password", "    password:\n      kind: workload_identity\n      audience: redis", 1)
	if data == string(example) {
		t.Fatal("example Redis password reference not found")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"validate-config", "worker"} {
		var output, errorsOut bytes.Buffer
		code := Execute(context.Background(), []string{command, "--config", path}, CommandOptions{Out: &output, ErrOut: &errorsOut,
			RunWorker: func(context.Context, []byte, io.Writer) error { return nil }})
		if code != 1 || !strings.Contains(errorsOut.String(), "state.redis.password uses workload_identity") {
			t.Fatalf("%s code=%d error=%q", command, code, errorsOut.String())
		}
	}
}

// Every row is a setting that validate-config used to accept and that then
// failed at worker start or on every matching request.
func TestValidateConfigRejectsSettingsThatFailAfterValidation(t *testing.T) {
	example, err := os.ReadFile(exampleConfigPath(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ name, old, replacement, want string }{
		{"window bucket count", "bucket: 1h", "bucket: 1s", "budgets.policies[0].windows[2] needs 2592002 buckets"},
		{"limit above the Redis range", `limit_usd: "25.000000000000000000"`, `limit_usd: "99999999999999999999"`, "budgets.policies[0].windows[0].limit_usd must not exceed"},
		{"policy ID of 127 bytes", "id: acme-production", "id: " + strings.Repeat("p", 127), "budgets.policies[0].windows[0] identity"},
		{"limit below one nano-USD", `limit_usd: "25.000000000000000000"`, `limit_usd: "0.0000000001"`, "budgets.policies[0].windows[0].limit_usd must be at least"},
		{"second document", "", "\n---\nbudgets:\n  require_match: false\n", "configuration YAML"},
	} {
		t.Run(row.name, func(t *testing.T) {
			if !strings.Contains(string(example), row.old) {
				t.Fatalf("example configuration no longer contains %q", row.old)
			}
			data := strings.Replace(string(example), row.old, row.replacement, 1)
			if row.old == "" {
				data = string(example) + row.replacement
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			var output, errorsOut bytes.Buffer
			code := Execute(context.Background(), []string{"validate-config", "--config", path}, CommandOptions{Out: &output, ErrOut: &errorsOut})
			if code != 1 || !strings.Contains(errorsOut.String(), row.want) {
				t.Fatalf("validate-config code=%d out=%q error=%q, want error naming %q", code, output.String(), errorsOut.String(), row.want)
			}
		})
	}
}
