package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// reloadFailureRecord reloads replacement into a running runtime, requires it
// to be rejected without publishing, and returns the one failure log record.
func reloadFailureRecord(t *testing.T, runtime *Runtime, logs *bytes.Buffer, replacement string) map[string]any {
	t.Helper()
	old := runtime.App.Current()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(replacement), 0o600); err != nil {
		t.Fatal(err)
	}
	logs.Reset()
	if err := runtime.ReloadFile(context.Background(), path); err == nil || err.Error() != "reload configuration failed" {
		t.Fatalf("reload error = %v, want sanitized rejection", err)
	}
	if runtime.App.Current() != old {
		t.Fatal("rejected replacement published a new snapshot")
	}
	if strings.Contains(logs.String(), path) {
		t.Fatalf("reload log leaked the configuration path: %q", logs.String())
	}
	var found map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if record["msg"] == "configuration reload failed" {
			if found != nil {
				t.Fatalf("more than one reload failure record: %q", logs.String())
			}
			found = record
		}
	}
	if found == nil || found["outcome"] != "failure" || found["error_code"] != "internal" {
		t.Fatalf("reload failure record = %v (logs %q)", found, logs.String())
	}
	return found
}

func replaceOnce(t *testing.T, data, old, replacement string) string {
	t.Helper()
	if strings.Count(data, old) != 1 {
		t.Fatalf("fixture contains %q %d times, want once", old, strings.Count(data, old))
	}
	return strings.Replace(data, old, replacement, 1)
}

func TestRuntimeReloadFailureLogNamesCauseAndFieldNeverValue(t *testing.T) {
	controller := &testWorker{}
	var closed atomic.Bool
	options := testRuntimeOptions(t, controller, &closed)
	var logs bytes.Buffer
	options.LogOutput = &logs
	var secretFails atomic.Bool
	options.Resolver = secrets.ConfigResolver{Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
		if secretFails.Load() {
			return nil, errors.New("environment secret \"REDIS_PASSWORD\" is not set")
		}
		return []byte("resolved"), nil
	})}
	data := string(runtimeConfig(t))
	runtime, err := New(context.Background(), []byte(data), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })

	tests := []struct {
		name        string
		replacement string
		cause       string
		field       string
		secret      string
	}{
		{name: "process-lifetime namespace", cause: "process_lifetime", field: "state.requests.namespace", secret: "other-namespace-value",
			replacement: replaceOnce(t, data, "namespace: worker-v1", "namespace: other-namespace-value")},
		{name: "process-lifetime task queue", cause: "process_lifetime", field: "temporal.task_queue", secret: "restart-required-queue",
			replacement: replaceOnce(t, data, "task_queue: llm-inference", "task_queue: restart-required-queue")},
		{name: "validation", cause: "validation", field: "state.redis.key_prefix", secret: "misplaced credential",
			replacement: replaceOnce(t, data, "key_prefix: llmtw", "key_prefix: \"misplaced credential\"")},
		{name: "validation of a quoted value", cause: "validation", field: "state.redis.admission_mode", secret: "sk-misplaced-credential",
			replacement: replaceOnce(t, data, "admission_mode: function", "admission_mode: sk-misplaced-credential")},
		{name: "yaml unknown field", cause: "yaml", secret: "not-a-valid-config",
			replacement: data + "\nnot-a-valid-config: sk-misplaced-credential\n"},
		{name: "yaml syntax", cause: "yaml", secret: "sk-misplaced-credential",
			replacement: "version: [sk-misplaced-credential\n"},
	}
	causes := map[string]struct{}{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := reloadFailureRecord(t, runtime, &logs, test.replacement)
			if record["cause"] != test.cause {
				t.Fatalf("cause = %v, want %q (record %v)", record["cause"], test.cause, record)
			}
			if test.field == "" {
				if _, present := record["config_field"]; present {
					t.Fatalf("unexpected config_field in %v", record)
				}
			} else if record["config_field"] != test.field {
				t.Fatalf("config_field = %v, want %q", record["config_field"], test.field)
			}
			if strings.Contains(logs.String(), test.secret) {
				t.Fatalf("reload log leaked an input value: %q", logs.String())
			}
			causes[test.cause] = struct{}{}
		})
	}
	t.Run("secret", func(t *testing.T) {
		secretFails.Store(true)
		defer secretFails.Store(false)
		record := reloadFailureRecord(t, runtime, &logs, replaceOnce(t, data, "items: 512", "items: 513"))
		if _, present := record["config_field"]; record["cause"] != "secret" || present {
			t.Fatalf("secret reference failure record = %v", record)
		}
		if strings.Contains(logs.String(), "REDIS_PASSWORD") {
			t.Fatalf("reload log leaked a reference name: %q", logs.String())
		}
		causes["secret"] = struct{}{}
	})
	if len(causes) != 4 {
		t.Fatalf("distinct causes = %v, want 4", causes)
	}
	if got := reloadOutcome(t, runtime.Metrics, "failure"); got != float64(len(tests)+1) {
		t.Fatalf("reload failure metric = %v, want %d", got, len(tests)+1)
	}
}

// The catalog digest is checked by the production snapshot loader while the
// replacement's clients are built, after validate-config would have accepted
// the file. The log must still name the cause.
func TestRuntimeReloadFailureLogNamesCatalogDigestMismatch(t *testing.T) {
	directory := t.TempDir()
	data := localCatalogConfig(t, directory)
	controller := &testWorker{}
	var closed atomic.Bool
	options := testRuntimeOptions(t, controller, &closed)
	var logs bytes.Buffer
	options.LogOutput = &logs
	inner := options.EngineFactory
	options.EngineFactory = EngineFactoryFunc(func(ctx context.Context, snapshot *config.Snapshot) (llm.Engine, app.ClientSet, error) {
		if _, err := (CatalogSnapshotLoader{}).Load(ctx, snapshot); err != nil {
			return nil, nil, fmt.Errorf("load engine snapshot: %w", err)
		}
		return inner.Build(ctx, snapshot)
	})
	runtime, err := New(context.Background(), []byte(data), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })

	pricesDigest := "aff38b31219810e9acc50d6b790a7b570e74fd84b2716a72c75435a7f9faecda"
	wrongDigest := strings.Repeat("ab", 32)
	record := reloadFailureRecord(t, runtime, &logs, replaceOnce(t, data, pricesDigest, wrongDigest))
	if _, present := record["config_field"]; record["cause"] != "catalog" || present {
		t.Fatalf("catalog digest mismatch record = %v", record)
	}
	for _, leaked := range []string{directory, wrongDigest, pricesDigest, "prices.yaml"} {
		if strings.Contains(logs.String(), leaked) {
			t.Fatalf("reload log leaked %q: %q", leaked, logs.String())
		}
	}
	// A catalog file that disappears is the same class, not a config read error.
	if err := os.Remove(filepath.Join(directory, "capabilities.yaml")); err != nil {
		t.Fatal(err)
	}
	record = reloadFailureRecord(t, runtime, &logs, replaceOnce(t, data, "request_bytes: 1048576", "request_bytes: 1048575"))
	if record["cause"] != "catalog" {
		t.Fatalf("missing catalog record = %v", record)
	}
}

// localCatalogConfig returns deploy/local's configuration with its catalogs
// copied under directory, so the real loader authenticates real files.
func localCatalogConfig(t *testing.T, directory string) string {
	t.Helper()
	read := func(name string) []byte {
		data, err := os.ReadFile(filepath.Join("../../deploy/local", name))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	data := string(read("config.yaml"))
	for _, name := range []string{"capabilities.yaml", "prices.yaml"} {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, read(name), 0o600); err != nil {
			t.Fatal(err)
		}
		data = replaceOnce(t, data, "/etc/llmtw/"+name, path)
	}
	return data
}

func TestClassifyReloadFailureIsBounded(t *testing.T) {
	stage := func(build error, validate func(*config.Snapshot, *config.Snapshot) error,
		clients func(context.Context, *config.Snapshot) (app.ClientSet, error), verify func(context.Context, *config.Snapshot, app.ClientSet) error) error {
		t.Helper()
		calls := 0
		application, err := app.New(context.Background(), app.Options{
			InitialConfig: replacementTestConfig(t, func(*config.Config) {}),
			Builder: app.SnapshotBuilder{References: config.ReferenceResolverFunc(func(context.Context, *config.Config) error {
				if calls++; calls > 1 {
					return build
				}
				return nil
			})},
			ReplacementValidator: validate,
			Clients: func(ctx context.Context, snapshot *config.Snapshot) (app.ClientSet, error) {
				if calls > 1 && clients != nil {
					return clients(ctx, snapshot)
				}
				return app.ClientSetFunc(func(context.Context) error { return nil }), nil
			},
			Verify: func(ctx context.Context, snapshot *config.Snapshot, set app.ClientSet) error {
				if calls > 1 && verify != nil {
					return verify(ctx, snapshot, set)
				}
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = application.Close(context.Background()) })
		err = application.Reload(context.Background(), replacementTestConfig(t, func(value *config.Config) { value.Limits.Items++ }))
		if err == nil {
			t.Fatal("reload unexpectedly succeeded")
		}
		return err
	}
	unavailable := errors.New("redis: dial tcp 10.0.0.1:6379: connection refused")
	tests := []struct {
		name  string
		err   error
		cause string
	}{
		{name: "unreadable file", err: fmt.Errorf("read configuration file: %w", &fs.PathError{Op: "open", Path: "/etc/llmtw/config.yaml", Err: fs.ErrNotExist}), cause: "read"},
		{name: "unstaged", err: errors.New("app is closed"), cause: "internal"},
		{name: "nil snapshot comparison", err: stage(nil, func(*config.Snapshot, *config.Snapshot) error { return errProcessLifetimeConfigurationChanged }, nil, nil), cause: "process_lifetime"},
		{name: "client construction", err: stage(nil, nil, func(context.Context, *config.Snapshot) (app.ClientSet, error) { return nil, unavailable }, nil), cause: "dependency"},
		{name: "dependency verification", err: stage(nil, nil, nil, func(context.Context, *config.Snapshot, app.ClientSet) error { return unavailable }), cause: "dependency"},
		{name: "dependency verification timeout", err: stage(nil, nil, nil, func(context.Context, *config.Snapshot, app.ClientSet) error { return context.DeadlineExceeded }), cause: "dependency"},
		{name: "canceled build", err: stage(context.Canceled, nil, nil, nil), cause: "canceled"},
		{name: "unclassified build", err: stage(errors.New("sk-misplaced-credential is wrong"), nil, nil, nil), cause: "validation"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cause, field := classifyReloadFailure(test.err)
			if cause != test.cause || field != "" {
				t.Fatalf("classifyReloadFailure(%v) = %q, %q; want %q with no field", test.err, cause, field, test.cause)
			}
		})
	}
}

func TestConfigFieldPathKeepsOnlySchemaNames(t *testing.T) {
	for message, want := range map[string]string{
		"state.redis.key_prefix must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}": "state.redis.key_prefix",
		"state.redis.admission_mode <value> is unsupported":                 "state.redis.admission_mode",
		"state.requests store mapping is invalid":                           "state.requests",
		"version must be <value>":                                           "version",
		"endpoints must not be empty":                                       "endpoints",
		// Operator-chosen map keys are not schema names.
		"endpoints.sk-misplaced-credential exceeds the 512-byte admission field limit": "endpoints.*",
		"endpoints.openai-prod.base_url must be https":                                 "endpoints.*.base_url",
		"models.gpt-4.1.routes[0].model is required":                                   "models.*",
		"models.summarizer.routes[12].model is required":                               "models.*.routes[12].model",
		"models.summarizer.routes[sk-misplaced].model is required":                     "models.*.routes",
		"budgets.policies[0].match must contain at least one restriction":              "budgets.policies[0].match",
		"state.redis.sk-misplaced-credential":                                          "state.redis",
		// Not a field path at all.
		"sk-misplaced-credential is wrong":        "",
		"configuration is empty":                  "",
		"use only one of budgets_json or budgets": "",
		"": "",
	} {
		if got := configFieldPath(message); got != want {
			t.Errorf("configFieldPath(%q) = %q, want %q", message, got, want)
		}
	}
}
