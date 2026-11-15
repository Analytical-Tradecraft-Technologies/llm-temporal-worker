package runtime

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/secrets"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/redis/go-redis/v9"
)

// A reload of byte-identical configuration still re-reads every file secret
// and rebuilds the snapshot's clients: App.Reload has no unchanged-digest
// short-circuit. Replacing a secret file and sending SIGHUP is therefore the
// rotation path for reloadable credentials (#1559). The test uses the real
// file resolver and the production factory's Redis and continuation-keyring
// construction.
func TestReloadOfUnchangedConfigRebuildsClientsFromRotatedSecretFiles(t *testing.T) {
	dir := t.TempDir()
	writeSecret := func(name, value string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		// Replace the file atomically, as a Kubernetes Secret volume does.
		temporary := path + ".tmp"
		if err := os.WriteFile(temporary, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(temporary, path); err != nil {
			t.Fatal(err)
		}
		return path
	}
	passwordPath := writeSecret("redis-password", "password-before-rotation")
	handlePath := writeSecret("continuation-key", strings.Repeat("a", 32))
	keySecretPath := writeSecret("redis-key-secret", strings.Repeat("k", 32))
	data := replacementTestConfig(t, func(value *config.Config) {
		value.State.Redis.Password = config.SecretRef{Kind: config.SecretFile, Path: passwordPath}
		value.State.Redis.KeySecret = config.SecretRef{Kind: config.SecretFile, Path: keySecretPath}
		value.Continuation.HandleKeys = []config.HandleKey{{ID: "key-2026-07", Primary: true, Secret: config.SecretRef{Kind: config.SecretFile, Path: handlePath}}}
	})
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatal(err)
	}

	// Environment references answer with a fixed value; file references are
	// read from disk by the default reader.
	resolver := secrets.New(secrets.Options{LookupEnv: func(name string) (string, bool) { return "env-" + name, true }})
	type built struct {
		password string
		keyring  *state.Keyring
		closed   *atomic.Bool
	}
	var builds []built
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		Resolver: resolver,
		RedisFactory: func(_ context.Context, _ config.RedisConfig, _, password string) (redis.UniversalClient, error) {
			builds = append(builds, built{password: password, closed: &atomic.Bool{}})
			return redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), nil
		},
	}}
	application := newTestApp(t, app.Options{
		InitialConfig:        data,
		Builder:              app.SnapshotBuilder{References: secrets.ConfigResolver{Resolver: resolver}},
		ReplacementValidator: validateRuntimeReplacement,
		Clients: func(ctx context.Context, snapshot *config.Snapshot) (app.ClientSet, error) {
			client, _, err := factory.buildRedis(ctx, snapshot.Config())
			if err != nil {
				return nil, err
			}
			keyring, err := factory.continuationKeyring(ctx, snapshot.Config())
			if err != nil {
				_ = client.Close()
				return nil, err
			}
			current := &builds[len(builds)-1]
			current.keyring = keyring
			closed := current.closed
			return app.ClientSetFunc(func(context.Context) error {
				closed.Store(true)
				return client.Close()
			}), nil
		},
	})
	before := application.Current()
	handle, err := builds[0].keyring.Issue("tenant")
	if err != nil {
		t.Fatal(err)
	}

	writeSecret("redis-password", "password-after-rotation")
	writeSecret("continuation-key", strings.Repeat("b", 32))
	if err := application.ReloadFile(context.Background(), configPath); err != nil {
		t.Fatalf("reload of unchanged configuration failed: %v", err)
	}

	after := application.Current()
	if after == before {
		t.Fatal("reload of unchanged configuration kept the previous snapshot and its clients")
	}
	if after.Config.ConfigVersion() != before.Config.ConfigVersion() {
		t.Fatal("fixture changed the configuration digest")
	}
	if len(builds) != 2 {
		t.Fatalf("client builds = %d, want 2", len(builds))
	}
	if builds[0].password != "password-before-rotation" || builds[1].password != "password-after-rotation" {
		t.Fatalf("Redis passwords = %q, %q; want the rotated file contents", builds[0].password, builds[1].password)
	}
	if err := before.WaitClosed(context.Background()); err != nil || !builds[0].closed.Load() {
		t.Fatalf("previous clients closed=%t err=%v, want them drained after the swap", builds[0].closed.Load(), err)
	}
	if builds[1].closed.Load() {
		t.Fatal("replacement clients were closed")
	}
	// The replacement keyring holds the new bytes under the same key ID, so a
	// handle signed before the reload no longer verifies. That is why handle
	// keys rotate by adding a new ID, never by changing the bytes behind one.
	if _, err := builds[1].keyring.Verify("tenant", handle); !errors.Is(err, state.ErrInvalidHandle) {
		t.Fatalf("handle signed with the replaced bytes verified: %v", err)
	}
}

// A production worker logs one bounded warning per reloadable credential read
// from the environment, at startup and after each published reload (#1559).
func TestRuntimeWarnsAboutRotatableEnvSecretsAtStartupAndReload(t *testing.T) {
	const filePassword = "password:\n      kind: file\n      path: /var/run/secrets/redis-password"
	const envPassword = "password:\n      kind: env\n      name: ROTATED_REDIS_PASSWORD"
	fileData := runtimeConfig(t)
	if !bytes.Contains(fileData, []byte(filePassword)) {
		t.Fatalf("runtime configuration does not contain %q", filePassword)
	}
	envData := bytes.Replace(fileData, []byte(filePassword), []byte(envPassword), 1)
	controller := &testWorker{}
	var closed atomic.Bool
	options := testRuntimeOptions(t, controller, &closed)
	var logs bytes.Buffer
	options.LogOutput = &logs
	runtime := newTestRuntime(t, context.Background(), envData, options)
	warnings := func() int {
		t.Helper()
		if strings.Contains(logs.String(), "ROTATED_REDIS_PASSWORD") {
			t.Fatalf("warning named the environment variable: %q", logs.String())
		}
		return strings.Count(logs.String(), envSecretWarning)
	}
	if got := warnings(); got != 1 || !strings.Contains(logs.String(), `"config_field":"state.redis.password"`) {
		t.Fatalf("startup warnings = %d, logs = %q; want one naming state.redis.password", got, logs.String())
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	for _, step := range []struct {
		data []byte
		want int
	}{{data: fileData, want: 1}, {data: envData, want: 2}} {
		if err := os.WriteFile(path, step.data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := runtime.ReloadFile(context.Background(), path); err != nil {
			t.Fatalf("reload failed: %v", err)
		}
		if got := warnings(); got != step.want {
			t.Fatalf("warnings after reload = %d, want %d", got, step.want)
		}
	}
}
