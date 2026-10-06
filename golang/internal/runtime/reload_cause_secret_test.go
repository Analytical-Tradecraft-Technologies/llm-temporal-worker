package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/secrets"
)

// rejectedReload publishes a valid snapshot, then reloads with the given
// reference resolver and client builder and returns the rejection.
func rejectedReload(t *testing.T, references config.ReferenceResolver, clients func(context.Context, *config.Snapshot) (app.ClientSet, error)) error {
	t.Helper()
	reloading := false
	application, err := app.New(context.Background(), app.Options{
		InitialConfig: replacementTestConfig(t, func(*config.Config) {}),
		Builder: app.SnapshotBuilder{References: config.ReferenceResolverFunc(func(ctx context.Context, value *config.Config) error {
			if !reloading || references == nil {
				return nil
			}
			return references.Resolve(ctx, value)
		})},
		Clients: func(ctx context.Context, snapshot *config.Snapshot) (app.ClientSet, error) {
			if reloading && clients != nil {
				return clients(ctx, snapshot)
			}
			return app.ClientSetFunc(func(context.Context) error { return nil }), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = application.Close(context.Background()) })
	reloading = true
	err = application.Reload(context.Background(), replacementTestConfig(t, func(value *config.Config) { value.Limits.Items++ }))
	if err == nil {
		t.Fatal("reload unexpectedly succeeded")
	}
	return err
}

// Provider auth names, the Redis key secret, continuation keys and the cloud
// request secret are resolved by the production factory while clients are
// built, not by ConfigResolver. A missing one is a secret cause, not a
// dependency outage.
func TestReloadClassifiesFactoryResolvedSecretsAsSecret(t *testing.T) {
	missing := errors.New("environment secret \"OPENAI_API_KEY\" is not set")
	for _, cause := range []struct {
		name    string
		resolve error
		want    string
	}{
		{name: "missing", resolve: missing, want: "secret"},
		{name: "canceled", resolve: context.Canceled, want: "canceled"},
	} {
		factory := &ProductionEngineFactory{options: ProductionFactoryOptions{Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
			return nil, cause.resolve
		})}}
		ref := config.SecretRef{Kind: config.SecretEnv, Name: "OPENAI_API_KEY"}
		sites := map[string]func(context.Context) error{
			"provider auth": func(ctx context.Context) error {
				_, err := factory.providerSecret(ctx, config.AuthConfig{Kind: "bearer_env", Name: ref.Name}, "endpoint")
				return err
			},
			"Redis key secret": func(ctx context.Context) error {
				_, err := factory.redisKeySecret(ctx, config.Config{State: config.StateConfig{Redis: config.RedisConfig{KeySecret: ref}}})
				return err
			},
			"continuation key": func(ctx context.Context) error {
				_, err := factory.continuationKeyring(ctx, config.Config{Continuation: config.ContinuationConfig{HandleKeys: []config.HandleKey{{ID: "key", Primary: true, Secret: ref}}}})
				return err
			},
			"cloud request secret": func(ctx context.Context) error {
				_, err := factory.buildCloudRequests(ctx, testCloudConfig())
				return err
			},
		}
		for site, build := range sites {
			t.Run(cause.name+"/"+site, func(t *testing.T) {
				direct := build(context.Background())
				if !errors.Is(direct, secrets.ErrReference) {
					t.Fatalf("factory error = %v, want it marked as an unresolved secret reference", direct)
				}
				if site == "provider auth" && direct.Error() != cause.resolve.Error() {
					t.Fatalf("marking changed the message: %q", direct.Error())
				}
				// The cloud request secret deliberately discards its cause.
				want := cause.want
				if site == "cloud request secret" {
					want = "secret"
				}
				err := rejectedReload(t, nil, func(ctx context.Context, _ *config.Snapshot) (app.ClientSet, error) {
					return nil, &engineFactoryError{cause: build(ctx)}
				})
				if got, field := classifyReloadFailure(err); got != want || field != "" {
					t.Fatalf("classifyReloadFailure(%v) = %q, %q; want %q", err, got, field, want)
				}
			})
		}
	}
}

// A reload canceled while ConfigResolver is resolving a reference was
// canceled; the reference is not at fault.
func TestReloadClassifiesCanceledSecretResolutionAsCanceled(t *testing.T) {
	for _, test := range []struct {
		resolve error
		want    string
	}{
		{resolve: context.Canceled, want: "canceled"},
		{resolve: context.DeadlineExceeded, want: "canceled"},
		{resolve: errors.New("environment secret \"REDIS_PASSWORD\" is not set"), want: "secret"},
	} {
		references := secrets.ConfigResolver{Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
			return nil, test.resolve
		})}
		err := rejectedReload(t, references, nil)
		if !errors.Is(err, secrets.ErrReference) {
			t.Fatalf("reload error = %v, want the secret reference marker", err)
		}
		if got, field := classifyReloadFailure(err); got != test.want || field != "" {
			t.Fatalf("classifyReloadFailure(%v) = %q, %q; want %q", err, got, field, test.want)
		}
	}
}

// Memory state ignores the Redis section, so a change there names no durable
// identity and must not block a reload.
func TestRuntimeReplacementValidatorIgnoresRedisIdentityInMemoryState(t *testing.T) {
	memory := func(edit func(*config.Config)) *config.Snapshot {
		t.Helper()
		snapshot, err := config.Compile(context.Background(), replacementTestConfig(t, func(value *config.Config) {
			value.State.Kind, value.State.Requests = config.StateKindMemory, nil
			value.BlobStore = config.BlobStoreConfig{Kind: "memory", InlineBytes: value.BlobStore.InlineBytes}
			edit(value)
		}), nil)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	current := memory(func(*config.Config) {})
	replacement := memory(func(value *config.Config) {
		value.State.Redis.AdmissionHashTag = "admission-b"
		value.State.Redis.KeySecret.Path = "/var/run/secrets/llmtw/other-key"
	})
	if current.ConfigVersion() == replacement.ConfigVersion() {
		t.Fatal("fixture did not change the configuration")
	}
	if err := validateRuntimeReplacement(current, replacement); err != nil {
		t.Fatalf("validateRuntimeReplacement() rejected ignored Redis settings in memory state: %v", err)
	}
}
