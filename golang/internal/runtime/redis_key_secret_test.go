package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/secrets"
	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
)

func TestRedisCredentialRotationPreservesBudgetManifestIdentity(t *testing.T) {
	// Existing namespaces can pin the old derived bytes before rotating ACLs.
	legacy := sha256.Sum256([]byte("llmtw:redis-key-v1:old-password"))
	stable := append([]byte(nil), legacy[:]...)
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{Resolver: secrets.ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
		if ref.Name != "STABLE_REDIS_IDENTITY" {
			t.Fatalf("identity used authentication reference: %+v", ref)
		}
		return stable, nil
	})}}
	value := config.Config{State: config.StateConfig{Redis: config.RedisConfig{KeyPrefix: "worker", AdmissionHashTag: "admission", KeySecret: config.SecretRef{Kind: config.SecretEnv, Name: "STABLE_REDIS_IDENTITY"}}}}
	var manifest string
	for _, password := range []string{"OLD_PASSWORD", "NEW_PASSWORD"} {
		value.State.Redis.Password = config.SecretRef{Kind: config.SecretEnv, Name: password}
		secret, err := factory.redisKeySecret(context.Background(), value)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(secret, legacy[:]) {
			t.Fatal("existing identity changed")
		}
		options, err := redisKeyOptions(value, secret)
		if err != nil {
			t.Fatal(err)
		}
		keys, err := redisstore.NewBudgetKeySpace(options)
		if err != nil {
			t.Fatal(err)
		}
		key := keys.ManifestKey("generation-1")
		if manifest != "" && key != manifest {
			t.Fatal("password rotation changed manifest address")
		}
		manifest = key
		secret[0] ^= 1
		if !bytes.Equal(stable, legacy[:]) {
			t.Fatal("returned secret aliases resolver buffer")
		}
	}
}

func TestRedisIdentitySecretFailsClosed(t *testing.T) {
	value := config.Config{State: config.StateConfig{Redis: config.RedisConfig{KeySecret: config.SecretRef{Kind: config.SecretEnv, Name: "STABLE_REDIS_IDENTITY"}}}}
	for _, size := range []int{0, 31} {
		factory := &ProductionEngineFactory{options: ProductionFactoryOptions{Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) { return bytes.Repeat([]byte{1}, size), nil })}}
		if _, err := factory.redisKeySecret(context.Background(), value); !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("size=%d accepted: %v", size, err)
		}
	}
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
		t.Fatal("missing reference must not resolve credentials")
		return nil, nil
	})}}
	value.State.Redis.KeySecret = config.SecretRef{}
	if _, err := factory.redisKeySecret(context.Background(), value); err == nil {
		t.Fatal("missing stable identity accepted")
	}
	factory.options.RedisKeySecret = []byte("short")
	if _, err := factory.redisKeySecret(context.Background(), value); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("short injected key accepted: %v", err)
	}
}
