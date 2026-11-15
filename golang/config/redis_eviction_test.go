package config_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

func TestDurableEphemeralRedisPolicy(t *testing.T) {
	data := string(exampleYAML(t))
	data = strings.Replace(data, "required_persistence: aof_and_rdb", "required_persistence: none\n    maxmemory_policy: volatile-lru", 1)
	snapshot := mustCompile(t, []byte(data))
	if got := snapshot.Config().State.Redis; got.RequiredPersistence != "none" || got.EffectiveMaxMemoryPolicy() != "volatile-lru" {
		t.Fatalf("ephemeral Redis policy was not preserved: %#v", got)
	}
	for _, policy := range []string{"allkeys-lru", "allkeys-lfu", "allkeys-random", "volatile-random", "unknown"} {
		requireLoadRejected(t, []byte(strings.Replace(data, "maxmemory_policy: volatile-lru", "maxmemory_policy: "+policy, 1)), "accepted unsafe/unsupported eviction policy %q", policy)
	}
	fixture := strings.Replace(data, "kind: durable", "kind: redis", 1)
	fixture = strings.Replace(fixture, cloudRequestSettings, "", 1)
	fixture = strings.Replace(fixture, "environment: production", "environment: development", 1)
	testassert.ErrorContains(t, testassert.Err(config.Load([]byte(fixture))), "maxmemory_policy")
}

func TestRedisEvictionDefaultsAndSnapshotDigest(t *testing.T) {
	data := string(exampleYAML(t))
	base := mustCompile(t, []byte(data))
	if base.Config().State.Redis.EffectiveMaxMemoryPolicy() != "noeviction" {
		t.Fatal("existing configuration eviction policy changed")
	}
	data = strings.Replace(data, "required_persistence: aof_and_rdb", "required_persistence: aof_and_rdb\n    maxmemory_policy: volatile-lru", 1)
	changed, err := config.Compile(context.Background(), []byte(data), nil)
	if err != nil || changed.Digest() == base.Digest() {
		t.Fatal("Redis policy not included in immutable configuration digest", err)
	}
}
