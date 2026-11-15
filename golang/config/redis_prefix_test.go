package config_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

func withoutRedisPrefixOverride(t *testing.T) {
	t.Helper()
	value, present := os.LookupEnv("LLMTW_REDIS_KEY_PREFIX")
	testassert.NoError(t, os.Unsetenv("LLMTW_REDIS_KEY_PREFIX"))
	t.Cleanup(func() {
		if present {
			_ = os.Setenv("LLMTW_REDIS_KEY_PREFIX", value)
		} else {
			_ = os.Unsetenv("LLMTW_REDIS_KEY_PREFIX")
		}
	})
}

func TestRedisKeyPrefixDefaultsToLlmtw(t *testing.T) {
	withoutRedisPrefixOverride(t)
	loaded := mustLoadConfig(t, exampleYAML(t))
	if got, want := loaded.State.Redis.KeyPrefix, "llmtw"; got != want {
		t.Fatalf("default Redis key prefix = %q, want %q", got, want)
	}
}

func TestRedisKeyPrefixEnvironmentOverrideIsEffectiveBeforeValidation(t *testing.T) {
	t.Setenv("LLMTW_REDIS_KEY_PREFIX", "tenant-a.v1")
	loaded := mustLoadConfig(t, exampleYAML(t))
	if got, want := loaded.State.Redis.KeyPrefix, "tenant-a.v1"; got != want {
		t.Fatalf("environment Redis key prefix = %q, want %q", got, want)
	}
	snapshot := mustCompile(t, exampleYAML(t))
	if !strings.Contains(string(snapshot.Canonical()), `"key_prefix":"tenant-a.v1"`) {
		t.Fatalf("effective config omitted overridden Redis key prefix: %s", snapshot.Canonical())
	}
}

func TestRedisKeyPrefixRejectsAmbiguousValues(t *testing.T) {
	for _, value := range []string{"", ":bad", "{bad}", "bad prefix", "bad\tvalue", "bad\nvalue", "-leading", "é", strings.Repeat("a", 65)} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("LLMTW_REDIS_KEY_PREFIX", value)
			testassert.ErrorContains(t, testassert.Err(config.Load(exampleYAML(t))), "state.redis.key_prefix", fmt.Sprintf("prefix %q accepted or wrong", value))
		})
	}
}
