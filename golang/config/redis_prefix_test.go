package config_test

import (
	"os"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
)

func withoutRedisPrefixOverride(t *testing.T) {
	t.Helper()
	value, present := os.LookupEnv("LLMTW_REDIS_KEY_PREFIX")
	if err := os.Unsetenv("LLMTW_REDIS_KEY_PREFIX"); err != nil {
		t.Fatal(err)
	}
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
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := loaded.State.Redis.KeyPrefix, "llmtw"; got != want {
		t.Fatalf("default Redis key prefix = %q, want %q", got, want)
	}
}

func TestRedisKeyPrefixEnvironmentOverrideIsEffectiveBeforeValidation(t *testing.T) {
	t.Setenv("LLMTW_REDIS_KEY_PREFIX", "tenant-a.v1")
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := loaded.State.Redis.KeyPrefix, "tenant-a.v1"; got != want {
		t.Fatalf("environment Redis key prefix = %q, want %q", got, want)
	}
	snapshot, err := config.Compile(t.Context(), exampleYAML(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(snapshot.Canonical()), `"key_prefix":"tenant-a.v1"`) {
		t.Fatalf("effective config omitted overridden Redis key prefix: %s", snapshot.Canonical())
	}
}

func TestRedisKeyPrefixRejectsAmbiguousValues(t *testing.T) {
	for _, value := range []string{"", ":bad", "{bad}", "bad prefix", "bad\tvalue", "bad\nvalue", "-leading", "é", strings.Repeat("a", 65)} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("LLMTW_REDIS_KEY_PREFIX", value)
			if _, err := config.Load(exampleYAML(t)); err == nil || !strings.Contains(err.Error(), "state.redis.key_prefix") {
				t.Fatalf("prefix %q accepted or wrong error: %v", value, err)
			}
		})
	}
}
