package config_test

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"testing"
)

func TestRedisRequiresIndependentIdentityReference(t *testing.T) {
	value := mustLoadConfig(t, exampleYAML(t))
	for _, ref := range []config.SecretRef{{}, {Kind: config.SecretEnv, Name: "invalid-name"}, {Kind: config.SecretFile, Path: "relative"}} {
		value.State.Redis.KeySecret = ref
		testassert.ErrorContains(t, value.Validate(), "key_secret")
	}
}
