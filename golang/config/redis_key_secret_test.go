package config_test

import (
	"github.com/mfow/llm-temporal-worker/golang/config"
	"strings"
	"testing"
)

func TestRedisRequiresIndependentIdentityReference(t *testing.T) {
	value, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range []config.SecretRef{{}, {Kind: config.SecretEnv, Name: "invalid-name"}, {Kind: config.SecretFile, Path: "relative"}} {
		value.State.Redis.KeySecret = ref
		if err := value.Validate(); err == nil || !strings.Contains(err.Error(), "key_secret") {
			t.Fatalf("invalid identity reference accepted: %v", err)
		}
	}
}
