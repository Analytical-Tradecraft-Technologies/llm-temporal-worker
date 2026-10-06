package config_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
)

func TestCompileAndSnapshotSourceAreAtomicAndRedacted(t *testing.T) {
	data := exampleYAML(t)
	first, err := config.Compile(context.Background(), data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.ConfigVersion()) != 64 || first.APIVersion() != config.APIVersion {
		t.Fatalf("snapshot identity = %q, %q", first.ConfigVersion(), first.APIVersion())
	}
	if string(first.Canonical()) == "" {
		t.Fatal("snapshot has no canonical representation")
	}
	source := config.NewSnapshotSource(first)
	bad := append(data, []byte("\nunknown: true\n")...)
	if err := source.Reload(context.Background(), bad, nil); err == nil {
		t.Fatal("invalid reload succeeded")
	}
	if source.Current() != first {
		t.Fatal("invalid reload replaced the prior snapshot")
	}
	copyOfConfig := first.Config()
	endpoint := copyOfConfig.Endpoints["openai-prod"]
	endpoint.ServiceClasses = nil
	copyOfConfig.Endpoints["openai-prod"] = endpoint
	if len(first.Config().Endpoints["openai-prod"].ServiceClasses) != 3 {
		t.Fatal("snapshot configuration was mutable through Config copy")
	}
	if containsSecret(first.Canonical()) {
		t.Fatal("snapshot canonical form contains a secret value")
	}
}

func TestCompileReferenceResolverFailureDoesNotPublish(t *testing.T) {
	_, err := config.Compile(context.Background(), exampleYAML(t), config.ReferenceResolverFunc(func(context.Context, *config.Config) error {
		return errors.New("catalog unavailable")
	}))
	if err == nil {
		t.Fatal("reference resolver failure was ignored")
	}
}

func containsSecret(data []byte) bool {
	for _, secret := range []string{"plaintext-secret", "provider-secret-value"} {
		if strings.Contains(string(data), secret) {
			return true
		}
	}
	return false
}
