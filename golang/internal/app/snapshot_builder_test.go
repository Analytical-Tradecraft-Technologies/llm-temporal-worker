package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
)

func exampleConfig(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "config.example.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSnapshotBuilderCompilesImmutableConfig(t *testing.T) {
	builder := SnapshotBuilder{}
	snapshot, err := builder.Build(context.Background(), exampleConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ConfigVersion() == "" || len(snapshot.Canonical()) == 0 {
		t.Fatal("compiled snapshot has no stable identity")
	}
	copy := snapshot.Canonical()
	copy[0] ^= 0xff
	if snapshot.Canonical()[0] == copy[0] {
		t.Fatal("canonical snapshot bytes were not copied")
	}
	if snapshot.Config().Version != config.APIVersion {
		t.Fatalf("version = %q", snapshot.Config().Version)
	}
}
