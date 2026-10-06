package llm_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func FuzzCanonicalJSONIdempotent(f *testing.F) {
	for _, path := range []string{
		filepath.Join("testdata", "request", "minimal.json"),
		filepath.Join("testdata", "request", "full.json"),
		filepath.Join("..", "api", "schema", "v1", "generate-request.schema.json"),
		filepath.Join("..", "api", "schema", "v1", "generate-response.schema.json"),
	} {
		f.Add(readCanonicalSeedForFuzz(f, path))
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > llm.DefaultCanonicalMaxBytes {
			t.Skip()
		}
		canonical, err := llm.CanonicalJSON(input)
		if err != nil {
			t.Skip()
		}
		again, err := llm.CanonicalJSON(canonical)
		if err != nil {
			t.Fatalf("canonical output was rejected: %v", err)
		}
		if !bytes.Equal(canonical, again) {
			t.Fatalf("canonicalization is not idempotent: %s != %s", canonical, again)
		}
	})
}

func readCanonicalSeedForFuzz(f *testing.F, path string) []byte {
	f.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		f.Fatalf("read fuzz seed %s: %v", path, err)
	}
	return data
}
