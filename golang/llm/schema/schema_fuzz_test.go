package schema_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

func FuzzSchemaCanonicalAndBounded(f *testing.F) {
	for _, path := range []string{
		filepath.Join("testdata", "valid", "object.json"),
		filepath.Join("testdata", "valid", "array.json"),
		filepath.Join("testdata", "valid", "enum-number.json"),
	} {
		data, err := os.ReadFile(path)
		testassert.NoError(f, err)
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > schema.DefaultLimits().MaxBytes {
			t.Skip()
		}
		compiled, err := schema.Parse(data)
		if err != nil {
			t.Skip()
		}
		canonical := compiled.Canonical()
		again, err := schema.Parse(canonical)
		testassert.NoError(t, err, "canonical schema was rejected")
		if !bytes.Equal(canonical, again.Canonical()) {
			t.Fatalf("schema canonicalization is not idempotent")
		}
	})
}
