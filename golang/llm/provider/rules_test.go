package provider_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every adapter that lowers tools refuses non-function kinds through the
// shared guard, so a new adapter cannot offer a provider or remote MCP tool
// as a callable function (#1665).
func TestEveryAdapterLowerToolsUsesTheFunctionToolGuard(t *testing.T) {
	files, err := filepath.Glob("*/*.go")
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(source)
		start := strings.Index(text, "\nfunc lowerTools(")
		if start < 0 {
			continue
		}
		found++
		body := text[start+1:]
		if end := strings.Index(body, "\n}\n"); end >= 0 {
			body = body[:end]
		}
		if !strings.Contains(body, "provider.RequireFunctionTool(") {
			t.Errorf("%s: lowerTools does not call provider.RequireFunctionTool", path)
		}
	}
	if found < 5 {
		t.Fatalf("found lowerTools in %d adapters, want at least 5", found)
	}
}
