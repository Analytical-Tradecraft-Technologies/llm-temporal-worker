package architecturetest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLocalContinuationKeyPathIsIgnored keeps the compose continuation key
// documented at golang/.local out of commits and Docker build contexts.
func TestLocalContinuationKeyPathIsIgnored(t *testing.T) {
	root := repositoryRoot(t)
	compose, err := os.ReadFile(filepath.Join(root, "golang", "compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(compose), ".local/continuation-hmac") {
		t.Skip("compose no longer documents the .local continuation key")
	}
	for path, want := range map[string]string{
		".gitignore":           "golang/.local/",
		"golang/.dockerignore": ".local/",
	} {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, line := range strings.Split(string(data), "\n") {
			if strings.TrimSpace(line) == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s does not ignore %s", path, want)
		}
	}
}
