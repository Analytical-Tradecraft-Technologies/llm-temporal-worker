package architecturetest

import (
	"strings"
	"testing"
)

// TestLocalContinuationKeyPathIsIgnored keeps the compose continuation key
// documented at golang/.local out of commits and Docker build contexts.
func TestLocalContinuationKeyPathIsIgnored(t *testing.T) {
	root := repositoryRoot(t)
	compose := readText(t, root, "golang", "compose.yaml")
	if !strings.Contains(compose, ".local/continuation-hmac") {
		t.Skip("compose no longer documents the .local continuation key")
	}
	for path, want := range map[string]string{
		".gitignore":           "golang/.local/",
		"golang/.dockerignore": ".local/",
	} {
		data := readRepositoryFile(t, root, path)
		found := false
		for _, line := range strings.Split(data, "\n") {
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
