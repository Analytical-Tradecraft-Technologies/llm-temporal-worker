package architecturetest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestReleaseEvidenceCollectionArmsComposeCleanupBeforeStartup keeps a
// partial start or health timeout from leaving the uniquely named compose
// project running: the EXIT trap must already own it when compose up runs.
func TestReleaseEvidenceCollectionArmsComposeCleanupBeforeStartup(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repositoryRoot(t), "scripts", "release", "collect.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(data)
	armed := strings.Index(script, "\n  compose_started=1\n")
	start := strings.Index(script, `up --wait --wait-timeout 180`)
	if armed < 0 || start < 0 {
		t.Fatal("collect.sh no longer has the expected compose cleanup and startup steps")
	}
	if armed > start {
		t.Fatal("collect.sh enables compose cleanup only after compose up succeeds")
	}
	if !strings.Contains(script, "trap cleanup EXIT") {
		t.Fatal("collect.sh no longer cleans up on exit")
	}
}
