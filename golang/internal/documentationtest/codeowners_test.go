package documentationtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCodeOwnersDocumentation keeps .github/CODEOWNERS and the operations
// guide's code-owner section in step. The master rulesets do not require
// code-owner review, so CODEOWNERS must not claim that it requires approval,
// and every code-owned path must be documented with its actual enforcement.
func TestCodeOwnersDocumentation(t *testing.T) {
	root := repositoryRoot(t)
	codeOwners, err := os.ReadFile(filepath.Join(root, ".github", "CODEOWNERS"))
	if err != nil {
		t.Fatal(err)
	}
	guide, err := os.ReadFile(filepath.Join(root, "docs", "architecture", "deployment-and-operations.md"))
	if err != nil {
		t.Fatal(err)
	}
	const heading = "## Code owners and merge controls\n"
	_, section, found := strings.Cut(string(guide), heading)
	if !found {
		t.Fatalf("deployment-and-operations.md is missing %q", strings.TrimSpace(heading))
	}
	if next := strings.Index(section, "\n## "); next >= 0 {
		section = section[:next]
	}

	var paths, comments []string
	for _, line := range strings.Split(string(codeOwners), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "":
		case strings.HasPrefix(trimmed, "#"):
			comments = append(comments, strings.TrimSpace(strings.TrimPrefix(trimmed, "#")))
		default:
			paths = append(paths, strings.Fields(trimmed)[0])
		}
	}
	for _, sentence := range strings.Split(strings.Join(comments, " "), ".") {
		lower := strings.ToLower(sentence)
		if strings.Contains(lower, "require") && strings.Contains(lower, "approval") && !strings.Contains(lower, "not require") {
			t.Errorf("CODEOWNERS claims approval is required, but the master ruleset does not require code-owner review: %q", strings.TrimSpace(sentence))
		}
	}
	if len(paths) == 0 {
		t.Fatal("CODEOWNERS lists no paths")
	}
	for _, path := range paths {
		if !strings.Contains(section, "`"+path+"`") {
			t.Errorf("code-owner section does not document CODEOWNERS path %s", path)
		}
	}
	for _, required := range []string{
		"require_code_owner_review: false",
		"required_approving_review_count: 0",
		"\"Queue master changes\" ruleset",
		"`Verify source safety and supply chain`",
	} {
		if !strings.Contains(section, required) {
			t.Errorf("code-owner section does not state %q", required)
		}
	}
	if !strings.Contains(string(codeOwners), "only requests review") {
		t.Error("CODEOWNERS header must say that it only requests review")
	}
}
