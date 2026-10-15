package architecturetest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSecurityDocMatchesModelRouteRestrictions keeps the security architecture
// document aligned with the restrictions the planner actually enforces: only
// allowed_tenants; data_regions is recorded but not enforced, and there are no
// project or data-classification restrictions.
func TestSecurityDocMatchesModelRouteRestrictions(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repositoryRoot(t), "docs", "architecture", "security-and-privacy.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := strings.Join(strings.Fields(string(data)), " ")
	for _, want := range []string{
		"`allowed_tenants`",
		"`data_regions` is recorded with the model but not enforced",
		"There are no project or data-classification route restrictions.",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("security architecture document is missing %q", want)
		}
	}
	for _, stale := range []string{
		"allowed tenants/projects",
		"A continuation cannot cross those restrictions.",
	} {
		if strings.Contains(doc, stale) {
			t.Errorf("security architecture document still claims %q", stale)
		}
	}
}
