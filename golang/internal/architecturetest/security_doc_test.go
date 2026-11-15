package architecturetest

import (
	"strings"
	"testing"
)

// TestSecurityDocMatchesModelRouteRestrictions keeps the security architecture
// document aligned with the restrictions the planner actually enforces: only
// allowed_tenants; data_regions is recorded but not enforced, and there are no
// project or data-classification restrictions.
func TestSecurityDocMatchesModelRouteRestrictions(t *testing.T) {
	data := readText(t, repositoryRoot(t), "docs", "architecture", "security-and-privacy.md")
	doc := strings.Join(strings.Fields(data), " ")
	expectContainsEach(t, doc, "security architecture document is missing %q",
		"`allowed_tenants`",
		"`data_regions` is recorded with the model but not enforced",
		"There are no project or data-classification route restrictions.",
	)
	expectOmitsEach(t, doc, "security architecture document still claims %q",
		"allowed tenants/projects",
		"A continuation cannot cross those restrictions.",
	)
}
