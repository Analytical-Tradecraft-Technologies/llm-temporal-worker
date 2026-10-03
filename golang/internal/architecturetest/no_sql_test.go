package architecturetest

import (
	"os/exec"
	"strings"
	"testing"
)

// Inspect the compiled worker closure as well as explicit source imports: a
// transitive SQL dependency must not silently restore the removed backend.
func TestWorkerHasNoSQLDependencies(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "./cmd/llm-temporal-worker")
	command.Dir = moduleRoot(t)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("list worker dependencies: %v\n%s", err, output)
	}
	for _, dependency := range strings.Fields(string(output)) {
		// google/uuid uses database/sql/driver value interfaces for serialization;
		// those interfaces do not open SQL connections or register a database driver.
		if dependency == "database/sql" {
			t.Errorf("worker depends on SQL connection API")
		}
		for _, prefix := range []string{"github.com/jackc/", "github.com/lib/pq", "github.com/go-sql-driver/", "modernc.org/sqlite", modulePath + "/storage/postgres"} {
			if strings.HasPrefix(dependency, prefix) {
				t.Errorf("worker depends on SQL package %s", dependency)
			}
		}
	}
}
