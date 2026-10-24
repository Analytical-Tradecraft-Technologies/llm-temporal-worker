package documentationtest

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
)

// TestActivityRuntimeListsEveryActivityAndWorkflow keeps the Temporal
// inventory in docs/reference/activity-runtime.md equal to what the worker
// registers (#1512).
func TestActivityRuntimeListsEveryActivityAndWorkflow(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "reference", "activity-runtime.md"))
	if err != nil {
		t.Fatal(err)
	}
	rows := regexp.MustCompile("(?m)^\\| `(llm\\.[A-Za-z0-9._]+)` \\|").FindAllStringSubmatch(string(data), -1)
	var documented []string
	for _, row := range rows {
		documented = append(documented, row[1])
	}
	descriptors, err := activity.V1ActivityDescriptors("queue")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{workflows.GenerateWorkflowName, workflows.CompactWorkflowName, workflows.QueryWorkflowName,
		workflows.RequestWorkflowName, workflows.BudgetWorkflowName, workflows.LangfuseExportWorkflowName}
	for _, descriptor := range descriptors {
		want = append(want, descriptor.Name)
	}
	slices.Sort(documented)
	slices.Sort(want)
	if !slices.Equal(documented, want) {
		t.Fatalf("activity-runtime.md tables list %v, want the registered %v", documented, want)
	}
	for _, phrase := range []string{"registers nine exact names", "all nine descriptors", "registers six workflows"} {
		if !regexp.MustCompile(regexp.QuoteMeta(phrase)).Match(data) {
			t.Fatalf("activity-runtime.md lacks %q", phrase)
		}
	}
	if len(descriptors) != 9 || len(want)-len(descriptors) != 6 {
		t.Fatalf("the worker now registers %d activities and %d workflows; update activity-runtime.md, internal-workflows.md and mvp-v1-status.md", len(descriptors), len(want)-len(descriptors))
	}
}
