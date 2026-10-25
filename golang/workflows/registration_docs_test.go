package workflows

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// recordingRegistry captures the names Register installs.
type recordingRegistry struct {
	worker.WorkflowRegistry
	names []string
}

func (registry *recordingRegistry) RegisterWorkflowWithOptions(_ interface{}, options workflow.RegisterOptions) {
	registry.names = append(registry.names, options.Name)
}

// The architecture and status pages name every registered workflow and
// Activity and state their counts, so a new registration cannot leave them
// behind as #1512 and #1684 did.
func TestRegistrationPagesNameEveryWorkflowAndActivity(t *testing.T) {
	registry := &recordingRegistry{}
	Register(registry, activity.PayloadLimits{MaxInlineBytes: 1 << 20})
	workflows := registry.names
	descriptors, err := activity.V1ActivityDescriptors("docs")
	if err != nil {
		t.Fatal(err)
	}
	counts := map[int]string{6: "six", 9: "nine"}
	workflowCount, activityCount := counts[len(workflows)], counts[len(descriptors)]
	if workflowCount == "" || activityCount == "" {
		t.Fatalf("%d workflows and %d Activities are registered; update the pages and this test", len(workflows), len(descriptors))
	}
	for _, page := range []string{"architecture/system-overview.md", "architecture/temporal-worker.md", "reference/mvp-v1-status.md"} {
		data, err := os.ReadFile(filepath.Join("..", "..", "docs", filepath.FromSlash(page)))
		if err != nil {
			t.Fatal(err)
		}
		text := strings.Join(strings.Fields(string(data)), " ")
		if !strings.Contains(text, workflowCount+" workflows and "+activityCount+" activities") && !strings.Contains(text, workflowCount+" Temporal workflows and "+activityCount+" activities") {
			t.Errorf("%s does not state %s workflows and %s activities", page, workflowCount, activityCount)
		}
		if page == "architecture/system-overview.md" {
			continue
		}
		for _, descriptor := range descriptors {
			if !strings.Contains(text, "`"+descriptor.Name+"`") {
				t.Errorf("%s does not name Activity %s", page, descriptor.Name)
			}
		}
		if page == "architecture/temporal-worker.md" {
			for _, name := range workflows {
				if !strings.Contains(text, "`"+name+"`") {
					t.Errorf("%s does not name workflow %s", page, name)
				}
			}
		}
	}
}
