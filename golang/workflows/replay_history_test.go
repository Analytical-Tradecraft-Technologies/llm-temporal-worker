package workflows

import (
	"path/filepath"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"go.temporal.io/sdk/worker"
)

// Histories recorded by a real Temporal server replay against the current
// workflows. The before-resolution histories come from workers that predate
// resolvedGenerationChange: a public Generate that hit the cache and one that
// missed, each with its request child. They must keep their plan Activity
// and request workflow. resolved-generate-hit is a hit answered by the
// resolve Activity, which pins the new command sequence.
func TestRecordedHistoriesReplay(t *testing.T) {
	for _, name := range []string{
		"before-resolution-generate-hit",
		"before-resolution-generate-miss",
		"before-resolution-request-hit",
		"before-resolution-request-miss",
		"resolved-generate-hit",
	} {
		t.Run(name, func(t *testing.T) {
			replayer := worker.NewWorkflowReplayer()
			Register(replayer, activity.PayloadLimits{})
			if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, filepath.Join("testdata", "replay", name+".json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
