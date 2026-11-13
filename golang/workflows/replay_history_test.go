package workflows

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"go.temporal.io/sdk/client"
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

// The pinned-limit histories were recorded on a real Temporal server by a
// worker with server.inline_payload_bytes at 16 KiB: a public Generate whose
// roughly 6 KiB request was resolved and handed to its request child, and that
// child with its Prepare Activity scheduled, both open when recorded; and a
// public Generate that completed from a cached hit with a roughly 6.5 KiB
// result. A worker restarted with a smaller limit must replay them with the
// same commands, so the request, the Activity argument, the child argument it
// re-encodes and the result it returns must be bounded by the limit recorded
// when each run started (#1511). The replayer uses the worker's bounded converter, as the worker
// does, so the converter's checks are part of the replay.
func TestPinnedLimitHistoriesReplayUnderSmallerLimit(t *testing.T) {
	const recorded = 16 << 10
	for _, name := range []string{"pinned-limit-generate", "pinned-limit-request", "pinned-limit-generate-complete"} {
		path := filepath.Join("testdata", "replay", name+".json")
		history, err := client.HistoryFromJSON(mustOpen(t, path), client.HistoryJSONOptions{})
		if err != nil {
			t.Fatal(err)
		}
		input := history.Events[0].GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()[0].GetData()
		// The completed history also returned a result over the smaller
		// limits, so its replay re-encodes that result under the pinned limit.
		var result []byte
		if completed := history.Events[len(history.Events)-1].GetWorkflowExecutionCompletedEventAttributes(); completed != nil {
			result = completed.GetResult().GetPayloads()[0].GetData()
		}
		if name == "pinned-limit-generate-complete" && result == nil {
			t.Fatalf("%s: history must end with the workflow's completion", name)
		}
		for _, limit := range []int{recorded, 4 << 10, 1 << 10} {
			if limit < recorded && len(input) <= limit {
				t.Fatalf("%s: recorded input is %d bytes; it must exceed the %d-byte replay limit", name, len(input), limit)
			}
			if limit < recorded && result != nil && len(result) <= limit {
				t.Fatalf("%s: recorded result is %d bytes; it must exceed the %d-byte replay limit", name, len(result), limit)
			}
			t.Run(name+"/"+strconv.Itoa(limit), func(t *testing.T) {
				limits := activity.PayloadLimits{MaxInlineBytes: limit}
				replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: activity.BoundedDataConverter(limits)})
				if err != nil {
					t.Fatal(err)
				}
				Register(replayer, limits)
				if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func mustOpen(t *testing.T, path string) *os.File {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}
