//go:build cloudworkflowintegration

package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
	enumspb "go.temporal.io/api/enums/v1"
)

// The companion executable links the actual OCaml SDK and public client. A
// separate process resumes the first process's saved execution while the Go
// worker is replaced; no mock transport or cross-language state is shared.
func TestCloudWorkflowLiveOCaml(t *testing.T) {
	binary := os.Getenv("LLMTW_CLOUD_OCAML_CLIENT")
	if binary == "" {
		t.Skip("run make cloud-workflow-ocaml-integration for the OCaml gate")
	}
	if !filepath.IsAbs(binary) || os.Getenv("LLMTW_CLOUD_TEST_PROVISION") != "1" {
		t.Fatal("OCaml executable requires an absolute path and the isolated harness")
	}
	h := newLiveCloudWorkflow(t, true, false)
	request := h.fixture.request
	request.Cache = &llm.CachePolicyV1{Variant: 1}
	policy := json.RawMessage(`{"recent_turns":0}`)
	request.SettingsPatch.CompactionPolicy.Set = &policy
	dir := t.TempDir()
	input, saved, output := filepath.Join(dir, "request.json"), filepath.Join(dir, "execution.json"), filepath.Join(dir, "response.json")
	writeCloudOCamlInput(t, input, request)
	address := "http://" + os.Getenv("LLMTW_TEMPORAL_ADDRESS")
	stop := h.startWorker(t)
	runCloudOCaml(t, h.ctx, binary, "start", address, h.queue, input, saved)
	select {
	case <-h.pending:
	case <-h.ctx.Done():
		t.Fatal("OCaml submission did not become durably pending")
	}
	stop()
	h.complete.Store(true)
	h.startWorker(t)
	runCloudOCaml(t, h.ctx, binary, "resume", address, h.queue, input, saved, output)
	assertCloudOCamlResult(t, output, request)
	if h.fixture.submits.Load() != 2 {
		t.Fatal("OCaml start retry/resume duplicated generation or lost compaction")
	}

	// Poll an OCaml parent on a separate queue. Both public helpers must launch
	// their children on the Go queue, and the actual history must retain paid
	// children when the parent closes.
	parentQueue := h.queue + "-ocaml"
	workerCtx, cancelWorker := context.WithCancel(h.ctx)
	var workerLog bytes.Buffer
	cmd := exec.CommandContext(workerCtx, binary, "worker", address, parentQueue, h.queue)
	cmd.Stdout, cmd.Stderr = &workerLog, &workerLog
	if err := cmd.Start(); err != nil {
		cancelWorker()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancelWorker()
		_ = cmd.Wait()
		if t.Failed() {
			t.Log(workerLog.String())
		}
	})
	request.OperationKey = h.queue + "-child-generation"
	request.Cache = &llm.CachePolicyV1{Variant: 2}
	writeCloudOCamlInput(t, input, request)
	runCloudOCaml(t, h.ctx, binary, "child", address, parentQueue, h.queue, input, output)
	assertCloudOCamlResult(t, output, request)
	history := h.client.GetWorkflowHistory(h.ctx, parentQueue+"-parent", "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	children := map[string]int{}
	for history.HasNext() {
		event, err := history.Next()
		if err != nil {
			t.Fatal(err)
		}
		if child := event.GetStartChildWorkflowExecutionInitiatedEventAttributes(); child != nil {
			if child.GetTaskQueue().GetName() != h.queue || child.GetParentClosePolicy() != enumspb.PARENT_CLOSE_POLICY_ABANDON {
				t.Fatal("OCaml child lost queue routing or paid-work lifetime policy")
			}
			children[child.GetWorkflowType().GetName()]++
		}
	}
	if len(children) != 2 || children[workflows.GenerateWorkflowName] != 1 || children[workflows.CompactWorkflowName] != 1 || h.fixture.submits.Load() != 4 {
		t.Fatalf("OCaml parent did not run both public workflows exactly once: %v", children)
	}
	h.assertSettled(t, 4)
}

func writeCloudOCamlInput(t *testing.T, path string, request llm.GenerateRequestV1) {
	t.Helper()
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func runCloudOCaml(t *testing.T, ctx context.Context, binary string, args ...string) {
	t.Helper()
	output, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("OCaml %s failed: %v\n%s", args[0], err, output)
	}
}

func assertCloudOCamlResult(t *testing.T, path string, request llm.GenerateRequestV1) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Generate llm.GenerateResponseV1 `json:"generate"`
		Compact  llm.CompactResponseV1  `json:"compact"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	generation, compaction := result.Generate, result.Compact
	if generation.OperationKey != request.OperationKey || generation.Checkpoint.Handle == "" || generation.Cache.Variant != request.Cache.Variant ||
		compaction.OperationKey != request.OperationKey+"-compact" || compaction.Checkpoint.Handle == "" || compaction.Checkpoint.Parent == nil || *compaction.Checkpoint.Parent != generation.Checkpoint.Handle || compaction.Cache.Variant != request.Cache.Variant {
		t.Fatal("OCaml result lost operation identity, sample index or checkpoint lineage")
	}
}
