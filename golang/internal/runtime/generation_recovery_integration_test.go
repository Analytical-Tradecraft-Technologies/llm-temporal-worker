//go:build cloudworkflowintegration

package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// This fixture executes the pre-binding public command sequence on a real
// Temporal server. It does not manufacture or import history events.
func legacyRecoveryGenerate(ctx workflow.Context, input llm.GenerateRequestV1) (*llm.GenerateResponseV1, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var plan llm.GenerationPlanV1
	if err := workflow.ExecuteActivity(ctx, activity.PlanGenerationActivityName, input).Get(ctx, &plan); err != nil {
		return nil, err
	}
	if !plan.CompactBeforeGenerate || input.Parent == nil {
		return nil, errors.New("legacy fixture requires compaction")
	}
	encoded, _ := input.MarshalJSON()
	digest := sha256.Sum256(encoded)
	compact := llm.CompactRequestV1{OperationKey: "llmtw_compact_" + hex.EncodeToString(digest[:]), Context: input.Context, Parent: *input.Parent, Cache: &llm.CachePolicyV1{}}
	var compacted llm.CompactResponseV1
	if err := workflow.ExecuteChildWorkflow(ctx, workflows.CompactWorkflowName, compact).Get(ctx, &compacted); err != nil {
		return nil, err
	}
	input.Parent = &compacted.Checkpoint.Handle
	var result llm.ExecutionResultV1
	if err := workflow.ExecuteChildWorkflow(ctx, workflows.RequestWorkflowName, llm.PrepareExecutionV1{Generate: &input}).Get(ctx, &result); err != nil {
		return nil, err
	}
	return result.Generate, nil
}

type generationRecoveryLegacyRuntime struct{ *cloudV1Runtime }

func (*generationRecoveryLegacyRuntime) PlanGenerationV1(context.Context, llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
	return llm.GenerationPlanV1{CompactBeforeGenerate: true}, nil
}

func TestCloudWorkflowLiveHistoricalGenerationRecovery(t *testing.T) {
	h := newLiveCloudWorkflow(t, false, false)
	f := h.fixture
	policy := json.RawMessage(`{"recent_turns":0,"materialization_threshold_bytes":1}`)
	f.request.SettingsPatch.CompactionPolicy.Set = &policy
	f.restart(t)
	root := f.finish(t)
	original := llm.GenerateRequestV1{OperationKey: "legacy-history-recovery", Context: f.request.Context, Parent: &root.Generate.Checkpoint.Handle, Append: []llm.Item{preparationMessage("historical follow-up")}}
	value := codecConfig(codecKey("history", "KEY", true))
	value.Server.InlinePayloadBytes = 1 << 20
	value.Temporal.Target, value.Temporal.Namespace = os.Getenv("LLMTW_TEMPORAL_ADDRESS"), "default"
	var dc converter.DataConverter
	factory := DefaultTemporalClientFactory{SecretResolver: codecSecrets(map[string][]byte{"KEY": bytes.Repeat([]byte{3}, 32)}), DialContext: func(ctx context.Context, options client.Options) (client.Client, error) {
		dc = options.DataConverter
		return client.DialContext(ctx, options)
	}}
	encryptedClient, err := factory.New(h.ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(encryptedClient.Close)
	h.client = encryptedClient
	w := worker.New(h.client, h.queue, worker.Options{WorkerStopTimeout: time.Second})
	workflows.RegisterInternal(w, activity.PayloadLimits{})
	w.RegisterWorkflowWithOptions(legacyRecoveryGenerate, workflow.RegisterOptions{Name: workflows.GenerateWorkflowName})
	w.RegisterWorkflowWithOptions(workflows.Compact, workflow.RegisterOptions{Name: workflows.CompactWorkflowName})
	a := &activity.Activities{V1Runtime: &generationRecoveryLegacyRuntime{cloudV1Runtime: &cloudV1Runtime{CloudExecutionRuntime: f.runtime}}}
	if err := a.RegisterForTaskQueue(w, h.queue); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	run := h.start(t, workflows.GenerateWorkflowName, original)
	var want llm.GenerateResponseV1
	if err := run.Get(h.ctx, &want); err != nil {
		t.Fatal(err)
	}
	w.Stop()
	op := publicGenerationOperation(original, time.Now())
	if _, err := f.repository.LoadGenerationPlan(h.ctx, op); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("fixture persisted a new generation plan: %v", err)
	}
	proof, err := readGenerationRecoveryHistory(h.ctx, h.client.GetWorkflowHistory(h.ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT), dc, "default")
	if err != nil || !recoveryJSONEqual(proof.original, original) {
		t.Fatalf("server history recovery failed: %v", err)
	}
	before := f.submits.Load()
	report, err := restoreGenerationRecovery(h.ctx, f.repository, f.options.ResolveScope, proof, false, time.Now())
	if err != nil || report.Status != "recovery_required" {
		t.Fatalf("dry run: %+v %v", report, err)
	}
	if _, err := restoreGenerationRecovery(h.ctx, f.repository, f.options.ResolveScope, proof, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	f.runtime.execution.admission.planning.providers.catalog.Models = map[string]routing.Model{}
	h.startWorker(t)
	got := h.generate(t, original)
	if !reflect.DeepEqual(got, want) || f.submits.Load() != before {
		t.Fatal("server-history recovery changed the result or dispatched again")
	}
}
