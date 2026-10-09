package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
)

func TestRecoveredHistoricalCompactionReplaysPublicGenerate(t *testing.T) {
	for _, mode := range []string{"completed", "prepared"} {
		t.Run(mode, func(t *testing.T) {
			f := boundedCloud(t, false)
			policy := json.RawMessage(`{"recent_turns":0,"materialization_threshold_bytes":1}`)
			f.request.SettingsPatch.CompactionPolicy.Set = &policy
			root := f.finish(t)
			f.now = f.now.Add(time.Minute)
			original := llm.GenerateRequestV1{OperationKey: "legacy-compacted-public", Context: f.request.Context, Parent: &root.Generate.Checkpoint.Handle, Append: []llm.Item{preparationMessage("legacy follow-up")}}
			encoded, _ := original.MarshalJSON()
			digest := sha256.Sum256(encoded)
			compact := llm.CompactRequestV1{OperationKey: "llmtw_compact_" + hex.EncodeToString(digest[:]), Context: original.Context, Parent: *original.Parent, Cache: &llm.CachePolicyV1{}}
			// Execute the same Compact and effective Generate that the legacy public
			// workflow issued, without the newer original plan/binding metadata.
			env := publicReplayEnvironment(f)
			env.ExecuteWorkflow(workflows.CompactWorkflowName, compact)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			var compacted llm.CompactResponseV1
			if err := env.GetWorkflowResult(&compacted); err != nil {
				t.Fatal(err)
			}
			effective := original
			effective.Parent = &compacted.Checkpoint.Handle
			f.request = effective
			f.now = f.now.Add(time.Second)
			var want llm.GenerateResponseV1
			if mode == "completed" {
				want = *f.finish(t).Generate
			} else {
				prepared, err := f.runtime.PrepareExecutionV1(context.Background(), llm.PrepareExecutionV1{Generate: &effective})
				boundedState(t, prepared, err, llm.ExecutionBudgetRequired)
			}
			op := publicGenerationOperation(original, f.now)
			if _, err := f.repository.LoadGenerationPlan(context.Background(), op); !errors.Is(err, contracts.ErrNotFound) {
				t.Fatalf("fixture is not legacy: %v", err)
			}
			before := f.submits.Load()
			f.now = f.now.Add(24*time.Hour - 500*time.Millisecond)
			f.restart(t)
			// Completed work needs no current routing. Unpaid prepared work still
			// needs its compatible provider configuration to finish normally.
			if mode == "completed" {
				f.runtime.execution.admission.planning.providers.catalog.Models = map[string]routing.Model{}
			}
			proof := generationRecoveryProof{original: original, effective: effective}
			report, err := restoreGenerationRecovery(context.Background(), f.repository, f.options.ResolveScope, proof, true, f.now)
			if err != nil || report.Status != "ready" || f.submits.Load() != before {
				t.Fatalf("restore dispatched or failed: %+v %v", report, err)
			}
			env = publicReplayEnvironment(f)
			env.ExecuteWorkflow(workflows.GenerateWorkflowName, original)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("recovered public replay: %v", err)
			}
			var got llm.GenerateResponseV1
			if err := env.GetWorkflowResult(&got); err != nil {
				t.Fatal(err)
			}
			if mode == "completed" {
				if !reflect.DeepEqual(got, want) || f.submits.Load() != before {
					t.Fatal("completed replay changed result or dispatched")
				}
			} else {
				if got.OperationKey != original.OperationKey || f.submits.Load() != before+1 {
					t.Fatal("prepared replay did not resume exactly one Generate")
				}
			}
		})
	}
}
