package runtime

import (
	"context"
	"errors"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

func generationRecoveryStoreFixture(t *testing.T) (*boundedCloudFixture, generationRecoveryProof) {
	f := boundedCloud(t, false)
	parent := f.finish(t).Generate.Checkpoint.Handle
	f.request.OperationKey = "legacy-retained"
	f.request.Parent = &parent
	f.finish(t)
	original := f.request
	old := llm.CheckpointHandle("historical-original")
	original.Parent = &old
	return f, generationRecoveryProof{original: original, effective: f.request}
}

func TestGenerationRecoveryDryRunAndReplayAreReadOnly(t *testing.T) {
	f, proof := generationRecoveryStoreFixture(t)
	denyWrites := func() {
		f.table.hook = func(string, kv.KeyValueItem) (error, error) {
			t.Error("recovery attempted table write")
			return nil, nil
		}
		f.blobs.hook = func(blob.BlobKey) (error, error) { t.Error("recovery attempted blob write"); return nil, nil }
	}
	denyWrites()
	report, err := restoreGenerationRecovery(context.Background(), f.repository, f.options.ResolveScope, proof, false, f.now)
	if err != nil || report.Status != "recovery_required" || report.Applied {
		t.Fatalf("dry run: %+v %v", report, err)
	}
	f.table.hook, f.blobs.hook = nil, nil
	before := f.submits.Load()
	report, err = restoreGenerationRecovery(context.Background(), f.repository, f.options.ResolveScope, proof, true, f.now)
	if err != nil || report.Status != "ready" || !report.Applied || f.submits.Load() != before {
		t.Fatalf("apply: %+v %v", report, err)
	}
	denyWrites()
	report, err = restoreGenerationRecovery(context.Background(), f.repository, f.options.ResolveScope, proof, true, f.now)
	if err != nil || report.Status != "ready" || report.Applied {
		t.Fatalf("repeat: %+v %v", report, err)
	}
}

func TestGenerationRecoveryDoesNotOverwriteConflictingPlanOrBinding(t *testing.T) {
	for _, kind := range []string{"plan", "binding", "effective", "missing"} {
		t.Run(kind, func(t *testing.T) {
			f, proof := generationRecoveryStoreFixture(t)
			op := publicGenerationOperation(proof.original, f.now)
			switch kind {
			case "plan":
				testassert.NoError(t, testassert.Err(f.repository.SaveGenerationPlan(context.Background(), op, cloudstate.GenerationPlan{})))
			case "binding":
				testassert.NoError(t, testassert.Err(f.repository.SaveGenerationPlan(context.Background(), op, cloudstate.GenerationPlan{CompactBeforeGenerate: true})))
				changed := proof.effective
				changed.OperationKey = "changed"
				manifest, _ := changed.MarshalJSON()
				testassert.NoError(t, f.repository.SaveGenerationBinding(context.Background(), op, manifest))
			case "effective":
				proof.effective.Append = []llm.Item{llm.HumanText("changed")}
				proof.original.Append = proof.effective.Append
			case "missing":
				proof.effective.OperationKey = "absent"
				proof.original.OperationKey = "absent"
			}
			f.table.hook = func(string, kv.KeyValueItem) (error, error) {
				t.Error("conflict attempted table write")
				return nil, nil
			}
			f.blobs.hook = func(blob.BlobKey) (error, error) { t.Error("conflict attempted blob write"); return nil, nil }
			_, err := restoreGenerationRecovery(context.Background(), f.repository, f.options.ResolveScope, proof, true, f.now)
			want := contracts.ErrConflict
			if kind == "missing" {
				want = contracts.ErrNotFound
			}
			if !errors.Is(err, want) {
				t.Fatalf("conflict %s accepted: %v", kind, err)
			}
		})
	}
}

func TestGenerationRecoveryReconcilesUnknownWriteAcknowledgements(t *testing.T) {
	for _, field := range []string{"generation_plan", "generation_binding"} {
		t.Run(field, func(t *testing.T) {
			f, proof := generationRecoveryStoreFixture(t)
			fired := false
			f.table.hook = func(action string, item kv.KeyValueItem) (error, error) {
				if _, ok := item.Fields[field]; ok && action == "create" && !fired {
					fired = true
					return nil, &contracts.StorageError{Kind: contracts.ErrOutcomeUnknown}
				}
				return nil, nil
			}
			if _, err := restoreGenerationRecovery(context.Background(), f.repository, f.options.ResolveScope, proof, true, f.now); !errors.Is(err, contracts.ErrOutcomeUnknown) || !fired {
				t.Fatalf("uncertain write not propagated: %v", err)
			}
			f.table.hook = nil
			report, err := restoreGenerationRecovery(context.Background(), f.repository, f.options.ResolveScope, proof, true, f.now)
			if err != nil || report.Status != "ready" {
				t.Fatalf("retry: %+v %v", report, err)
			}
		})
	}
}

func TestGenerationRecoveryAuthorizesBeforeStorage(t *testing.T) {
	f, proof := generationRecoveryStoreFixture(t)
	denied := errors.New("denied")
	// Nil store methods would panic if any storage access preceded authorization.
	var store generationRecoveryStore = &unavailableGenerationRecoveryStore{}
	testassert.ErrorIs(t, testassert.Err(restoreGenerationRecovery(context.Background(), store, func(context.Context, llm.RequestContext) (string, error) { return "", denied }, proof, true, f.now)), denied, "authorization ignored")
}

type unavailableGenerationRecoveryStore struct{ generationRecoveryStore }
