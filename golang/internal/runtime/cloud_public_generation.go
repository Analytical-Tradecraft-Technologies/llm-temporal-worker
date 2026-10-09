package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

func (r *CloudExecutionRuntime) publicGenerationOperation(request llm.GenerateRequestV1) cloudstate.Operation {
	return publicGenerationOperation(request, r.now())
}

func publicGenerationOperation(request llm.GenerateRequestV1, now time.Time) cloudstate.Operation {
	manifest, _ := request.MarshalJSON()
	var index int64
	if request.Cache != nil {
		index = int64(request.Cache.Variant)
	}
	return cloudstate.Operation{Scope: cloudstate.Scope{Tenant: request.Context.Tenant, Project: request.Context.Project}, Kind: "generate", Key: request.OperationKey, RequestIndex: index, Manifest: manifest, Now: now}
}

func samePublicGeneration(original, effective llm.GenerateRequestV1) bool {
	effective.Parent = original.Parent
	want, err := original.MarshalJSON()
	if err != nil {
		return false
	}
	got, err := effective.MarshalJSON()
	return err == nil && bytes.Equal(want, got)
}

func (r *CloudExecutionRuntime) publicGenerationPlan(ctx context.Context, op cloudstate.Operation, original llm.GenerateRequestV1, saved cloudstate.GenerationPlan) (llm.GenerationPlanV1, error) {
	plan := llm.GenerationPlanV1{CompactBeforeGenerate: saved.CompactBeforeGenerate}
	if !saved.CompactBeforeGenerate {
		return plan, nil
	}
	binding, err := r.store.LoadGenerationBinding(ctx, op)
	if errors.Is(err, contracts.ErrNotFound) && !errors.Is(err, contracts.ErrOutcomeUnknown) {
		return plan, nil
	}
	if err != nil {
		return plan, cloudRuntimeError(err, false)
	}
	var effective llm.GenerateRequestV1
	if json.Unmarshal(binding, &effective) != nil || effective.Parent == nil || !samePublicGeneration(original, effective) {
		return plan, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	plan.EffectiveParent = effective.Parent
	return plan, nil
}

func (r *CloudExecutionRuntime) bindPublicGeneration(ctx context.Context, input llm.PrepareExecutionV1) error {
	if _, err := input.MarshalJSON(); err != nil {
		return executionError(provider.CodeInvalidArgument)
	}
	effective := *input.Generate
	original := effective
	original.Parent = input.OriginalGenerate.Parent
	if _, err := r.preparation.authorize(ctx, original.Context); err != nil {
		return err
	}
	if !samePublicGeneration(original, effective) {
		return cloudRuntimeError(contracts.ErrConflict, false)
	}
	op := r.publicGenerationOperation(original)
	plan, err := r.store.LoadGenerationPlan(ctx, op)
	if err != nil {
		return cloudRuntimeError(err, false)
	}
	manifest, err := effective.MarshalJSON()
	if err != nil {
		return executionError(provider.CodeInvalidArgument)
	}
	if !plan.CompactBeforeGenerate {
		if !bytes.Equal(manifest, op.Manifest) {
			return cloudRuntimeError(contracts.ErrConflict, false)
		}
		return nil
	}
	if original.Parent == nil || effective.Parent == nil {
		return executionError(provider.CodeStateCorrupt)
	}
	bound, err := r.store.LoadGenerationBinding(ctx, op)
	if err == nil {
		var saved llm.GenerateRequestV1
		if json.Unmarshal(bound, &saved) != nil {
			return cloudRuntimeError(cloudstate.ErrCorrupt, false)
		}
		encoded, _ := saved.MarshalJSON()
		if !bytes.Equal(encoded, manifest) {
			return cloudRuntimeError(contracts.ErrConflict, false)
		}
		return nil
	}
	if !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, contracts.ErrOutcomeUnknown) {
		return cloudRuntimeError(err, false)
	}
	digest := sha256.Sum256(op.Manifest)
	compact := llm.CompactRequestV1{OperationKey: "llmtw_compact_" + hex.EncodeToString(digest[:]), Context: original.Context, Parent: *original.Parent, Cache: &llm.CachePolicyV1{}}
	compactManifest, _ := compact.MarshalJSON()
	record, err := r.store.LookupOperation(ctx, cloudstate.Operation{Scope: op.Scope, Kind: "compact", Key: compact.OperationKey, Manifest: compactManifest, Now: r.now()})
	if err != nil {
		return cloudRuntimeError(err, false)
	}
	if record.Status != cloudstate.StatusCompleted {
		return executionError(provider.CodeStateCorrupt)
	}
	// Preparation verifies the exact child manifest and restores its retained
	// completed response, without materializing the checkpoint or dispatching.
	result, err := r.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Compact: &compact})
	if err != nil {
		return err
	}
	if result.State != llm.ExecutionCompleted || result.Compact == nil || result.Compact.OperationKey != compact.OperationKey || result.Compact.Checkpoint.Handle != *effective.Parent {
		return cloudRuntimeError(contracts.ErrConflict, false)
	}
	if err := r.store.SaveGenerationBinding(ctx, op, manifest); err != nil {
		return cloudRuntimeError(err, false)
	}
	return nil
}
