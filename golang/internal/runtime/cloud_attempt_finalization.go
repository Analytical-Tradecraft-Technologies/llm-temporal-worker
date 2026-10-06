package runtime

import (
	"context"
	"encoding/json"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

type cloudAttemptResultStore interface {
	LoadRequestPreparation(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.RequestPreparation, error)
	Read(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.Record, error)
	LoadRequestAttempt(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.RequestAttempt, error)
	LoadProviderExecution(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.SavedProviderExecution, error)
}

// CloudAttemptResult retains the paid identity and saved settlement separately
// from the response projected to the original public operation. Reading it does
// not acquire budget, invoke a provider or change either request record.
type CloudAttemptResult struct {
	CheckpointScope string
	Attempt         cloudstate.RequestAttempt
	Saved           cloudstate.SavedProviderExecution
	Response        llm.Response
}

// LoadAttemptResult requires an already authorized scope. It verifies the
// active child's immutable parent and terminal execution before projecting its
// response to the public identity expected by checkpoint publication.
func (f *CloudFinalizer) LoadAttemptResult(ctx context.Context, scope cloudstate.Scope, rootID cloudstate.RequestID) (CloudAttemptResult, error) {
	if f == nil || ctx == nil {
		return CloudAttemptResult{}, cloudRuntimeError(cloudstate.ErrInvalid, false)
	}
	result, _, err := f.loadAttemptResult(ctx, scope, rootID)
	if err != nil {
		return CloudAttemptResult{}, cloudRuntimeError(err, false)
	}
	return result, nil
}

func (f *CloudFinalizer) loadAttemptResult(ctx context.Context, scope cloudstate.Scope, rootID cloudstate.RequestID) (CloudAttemptResult, string, error) {
	var zero CloudAttemptResult
	store, ok := f.requests.(cloudAttemptResultStore)
	if !ok || isNilCapability(store) {
		return zero, "", cloudstate.ErrCorrupt
	}
	root, err := store.Read(ctx, scope, rootID)
	if err != nil {
		return zero, "", err
	}
	attempt, err := store.LoadRequestAttempt(ctx, scope, rootID)
	if err != nil {
		return zero, "", err
	}
	if attempt.Validate() != nil || attempt.RootID != rootID || root.Request.ID != rootID || root.Request.Scope != scope {
		return zero, "", cloudstate.ErrCorrupt
	}
	child, err := store.Read(ctx, scope, attempt.ID)
	if err != nil {
		return zero, "", err
	}
	linked, err := cloudRequestAttempt(child)
	if err != nil || linked == nil || !equalFinalizationValue(*linked, attempt) || child.Request.Scope != scope ||
		child.Request.Kind != root.Request.Kind || child.Request.RequestIndex != root.Request.RequestIndex || !equalFinalizationJSON(child.Request.Manifest, root.Request.Manifest) {
		return zero, "", cloudstate.ErrCorrupt
	}
	saved, err := store.LoadProviderExecution(ctx, scope, attempt.ID)
	if err != nil {
		return zero, "", err
	}
	preparation, _, validated, err := loadRequestPreparation(ctx, store, scope, rootID)
	if err != nil {
		return zero, "", err
	}
	plan, execution := saved.Plan, saved.Execution
	if (!validated && preparation.Validate() != nil) || preparation.ConfigDigest != plan.ConfigDigest {
		return zero, "", cloudstate.ErrCorrupt
	}
	if execution.Validate(plan) != nil || plan.Kind != root.Request.Kind || plan.Route.OperationID != durable.OperationID(attempt.ID) ||
		execution.Stage != cloudstate.ExecutionSucceeded || !execution.Settled || execution.Response.OperationID != string(attempt.ID) {
		return zero, "", cloudstate.ErrCorrupt
	}
	var original struct {
		OperationKey string `json:"operation_key"`
	}
	if json.Unmarshal(root.Request.Manifest, &original) != nil || original.OperationKey == "" {
		return zero, "", cloudstate.ErrCorrupt
	}
	providerKey, err := cloudProviderOperationKey(child, original.OperationKey)
	if err != nil || execution.Response.OperationKey != providerKey {
		return zero, "", cloudstate.ErrCorrupt
	}
	// Round-trip detaches nested output, usage and diagnostic data from the
	// repository's saved execution. Projection never changes the paid record.
	data, err := json.Marshal(execution.Response)
	var response llm.Response
	if err != nil || json.Unmarshal(data, &response) != nil {
		return zero, "", cloudstate.ErrCorrupt
	}
	response.OperationID, response.OperationKey = string(rootID), original.OperationKey
	return CloudAttemptResult{CheckpointScope: preparation.CheckpointScope, Attempt: attempt, Saved: saved, Response: response}, root.Request.Kind, nil
}

func (f *CloudFinalizer) verifyProviderFinalization(ctx context.Context, scope cloudstate.Scope, rootID cloudstate.RequestID, kind, checkpointScope string, payload cloudFinalizationPayload) error {
	effects := payload.Effects.Provider
	if effects == nil || effects.AttemptID == "" {
		return f.verifyUnreserved(ctx, scope, rootID, kind, payload)
	}
	result, storedKind, err := f.loadAttemptResult(ctx, scope, rootID)
	if err != nil {
		return err
	}
	plan, execution := result.Saved.Plan, result.Saved.Execution
	if storedKind != kind || result.CheckpointScope != checkpointScope || result.Attempt.ID != effects.AttemptID ||
		effects.Unreserved == plan.RequiresReservation() || !execution.CompletedAt.Equal(effects.Completion.CompletedAt) {
		return cloudstate.ErrCorrupt
	}
	if !effects.Uncached && (string(effects.Lease.OperationID) != string(rootID) ||
		effects.Lease.Attempt != string(result.Attempt.ID) || plan.Route.CacheIdentity != effects.Lease.Key.Route) {
		return cloudstate.ErrCorrupt
	}
	if !effects.Unreserved && (execution.Settlement == nil || !equalFinalizationValue(*execution.Settlement, effects.Budget)) {
		return cloudstate.ErrCorrupt
	}
	var cost llm.CostV1
	var usage *llm.Usage
	if kind == "generate" {
		var response llm.GenerateResponseV1
		if json.Unmarshal(payload.Response, &response) != nil || response.OperationID != result.Response.OperationID || response.OperationKey != result.Response.OperationKey || response.Status != result.Response.Status ||
			!equalFinalizationValue(response.Output, result.Response.Output) || !equalFinalizationValue(response.Route, &result.Response.Route) {
			return cloudstate.ErrCorrupt
		}
		cost, usage = response.Cost, response.Usage
	} else {
		var response llm.CompactResponseV1
		if json.Unmarshal(payload.Response, &response) != nil || response.OperationID != result.Response.OperationID || response.OperationKey != result.Response.OperationKey {
			return cloudstate.ErrCorrupt
		}
		cost, usage = response.Cost, response.Usage
	}
	if !equalFinalizationValue(cost, publicationCost(result.Response.Cost)) || !equalFinalizationValue(usage, &result.Response.Usage) {
		return cloudstate.ErrCorrupt
	}
	return nil
}
