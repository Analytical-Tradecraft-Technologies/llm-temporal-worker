package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func cloudCachePolicy(p PreparedCloudRequest) *llm.CachePolicyV1 {
	if p.Generate != nil {
		return p.Generate.Cache
	}
	return p.Compact.Cache
}
func (r *CloudExecutionRuntime) cacheLease(ctx context.Context, p PreparedCloudRequest, attempt cloudstate.RequestAttempt, plan cloudstate.BudgetPlan) (*cache.FillLease, error) {
	policy := cloudCachePolicy(p)
	if policy == nil {
		return nil, nil
	}
	var request llm.Request
	var policyVersion, promptVersion string
	var temperature *llm.DecimalV1
	if p.Generate != nil {
		input, err := PrepareGenerateInput(ctx, *p.Generate, p.GenerateReplay)
		if err != nil {
			return nil, err
		}
		request = input.Request
		temperature = input.Settings.TemperatureDecimal
	} else {
		input, err := PrepareCompactInput(ctx, *p.Compact, p.CompactReplay)
		if err != nil {
			return nil, err
		}
		if input.Request == nil {
			return nil, executionError(provider.CodeStateCorrupt)
		}
		request = *input.Request
		policyVersion, promptVersion = input.Policy.Version, input.Policy.PromptVersion
	}
	// Hash the complete normalized semantic payload at the request bound, then
	// put that digest in the small keyed cache manifest. Large transcripts do
	// not hit the manifest's 256 KiB audit bound. The exact v1 temperature also participates before provider projection.
	request.OperationKey = "cache-semantic-input"
	request.Context = llm.RequestContext{}
	request.ServiceClass, request.ServiceClassFallbacks = llm.ServiceClassStandard, nil
	request.Continuation = nil
	request, err := llm.NormalizeRequest(request)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(struct {
		Request       llm.Request
		Temperature   *llm.DecimalV1
		PolicyVersion string
		PromptVersion string
	}{request, temperature, policyVersion, promptVersion})
	if err != nil {
		return nil, err
	}
	canonical, err := llm.CanonicalJSONWithLimits(encoded, 16<<20, 128)
	if err != nil {
		return nil, executionError(provider.CodeInvalidArgument)
	}
	digest := sha256.Sum256(canonical)
	scope := p.Record.Request.Scope
	input := cache.Input{Operation: cache.OperationKind(p.Record.Request.Kind), Namespace: cache.Namespace{Tenant: scope.Tenant, Project: scope.Project}, Config: cache.ConfigDigest(hex.EncodeToString(plan.ConfigDigest[:])),
		Route: plan.Route.CacheIdentity, CapabilityLowering: cache.CapabilityVersion(plan.CapabilityVersion), Epoch: cache.CacheEpoch(plan.CompilerVersion), Conversation: cache.ConversationDigest(hex.EncodeToString(digest[:])),
		Request: llm.Request{OperationKey: "cache-semantic-input", Model: request.Model}, Variant: policy.Variant}
	fingerprint, err := r.store.CacheFingerprint(input)
	if err != nil {
		return nil, executionError(provider.CodeStateCorrupt)
	}
	return &cache.FillLease{Key: cache.ResponseKey{ScopeID: p.Preparation.CheckpointScope, Operation: input.Operation, Route: plan.Route.CacheIdentity, Fingerprint: fingerprint, RequestIndex: p.Record.Request.RequestIndex},
		OperationID: state.OperationID(p.Record.Request.ID), Attempt: string(attempt.ID), AcquiredAt: attempt.CreatedAt, ExpiresAt: attempt.CreatedAt.Add(cache.MaxFillLease)}, nil
}

func (r *CloudExecutionRuntime) prepareCache(ctx context.Context, p PreparedCloudRequest, lease cache.FillLease) (llm.ExecutionResultV1, bool, error) {
	helper, err := durable.NewResponseCache(r.capabilities.Responses, r.capabilities.ResponseFills, r.capabilities.Clock)
	if err != nil {
		return llm.ExecutionResultV1{}, false, err
	}
	var disposition durable.CacheDisposition
	var origin *cache.ResponseEntry
	if p.Generate != nil {
		decision, lookupErr := helper.PrepareGenerate(ctx, lease, cacheMaximumAge(cloudCachePolicy(p)))
		err, disposition, origin = lookupErr, decision.Disposition, decision.Entry()
	} else {
		// Artifact reuse is content/policy based; generation freshness does not
		// expire an otherwise compatible compaction artifact.
		decision, lookupErr := helper.PrepareCompact(ctx, lease, nil)
		err, disposition, origin = lookupErr, decision.Disposition, decision.Entry()
	}
	if err != nil {
		return llm.ExecutionResultV1{}, false, responseCacheLookupError(err)
	}
	switch disposition {
	case durable.CacheHit:
		result, err := r.finishCache(ctx, p, *origin)
		return result, true, err
	case durable.CacheMiss:
		return llm.ExecutionResultV1{}, false, nil
	case durable.CacheWait, durable.CacheRecoveryRequired:
		return cloudStatus(p, llm.ExecutionCacheWait, 5*time.Second), true, nil
	default:
		return llm.ExecutionResultV1{}, false, executionError(provider.CodeStateCorrupt)
	}
}

func (r *CloudExecutionRuntime) finishUnknownFill(ctx context.Context, p PreparedCloudRequest, attempt cloudstate.RequestAttempt, saved cloudstate.SavedProviderExecution) error {
	lease, err := r.cacheLease(ctx, p, attempt, saved.Plan)
	if err != nil || lease == nil {
		return err
	}
	// Acquire is only an observation of this existing, expired attempt. It
	// cannot create a new lease or start work. Held fills can be taken over by
	// the next child after expiry; started fills require this stable receipt.
	decision, err := r.capabilities.ResponseFills.Acquire(ctx, *lease)
	if err != nil {
		// The expired lease is fenced once another attempt took the fill over
		// and ended it. That fill is no longer this attempt's to finish.
		if other := decision.Record.Lease.Attempt; errors.Is(err, contracts.ErrConflict) && other != "" && other != lease.Attempt {
			return nil
		}
		return cloudRuntimeError(err, false)
	}
	if decision.Record.Lease.Attempt != lease.Attempt {
		return nil
	}
	if decision.Record.State == cache.FillHeld || decision.Record.State == cache.FillReleased {
		return nil
	}
	completion := cache.FillCompletion{Outcome: cache.FillUnknown, CompletedAt: saved.Execution.RecoverAfter}
	if err := r.capabilities.ResponseFills.Complete(ctx, *lease, completion); err != nil {
		return cloudRuntimeError(err, true)
	}
	return nil
}

func (r *CloudExecutionRuntime) finishTerminalFill(ctx context.Context, p PreparedCloudRequest, attempt cloudstate.RequestAttempt, saved cloudstate.SavedProviderExecution, outcome cache.FillOutcome) error {
	if !saved.Execution.Settled {
		return executionError(provider.CodeStateCorrupt)
	}
	lease, err := r.cacheLease(ctx, p, attempt, saved.Plan)
	if err != nil || lease == nil {
		return err
	}
	if err := r.capabilities.ResponseFills.Complete(ctx, *lease, cache.FillCompletion{Outcome: outcome, CompletedAt: saved.Execution.CompletedAt}); err != nil {
		// A transient storage failure must stay retryable: otherwise the
		// Activity fails permanently and the fill stays started for waiters.
		return cloudRuntimeError(err, true)
	}
	return nil
}

func (r *CloudExecutionRuntime) finishCache(ctx context.Context, p PreparedCloudRequest, origin cache.ResponseEntry) (llm.ExecutionResultV1, error) {
	at := p.Preparation.PreparedAt
	if at.Before(origin.CompletedAt) {
		at = origin.CompletedAt
	}
	identity := r.publicationIdentity(p, at)
	effects := FinalizationEffects{Cache: &CacheFinalizationEffects{Origin: origin, Use: cache.ResponseUse{ScopeID: identity.Scope, OperationID: identity.OperationID, EntryID: origin.ID, CheckpointID: identity.CheckpointID, CompletedAt: at}}}
	zero := pricing.MustUSD("0")
	model := llm.Response{APIVersion: llm.APIVersion, OperationKey: cloudPublicKey(p), OperationID: string(identity.OperationID), Status: llm.ResponseStatusCompleted, Cost: llm.Cost{Status: llm.CostStatusKnown, ActualCostUSD: &zero, Method: "provider_reported"}}
	disposition := llm.CacheDispositionV1{Disposition: "hit", Variant: int32(p.Record.Request.RequestIndex)}
	if p.Generate != nil {
		var response llm.GenerateResponseV1
		if json.Unmarshal(origin.Response, &response) != nil || response.Route == nil {
			return llm.ExecutionResultV1{}, executionError(provider.CodeStateCorrupt)
		}
		model.Status, model.Output, model.Route = response.Status, response.Output, *response.Route
	} else {
		cp, err := r.capabilities.Checkpoints.Repository.Get(ctx, identity.Scope, origin.OriginCheckpointID)
		if err != nil {
			return llm.ExecutionResultV1{}, cloudRuntimeError(err, false)
		}
		if cp.ScopeID != identity.Scope || cp.ID != origin.OriginCheckpointID || cp.Kind != state.CheckpointCompaction || cp.OriginOperationID != origin.OriginOperationID {
			return llm.ExecutionResultV1{}, executionError(provider.CodeStateCorrupt)
		}
		data, err := r.capabilities.Checkpoints.Blobs.Read(ctx, identity.Scope, cp.ResponseBlob)
		if err != nil {
			return llm.ExecutionResultV1{}, cloudRuntimeError(err, false)
		}
		if sha256.Sum256(data) != cp.ResponseBlob.Digest || int64(len(data)) != cp.ResponseBlob.ByteLength {
			return llm.ExecutionResultV1{}, executionError(provider.CodeStateCorrupt)
		}
		model.Output, err = r.publication.codec.DecodeResponse(data)
		if err != nil {
			return llm.ExecutionResultV1{}, executionError(provider.CodeStateCorrupt)
		}
	}
	return r.publish(ctx, p, identity, &model, disposition, &origin, effects)
}
func (r *CloudExecutionRuntime) finishNoWork(ctx context.Context, p PreparedCloudRequest) (llm.ExecutionResultV1, error) {
	identity := r.publicationIdentity(p, p.Preparation.PreparedAt)
	return r.publish(ctx, p, identity, nil, llm.CacheDispositionV1{Disposition: "disabled", Variant: int32(p.Record.Request.RequestIndex)}, nil, FinalizationEffects{NoWork: &NoWorkFinalizationEffects{}})
}
func (r *CloudExecutionRuntime) finishAttempt(ctx context.Context, p PreparedCloudRequest, attempt cloudstate.RequestAttempt, saved cloudstate.SavedProviderExecution) (llm.ExecutionResultV1, error) {
	result, err := r.capabilities.Finalizer.LoadAttemptResult(ctx, p.Record.Request.Scope, p.Record.Request.ID)
	if err != nil {
		return llm.ExecutionResultV1{}, err
	}
	if result.Attempt.ID != attempt.ID {
		return llm.ExecutionResultV1{}, executionError(provider.CodeStateCorrupt)
	}
	lease, err := r.cacheLease(ctx, p, attempt, saved.Plan)
	if err != nil {
		return llm.ExecutionResultV1{}, err
	}
	identity := r.publicationIdentity(p, p.Preparation.PreparedAt)
	effects := &ProviderFinalizationEffects{AttemptID: attempt.ID, Unreserved: !saved.Plan.RequiresReservation(), Uncached: lease == nil, Completion: cache.FillCompletion{Outcome: cache.FillNotCacheable, CompletedAt: saved.Execution.CompletedAt}}
	if saved.Execution.Settlement != nil {
		effects.Budget = *saved.Execution.Settlement
	}
	disposition := llm.CacheDispositionV1{Disposition: "disabled", Variant: int32(p.Record.Request.RequestIndex)}
	if lease != nil {
		effects.Lease = *lease
		disposition.Disposition = "miss_not_populated"
		if result.Response.Status == llm.ResponseStatusCompleted || (p.Generate != nil && result.Response.Status == llm.ResponseStatusToolCalls) {
			disposition.Disposition = "miss_populated"
			effects.Entry = &cache.ResponseEntry{ID: state.CacheEntryID(attempt.ID), Key: lease.Key, OriginOperationID: identity.OperationID, OriginCheckpointID: identity.CheckpointID, CompletedAt: saved.Execution.CompletedAt}
			effects.Completion.Outcome, effects.Completion.EntryID = cache.FillPublished, effects.Entry.ID
		}
	}
	return r.publish(ctx, p, identity, &result.Response, disposition, nil, FinalizationEffects{Provider: effects})
}
func (r *CloudExecutionRuntime) publish(ctx context.Context, p PreparedCloudRequest, identity CheckpointPublicationIdentity, model *llm.Response, disposition llm.CacheDispositionV1, origin *cache.ResponseEntry, effects FinalizationEffects) (llm.ExecutionResultV1, error) {
	var data []byte
	if p.Generate != nil {
		cp, response, err := r.publication.Generate(ctx, identity, *p.Generate, p.GenerateReplay, *model, disposition, origin)
		if err != nil {
			return llm.ExecutionResultV1{}, err
		}
		data, err = json.Marshal(response)
		if err != nil {
			return llm.ExecutionResultV1{}, err
		}
		if effects.Provider != nil && effects.Provider.Entry != nil {
			effects.Provider.Entry.Response = data
		}
		if err := r.capabilities.Finalizer.CommitGenerate(ctx, *p.Generate, cp, response, effects); err != nil {
			return llm.ExecutionResultV1{}, err
		}
	} else {
		cp, response, err := r.publication.Compact(ctx, identity, *p.Compact, p.CompactReplay, model, disposition, origin)
		if err != nil {
			return llm.ExecutionResultV1{}, err
		}
		data, err = json.Marshal(response)
		if err != nil {
			return llm.ExecutionResultV1{}, err
		}
		if effects.Provider != nil && effects.Provider.Entry != nil {
			effects.Provider.Entry.Response = data
		}
		if err := r.capabilities.Finalizer.CommitCompact(ctx, *p.Compact, cp, response, effects); err != nil {
			return llm.ExecutionResultV1{}, err
		}
	}
	return r.storeResult(ctx, p, data)
}
