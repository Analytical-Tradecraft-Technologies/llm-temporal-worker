package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// CloudFinalizationStore persists immutable instructions in the opened request stores.
// Implementations must verify the committed checkpoint before saving or loading.
type CloudFinalizationStore interface {
	SaveFinalizationHandoff(context.Context, cloudstate.Scope, cloudstate.RequestID, cloudstate.FinalizationHandoff, time.Time) error
	LoadFinalizationHandoff(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.FinalizationHandoff, error)
}

// ProviderFinalizationEffects contains the original attempt's exact settlement
// inputs. Do not regenerate timestamps, IDs or costs on a retry. Entry is nil
// for an incomplete/otherwise uncacheable response. No new budget is acquired.
type ProviderFinalizationEffects struct {
	Lease      cache.FillLease          `json:"lease"`
	Completion cache.FillCompletion     `json:"completion"`
	Entry      *cache.ResponseEntry     `json:"entry,omitempty"`
	Budget     durable.ReconcileRequest `json:"budget"`
}

type CacheFinalizationEffects struct {
	Origin cache.ResponseEntry `json:"origin"`
	Use    cache.ResponseUse   `json:"use"`
}

// FinalizationEffects selects exactly one path: paid provider settlement or a
// zero-cost cache-use receipt. The latter never touches Redis budget.
type FinalizationEffects struct {
	Provider *ProviderFinalizationEffects `json:"provider,omitempty"`
	Cache    *CacheFinalizationEffects    `json:"cache,omitempty"`
}

type cloudFinalizationPayload struct {
	Version  int                 `json:"version"`
	Response json.RawMessage     `json:"response"`
	Effects  FinalizationEffects `json:"effects"`
}

// CloudFinalizer is snapshot-owned. Phase finalizers save a handoff AFTER the
// consumer/origin checkpoint commits and BEFORE publication or settlement.
// The outer cloud runtime resumes it before invoking any inner phase on retry.
// This does not recover a crash before Save succeeds; the inner phases must
// recover that interval without redispatching an already claimed attempt.
type CloudFinalizer struct {
	requests CloudRequestRepository
	store    CloudFinalizationStore
	cache    *durable.ResponseCache
	budgets  durable.BudgetLeaser
	clock    func() time.Time
}

func newCloudFinalizer(repository CloudRequestRepository, responses cache.ResponseRepository, fills cache.FillRepository, budgets durable.BudgetLeaser, clock func() time.Time) (*CloudFinalizer, error) {
	store, ok := repository.(CloudFinalizationStore)
	if !ok || isNilCapability(store) || clock == nil {
		return nil, errors.New("cloud finalization store and clock are required")
	}
	responseCache, err := durable.NewResponseCache(responses, fills, clock)
	if err != nil {
		return nil, err
	}
	return &CloudFinalizer{requests: repository, store: store, cache: responseCache, budgets: budgets, clock: clock}, nil
}

// SaveGenerate persists a typed handoff bound to every input of this operation.
// CheckpointScope and CheckpointID are trusted internal values, never handles.
func (f *CloudFinalizer) SaveGenerate(ctx context.Context, request llm.GenerateRequestV1, checkpointScope string, checkpointID state.CheckpointID, response llm.GenerateResponseV1, effects FinalizationEffects) error {
	input, err := json.Marshal(request)
	if err != nil {
		return cloudRuntimeError(cloudstate.ErrInvalid, false)
	}
	index := int64(0)
	if request.Cache != nil {
		index = int64(request.Cache.Variant)
	}
	data, err := json.Marshal(response)
	if err != nil {
		return cloudRuntimeError(cloudstate.ErrInvalid, true)
	}
	return f.save(ctx, request.Context, "generate", request.OperationKey, index, input, checkpointScope, checkpointID, data, effects)
}

func (f *CloudFinalizer) SaveCompact(ctx context.Context, request llm.CompactRequestV1, checkpointScope string, checkpointID state.CheckpointID, response llm.CompactResponseV1, effects FinalizationEffects) error {
	input, err := json.Marshal(request)
	if err != nil {
		return cloudRuntimeError(cloudstate.ErrInvalid, false)
	}
	data, err := json.Marshal(response)
	if err != nil {
		return cloudRuntimeError(cloudstate.ErrInvalid, true)
	}
	return f.save(ctx, request.Context, "compact", request.OperationKey, 0, input, checkpointScope, checkpointID, data, effects)
}

func (f *CloudFinalizer) save(ctx context.Context, caller llm.RequestContext, kind, key string, index int64, input json.RawMessage, checkpointScope string, checkpointID state.CheckpointID, response json.RawMessage, effects FinalizationEffects) error {
	if f == nil || ctx == nil {
		return cloudRuntimeError(cloudstate.ErrInvalid, true)
	}
	payload := cloudFinalizationPayload{Version: 1, Response: response, Effects: effects}
	handoff := cloudstate.FinalizationHandoff{CheckpointScope: checkpointScope, CheckpointID: checkpointID, Mode: "provider"}
	if effects.Cache != nil {
		handoff.Mode = "cache"
	}
	operationID, err := validateFinalizationPayload(kind, key, index, handoff, payload)
	if err != nil {
		return cloudRuntimeError(cloudstate.ErrInvalid, true)
	}
	handoff.OperationID = operationID
	handoff.Payload, err = json.Marshal(payload)
	if err != nil {
		return cloudRuntimeError(cloudstate.ErrInvalid, true)
	}
	if effects.Provider != nil && isNilCapability(f.budgets) {
		return cloudRuntimeError(cloudstate.ErrInvalid, true)
	}
	scope := cloudstate.Scope{Tenant: caller.Tenant, Project: caller.Project}
	record, err := f.requests.BeginOperation(ctx, cloudstate.Operation{Scope: scope, Kind: kind, Key: key, RequestIndex: index, Manifest: input, Now: f.clock()})
	if err == nil {
		err = f.store.SaveFinalizationHandoff(ctx, scope, record.Request.ID, handoff, f.clock())
	}
	if err != nil {
		return cloudRuntimeError(err, true)
	}
	return nil
}

// replay returns a missing handoff as found=false. Only the inner runner may
// decide whether earlier phases are recoverable. Any other failure stops it.
func (f *CloudFinalizer) replay(ctx context.Context, scope cloudstate.Scope, record cloudstate.Record, kind, key string, index int64) ([]byte, bool, error) {
	handoff, err := f.store.LoadFinalizationHandoff(ctx, scope, record.Request.ID)
	var progress map[string]json.RawMessage
	_ = json.Unmarshal(record.Progress, &progress)
	_, saved := progress["finalization_handoff"]
	if errors.Is(err, cloudstate.ErrFinalizationHandoffMissing) && !saved {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, cloudRuntimeError(err, true)
	}
	var payload cloudFinalizationPayload
	decoder := json.NewDecoder(bytes.NewReader(handoff.Payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil {
		return nil, true, cloudRuntimeError(cloudstate.ErrCorrupt, true)
	}
	operationID, err := validateFinalizationPayload(kind, key, index, handoff, payload)
	if err != nil || operationID != handoff.OperationID {
		return nil, true, cloudRuntimeError(cloudstate.ErrCorrupt, true)
	}
	if err := f.finish(ctx, payload.Effects); err != nil {
		return nil, true, cloudRuntimeError(err, true)
	}
	return payload.Response, true, nil
}

// CompleteGenerate saves the handoff before running the finalization effects.
// Bind this to the phase's reconciliation callback after checkpoint commit.
func (f *CloudFinalizer) CompleteGenerate(ctx context.Context, request llm.GenerateRequestV1, scope string, checkpoint state.CheckpointID, response llm.GenerateResponseV1, effects FinalizationEffects) error {
	if err := f.SaveGenerate(ctx, request, scope, checkpoint, response, effects); err != nil {
		return err
	}
	if err := f.finish(ctx, effects); err != nil {
		return cloudRuntimeError(err, true)
	}
	return nil
}

// CompleteCompact is the equivalent boundary for the compaction finalizer.
func (f *CloudFinalizer) CompleteCompact(ctx context.Context, request llm.CompactRequestV1, scope string, checkpoint state.CheckpointID, response llm.CompactResponseV1, effects FinalizationEffects) error {
	if err := f.SaveCompact(ctx, request, scope, checkpoint, response, effects); err != nil {
		return err
	}
	if err := f.finish(ctx, effects); err != nil {
		return cloudRuntimeError(err, true)
	}
	return nil
}

func (f *CloudFinalizer) finish(ctx context.Context, effects FinalizationEffects) error {
	if paid := effects.Provider; paid != nil {
		if isNilCapability(f.budgets) {
			return cloudstate.ErrCorrupt
		}
		return f.cache.CompleteAttempt(ctx, paid.Lease, paid.Completion, paid.Entry, func(ctx context.Context) error { return f.budgets.Reconcile(ctx, paid.Budget) })
	}
	return f.cache.RecordUse(ctx, effects.Cache.Origin, effects.Cache.Use)
}

func validateFinalizationPayload(kind, key string, index int64, handoff cloudstate.FinalizationHandoff, payload cloudFinalizationPayload) (state.OperationID, error) {
	invalid := cloudstate.ErrInvalid
	if payload.Version != 1 || handoff.CheckpointScope == "" || handoff.CheckpointID == "" || (payload.Effects.Provider == nil) == (payload.Effects.Cache == nil) {
		return "", invalid
	}
	var operationID string
	var disposition string
	var cost llm.CostV1
	var publishable bool
	switch kind {
	case "generate":
		var response llm.GenerateResponseV1
		if json.Unmarshal(payload.Response, &response) != nil || response.OperationKey != key || int64(response.Cache.Variant) != index {
			return "", invalid
		}
		operationID, disposition, cost = response.OperationID, response.Cache.Disposition, response.Cost
		publishable = response.Status == llm.ResponseStatusCompleted || response.Status == llm.ResponseStatusToolCalls
	case "compact":
		var response llm.CompactResponseV1
		if json.Unmarshal(payload.Response, &response) != nil || response.OperationKey != key || response.Cache.Variant != 0 {
			return "", invalid
		}
		operationID, disposition, cost = response.OperationID, response.Cache.Disposition, response.Cost
		publishable = true
	default:
		return "", invalid
	}
	if operationID == "" {
		return "", invalid
	}
	if paid := payload.Effects.Provider; paid != nil {
		if handoff.Mode != "provider" || disposition == "hit" ||
			paid.Lease.Key.ScopeID != handoff.CheckpointScope ||
			string(paid.Lease.Key.Operation) != kind ||
			paid.Lease.Key.RequestIndex != index ||
			string(paid.Lease.OperationID) != operationID ||
			string(paid.Budget.OperationID) != operationID ||
			string(paid.Budget.GenerationID) != paid.Lease.Attempt ||
			paid.Budget.Validate() != nil ||
			durable.ValidateAttemptCompletion(paid.Lease, paid.Completion, paid.Entry) != nil {
			return "", invalid
		}
		for _, event := range paid.Budget.Events {
			if cost.Status == "exact" {
				if cost.ActualCostUSD == nil || event.Kind != budget.JournalFinalizeExact || event.ActualCostUSD == nil {
					return "", invalid
				}
				actual, err := pricing.ParseUSD(*cost.ActualCostUSD)
				if err != nil || actual.Cmp(*event.ActualCostUSD) != 0 {
					return "", invalid
				}
			} else if cost.Status != "unknown" || event.Kind != budget.JournalFinalizeUnknown {
				return "", invalid
			}
		}
		// A response handoff settles an already completed model response, not a
		// provider failure or unresolved submission (those have separate recovery).
		if paid.Completion.Outcome != cache.FillPublished && paid.Completion.Outcome != cache.FillNotCacheable {
			return "", invalid
		}
		if paid.Entry != nil && (!publishable ||
			paid.Entry.OriginCheckpointID != handoff.CheckpointID || !equalFinalizationJSON(paid.Entry.Response, payload.Response)) {
			return "", invalid
		}
	} else {
		hit := payload.Effects.Cache
		if handoff.Mode != "cache" || disposition != "hit" || !publishable || cost.ActualCostUSD == nil || *cost.ActualCostUSD != "0" || cost.Status != "exact" ||
			hit.Use.ScopeID != handoff.CheckpointScope ||
			hit.Use.CheckpointID != handoff.CheckpointID ||
			string(hit.Use.OperationID) != operationID ||
			string(hit.Origin.Key.Operation) != kind ||
			hit.Origin.Key.RequestIndex != index ||
			durable.ValidateResponseUse(hit.Origin, hit.Use) != nil {
			return "", invalid
		}
	}
	return state.OperationID(operationID), nil
}

func equalFinalizationJSON(left, right json.RawMessage) bool {
	var l, r any
	leftDecoder, rightDecoder := json.NewDecoder(bytes.NewReader(left)), json.NewDecoder(bytes.NewReader(right))
	leftDecoder.UseNumber()
	rightDecoder.UseNumber()
	if leftDecoder.Decode(&l) != nil || rightDecoder.Decode(&r) != nil {
		return false
	}
	a, _ := json.Marshal(l)
	b, _ := json.Marshal(r)
	return bytes.Equal(a, b)
}
