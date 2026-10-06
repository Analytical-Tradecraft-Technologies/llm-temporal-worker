package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// ProviderRecoveryBinding contains original selection facts supplied by durable
// composition. It has no SDK objects, clients, credentials or dispatch grant.
// OperationKeyDigest binds the original stored input separately because the
// semantic request digest deliberately excludes the provider operation key.
type ProviderRecoveryBinding struct {
	ConfigDigest [32]byte
	ConfigEpoch  string
	// EndpointDigest is zero for plans saved before it was recorded. Such a
	// plan can only be recovered under its original configuration.
	EndpointDigest     [32]byte
	RequestDigest      [32]byte
	OperationKeyDigest [32]byte
	CandidateID        string
	Route              durable.RoutePlan
	Family             string
	CapabilityVersion  string
	ProviderTier       string
	RequestedClass     llm.ServiceClass
	AttemptedClass     llm.ServiceClass
}

// RecoveryBinding projects a valid invocation-local call. A resolved quote may
// supply the price version when route configuration did not pin one. Recovery
// does not look that price up again; the original quote/reservation stays owned
// by durable composition.
func (planned PlannedProviderCall) RecoveryBinding(route durable.RoutePlan) (ProviderRecoveryBinding, error) {
	expected, err := planned.Route(route.OperationID, route.GenerationID)
	if err != nil {
		return ProviderRecoveryBinding{}, err
	}
	if expected.PriceVersion == "" {
		expected.PriceVersion = route.PriceVersion
	}
	if route != expected || planned.ConfigDigest == ([32]byte{}) || planned.ConfigEpoch == "" || planned.Candidate.ID == "" {
		return ProviderRecoveryBinding{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	return ProviderRecoveryBinding{ConfigDigest: planned.ConfigDigest, ConfigEpoch: planned.ConfigEpoch, EndpointDigest: planned.Candidate.EndpointDigest,
		RequestDigest: planned.Call.Metadata.SchemaDigest, OperationKeyDigest: ProviderRecoveryOperationKeyDigest(planned.Call.OperationKey),
		CandidateID: planned.Candidate.ID, Route: route, Family: planned.Candidate.Family,
		CapabilityVersion: planned.CapabilityVersion, ProviderTier: planned.Candidate.ProviderTier,
		RequestedClass: planned.Candidate.RequestedClass, AttemptedClass: planned.Candidate.AttemptedClass}, nil
}

// ProviderRecovery reconstructs only the bound route. It does not call the
// injected selection planner, resolve prices, change budget or invoke providers.
type ProviderRecovery struct {
	providers *ProviderPlanning
}

func (capabilities V1RuntimeCapabilities) NewProviderRecovery(ctx context.Context) (*ProviderRecovery, error) {
	providers, err := capabilities.captureProviderPlanning(ctx)
	if err != nil {
		return nil, err
	}
	if providers.configDigest == ([32]byte{}) || providers.configEpoch == "" {
		return nil, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	return &ProviderRecovery{providers: providers}, nil
}

func (recovery *ProviderRecovery) Generate(ctx context.Context, prepared PreparedGenerateInput, binding ProviderRecoveryBinding) (PlannedProviderCall, error) {
	return recovery.recover(ctx, prepared.Request, prepared.pins, binding)
}

func (recovery *ProviderRecovery) Compact(ctx context.Context, prepared PreparedCompactInput, binding ProviderRecoveryBinding) (PlannedProviderCall, error) {
	if prepared.Request == nil {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeInvalidArgument, provider.PhasePlan, provider.RetryNever)
	}
	return recovery.recover(ctx, *prepared.Request, providerStatePins{}, binding)
}

// recover accepts a binding saved under another configuration only when the
// bound route, its endpoint configuration and the compiled request are all
// unchanged. Anything else waits for a compatible worker: a reload or rollout
// must neither reroute bound work nor fail it permanently.
func (recovery *ProviderRecovery) recover(ctx context.Context, request llm.Request, pins providerStatePins, binding ProviderRecoveryBinding) (PlannedProviderCall, error) {
	if ctx == nil || recovery == nil || recovery.providers == nil || isNilCapability(recovery.providers.adapters) {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	if err := ctx.Err(); err != nil {
		return PlannedProviderCall{}, err
	}
	planning := recovery.providers
	reloaded := binding.ConfigDigest != ([32]byte{}) && (binding.ConfigDigest != planning.configDigest || binding.ConfigEpoch != planning.configEpoch)
	planned, err := recovery.recoverBound(ctx, request, pins, binding, reloaded)
	var failure *provider.Error
	if reloaded && errors.As(err, &failure) && failure.Retry == provider.RetryNever && failure.Dispatch == provider.DispatchNotDispatched {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeStateUnavailable, provider.PhasePlan, provider.RetrySameOperation)
	}
	return planned, err
}

func (recovery *ProviderRecovery) recoverBound(ctx context.Context, request llm.Request, pins providerStatePins, binding ProviderRecoveryBinding, reloaded bool) (PlannedProviderCall, error) {
	planning := recovery.providers
	if binding.ConfigDigest == ([32]byte{}) || (reloaded && binding.EndpointDigest == ([32]byte{})) ||
		binding.RequestDigest == ([32]byte{}) || binding.OperationKeyDigest == ([32]byte{}) || binding.CandidateID == "" ||
		binding.Route.Validate() != nil || !provider.Family(binding.Family).Valid() || binding.CapabilityVersion == "" || binding.ProviderTier == "" ||
		!binding.RequestedClass.Valid() || !binding.AttemptedClass.Valid() {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	semantic, err := planning.normalizeRequest(request)
	if err != nil || semantic.Continuation != nil {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeInvalidArgument, provider.PhasePlan, provider.RetryNever)
	}
	if ProviderRecoveryOperationKeyDigest(semantic.OperationKey) != binding.OperationKeyDigest {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	// Recheck configured eligibility, including tenant/region, capabilities and
	// authorized classes, without the selection planner or transient health.
	// Candidate ID binds route/fallback order as well as extension/pinning facts.
	candidates, err := (routing.DeterministicPlanner{}).Plan(ctx, routing.Input{Request: semantic, Catalog: planning.catalog})
	if ctx.Err() != nil {
		return PlannedProviderCall{}, ctx.Err()
	}
	if err != nil {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	for _, candidate := range candidates.Candidates {
		if candidate.ID != binding.CandidateID && !planning.pinnedCandidate(semantic, candidate, binding) {
			continue
		}
		if !planning.containsCandidate(semantic, candidate) || candidate.RouteID != binding.Route.RouteID || candidate.EndpointID != binding.Route.EndpointID ||
			candidate.Provider != binding.Route.Provider || candidate.Model != binding.Route.Model || providerCacheIdentity(candidate) != binding.Route.CacheIdentity ||
			candidate.Family != binding.Family || candidate.CapabilityVersion != binding.CapabilityVersion || candidate.ProviderTier != binding.ProviderTier ||
			candidate.RequestedClass != binding.RequestedClass || candidate.AttemptedClass != binding.AttemptedClass ||
			(reloaded && candidate.EndpointDigest != binding.EndpointDigest) ||
			(candidate.PriceVersion != "" && candidate.PriceVersion != binding.Route.PriceVersion) {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
		}
		// Changed input is rejected before any adapter lookup. Which form of a
		// summarizer request was bound is an endpoint fact, so this accepts
		// either and the compiled call settles it below.
		if !plausibleCandidateDigest(semantic, pins, candidate, binding.RequestDigest) {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhaseCompile, provider.RetryNever)
		}
		if health, present := planning.health.Routes[candidate.RouteID]; present && (!health.Enabled || health.Open || health.AuthOpen) {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeNoRoute, provider.PhasePlan, provider.RetrySameOperation)
		}
		planned, rejection, err := planning.compileCandidate(ctx, semantic, pins, candidate)
		if err != nil {
			return PlannedProviderCall{}, err
		}
		if rejection != nil {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeStateUnavailable, provider.PhaseCompile, provider.RetrySameOperation)
		}
		// compileCandidate derived its digest through resolveCandidateRequest
		// with the endpoint's adapter; only that exact request was bound.
		if planned.Call.Metadata.SchemaDigest != binding.RequestDigest {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhaseCompile, provider.RetryNever)
		}
		return planned, nil
	}
	return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
}

// pinnedCandidate reports whether the binding recorded this candidate under the
// ID it carried while its route still pinned the bound price version. A route
// whose catalog schedules a price version change compiles unpinned, but a plan
// made by an earlier snapshot of the same configuration, and still awaiting
// its paid dispatch, names the pinned ID. The bound route, quote and
// reservation stay exactly as recorded.
func (planning *ProviderPlanning) pinnedCandidate(request llm.Request, candidate routing.Candidate, binding ProviderRecoveryBinding) bool {
	if candidate.PriceVersion != "" || binding.Route.PriceVersion == "" || !planning.containsCandidate(request, candidate) {
		return false
	}
	route := planning.catalog.Models[request.Model].Routes[candidate.RouteIndex]
	id, err := candidate.PinnedPriceID(route, binding.Route.PriceVersion)
	return err == nil && id == binding.CandidateID
}

func planEndpointDigest(plan cloudstate.BudgetPlan) [32]byte {
	var digest [32]byte
	if decoded, err := hex.DecodeString(plan.EndpointDigest); err == nil && len(decoded) == len(digest) {
		copy(digest[:], decoded)
	}
	return digest
}

// ProviderRecoveryOperationKeyDigest binds the original key read from durable
// input. It is separate from the semantic/cache digest and is not an auth token.
func ProviderRecoveryOperationKeyDigest(key string) [32]byte {
	return sha256.Sum256([]byte("llmtw/cloud-provider-recovery/operation-key/v1\x00" + key))
}
