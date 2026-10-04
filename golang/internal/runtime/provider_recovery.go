package runtime

import (
	"context"
	"crypto/sha256"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// ProviderRecoveryBinding contains original selection facts supplied by durable
// composition. It has no SDK objects, clients, credentials or dispatch grant.
// OperationKeyDigest binds the original stored input separately because the
// semantic request digest deliberately excludes the provider operation key.
type ProviderRecoveryBinding struct {
	ConfigDigest       [32]byte
	ConfigEpoch        string
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
	return ProviderRecoveryBinding{ConfigDigest: planned.ConfigDigest, ConfigEpoch: planned.ConfigEpoch,
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
	return recovery.recover(ctx, prepared.Request, binding)
}

func (recovery *ProviderRecovery) Compact(ctx context.Context, prepared PreparedCompactInput, binding ProviderRecoveryBinding) (PlannedProviderCall, error) {
	if prepared.Request == nil {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeInvalidArgument, provider.PhasePlan, provider.RetryNever)
	}
	return recovery.recover(ctx, *prepared.Request, binding)
}

func (recovery *ProviderRecovery) recover(ctx context.Context, request llm.Request, binding ProviderRecoveryBinding) (PlannedProviderCall, error) {
	if ctx == nil || recovery == nil || recovery.providers == nil || isNilCapability(recovery.providers.adapters) {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	if err := ctx.Err(); err != nil {
		return PlannedProviderCall{}, err
	}
	planning := recovery.providers
	if binding.ConfigDigest == ([32]byte{}) || binding.ConfigDigest != planning.configDigest || binding.ConfigEpoch != planning.configEpoch ||
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
		if candidate.ID != binding.CandidateID {
			continue
		}
		if !planning.containsCandidate(semantic, candidate) || candidate.RouteID != binding.Route.RouteID || candidate.EndpointID != binding.Route.EndpointID ||
			candidate.Provider != binding.Route.Provider || candidate.Model != binding.Route.Model || providerCacheIdentity(candidate) != binding.Route.CacheIdentity ||
			candidate.Family != binding.Family || candidate.CapabilityVersion != binding.CapabilityVersion || candidate.ProviderTier != binding.ProviderTier ||
			candidate.RequestedClass != binding.RequestedClass || candidate.AttemptedClass != binding.AttemptedClass ||
			(candidate.PriceVersion != "" && candidate.PriceVersion != binding.Route.PriceVersion) {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
		}
		resolved, _ := llm.NormalizeRequest(semantic)
		resolved.Model, resolved.ServiceClass = candidate.Model, candidate.AttemptedClass
		resolved.ServiceClassFallbacks = nil
		digest, err := llm.RequestDigest(resolved)
		if err != nil || digest != binding.RequestDigest {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhaseCompile, provider.RetryNever)
		}
		if health, present := planning.health.Routes[candidate.RouteID]; present && (!health.Enabled || health.Open || health.AuthOpen) {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeNoRoute, provider.PhasePlan, provider.RetrySameOperation)
		}
		planned, usable, err := planning.compileCandidate(ctx, semantic, candidate)
		if err != nil {
			return PlannedProviderCall{}, err
		}
		if !usable {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeStateUnavailable, provider.PhaseCompile, provider.RetrySameOperation)
		}
		return planned, nil
	}
	return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
}

// ProviderRecoveryOperationKeyDigest binds the original key read from durable
// input. It is separate from the semantic/cache digest and is not an auth token.
func ProviderRecoveryOperationKeyDigest(key string) [32]byte {
	return sha256.Sum256([]byte("llmtw/cloud-provider-recovery/operation-key/v1\x00" + key))
}
