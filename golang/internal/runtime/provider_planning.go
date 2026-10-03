package runtime

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// cloudCompilerVersion changes when this projection/lowering contract changes.
// It is independent of the public activity version.
const cloudCompilerVersion = "cloud-v1"

// PlannedProviderCall is an invocation-local compiled call, not a dispatch grant
// or a persistable recovery record. SDKParams and Adapter stay in this process.
// Composition must persist its route/attempt identity and recover earlier paid
// work before reserving budget; only a fresh Redis claim permits submission.
type PlannedProviderCall struct {
	Candidate         routing.Candidate
	CacheIdentity     cache.RouteIdentity
	CapabilityVersion string
	ConfigDigest      [32]byte
	ConfigEpoch       string
	Call              provider.Call
	Adapter           provider.Adapter
}

// Route binds the already compiled selection to durable, caller-supplied IDs.
// Generate and Compact use the same identity for cache lookup and admission.
func (planned PlannedProviderCall) Route(operation durable.OperationID, generation durable.GenerationID) (durable.RoutePlan, error) {
	if isNilCapability(planned.Adapter) || !validPlannedCall(planned.Call, planned.Candidate, planned.Call.OperationKey, planned.Call.Metadata.SchemaDigest) ||
		planned.CacheIdentity != providerCacheIdentity(planned.Candidate) || planned.CapabilityVersion != planned.Candidate.CapabilityVersion {
		return durable.RoutePlan{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	plan := durable.RoutePlan{OperationID: operation, GenerationID: generation,
		RouteID: planned.Candidate.RouteID, EndpointID: planned.Candidate.EndpointID,
		Provider: planned.Candidate.Provider, Model: planned.Candidate.Model,
		PriceVersion: planned.Candidate.PriceVersion, CacheIdentity: planned.CacheIdentity}
	if plan.Validate() != nil {
		return durable.RoutePlan{}, providerPlanningError(provider.CodeInvalidArgument, provider.PhasePlan, provider.RetryNever)
	}
	return plan, nil
}

// ProviderPlanning captures routing inputs once per runtime snapshot. It holds
// no operation state and never invokes a provider or touches budget/storage.
// The injected planner and registry must themselves be snapshot-owned.
type ProviderPlanning struct {
	catalog        routing.Catalog
	health         routing.HealthView
	planner        routing.Planner
	adapters       engine.AdapterRegistry
	configDigest   [32]byte
	configEpoch    string
	budgetSnapshot engine.Snapshot
}

func (capabilities V1RuntimeCapabilities) NewProviderPlanning(ctx context.Context) (*ProviderPlanning, error) {
	if ctx == nil || isNilCapability(capabilities.Snapshot) || isNilCapability(capabilities.Planner) || isNilCapability(capabilities.Adapters) {
		return nil, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	snapshot, err := capabilities.Snapshot.Current(ctx)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, providerPlanningError(provider.CodeStateUnavailable, provider.PhasePlan, provider.RetrySameOperation)
	}
	if capabilities.ConfigDigest != ([32]byte{}) && capabilities.ConfigDigest != snapshot.ConfigDigest {
		return nil, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	catalog, err := copyProviderCatalog(snapshot.Routes)
	if err != nil {
		return nil, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	return &ProviderPlanning{catalog: catalog, health: copyProviderHealth(snapshot.Health),
		planner: capabilities.Planner, adapters: capabilities.Adapters,
		configDigest: snapshot.ConfigDigest, configEpoch: snapshot.ConfigEpoch,
		budgetSnapshot: copyBudgetSnapshot(snapshot)}, nil
}

func (planning *ProviderPlanning) Generate(ctx context.Context, prepared PreparedGenerateInput) (PlannedProviderCall, error) {
	return planning.plan(ctx, prepared.Request)
}

func (planning *ProviderPlanning) Compact(ctx context.Context, prepared PreparedCompactInput) (PlannedProviderCall, error) {
	if prepared.Request == nil {
		// An empty/safely retained prefix must not reach route or budget work.
		return PlannedProviderCall{}, providerPlanningError(provider.CodeInvalidArgument, provider.PhasePlan, provider.RetryNever)
	}
	return planning.plan(ctx, *prepared.Request)
}

func (planning *ProviderPlanning) plan(ctx context.Context, request llm.Request) (PlannedProviderCall, error) {
	return planning.selectCall(ctx, request, nil)
}

// selectCall permits the budget planner to reject a compiled candidate before
// it wins selection. Neither callback nor compilation may perform paid work.
func (planning *ProviderPlanning) selectCall(ctx context.Context, request llm.Request, accept func(PlannedProviderCall) (bool, error)) (PlannedProviderCall, error) {
	if ctx == nil || planning == nil || isNilCapability(planning.planner) || isNilCapability(planning.adapters) {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	if err := ctx.Err(); err != nil {
		return PlannedProviderCall{}, err
	}
	semantic, err := llm.NormalizeRequest(request)
	if err != nil || semantic.Continuation != nil {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeInvalidArgument, provider.PhasePlan, provider.RetryNever)
	}
	// Give even a custom planner its own maps and request. It cannot change the
	// captured snapshot or the semantic input used for provider compilation.
	catalog, _ := copyProviderCatalog(planning.catalog)
	plannerRequest, _ := llm.NormalizeRequest(semantic)
	plan, err := planning.planner.Plan(ctx, routing.Input{Request: plannerRequest, Catalog: catalog, Health: copyProviderHealth(planning.health)})
	if ctx.Err() != nil {
		return PlannedProviderCall{}, ctx.Err()
	}
	if err != nil || len(plan.Candidates) == 0 {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeNoRoute, provider.PhasePlan, provider.RetryNever)
	}
	lastPhase := provider.PhaseCompile
	for _, candidate := range plan.Candidates {
		if !planning.containsCandidate(semantic, candidate) {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
		}
		adapter, err := planning.adapters.Adapter(ctx, candidate)
		if ctx.Err() != nil {
			return PlannedProviderCall{}, ctx.Err()
		}
		if err != nil {
			if err := unsafePlanningFailure(err); err != nil {
				return PlannedProviderCall{}, err
			}
			continue
		}
		if isNilCapability(adapter) {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhaseCompile, provider.RetryNever)
		}
		query := provider.CapabilityQuery{EndpointID: candidate.EndpointID, Family: provider.Family(candidate.Family), Model: candidate.Model, ServiceClass: candidate.AttemptedClass}
		capability, err := adapter.Capabilities(ctx, query)
		if ctx.Err() != nil {
			return PlannedProviderCall{}, ctx.Err()
		}
		if err != nil {
			if err := unsafePlanningFailure(err); err != nil {
				return PlannedProviderCall{}, err
			}
			continue
		}
		if capability.Version != candidate.CapabilityVersion {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhaseCompile, provider.RetryNever)
		}
		resolved, _ := llm.NormalizeRequest(semantic)
		// The adapter needs the provider model and attempted class, not the
		// logical alias or the originally requested fallback class.
		resolved.Model, resolved.ServiceClass = candidate.Model, candidate.AttemptedClass
		resolved.ServiceClassFallbacks = nil
		digest, err := llm.RequestDigest(resolved)
		if err != nil {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeInvalidArgument, provider.PhaseCompile, provider.RetryNever)
		}
		call, err := adapter.Compile(ctx, provider.CompileInput{Request: resolved, Query: query,
			Capability: capability, Strict: semantic.Portability != llm.PortabilityBestEffort,
			Metadata: provider.CallMetadata{SchemaDigest: digest, CapabilityVersion: candidate.CapabilityVersion, ProviderTier: candidate.ProviderTier}})
		if ctx.Err() != nil {
			return PlannedProviderCall{}, ctx.Err()
		}
		if err != nil {
			if err := unsafePlanningFailure(err); err != nil {
				return PlannedProviderCall{}, err
			}
			continue
		}
		if !validPlannedCall(call, candidate, semantic.OperationKey, digest) {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhaseCompile, provider.RetryNever)
		}
		planned := PlannedProviderCall{Candidate: candidate, CacheIdentity: providerCacheIdentity(candidate),
			CapabilityVersion: capability.Version, ConfigDigest: planning.configDigest, ConfigEpoch: planning.configEpoch,
			Call: call, Adapter: adapter}
		if accept != nil {
			accepted, err := accept(planned)
			if ctx.Err() != nil {
				return PlannedProviderCall{}, ctx.Err()
			}
			if err != nil {
				return PlannedProviderCall{}, err
			}
			if !accepted {
				lastPhase = provider.PhasePrice
				continue
			}
		}
		return planned, nil
	}
	return PlannedProviderCall{}, providerPlanningError(provider.CodeNoRoute, lastPhase, provider.RetryNever)
}

func copyBudgetSnapshot(source engine.Snapshot) engine.Snapshot {
	switch prices := source.Prices.(type) {
	case *pricing.PriceResolver:
		source.Prices = prices.Snapshot()
	case pricing.Catalog:
		source.Prices = pricing.NewResolver(prices).Snapshot()
	}
	source.BudgetPolicies = append([]budget.Policy(nil), source.BudgetPolicies...)
	for index := range source.BudgetPolicies {
		source.BudgetPolicies[index].Windows = append([]budget.Window(nil), source.BudgetPolicies[index].Windows...)
	}
	return source
}

func (planning *ProviderPlanning) containsCandidate(request llm.Request, candidate routing.Candidate) bool {
	model := planning.catalog.Models[request.Model]
	if candidate.RouteIndex < 0 || candidate.RouteIndex >= len(model.Routes) || candidate.FallbackIndex < 0 {
		return false
	}
	route := model.Routes[candidate.RouteIndex]
	classes := append([]llm.ServiceClass{request.ServiceClass}, request.ServiceClassFallbacks...)
	return candidate.FallbackIndex < len(classes) && candidate.AttemptedClass == classes[candidate.FallbackIndex] && candidate.RequestedClass == request.ServiceClass &&
		candidate.ID != "" && candidate.RouteID == route.ID && candidate.EndpointID == route.EndpointID && candidate.Provider == route.Provider &&
		provider.Family(candidate.Family).Valid() && candidate.Family == route.Family && candidate.Model == route.Model && candidate.ModelRevision == route.ModelRevision &&
		candidate.EndpointAccountHMAC == route.EndpointAccountHMAC && candidate.Region == route.Region &&
		candidate.CapabilityVersion != "" && candidate.CapabilityVersion == route.Capabilities.Version && candidate.ProviderTier != "" && candidate.ProviderTier == route.ProviderTiers[candidate.AttemptedClass] &&
		candidate.PriceVersion == route.PriceVersion
}

func validPlannedCall(call provider.Call, candidate routing.Candidate, operationKey string, digest [32]byte) bool {
	return call.EndpointID == candidate.EndpointID && call.Family == provider.Family(candidate.Family) && call.Model == candidate.Model &&
		operationKey != "" && call.OperationKey == operationKey && call.ServiceClass == candidate.AttemptedClass && !isNilCapability(call.SDKParams) &&
		digest != ([32]byte{}) && call.Metadata.SchemaDigest == digest && call.Metadata.CapabilityVersion == candidate.CapabilityVersion &&
		call.Metadata.ProviderTier == candidate.ProviderTier && !call.Metadata.OpaqueStateRequired
}

func providerCacheIdentity(candidate routing.Candidate) cache.RouteIdentity {
	account := ""
	if candidate.EndpointAccountHMAC != ([32]byte{}) {
		account = hex.EncodeToString(candidate.EndpointAccountHMAC[:])
	}
	return cache.RouteIdentity{Provider: cache.Provider(candidate.Provider), Endpoint: cache.Endpoint(candidate.EndpointID),
		Account: cache.Account(account), Region: cache.Region(candidate.Region), Model: cache.Model(candidate.Model),
		Revision: cache.ModelRevision(candidate.ModelRevision), Compiler: cache.CompilerProfile(candidate.Family + "/" + cloudCompilerVersion)}
}

func unsafePlanningFailure(err error) error {
	var mapped *provider.Error
	if errors.As(err, &mapped) && mapped.Dispatch != provider.DispatchNotDispatched {
		// A compiler/registry must not perform paid work. Do not fallback when
		// it reports that it did; preserve ambiguity without leaking its cause.
		return provider.NewError(provider.CodeAmbiguousDispatch, provider.PhaseCompile, provider.DispatchAmbiguous, provider.RetryNever, "provider planning reported possible dispatch")
	}
	return nil
}

func providerPlanningError(code provider.Code, phase provider.Phase, retry provider.RetryDisposition) error {
	return provider.NewError(code, phase, provider.DispatchNotDispatched, retry, "provider planning failed")
}

func copyProviderCatalog(source routing.Catalog) (routing.Catalog, error) {
	catalog, err := routing.CompileCatalog(source.Version, source.Models)
	if err != nil {
		return routing.Catalog{}, err
	}
	for _, model := range catalog.Models {
		for index := range model.Routes {
			model.Routes[index].Capabilities.Features = cloneRoutingFeaturesForPlanning(model.Routes[index].Capabilities.Features)
		}
	}
	return catalog, nil
}

func cloneRoutingFeaturesForPlanning(source map[routing.Feature]routing.Capability) map[routing.Feature]routing.Capability {
	result := make(map[routing.Feature]routing.Capability, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func copyProviderHealth(source routing.HealthView) routing.HealthView {
	result := routing.HealthView{Routes: make(map[string]routing.RouteHealth, len(source.Routes))}
	for key, value := range source.Routes {
		result.Routes[key] = value
	}
	return result
}
