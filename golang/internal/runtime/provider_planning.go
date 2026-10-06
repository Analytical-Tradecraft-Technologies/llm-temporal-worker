package runtime

import (
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/compaction"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// cloudCompilerVersion changes when this projection/lowering contract changes.
// It is independent of the public activity version. cloud-v2 flattens the
// summarizer instructions for candidates that cannot keep the levels apart.
const cloudCompilerVersion = "cloud-v2"

// PlannedProviderCall is an invocation-local compiled call, not a dispatch grant
// or a persistable recovery record. SDKParams and Adapter stay in this process.
// Composition must persist its route/attempt identity and recover earlier paid
// work before reserving budget; only a fresh Redis claim permits submission.
type PlannedProviderCall struct {
	Semantic          llm.Request
	LogicalModel      string
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
// no operation state and never invokes a provider or changes budget/storage. Shared health is read per selection.
// The injected planner and registry must themselves be snapshot-owned.
type ProviderPlanning struct {
	routeStatus      ProviderRouteStatusReader
	clock            func() time.Time
	catalog          routing.Catalog
	health           routing.HealthView
	planner          routing.Planner
	adapters         engine.AdapterRegistry
	configDigest     [32]byte
	configEpoch      string
	budgetSnapshot   engine.Snapshot
	outputLimit      int64
	contextEstimator budget.Estimator
}

func (capabilities V1RuntimeCapabilities) NewProviderPlanning(ctx context.Context) (*ProviderPlanning, error) {
	if isNilCapability(capabilities.Planner) {
		return nil, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	return capabilities.captureProviderPlanning(ctx)
}

func (capabilities V1RuntimeCapabilities) captureProviderPlanning(ctx context.Context) (*ProviderPlanning, error) {
	if ctx == nil || isNilCapability(capabilities.Snapshot) || isNilCapability(capabilities.Adapters) {
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
	clock := capabilities.Clock
	if clock == nil {
		clock = time.Now
	}
	return &ProviderPlanning{routeStatus: capabilities.ProviderRouteStatus, clock: clock, catalog: catalog, health: copyProviderHealth(snapshot.Health),
		planner: capabilities.Planner, adapters: capabilities.Adapters,
		configDigest: snapshot.ConfigDigest, configEpoch: snapshot.ConfigEpoch,
		outputLimit: capabilities.BudgetEstimator.MaxOutput, contextEstimator: copyBudgetEstimator(capabilities.BudgetEstimator), budgetSnapshot: copyBudgetSnapshot(snapshot)}, nil
}

func (planning *ProviderPlanning) Generate(ctx context.Context, prepared PreparedGenerateInput) (PlannedProviderCall, error) {
	return planning.plan(ctx, prepared.Request, prepared.pins)
}

func (planning *ProviderPlanning) Compact(ctx context.Context, prepared PreparedCompactInput) (PlannedProviderCall, error) {
	if prepared.Request == nil {
		// An empty/safely retained prefix must not reach route or budget work.
		return PlannedProviderCall{}, providerPlanningError(provider.CodeInvalidArgument, provider.PhasePlan, provider.RetryNever)
	}
	return planning.plan(ctx, *prepared.Request, providerStatePins{})
}

func (planning *ProviderPlanning) plan(ctx context.Context, request llm.Request, pins providerStatePins) (PlannedProviderCall, error) {
	return planning.selectCall(ctx, request, pins, nil)
}

// selectCall permits the budget planner to reject a compiled candidate before
// it wins selection. Neither callback nor compilation may perform paid work.
func (planning *ProviderPlanning) selectCall(ctx context.Context, request llm.Request, pins providerStatePins, accept func(PlannedProviderCall) (bool, error), priorCandidates ...string) (PlannedProviderCall, error) {
	if ctx == nil || planning == nil || isNilCapability(planning.planner) || isNilCapability(planning.adapters) {
		return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
	}
	if err := ctx.Err(); err != nil {
		return PlannedProviderCall{}, err
	}
	semantic, err := planning.normalizeRequest(request)
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
	rejections := planning.plannerRejections(semantic.Model, plan.Rejections)
	if err != nil || len(plan.Candidates) == 0 {
		return PlannedProviderCall{}, selectionError(ctx, rejections, false, provider.PhasePlan)
	}
	healthBlocked := false
	// Preserve normal route priority among equally tried candidates, while
	// giving alternatives a chance before retrying an earlier failed route.
	if len(priorCandidates) > 0 {
		counts := make(map[string]int, len(priorCandidates))
		for _, id := range priorCandidates {
			counts[id]++
		}
		plan.Candidates = append([]routing.Candidate(nil), plan.Candidates...)
		sort.SliceStable(plan.Candidates, func(i, j int) bool { return counts[plan.Candidates[i].ID] < counts[plan.Candidates[j].ID] })
	}
	lastPhase := provider.PhaseCompile
	for _, candidate := range plan.Candidates {
		if !planning.containsCandidate(semantic, candidate) {
			return PlannedProviderCall{}, providerPlanningError(provider.CodeConfiguration, provider.PhasePlan, provider.RetryNever)
		}
		// Recorded provider state pins the lineage. This runs before the
		// health check so another lineage's route never reads as a blocked
		// one; the pinned route's own health still decides retryability.
		if !pins.admits(semantic.Portability, candidate) {
			rejections = append(rejections, planningRejection{RouteID: candidate.RouteID, Reason: routing.RejectContinuation})
			continue
		}
		if !planning.catalog.Models[semantic.Model].Routes[candidate.RouteIndex].SupportsOutputLimit(semantic) {
			rejections = append(rejections, planningRejection{RouteID: candidate.RouteID, Reason: routing.RejectCapability})
			continue
		}
		if err := planning.contextEstimator.ValidateContext(semantic, candidate); err != nil {
			if errors.Is(err, budget.ErrContextLimit) {
				rejections = append(rejections, planningRejection{RouteID: candidate.RouteID, Reason: routing.RejectContext})
				continue
			}
			return PlannedProviderCall{}, providerPlanningError(provider.CodeInvalidArgument, provider.PhasePlan, provider.RetryNever)
		}
		blocked, healthErr := planning.routeBlocked(ctx, candidate)
		if healthErr != nil {
			return PlannedProviderCall{}, healthErr
		}
		if blocked {
			healthBlocked = true
			rejections = append(rejections, planningRejection{RouteID: candidate.RouteID, Reason: routing.RejectHealth})
			continue
		}
		planned, rejection, err := planning.compileCandidate(ctx, semantic, pins, candidate)
		if err != nil {
			return PlannedProviderCall{}, err
		}
		if rejection != nil {
			rejections = append(rejections, *rejection)
			continue
		}
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
				rejections = append(rejections, planningRejection{RouteID: candidate.RouteID, Reason: rejectQuote})
				continue
			}
		}
		return planned, nil
	}
	return PlannedProviderCall{}, selectionError(ctx, rejections, healthBlocked, lastPhase)
}

// compileCandidate is shared by new selection and exact-route recovery.
// An ordinary local failure returns a rejection; possible dispatch is always fatal.
func (planning *ProviderPlanning) compileCandidate(ctx context.Context, semantic llm.Request, pins providerStatePins, candidate routing.Candidate) (PlannedProviderCall, *planningRejection, error) {
	adapter, err := planning.adapters.Adapter(ctx, candidate)
	if ctx.Err() != nil {
		return PlannedProviderCall{}, nil, ctx.Err()
	}
	if err != nil {
		return rejectedCandidate(candidate, err, false)
	}
	if isNilCapability(adapter) {
		return PlannedProviderCall{}, nil, providerPlanningError(provider.CodeConfiguration, provider.PhaseCompile, provider.RetryNever)
	}
	query := provider.CapabilityQuery{EndpointID: candidate.EndpointID, Family: provider.Family(candidate.Family), Model: candidate.Model, ServiceClass: candidate.AttemptedClass}
	capability, err := adapter.Capabilities(ctx, query)
	if ctx.Err() != nil {
		return PlannedProviderCall{}, nil, ctx.Err()
	}
	if err != nil {
		return rejectedCandidate(candidate, err, false)
	}
	if capability.Version != candidate.CapabilityVersion {
		return PlannedProviderCall{}, nil, providerPlanningError(provider.CodeConfiguration, provider.PhaseCompile, provider.RetryNever)
	}
	resolved := resolveCandidateRequest(semantic, pins, candidate, adapter)
	digest, err := llm.RequestDigest(resolved)
	if err != nil {
		return PlannedProviderCall{}, nil, providerPlanningError(provider.CodeInvalidArgument, provider.PhaseCompile, provider.RetryNever)
	}
	call, err := adapter.Compile(ctx, provider.CompileInput{Request: resolved, Query: query,
		Capability: capability, Strict: semantic.Portability != llm.PortabilityBestEffort,
		Metadata: provider.CallMetadata{SchemaDigest: digest, CapabilityVersion: candidate.CapabilityVersion, ProviderTier: candidate.ProviderTier}})
	if ctx.Err() != nil {
		return PlannedProviderCall{}, nil, ctx.Err()
	}
	if err != nil {
		return rejectedCandidate(candidate, err, true)
	}
	if !validPlannedCall(call, candidate, semantic.OperationKey, digest) {
		return PlannedProviderCall{}, nil, providerPlanningError(provider.CodeConfiguration, provider.PhaseCompile, provider.RetryNever)
	}
	return PlannedProviderCall{Candidate: candidate, CacheIdentity: providerCacheIdentity(candidate),
		CapabilityVersion: capability.Version, ConfigDigest: planning.configDigest, ConfigEpoch: planning.configEpoch,
		Call: call, Adapter: adapter, Semantic: resolved, LogicalModel: semantic.Model}, nil, nil
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
		candidate.EndpointAccountHMAC == route.EndpointAccountHMAC && candidate.EndpointAccountDigest == route.EndpointAccountDigest && candidate.EndpointDigest == route.EndpointDigest && candidate.Region == route.Region &&
		candidate.CapabilityVersion != "" && candidate.CapabilityVersion == route.Capabilities.Version && candidate.ProviderTier != "" && candidate.ProviderTier == route.ProviderTiers[candidate.AttemptedClass] &&
		candidate.PriceVersion == route.PriceVersion && candidate.ContextTokens == route.ContextTokens
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

// Reasons a route was passed over in addition to the routing planner's own
// rejection codes. Together they are the closed vocabulary of a rejection.
const (
	rejectCompile            = "route_compile_rejected"
	rejectAdapterUnavailable = "route_adapter_unavailable"
	rejectQuote              = "route_quote_unavailable"
)

// maxPlanningRejectionDetails and maxPlanningRejectionLogs bound what one
// failed selection adds to an error and to the log.
const (
	maxPlanningRejectionDetails = 4
	maxPlanningRejectionLogs    = 16
)

// planningRejection records why one route could not serve a request. Reason
// and Feature are closed vocabularies and RouteID is operator configuration;
// compiler and configuration messages are never copied.
type planningRejection struct {
	RouteID string
	Reason  string
	Feature string
}

// capability reports whether the route was otherwise eligible but cannot
// represent the request.
func (rejection planningRejection) capability() bool {
	switch rejection.Reason {
	case routing.RejectCapability, routing.RejectExtension, rejectCompile:
		return true
	}
	return false
}

// plannerRejections keeps a planner's rejections within the closed vocabulary.
// A custom planner is not bound by DeterministicPlanner, so a code outside the
// routing constants or a route outside the captured catalog is recorded as an
// invalid rejection of no route rather than copied into details or logs.
func (planning *ProviderPlanning) plannerRejections(model string, source []routing.Rejection) []planningRejection {
	result := make([]planningRejection, 0, len(source))
	for _, rejection := range source {
		mapped := planningRejection{Reason: routing.RejectInvalid}
		if knownRejection(rejection.Code) && planning.containsRoute(model, rejection.RouteID) {
			mapped.RouteID, mapped.Reason = rejection.RouteID, rejection.Code
			if rejection.Code == routing.RejectCapability {
				mapped.Feature = knownFeature(strings.TrimPrefix(rejection.Path, "capabilities."))
			}
		}
		result = append(result, mapped)
	}
	return result
}

func knownRejection(code string) bool {
	switch code {
	case routing.RejectTenant, routing.RejectRegion, routing.RejectModel, routing.RejectHealth, routing.RejectCapability, routing.RejectPrice,
		routing.RejectExtension, routing.RejectContext, routing.RejectContinuation, routing.RejectClass, routing.RejectInvalid:
		return true
	}
	return false
}

func (planning *ProviderPlanning) containsRoute(model, routeID string) bool {
	for _, route := range planning.catalog.Models[model].Routes {
		if route.ID == routeID {
			return routeID != ""
		}
	}
	return false
}

// rejectedCandidate classifies a local adapter failure. Only a request the
// adapter's compiler declined counts against the request; a failed adapter or
// capability lookup is a worker fault.
func rejectedCandidate(candidate routing.Candidate, err error, compiled bool) (PlannedProviderCall, *planningRejection, error) {
	if unsafe := unsafePlanningFailure(err); unsafe != nil {
		return PlannedProviderCall{}, nil, unsafe
	}
	rejection := &planningRejection{RouteID: candidate.RouteID, Reason: rejectAdapterUnavailable}
	var mapped *provider.Error
	if compiled && errors.As(err, &mapped) && (mapped.Code == provider.CodeUnsupportedCapability || mapped.Code == provider.CodeInvalidArgument) {
		rejection.Reason = rejectCompile
		// Adapters prefix a capability rejection with its feature name.
		feature, _, _ := strings.Cut(mapped.SafeMessage, ":")
		rejection.Feature = knownFeature(feature)
	}
	return PlannedProviderCall{}, rejection, nil
}

func knownFeature(name string) string {
	switch provider.Feature(name) {
	case provider.FeatureText, provider.FeatureImage, provider.FeatureDocument, provider.FeatureToolCall, provider.FeatureStructuredOutput,
		provider.FeatureReasoning, provider.FeatureContinuation, provider.FeatureStreaming, provider.FeatureUsage:
		return name
	}
	return ""
}

// selectionError ends a selection that found no usable candidate. A blocked
// route may recover and an unpriced or unbudgeted one is a worker fault, so
// both keep their codes. Otherwise the request is unsupported when a route
// declined it and no route failed for a reason the request does not control.
func selectionError(ctx context.Context, rejections []planningRejection, healthBlocked bool, phase provider.Phase) error {
	code, retry := provider.CodeNoRoute, provider.RetryNever
	if healthBlocked {
		code, phase, retry = provider.CodeProviderUnavailable, provider.PhasePlan, provider.RetrySameOperation
	} else if phase != provider.PhasePrice {
		unsupported := false
		for _, rejection := range rejections {
			unsupported = unsupported || rejection.capability()
			if rejection.Reason == routing.RejectContext || rejection.Reason == rejectAdapterUnavailable {
				unsupported = false
				break
			}
		}
		if unsupported {
			code = provider.CodeUnsupportedCapability
		}
	}
	failure := provider.NewError(code, phase, provider.DispatchNotDispatched, retry, "provider planning failed")
	if len(rejections) == 0 {
		return failure
	}
	// Lead with the rejections that explain an unsupported request.
	sort.SliceStable(rejections, func(i, j int) bool { return rejections[i].capability() && !rejections[j].capability() })
	failure.SafeDetails = map[string]string{"rejected_routes": strconv.Itoa(len(rejections))}
	continuationPinnedFailure(failure, rejections)
	logger := observability.LoggerFromContext(ctx)
	for index, rejection := range rejections {
		cause := rejection.Reason
		if rejection.Feature != "" {
			cause += ":" + rejection.Feature
		}
		if index < maxPlanningRejectionDetails {
			position := strconv.Itoa(index + 1)
			failure.SafeDetails["route_"+position], failure.SafeDetails["reason_"+position] = rejection.RouteID, cause
		}
		if index < maxPlanningRejectionLogs {
			logger.Warn(ctx, "route rejected during provider planning", slog.String("route_id", rejection.RouteID), slog.String("cause", cause),
				slog.String("error_code", string(code)), slog.String("phase", string(phase)))
		}
	}
	return failure
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

// Budgeted planning and recovery must use the same explicit provider limit.
// Standalone planning without a configured budget retains its original defaults.
func (planning *ProviderPlanning) normalizeRequest(request llm.Request) (llm.Request, error) {
	if planning.outputLimit > 0 {
		return (budget.Estimator{MaxOutput: planning.outputLimit}).PrepareRequest(request)
	}
	return llm.NormalizeRequest(request)
}

// resolveCandidateRequest is the one place that turns the semantic request into
// the request a candidate compiles, prices and binds. Selection, budget quoting
// and exact-route recovery must all derive their digest from it; a second
// derivation that skips a step makes the bound digest unreproducible.
func resolveCandidateRequest(semantic llm.Request, pins providerStatePins, candidate routing.Candidate, adapter provider.Adapter) llm.Request {
	resolved := candidateRequest(semantic, pins, candidate)
	if !preservesInstructionHierarchy(provider.Family(candidate.Family), adapter) {
		// Only a worker-built summarizer request changes; any other request
		// is returned as is and keeps the adapter's strict rejection.
		resolved = compaction.FlattenSummarizerInstructions(resolved)
	}
	return resolved
}

func candidateRequest(semantic llm.Request, pins providerStatePins, candidate routing.Candidate) llm.Request {
	resolved, _ := llm.NormalizeRequest(semantic)
	// The adapter needs the provider model and attempted class, not the
	// logical alias or the originally requested fallback class.
	resolved.Model, resolved.ServiceClass = candidate.Model, candidate.AttemptedClass
	resolved.ServiceClassFallbacks = nil
	// Provider state recorded for another lineage is never replayed to this
	// candidate. Selection admitted the candidate only if that is permitted.
	resolved.Input, _ = pins.strip(resolved.Input, candidatePinning(candidate))
	return resolved
}

// plausibleCandidateDigest reports whether digest is one resolveCandidateRequest
// could produce for this input and candidate, without resolving the adapter.
// Recovery uses it to reject changed input early; it is not the final check.
func plausibleCandidateDigest(semantic llm.Request, pins providerStatePins, candidate routing.Candidate, digest [32]byte) bool {
	resolved := candidateRequest(semantic, pins, candidate)
	for _, request := range []llm.Request{resolved, compaction.FlattenSummarizerInstructions(resolved)} {
		if got, err := llm.RequestDigest(request); err == nil && got == digest {
			return true
		}
	}
	return false
}

// preservesInstructionHierarchy reports whether a candidate can carry policy
// and application instructions separately. Messages and Converse have a single
// system prompt and reject mixed levels in strict portability, so worker-built
// summarizer requests are flattened for them. A Chat endpoint decides through
// its profile: one that sends application instructions with the system role
// has the same limitation. The decision depends only on the candidate family
// and the configured endpoint profile, which keeps exact-route recovery stable.
func preservesInstructionHierarchy(family provider.Family, adapter provider.Adapter) bool {
	switch family {
	case provider.FamilyAnthropicMessages, provider.FamilyBedrockMessages, provider.FamilyBedrockConverse:
		return false
	}
	if reporter, ok := adapter.(provider.InstructionHierarchyReporter); ok {
		return reporter.PreservesInstructionHierarchy()
	}
	return true
}
